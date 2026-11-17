"""
Unit tests for the zero-cost Databricks Unity Catalog sync module (pipeline.sync_databricks).
"""

import json
from pathlib import Path

import duckdb
import httpx
import pytest

from pipeline.load_duckdb import DEFAULT_DB_PATH
from pipeline.sync_databricks import (
    DatabricksConfig,
    DatabricksSqlClient,
    DatabricksSqlError,
    EXPECTED_UC_OBJECTS,
    build_ddl_statements,
    build_merge_commodity_prices_sql,
    build_merge_execution_logs_sql,
    extract_warehouse_id,
    normalize_host,
    read_duckdb_sync_payload,
    sync_duckdb_to_databricks,
    validate_sql_identifier,
)


def _make_sample_duckdb(db_path: Path) -> Path:
    """Creates a minimal DuckDB database matching production table schemas."""
    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE commodity_prices (
                date DATE NOT NULL,
                commodity_group VARCHAR NOT NULL,
                commodity VARCHAR NOT NULL,
                unit VARCHAR NOT NULL,
                price_prevailing DOUBLE,
                source_url VARCHAR,
                PRIMARY KEY (date, commodity)
            );
        """)
        rows = [
            ("2026-10-01", "Rice", "Regular Milled Rice", "kg", 44.5, "https://example.com/2026-10-01.pdf"),
            ("2026-10-10", "Rice", "Regular Milled Rice", "kg", None, "https://example.com/2026-10-10-DPI-AFC.pdf"),
            ("2026-10-20", "Rice", "Regular Milled Rice", "kg", 46.0, "https://example.com/Daily-Price-Index-October-20-2026.pdf"),
            ("2026-10-20", "Fish", "Tilapia", "kg", 150.0, "https://example.com/Daily-Price-Index-October-20-2026.pdf"),
            ("2026-10-20", "Meat and Poultry", "Whole Chicken", "kg", 210.0, "https://example.com/Daily-Price-Index-October-20-2026.pdf"),
        ]
        con.executemany("INSERT INTO commodity_prices VALUES (?, ?, ?, ?, ?, ?);", rows)

        con.execute("""
            CREATE TABLE pipeline_execution_logs (
                run_id VARCHAR PRIMARY KEY,
                executed_at TIMESTAMP NOT NULL,
                trigger VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                latest_bulletin_date DATE,
                bulletins_checked INTEGER NOT NULL DEFAULT 0,
                bulletins_downloaded INTEGER NOT NULL DEFAULT 0,
                records_extracted INTEGER NOT NULL DEFAULT 0,
                duration_seconds DOUBLE NOT NULL DEFAULT 0.0,
                message VARCHAR
            );
        """)
        con.execute("""
            INSERT INTO pipeline_execution_logs VALUES
            ('run-1', '2026-10-10 10:00:00', 'schedule', 'NO_NEW_DATA', NULL, 1, 0, 0, 1.2, 'No new data'),
            ('run-2', '2026-10-20 10:00:00', 'schedule', 'SUCCESS', '2026-10-20', 1, 1, 50, 4.5, 'Updated');
        """)
    finally:
        con.close()
    return db_path


def test_config_from_env_and_http_path_extraction(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dbc-abc123.cloud.databricks.com/")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/wh999888/")
    monkeypatch.delenv("DATABRICKS_CATALOG", raising=False)
    monkeypatch.delenv("DATABRICKS_SCHEMA", raising=False)

    cfg = DatabricksConfig.from_env()
    assert cfg.host == "https://dbc-abc123.cloud.databricks.com"
    assert cfg.token == "dapi-test-token"
    assert cfg.warehouse_id == "wh999888"
    assert cfg.catalog == "workspace"
    assert cfg.schema == "ph_commodities"
    assert cfg.is_configured is True
    assert normalize_host("") == ""
    assert extract_warehouse_id("explicit_id", "/sql/1.0/warehouses/ignored") == "explicit_id"


def test_sql_identifier_validation_rejects_injection():
    assert validate_sql_identifier("my-catalog_01", "catalog") == "my-catalog_01"
    for bad in ["", "bad name", "foo;DROP TABLE", "cat`log", "-leading-hyphen"]:
        with pytest.raises(ValueError):
            validate_sql_identifier(bad, "catalog")


def test_build_ddl_statements_parity_and_dialect():
    ddl = build_ddl_statements("my-catalog", "ph_commodities")
    assert "`my-catalog`.`ph_commodities`.`commodity_prices`" in ddl["create_commodity_prices"]
    assert "CONSTRAINT pk_commodity_prices PRIMARY KEY (date, commodity) NOT ENFORCED" in ddl["create_commodity_prices"]
    assert "CLUSTER BY (commodity, date)" in ddl["create_commodity_prices"]
    assert "MIN_BY(price_prevailing, date) FILTER (WHERE price_prevailing IS NOT NULL)" in ddl["create_v_category_indices"]
    assert "STDDEV_SAMP(price_prevailing)" in ddl["create_v_commodity_analytics"]
    assert "ILIKE '%dpi-afc%'" in ddl["create_v_commodity_analytics"]
    assert "split_part(split_part(COALESCE(source_url, ''), '?', 1), '/', -1)" in ddl["create_audit_prices_view"]
    assert "WHERE date = (SELECT MAX(date) FROM `my-catalog`.`ph_commodities`.`commodity_prices`)" in ddl["create_v_latest_snapshot"]

    hive_ddl = build_ddl_statements("hive_metastore", "ph_commodities")
    assert "PRIMARY KEY" not in hive_ddl["create_commodity_prices"]
    assert "PRIMARY KEY" not in hive_ddl["create_pipeline_execution_logs"]


def test_merge_sql_builders():
    prices_sql = build_merge_commodity_prices_sql("workspace", "ph_commodities")
    assert "MERGE INTO `workspace`.`ph_commodities`.`commodity_prices` AS target" in prices_sql
    assert ":rows_json" in prices_sql
    assert "<=>" in prices_sql

    logs_sql = build_merge_execution_logs_sql("workspace", "ph_commodities")
    assert "MERGE INTO `workspace`.`ph_commodities`.`pipeline_execution_logs` AS target" in logs_sql
    assert "ON target.run_id = source.run_id" in logs_sql


def test_sync_skips_gracefully_when_unconfigured(monkeypatch, tmp_path):
    db_file = _make_sample_duckdb(tmp_path / "sample.duckdb")
    for var in ("DATABRICKS_HOST", "DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH"):
        monkeypatch.delenv(var, raising=False)

    res = sync_duckdb_to_databricks(db_path=db_file, skip_if_unconfigured=True)
    assert res["status"] == "SKIPPED"
    assert res["statements_executed"] == 0

    with pytest.raises(ValueError, match="Databricks credentials not configured"):
        sync_duckdb_to_databricks(db_path=db_file, skip_if_unconfigured=False)


def test_read_duckdb_sync_payload_incremental_and_full(tmp_path):
    db_file = _make_sample_duckdb(tmp_path / "sample.duckdb")

    inc_prices, inc_logs = read_duckdb_sync_payload(db_file, mode="incremental", incremental_days=14)
    # 2026-10-01 is outside the 14-day window ending 2026-10-20; 2026-10-10 and 2026-10-20 are inside
    assert len(inc_prices) == 4
    assert all(r["date"] >= "2026-10-07" for r in inc_prices)
    assert any(r["price_prevailing"] is None for r in inc_prices)
    assert len(inc_logs) == 2
    assert any(r["latest_bulletin_date"] is None for r in inc_logs)
    # Verify strict JSON compatibility (no NaN / non-serializable types)
    json.dumps(inc_prices, allow_nan=False)
    json.dumps(inc_logs, allow_nan=False)

    full_prices, full_logs = read_duckdb_sync_payload(db_file, mode="full")
    assert len(full_prices) == 5
    assert len(full_logs) == 2


def test_sync_incremental_skips_ddl_when_objects_already_exist(tmp_path):
    db_file = _make_sample_duckdb(tmp_path / "sample.duckdb")
    cfg = DatabricksConfig(
        host="https://dbc-test.cloud.databricks.com",
        token="secret-token",
        warehouse_id="wh123",
    )
    executed_statements: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer secret-token"
        body = json.loads(request.content.decode("utf-8"))
        stmt = body["statement"]
        executed_statements.append(stmt)
        if stmt.startswith("SHOW TABLES"):
            return httpx.Response(
                200,
                json={
                    "statement_id": "stmt-show",
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "schema": {
                            "columns": [
                                {"name": "database"},
                                {"name": "tableName"},
                                {"name": "isTemporary"},
                            ]
                        }
                    },
                    "result": {
                        "data_array": [["ph_commodities", obj, False] for obj in EXPECTED_UC_OBJECTS]
                    },
                },
            )
        return httpx.Response(200, json={"statement_id": "stmt-merge", "status": {"state": "SUCCEEDED"}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res = sync_duckdb_to_databricks(
            db_path=db_file,
            mode="incremental",
            config=cfg,
            http_client=client,
        )

    assert res["status"] == "SYNCED"
    assert res["price_rows_synced"] == 4
    assert res["log_rows_synced"] == 2
    # Only SHOW TABLES + 2 MERGE statements (0 DDL statements!)
    assert res["statements_executed"] == 3
    assert executed_statements[0].startswith("SHOW TABLES")
    assert "MERGE INTO `workspace`.`ph_commodities`.`commodity_prices`" in executed_statements[1]
    assert "MERGE INTO `workspace`.`ph_commodities`.`pipeline_execution_logs`" in executed_statements[2]


def test_sync_incremental_initializes_ddl_when_schema_missing(tmp_path):
    db_file = _make_sample_duckdb(tmp_path / "sample.duckdb")
    cfg = DatabricksConfig(
        host="https://dbc-test.cloud.databricks.com",
        token="secret-token",
        warehouse_id="wh123",
    )
    executed_statements: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        stmt = body["statement"]
        executed_statements.append(stmt)
        if stmt.startswith("SHOW TABLES"):
            return httpx.Response(
                200,
                json={
                    "statement_id": "stmt-show",
                    "status": {
                        "state": "FAILED",
                        "error": {"error_code": "SCHEMA_NOT_FOUND", "message": "Schema does not exist"},
                    },
                },
            )
        return httpx.Response(200, json={"statement_id": "stmt-ok", "status": {"state": "SUCCEEDED"}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res = sync_duckdb_to_databricks(
            db_path=db_file,
            mode="incremental",
            config=cfg,
            http_client=client,
        )

    assert res["status"] == "SYNCED"
    # SHOW TABLES (1) + 7 DDL statements + 2 MERGE statements = 10
    assert res["statements_executed"] == 10
    assert any(s.startswith("CREATE SCHEMA IF NOT EXISTS") for s in executed_statements)
    assert any("CREATE OR REPLACE VIEW `workspace`.`ph_commodities`.`v_category_indices`" in s for s in executed_statements)


def test_sync_full_mode_batches_and_polls_async_statement(monkeypatch, tmp_path):
    monkeypatch.setattr("pipeline.sync_databricks.time.sleep", lambda *_: None)
    db_file = _make_sample_duckdb(tmp_path / "sample.duckdb")
    cfg = DatabricksConfig(
        host="https://dbc-test.cloud.databricks.com",
        token="secret-token",
        warehouse_id="wh123",
    )
    poll_count = 0
    merge_batches: list[list[dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal poll_count
        if request.method == "GET":
            poll_count += 1
            state = "RUNNING" if poll_count == 1 else "SUCCEEDED"
            return httpx.Response(200, json={"statement_id": "stmt-async", "status": {"state": state}})

        body = json.loads(request.content.decode("utf-8"))
        stmt = body["statement"]
        if stmt.startswith("MERGE INTO `workspace`.`ph_commodities`.`commodity_prices`"):
            rows = json.loads(body["parameters"][0]["value"])
            merge_batches.append(rows)
            if len(merge_batches) == 1:
                return httpx.Response(200, json={"statement_id": "stmt-async", "status": {"state": "PENDING"}})
        return httpx.Response(200, json={"statement_id": "stmt-sync", "status": {"state": "SUCCEEDED"}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res = sync_duckdb_to_databricks(
            db_path=db_file,
            mode="full",
            batch_size=2,
            config=cfg,
            http_client=client,
        )

    assert res["status"] == "SYNCED"
    assert res["price_rows_synced"] == 5
    # 5 price rows with batch_size=2 -> 3 batches; 2 log rows with batch_size=2 -> 1 batch; + 7 DDL = 11
    assert len(merge_batches) == 3
    assert [len(b) for b in merge_batches] == [2, 2, 1]
    assert res["statements_executed"] == 11
    assert poll_count == 2


def test_client_raises_databricks_sql_error_on_failed_statement():
    cfg = DatabricksConfig(
        host="https://dbc-test.cloud.databricks.com",
        token="secret-token",
        warehouse_id="wh123",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "statement_id": "stmt-err",
                "status": {
                    "state": "FAILED",
                    "error": {"error_code": "SYNTAX_ERROR", "message": "Invalid SQL"},
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        dbx = DatabricksSqlClient(cfg, http_client=client)
        with pytest.raises(DatabricksSqlError, match="SYNTAX_ERROR"):
            dbx.execute("SELECT 1")


def test_sync_dry_run_against_production_duckdb():
    if not DEFAULT_DB_PATH.exists():
        pytest.skip("Production commodities.duckdb not found")

    res = sync_duckdb_to_databricks(
        db_path=DEFAULT_DB_PATH,
        mode="incremental",
        dry_run=True,
    )
    assert res["status"] == "DRY_RUN"
    assert res["price_rows_synced"] > 0
    assert res["statements_executed"] == 0
