"""
Stage 6 (Lakehouse Sync): Zero-Cost Databricks Unity Catalog Sync Client.

Syncs validated Silver tables (`commodity_prices`, `pipeline_execution_logs`) from
the ephemeral DuckDB analytical warehouse (`data/commodities.duckdb`) into
persistent Databricks Unity Catalog Delta tables and recreates parity Gold SQL
views (`audit_prices`, `v_commodity_analytics`, `v_latest_snapshot`,
`v_category_indices`) for Power BI, Tableau, and Databricks AI/BI Dashboards.

Uses the Databricks SQL Statement Execution REST API (`/api/2.0/sql/statements`)
over `httpx` (already a core project dependency) so zero heavy SDK/Thrift binaries
are required in GitHub Actions.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import datetime
import json
import logging
import math
import os
from pathlib import Path
import re
import time
from typing import Any

import duckdb
import httpx

from pipeline.load_duckdb import DEFAULT_DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sync_databricks")

DEFAULT_DATABRICKS_CATALOG = "workspace"
DEFAULT_DATABRICKS_SCHEMA = "ph_commodities"
DEFAULT_INCREMENTAL_DAYS = 14
DEFAULT_LOG_SYNC_LIMIT = 50
DEFAULT_BATCH_SIZE = 1000
DEFAULT_POLL_TIMEOUT_SECONDS = 300.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 65.0

_VALID_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")

EXPECTED_UC_OBJECTS = (
    "commodity_prices",
    "pipeline_execution_logs",
    "audit_prices",
    "v_commodity_analytics",
    "v_latest_snapshot",
    "v_category_indices",
)

COMMODITY_PRICES_STRUCT_SCHEMA = (
    "ARRAY<STRUCT<"
    "date: STRING, "
    "commodity_group: STRING, "
    "commodity: STRING, "
    "unit: STRING, "
    "price_prevailing: DOUBLE, "
    "source_url: STRING"
    ">>"
)

EXECUTION_LOGS_STRUCT_SCHEMA = (
    "ARRAY<STRUCT<"
    "run_id: STRING, "
    "executed_at: STRING, "
    "trigger: STRING, "
    "status: STRING, "
    "latest_bulletin_date: STRING, "
    "bulletins_checked: INT, "
    "bulletins_downloaded: INT, "
    "records_extracted: INT, "
    "duration_seconds: DOUBLE, "
    "message: STRING"
    ">>"
)


class DatabricksSqlError(RuntimeError):
    """Raised when a Databricks SQL Statement Execution API call fails."""

    def __init__(self, message: str, error_code: str | None = None, state: str | None = None):
        super().__init__(message)
        self.error_code = error_code
        self.state = state


def validate_sql_identifier(name: str, label: str = "identifier") -> str:
    """Validates a Unity Catalog identifier (catalog, schema, or table name)."""
    cleaned = name.strip()
    if not cleaned or not _VALID_IDENTIFIER_RE.match(cleaned):
        raise ValueError(
            f"Invalid Databricks {label} {name!r}. "
            "Must start with alphanumeric/underscore and contain only letters, digits, underscores, or hyphens."
        )
    return cleaned


def quote_ident(name: str) -> str:
    """Backtick-quotes a validated Databricks SQL identifier."""
    return f"`{name.replace('`', '``')}`"


def fqn(catalog: str, schema: str, obj: str) -> str:
    """Builds a backtick-quoted 3-level Unity Catalog namespace."""
    return f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(obj)}"


def normalize_host(host: str | None) -> str:
    """Normalizes a Databricks workspace hostname or URL to https://<host>."""
    if not host:
        return ""
    cleaned = host.strip().rstrip("/")
    if not cleaned:
        return ""
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"
    return cleaned


def extract_warehouse_id(warehouse_id: str | None, http_path: str | None) -> str:
    """Resolves a Databricks SQL warehouse ID from an explicit ID or HTTP Path."""
    if warehouse_id and warehouse_id.strip():
        return warehouse_id.strip()
    if http_path and http_path.strip():
        parts = [part for part in http_path.strip().rstrip("/").split("/") if part]
        if parts:
            return parts[-1]
    return ""


@dataclass(frozen=True)
class DatabricksConfig:
    """Connection and target namespace configuration for Databricks SQL sync."""

    host: str
    token: str
    warehouse_id: str
    catalog: str = DEFAULT_DATABRICKS_CATALOG
    schema: str = DEFAULT_DATABRICKS_SCHEMA

    def __post_init__(self) -> None:
        validate_sql_identifier(self.catalog, "catalog")
        validate_sql_identifier(self.schema, "schema")

    @property
    def is_configured(self) -> bool:
        """Returns True when host, token, and warehouse_id are all non-empty."""
        return bool(self.host and self.token and self.warehouse_id)

    @property
    def namespace(self) -> str:
        """Returns the unquoted 2-level catalog.schema label for display/logging."""
        return f"{self.catalog}.{self.schema}"

    @classmethod
    def from_env(
        cls,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> DatabricksConfig:
        """Builds configuration from environment variables with Free Edition defaults."""
        host = normalize_host(
            os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_SERVER_HOSTNAME")
        )
        token = (os.environ.get("DATABRICKS_TOKEN") or "").strip()
        warehouse_id = extract_warehouse_id(
            os.environ.get("DATABRICKS_WAREHOUSE_ID"),
            os.environ.get("DATABRICKS_HTTP_PATH"),
        )
        resolved_catalog = (
            catalog
            or os.environ.get("DATABRICKS_CATALOG")
            or DEFAULT_DATABRICKS_CATALOG
        ).strip()
        resolved_schema = (
            schema
            or os.environ.get("DATABRICKS_SCHEMA")
            or DEFAULT_DATABRICKS_SCHEMA
        ).strip()
        return cls(
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            catalog=resolved_catalog,
            schema=resolved_schema,
        )


def build_ddl_statements(catalog: str, schema: str) -> dict[str, str]:
    """
    Generates all Databricks SQL DDL statements (schema, Silver Delta tables,
    audit view, and Gold analytical views) matching DuckDB's schema and metrics.
    """
    validate_sql_identifier(catalog, "catalog")
    validate_sql_identifier(schema, "schema")

    schema_fqn = f"{quote_ident(catalog)}.{quote_ident(schema)}"
    prices_fqn = fqn(catalog, schema, "commodity_prices")
    logs_fqn = fqn(catalog, schema, "pipeline_execution_logs")
    audit_fqn = fqn(catalog, schema, "audit_prices")
    analytics_fqn = fqn(catalog, schema, "v_commodity_analytics")
    snapshot_fqn = fqn(catalog, schema, "v_latest_snapshot")
    indices_fqn = fqn(catalog, schema, "v_category_indices")

    is_uc = catalog.lower() != "hive_metastore"
    prices_pk = (
        ",\n    CONSTRAINT pk_commodity_prices PRIMARY KEY (date, commodity) NOT ENFORCED"
        if is_uc
        else ""
    )
    logs_pk = (
        ",\n    CONSTRAINT pk_pipeline_execution_logs PRIMARY KEY (run_id) NOT ENFORCED"
        if is_uc
        else ""
    )

    return {
        "create_schema": (
            f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}\n"
            "COMMENT 'Philippine Department of Agriculture daily commodity retail prices (NCR Wet Markets)';"
        ),
        "create_commodity_prices": (
            f"CREATE TABLE IF NOT EXISTS {prices_fqn} (\n"
            "    date DATE NOT NULL COMMENT 'Survey observation date (YYYY-MM-DD)',\n"
            "    commodity_group STRING NOT NULL COMMENT 'Standardized category (7 commodity groups)',\n"
            "    commodity STRING NOT NULL COMMENT 'Standardized commodity name (50 items)',\n"
            "    unit STRING NOT NULL COMMENT 'Standard unit (kg, piece, bottle (1L), bundle)',\n"
            "    price_prevailing DOUBLE COMMENT 'Prevailing retail price (PHP); NULL when unobserved',\n"
            "    source_url STRING COMMENT 'Official DA Daily Price Index PDF bulletin URL'"
            f"{prices_pk}\n"
            ")\n"
            "USING DELTA\n"
            "CLUSTER BY (commodity, date)\n"
            "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')\n"
            "COMMENT 'Normalized 50-commodity daily retail price panel from DA AMAS bulletins';"
        ),
        "create_pipeline_execution_logs": (
            f"CREATE TABLE IF NOT EXISTS {logs_fqn} (\n"
            "    run_id STRING NOT NULL COMMENT 'Unique pipeline execution identifier',\n"
            "    executed_at TIMESTAMP NOT NULL COMMENT 'Execution timestamp (PHT / UTC+8)',\n"
            "    trigger STRING NOT NULL COMMENT 'Execution trigger source',\n"
            "    status STRING NOT NULL COMMENT 'Run outcome (SUCCESS, NO_NEW_DATA, TIMEOUT, etc.)',\n"
            "    latest_bulletin_date DATE COMMENT 'Most recent bulletin date tracked',\n"
            "    bulletins_checked INT COMMENT 'Calendar dates evaluated',\n"
            "    bulletins_downloaded INT COMMENT 'Bulletins downloaded and parsed',\n"
            "    records_extracted INT COMMENT 'Total records extracted',\n"
            "    duration_seconds DOUBLE COMMENT 'Execution duration in seconds',\n"
            "    message STRING COMMENT 'Diagnostic status message'"
            f"{logs_pk}\n"
            ")\n"
            "USING DELTA\n"
            "CLUSTER BY (executed_at)\n"
            "COMMENT 'Audit trail of automated pipeline executions';"
        ),
        "create_audit_prices_view": (
            f"CREATE OR REPLACE VIEW {audit_fqn} AS\n"
            "SELECT\n"
            "    date,\n"
            "    commodity_group,\n"
            "    commodity,\n"
            "    unit,\n"
            "    price_prevailing,\n"
            "    'NCR Wet Markets' AS market_scope,\n"
            "    'da_amas' AS source,\n"
            "    FALSE AS is_synthetic,\n"
            "    CASE WHEN price_prevailing IS NOT NULL THEN 1 ELSE 0 END AS raw_count,\n"
            "    split_part(split_part(COALESCE(source_url, ''), '?', 1), '/', -1) AS raw_file,\n"
            "    COALESCE(source_url, '') AS source_url\n"
            f"FROM {prices_fqn};"
        ),
        "create_v_commodity_analytics": (
            f"CREATE OR REPLACE VIEW {analytics_fqn} AS\n"
            "WITH lagged AS (\n"
            "    SELECT\n"
            "        date,\n"
            "        commodity_group,\n"
            "        commodity,\n"
            "        unit,\n"
            "        price_prevailing,\n"
            "        source_url,\n"
            "        AVG(price_prevailing) OVER (\n"
            "            PARTITION BY commodity ORDER BY date\n"
            "            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW\n"
            "        ) AS sma_7d,\n"
            "        AVG(price_prevailing) OVER (\n"
            "            PARTITION BY commodity ORDER BY date\n"
            "            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW\n"
            "        ) AS sma_30d,\n"
            "        STDDEV_SAMP(price_prevailing) OVER (\n"
            "            PARTITION BY commodity ORDER BY date\n"
            "            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW\n"
            "        ) AS volatility_30d,\n"
            "        LAG(price_prevailing, 1) OVER (\n"
            "            PARTITION BY commodity ORDER BY date\n"
            "        ) AS price_prev_1d,\n"
            "        LAG(price_prevailing, 7) OVER (\n"
            "            PARTITION BY commodity ORDER BY date\n"
            "        ) AS price_prev_7d,\n"
            "        LAG(price_prevailing, 30) OVER (\n"
            "            PARTITION BY commodity ORDER BY date\n"
            "        ) AS price_prev_30d\n"
            f"    FROM {prices_fqn}\n"
            ")\n"
            "SELECT\n"
            "    date,\n"
            "    commodity_group,\n"
            "    commodity,\n"
            "    unit,\n"
            "    price_prevailing,\n"
            "    sma_7d,\n"
            "    sma_30d,\n"
            "    volatility_30d,\n"
            "    volatility_30d / NULLIF(sma_30d, 0) AS cv_30d,\n"
            "    price_prev_1d,\n"
            "    price_prev_7d,\n"
            "    price_prev_30d,\n"
            "    (price_prevailing - price_prev_1d) AS dod_abs,\n"
            "    ((price_prevailing - price_prev_1d) / NULLIF(price_prev_1d, 0)) * 100.0 AS dod_pct,\n"
            "    (price_prevailing - price_prev_7d) AS wow_abs,\n"
            "    ((price_prevailing - price_prev_7d) / NULLIF(price_prev_7d, 0)) * 100.0 AS wow_pct,\n"
            "    (price_prevailing - price_prev_30d) AS mom_abs,\n"
            "    ((price_prevailing - price_prev_30d) / NULLIF(price_prev_30d, 0)) * 100.0 AS mom_pct,\n"
            "    (COALESCE(source_url, '') ILIKE '%dpi-afc%' OR COALESCE(source_url, '') ILIKE '%dpi_afc%') AS is_provisional,\n"
            "    source_url\n"
            "FROM lagged;"
        ),
        "create_v_latest_snapshot": (
            f"CREATE OR REPLACE VIEW {snapshot_fqn} AS\n"
            "SELECT *\n"
            f"FROM {analytics_fqn}\n"
            f"WHERE date = (SELECT MAX(date) FROM {prices_fqn})\n"
            "ORDER BY dod_pct DESC NULLS LAST;"
        ),
        "create_v_category_indices": (
            f"CREATE OR REPLACE VIEW {indices_fqn} AS\n"
            "WITH base_prices AS (\n"
            "    SELECT\n"
            "        commodity,\n"
            "        commodity_group,\n"
            "        MIN_BY(price_prevailing, date) FILTER (WHERE price_prevailing IS NOT NULL) AS base_price\n"
            f"    FROM {prices_fqn}\n"
            "    GROUP BY commodity, commodity_group\n"
            "),\n"
            "commodity_relatives AS (\n"
            "    SELECT\n"
            "        cp.date,\n"
            "        cp.commodity_group,\n"
            "        cp.commodity,\n"
            "        cp.price_prevailing,\n"
            "        bp.base_price,\n"
            "        (cp.price_prevailing / NULLIF(bp.base_price, 0)) * 100.0 AS price_relative\n"
            f"    FROM {prices_fqn} cp\n"
            "    JOIN base_prices bp ON cp.commodity = bp.commodity\n"
            ")\n"
            "SELECT\n"
            "    date,\n"
            "    commodity_group,\n"
            "    AVG(price_relative) AS index_base100,\n"
            "    AVG(price_relative) AS index_value,\n"
            "    COUNT(price_prevailing) AS observed_commodities,\n"
            "    COUNT(*) AS total_commodities\n"
            "FROM commodity_relatives\n"
            "GROUP BY date, commodity_group\n"
            "ORDER BY date, commodity_group;"
        ),
    }


def build_merge_commodity_prices_sql(catalog: str, schema: str) -> str:
    """Builds the parameterized Delta MERGE INTO SQL statement for commodity_prices."""
    prices_fqn = fqn(catalog, schema, "commodity_prices")
    return (
        f"MERGE INTO {prices_fqn} AS target\n"
        "USING (\n"
        "    SELECT\n"
        "        CAST(NULLIF(s.date, '') AS DATE) AS date,\n"
        "        s.commodity_group,\n"
        "        s.commodity,\n"
        "        s.unit,\n"
        "        s.price_prevailing,\n"
        "        s.source_url\n"
        "    FROM (\n"
        "        SELECT inline(\n"
        "            from_json(\n"
        "                t.json_string,\n"
        f"                '{COMMODITY_PRICES_STRUCT_SCHEMA}'\n"
        "            )\n"
        "        )\n"
        "        FROM (SELECT CAST(:rows_json AS STRING) AS json_string) AS t\n"
        "    ) AS s\n"
        ") AS source\n"
        "ON target.date = source.date AND target.commodity = source.commodity\n"
        "WHEN MATCHED AND (\n"
        "    NOT (target.commodity_group <=> source.commodity_group)\n"
        "    OR NOT (target.unit <=> source.unit)\n"
        "    OR NOT (target.price_prevailing <=> source.price_prevailing)\n"
        "    OR NOT (target.source_url <=> source.source_url)\n"
        ") THEN UPDATE SET\n"
        "    target.commodity_group = source.commodity_group,\n"
        "    target.unit = source.unit,\n"
        "    target.price_prevailing = source.price_prevailing,\n"
        "    target.source_url = source.source_url\n"
        "WHEN NOT MATCHED THEN INSERT (\n"
        "    date, commodity_group, commodity, unit, price_prevailing, source_url\n"
        ") VALUES (\n"
        "    source.date, source.commodity_group, source.commodity, source.unit,\n"
        "    source.price_prevailing, source.source_url\n"
        ");"
    )


def build_merge_execution_logs_sql(catalog: str, schema: str) -> str:
    """Builds the parameterized Delta MERGE INTO SQL statement for pipeline_execution_logs."""
    logs_fqn = fqn(catalog, schema, "pipeline_execution_logs")
    return (
        f"MERGE INTO {logs_fqn} AS target\n"
        "USING (\n"
        "    SELECT\n"
        "        s.run_id,\n"
        "        CAST(NULLIF(s.executed_at, '') AS TIMESTAMP) AS executed_at,\n"
        "        s.trigger,\n"
        "        s.status,\n"
        "        CAST(NULLIF(s.latest_bulletin_date, '') AS DATE) AS latest_bulletin_date,\n"
        "        s.bulletins_checked,\n"
        "        s.bulletins_downloaded,\n"
        "        s.records_extracted,\n"
        "        s.duration_seconds,\n"
        "        s.message\n"
        "    FROM (\n"
        "        SELECT inline(\n"
        "            from_json(\n"
        "                t.json_string,\n"
        f"                '{EXECUTION_LOGS_STRUCT_SCHEMA}'\n"
        "            )\n"
        "        )\n"
        "        FROM (SELECT CAST(:rows_json AS STRING) AS json_string) AS t\n"
        "    ) AS s\n"
        ") AS source\n"
        "ON target.run_id = source.run_id\n"
        "WHEN MATCHED AND (\n"
        "    NOT (target.executed_at <=> source.executed_at)\n"
        "    OR NOT (target.trigger <=> source.trigger)\n"
        "    OR NOT (target.status <=> source.status)\n"
        "    OR NOT (target.latest_bulletin_date <=> source.latest_bulletin_date)\n"
        "    OR NOT (target.bulletins_checked <=> source.bulletins_checked)\n"
        "    OR NOT (target.bulletins_downloaded <=> source.bulletins_downloaded)\n"
        "    OR NOT (target.records_extracted <=> source.records_extracted)\n"
        "    OR NOT (target.duration_seconds <=> source.duration_seconds)\n"
        "    OR NOT (target.message <=> source.message)\n"
        ") THEN UPDATE SET\n"
        "    target.executed_at = source.executed_at,\n"
        "    target.trigger = source.trigger,\n"
        "    target.status = source.status,\n"
        "    target.latest_bulletin_date = source.latest_bulletin_date,\n"
        "    target.bulletins_checked = source.bulletins_checked,\n"
        "    target.bulletins_downloaded = source.bulletins_downloaded,\n"
        "    target.records_extracted = source.records_extracted,\n"
        "    target.duration_seconds = source.duration_seconds,\n"
        "    target.message = source.message\n"
        "WHEN NOT MATCHED THEN INSERT (\n"
        "    run_id, executed_at, trigger, status, latest_bulletin_date,\n"
        "    bulletins_checked, bulletins_downloaded, records_extracted, duration_seconds, message\n"
        ") VALUES (\n"
        "    source.run_id, source.executed_at, source.trigger, source.status,\n"
        "    source.latest_bulletin_date, source.bulletins_checked, source.bulletins_downloaded,\n"
        "    source.records_extracted, source.duration_seconds, source.message\n"
        ");"
    )


def _sanitize_json_value(val: Any) -> Any:
    """Converts NaN/Inf floats and date/datetime values into strict RFC-8259 JSON primitives."""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    return val


def _query_dicts(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Executes a DuckDB query and returns JSON-safe native Python row dicts."""
    cur = con.execute(sql, params or [])
    columns = [desc[0] for desc in (cur.description or [])]
    return [
        {col: _sanitize_json_value(val) for col, val in zip(columns, row)}
        for row in cur.fetchall()
    ]


def _chunk_records(records: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    """Splits records into fixed-size batches."""
    if batch_size <= 0:
        raise ValueError(f"batch_size must be positive, got {batch_size}")
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


class DatabricksSqlClient:
    """Lightweight REST client for Databricks SQL Statement Execution API v2.0."""

    def __init__(
        self,
        config: DatabricksConfig,
        http_client: httpx.Client | None = None,
        poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
        max_retries: int = 3,
    ) -> None:
        self.config = config
        self.poll_timeout_seconds = poll_timeout_seconds
        self.max_retries = max_retries
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(
            timeout=httpx.Timeout(DEFAULT_HTTP_TIMEOUT_SECONDS, connect=10.0),
        )

    def __enter__(self) -> DatabricksSqlClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        """Closes the underlying HTTP client when owned by this instance."""
        if self._owns_client:
            self._client.close()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.token}",
            "Content-Type": "application/json",
        }

    def _request_json(self, method: str, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.config.host}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self._client.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    json=json_body,
                )
            except httpx.HTTPError as exc:
                last_exc = exc
                if attempt + 1 < self.max_retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise DatabricksSqlError(f"HTTP error calling Databricks SQL API: {exc}") from exc

            if resp.status_code in {429, 502, 503, 504} and attempt + 1 < self.max_retries:
                time.sleep(1.5 * (attempt + 1))
                continue

            if resp.status_code >= 400:
                detail = resp.text[:500]
                raise DatabricksSqlError(
                    f"Databricks SQL API returned HTTP {resp.status_code}: {detail}"
                )

            return resp.json()

        raise DatabricksSqlError(f"Failed calling Databricks SQL API: {last_exc}")

    def execute(
        self,
        statement: str,
        parameters: list[dict[str, Any]] | None = None,
        wait_timeout: str = "50s",
    ) -> dict[str, Any]:
        """Executes a SQL statement and polls until terminal completion."""
        payload: dict[str, Any] = {
            "warehouse_id": self.config.warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
            "on_wait_timeout": "CONTINUE",
        }
        if parameters:
            payload["parameters"] = parameters

        result = self._request_json("POST", "/api/2.0/sql/statements", json_body=payload)
        statement_id = result.get("statement_id")
        deadline = time.monotonic() + self.poll_timeout_seconds
        poll_delay = 1.0

        while True:
            status = result.get("status") or {}
            state = status.get("state")
            if state == "SUCCEEDED":
                return result
            if state in {"FAILED", "CANCELED", "CLOSED"}:
                err = status.get("error") or {}
                err_code = err.get("error_code")
                err_msg = err.get("message") or f"Statement ended with state={state}"
                raise DatabricksSqlError(
                    f"Databricks SQL execution {state} ({err_code}): {err_msg}",
                    error_code=err_code,
                    state=state,
                )
            if not statement_id or time.monotonic() >= deadline:
                raise DatabricksSqlError(
                    f"Timed out waiting for Databricks statement {statement_id} (last state={state})",
                    state=state,
                )
            time.sleep(poll_delay)
            poll_delay = min(poll_delay * 1.5, 5.0)
            result = self._request_json("GET", f"/api/2.0/sql/statements/{statement_id}")


def extract_table_names_from_show_tables(response: dict[str, Any]) -> set[str]:
    """Parses table and view names returned by Databricks SHOW TABLES."""
    columns = ((response.get("manifest") or {}).get("schema") or {}).get("columns") or []
    name_idx = 1
    for idx, col in enumerate(columns):
        if str(col.get("name", "")).lower() in {"tablename", "table_name"}:
            name_idx = idx
            break

    rows = (response.get("result") or {}).get("data_array") or []
    names: set[str] = set()
    for row in rows:
        if isinstance(row, list) and len(row) > name_idx and row[name_idx] is not None:
            names.add(str(row[name_idx]).lower())
    return names


def check_existing_objects(client: DatabricksSqlClient, catalog: str, schema: str) -> set[str]:
    """Returns lowercase names of existing tables/views in catalog.schema, or empty set if missing."""
    schema_fqn = f"{quote_ident(catalog)}.{quote_ident(schema)}"
    try:
        resp = client.execute(f"SHOW TABLES IN {schema_fqn};")
    except DatabricksSqlError as exc:
        msg = str(exc).upper()
        if any(tok in msg for tok in ("SCHEMA_NOT_FOUND", "DATABASE_NOT_FOUND", "DOES_NOT_EXIST")):
            return set()
        raise
    return extract_table_names_from_show_tables(resp)


def ensure_databricks_objects(client: DatabricksSqlClient, catalog: str, schema: str) -> int:
    """Creates schema, Silver Delta tables, and Gold views idempotently. Returns DDL count."""
    ddl = build_ddl_statements(catalog, schema)
    executed = 0

    try:
        client.execute(ddl["create_schema"])
        executed += 1
    except DatabricksSqlError as exc:
        # Handle restricted environments where schema was pre-provisioned by an admin
        if "PERMISSION_DENIED" in str(exc).upper() or "UNAUTHORIZED" in str(exc).upper():
            existing_schemas = client.execute(
                f"SHOW SCHEMAS IN {quote_ident(catalog)} LIKE '{schema}';"
            )
            rows = (existing_schemas.get("result") or {}).get("data_array") or []
            if not rows:
                raise
            logger.info("Schema %s.%s already exists (skipping CREATE SCHEMA).", catalog, schema)
        else:
            raise

    for key in (
        "create_commodity_prices",
        "create_pipeline_execution_logs",
        "create_audit_prices_view",
        "create_v_commodity_analytics",
        "create_v_latest_snapshot",
        "create_v_category_indices",
    ):
        client.execute(ddl[key])
        executed += 1

    return executed


def read_duckdb_sync_payload(
    db_path: Path,
    mode: str = "incremental",
    incremental_days: int = DEFAULT_INCREMENTAL_DAYS,
    log_limit: int = DEFAULT_LOG_SYNC_LIMIT,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Reads validated commodity_prices and pipeline_execution_logs records from DuckDB.
    """
    if mode not in {"incremental", "full"}:
        raise ValueError(f"Unsupported sync mode {mode!r}; expected 'incremental' or 'full'.")
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found at {db_path}")

    select_prices = """
        SELECT
            strftime(date, '%Y-%m-%d') AS date,
            commodity_group,
            commodity,
            unit,
            price_prevailing,
            source_url
        FROM commodity_prices
        {where_clause}
        ORDER BY date, commodity_group, commodity;
    """
    select_logs = """
        SELECT
            run_id,
            strftime(executed_at, '%Y-%m-%d %H:%M:%S') AS executed_at,
            trigger,
            status,
            strftime(latest_bulletin_date, '%Y-%m-%d') AS latest_bulletin_date,
            bulletins_checked,
            bulletins_downloaded,
            records_extracted,
            duration_seconds,
            message
        FROM pipeline_execution_logs
        {order_limit};
    """

    with duckdb.connect(str(db_path), read_only=True) as con:
        if mode == "incremental":
            max_date = con.execute("SELECT MAX(date) FROM commodity_prices;").fetchone()[0]
            if max_date is None:
                price_records: list[dict[str, Any]] = []
            else:
                cutoff = max_date - datetime.timedelta(days=max(incremental_days - 1, 0))
                price_records = _query_dicts(
                    con,
                    select_prices.format(where_clause="WHERE date >= ?"),
                    [cutoff],
                )
            log_records = _query_dicts(
                con,
                select_logs.format(order_limit="ORDER BY executed_at DESC LIMIT ?"),
                [log_limit],
            )
        else:
            price_records = _query_dicts(con, select_prices.format(where_clause=""))
            log_records = _query_dicts(con, select_logs.format(order_limit="ORDER BY executed_at ASC"))

    return price_records, log_records


def _merge_records_in_batches(
    client: DatabricksSqlClient,
    merge_sql: str,
    records: list[dict[str, Any]],
    batch_size: int,
    table_label: str,
) -> int:
    """Executes parameterized Delta MERGE INTO statements across batched JSON payloads."""
    if not records:
        return 0

    batches = _chunk_records(records, batch_size)
    for idx, batch in enumerate(batches, start=1):
        payload_json = json.dumps(batch, separators=(",", ":"), allow_nan=False)
        client.execute(
            merge_sql,
            parameters=[{"name": "rows_json", "value": payload_json, "type": "STRING"}],
        )
        logger.info(
            "Merged %s batch %d/%d (%d rows) into Databricks.",
            table_label,
            idx,
            len(batches),
            len(batch),
        )
    return len(batches)


def sync_duckdb_to_databricks(
    db_path: str | Path = DEFAULT_DB_PATH,
    mode: str = "incremental",
    incremental_days: int = DEFAULT_INCREMENTAL_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    catalog: str | None = None,
    schema: str | None = None,
    config: DatabricksConfig | None = None,
    skip_if_unconfigured: bool = False,
    dry_run: bool = False,
    http_client: httpx.Client | None = None,
) -> dict[str, Any]:
    """
    Syncs DuckDB Silver tables into persistent Databricks Unity Catalog Delta tables
    and ensures Gold SQL views exist.

    Returns a structured execution summary dictionary.
    """
    start_time = time.time()
    db_file = Path(db_path)
    resolved_config = config or DatabricksConfig.from_env(catalog=catalog, schema=schema)

    if not dry_run and not resolved_config.is_configured:
        msg = (
            "Databricks credentials not configured "
            "(requires DATABRICKS_HOST/SERVER_HOSTNAME, DATABRICKS_TOKEN, and "
            "DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH)."
        )
        if skip_if_unconfigured:
            logger.info("%s Skipping Databricks sync.", msg)
            return {
                "status": "SKIPPED",
                "mode": mode,
                "namespace": resolved_config.namespace,
                "price_rows_synced": 0,
                "log_rows_synced": 0,
                "statements_executed": 0,
                "duration": time.time() - start_time,
                "message": msg,
            }
        raise ValueError(msg)

    price_records, log_records = read_duckdb_sync_payload(
        db_path=db_file,
        mode=mode,
        incremental_days=incremental_days,
    )

    if dry_run:
        price_batches = len(_chunk_records(price_records, batch_size)) if price_records else 0
        log_batches = len(_chunk_records(log_records, batch_size)) if log_records else 0
        logger.info(
            "DRY-RUN Databricks Sync (%s): target=%s, price_rows=%d (%d batches), log_rows=%d (%d batches)",
            mode,
            resolved_config.namespace,
            len(price_records),
            price_batches,
            len(log_records),
            log_batches,
        )
        return {
            "status": "DRY_RUN",
            "mode": mode,
            "namespace": resolved_config.namespace,
            "price_rows_synced": len(price_records),
            "log_rows_synced": len(log_records),
            "statements_executed": 0,
            "duration": time.time() - start_time,
        }

    statements_executed = 0
    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: Databricks Sync (Stage 6)")
    logger.info("Mode             : %s", mode)
    logger.info("Target Namespace : %s", resolved_config.namespace)
    logger.info("Price Rows       : %d", len(price_records))
    logger.info("Log Rows         : %d", len(log_records))
    logger.info("==================================================")

    with DatabricksSqlClient(resolved_config, http_client=http_client) as client:
        need_ddl = mode == "full"
        if not need_ddl:
            existing_objects = check_existing_objects(
                client, resolved_config.catalog, resolved_config.schema
            )
            statements_executed += 1
            missing = [obj for obj in EXPECTED_UC_OBJECTS if obj.lower() not in existing_objects]
            need_ddl = bool(missing)
            if missing:
                logger.info("Missing Unity Catalog objects %s; running DDL initialization...", missing)

        if need_ddl:
            statements_executed += ensure_databricks_objects(
                client, resolved_config.catalog, resolved_config.schema
            )

        merge_prices_sql = build_merge_commodity_prices_sql(
            resolved_config.catalog, resolved_config.schema
        )
        merge_logs_sql = build_merge_execution_logs_sql(
            resolved_config.catalog, resolved_config.schema
        )

        statements_executed += _merge_records_in_batches(
            client=client,
            merge_sql=merge_prices_sql,
            records=price_records,
            batch_size=batch_size,
            table_label="commodity_prices",
        )
        statements_executed += _merge_records_in_batches(
            client=client,
            merge_sql=merge_logs_sql,
            records=log_records,
            batch_size=batch_size,
            table_label="pipeline_execution_logs",
        )

    elapsed = time.time() - start_time
    logger.info(
        "Databricks sync completed in %.2fs (%d statements executed).",
        elapsed,
        statements_executed,
    )
    return {
        "status": "SYNCED",
        "mode": mode,
        "namespace": resolved_config.namespace,
        "price_rows_synced": len(price_records),
        "log_rows_synced": len(log_records),
        "statements_executed": statements_executed,
        "duration": elapsed,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync validated DuckDB tables into persistent Databricks Unity Catalog Delta tables."
    )
    parser.add_argument(
        "--db",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Sync mode: incremental (rolling window) or full (complete history + view refresh)",
    )
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=DEFAULT_INCREMENTAL_DAYS,
        help=f"Rolling window in days for incremental sync (default: {DEFAULT_INCREMENTAL_DAYS})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per Delta MERGE batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help=f"Target Unity Catalog name (default env DATABRICKS_CATALOG or {DEFAULT_DATABRICKS_CATALOG!r})",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help=f"Target schema name (default env DATABRICKS_SCHEMA or {DEFAULT_DATABRICKS_SCHEMA!r})",
    )
    parser.add_argument(
        "--skip-if-unconfigured",
        action="store_true",
        help="Exit gracefully with status SKIPPED when Databricks credentials are absent",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect DuckDB payload and batch counts without calling Databricks",
    )

    args = parser.parse_args()
    result = sync_duckdb_to_databricks(
        db_path=args.db,
        mode=args.mode,
        incremental_days=args.incremental_days,
        batch_size=args.batch_size,
        catalog=args.catalog,
        schema=args.schema,
        skip_if_unconfigured=args.skip_if_unconfigured,
        dry_run=args.dry_run,
    )
    logger.info("Databricks sync result: %s", result)
