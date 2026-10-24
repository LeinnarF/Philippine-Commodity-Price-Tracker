"""
Unit tests for the DuckDB analytical warehouse loader (pipeline.load_duckdb).
"""

from pathlib import Path
import duckdb
import pytest

from pipeline.load_duckdb import (
    load_duckdb,
    validate_database_integrity,
    DEFAULT_DB_PATH,
    DEFAULT_CLEAN_CSV,
    DEFAULT_SELECTED_CSV,
)


def test_load_duckdb_execution(tmp_path):
    """
    Verify load_duckdb creates silver tables, gold views, enforces 21,648 rows,
    451 dates, 48 commodities, 7 groups, and correct window metrics.
    """
    if not DEFAULT_CLEAN_CSV.exists() or not DEFAULT_SELECTED_CSV.exists():
        pytest.skip("Source CSV files not found")

    temp_db_path = tmp_path / "test_commodities.duckdb"
    con = load_duckdb(
        db_path=temp_db_path,
        clean_csv=DEFAULT_CLEAN_CSV,
        selected_csv=DEFAULT_SELECTED_CSV,
    )

    try:
        # 1. Tables and views existence
        tables = [t[0] for t in con.execute("SHOW TABLES;").fetchall()]
        expected_objects = {
            "commodity_prices",
            "audit_prices",
            "v_commodity_analytics",
            "v_latest_snapshot",
            "v_category_indices",
        }
        for obj in expected_objects:
            assert obj in tables, f"Expected {obj} to exist in database"
        assert "fetch_log" not in tables

        # 2. Dimensions & shape
        cp_count = con.execute("SELECT COUNT(*) FROM commodity_prices;").fetchone()[0]
        audit_count = con.execute("SELECT COUNT(*) FROM audit_prices;").fetchone()[0]
        assert cp_count == 21648
        assert audit_count == 21648

        n_dates = con.execute("SELECT COUNT(DISTINCT date) FROM commodity_prices;").fetchone()[0]
        n_commodities = con.execute("SELECT COUNT(DISTINCT commodity) FROM commodity_prices;").fetchone()[0]
        n_groups = con.execute("SELECT COUNT(DISTINCT commodity_group) FROM commodity_prices;").fetchone()[0]
        assert n_dates == 451
        assert n_commodities == 48
        assert n_groups == 7

        # 3. Null values: strictly 42 in Pork Pigue leading gap
        null_count = con.execute("SELECT COUNT(*) - COUNT(price_prevailing) FROM commodity_prices;").fetchone()[0]
        pigue_nulls = con.execute(
            "SELECT COUNT(*) - COUNT(price_prevailing) FROM commodity_prices WHERE commodity = 'Pork Pigue (Hind Leg / Ham)';"
        ).fetchone()[0]
        non_pigue_nulls = con.execute(
            "SELECT COUNT(*) - COUNT(price_prevailing) FROM commodity_prices WHERE commodity != 'Pork Pigue (Hind Leg / Ham)';"
        ).fetchone()[0]

        assert null_count == 42
        assert pigue_nulls == 42
        assert non_pigue_nulls == 0

        # 4. Gold view: v_commodity_analytics
        v_analytics_count = con.execute("SELECT COUNT(*) FROM v_commodity_analytics;").fetchone()[0]
        assert v_analytics_count == 21648

        sample_analytics = con.execute("""
            SELECT
                date, commodity, price_prevailing, sma_7d, sma_30d, volatility_30d, cv_30d,
                price_prev_1d, dod_abs, dod_pct, wow_abs, wow_pct, mom_abs, mom_pct
            FROM v_commodity_analytics
            WHERE commodity = 'Whole Chicken' AND date = '2026-10-20';
        """).fetchone()

        assert sample_analytics is not None
        # Verify metrics are computed and non-null on the latest date
        for idx, val in enumerate(sample_analytics):
            assert val is not None, f"Metric at index {idx} should not be None for Whole Chicken on 2026-10-20"

        # 5. Gold view: v_latest_snapshot
        v_snapshot = con.execute("SELECT * FROM v_latest_snapshot;").df()
        assert len(v_snapshot) == 48
        max_date = con.execute("SELECT MAX(date) FROM commodity_prices;").fetchone()[0]
        snapshot_dates = con.execute("SELECT DISTINCT date FROM v_latest_snapshot;").fetchall()
        assert snapshot_dates == [(max_date,)]
        assert v_snapshot["commodity"].nunique() == 48

        # 6. Gold view: v_category_indices
        v_indices_count = con.execute("SELECT COUNT(*) FROM v_category_indices;").fetchone()[0]
        assert v_indices_count == 451 * 7

        # Start date base-100 check
        start_indices = con.execute("""
            SELECT commodity_group, index_base100, index_value
            FROM v_category_indices
            WHERE date = '2025-03-01';
        """).fetchall()
        assert len(start_indices) == 7
        for group, idx_b100, idx_val in start_indices:
            assert abs(idx_b100 - 100.0) < 0.001, f"Group {group} start index should be 100.0"
            assert abs(idx_val - 100.0) < 0.001

    finally:
        con.close()


def test_load_duckdb_idempotency(tmp_path):
    """
    Verify that executing load_duckdb multiple times consecutively on the same database
    runs cleanly and produces the exact same row counts and schema.
    """
    if not DEFAULT_CLEAN_CSV.exists() or not DEFAULT_SELECTED_CSV.exists():
        pytest.skip("Source CSV files not found")

    temp_db_path = tmp_path / "idempotent.duckdb"

    # First run
    con1 = load_duckdb(
        db_path=temp_db_path,
        clean_csv=DEFAULT_CLEAN_CSV,
        selected_csv=DEFAULT_SELECTED_CSV,
    )
    con1.close()

    # Second run
    con2 = load_duckdb(
        db_path=temp_db_path,
        clean_csv=DEFAULT_CLEAN_CSV,
        selected_csv=DEFAULT_SELECTED_CSV,
    )

    try:
        count = con2.execute("SELECT COUNT(*) FROM commodity_prices;").fetchone()[0]
        assert count == 21648
        audit_count = con2.execute("SELECT COUNT(*) FROM audit_prices;").fetchone()[0]
        assert audit_count == 21648
    finally:
        con2.close()


def test_legacy_cleanup(tmp_path):
    """
    Verify that legacy pre-refactor tables (such as fetch_log) are cleanly dropped.
    """
    if not DEFAULT_CLEAN_CSV.exists() or not DEFAULT_SELECTED_CSV.exists():
        pytest.skip("Source CSV files not found")

    temp_db_path = tmp_path / "legacy_test.duckdb"

    # Pre-populate with legacy table
    prep_con = duckdb.connect(str(temp_db_path))
    prep_con.execute("CREATE TABLE fetch_log (id INTEGER, status VARCHAR);")
    prep_con.execute("INSERT INTO fetch_log VALUES (1, 'OK');")
    prep_con.close()

    con = load_duckdb(
        db_path=temp_db_path,
        clean_csv=DEFAULT_CLEAN_CSV,
        selected_csv=DEFAULT_SELECTED_CSV,
    )

    try:
        tables = [t[0] for t in con.execute("SHOW TABLES;").fetchall()]
        assert "fetch_log" not in tables
    finally:
        con.close()


def test_constraints_enforcement(tmp_path):
    """
    Verify DuckDB CHECK constraints and PRIMARY KEY constraints reject invalid data.
    """
    if not DEFAULT_CLEAN_CSV.exists() or not DEFAULT_SELECTED_CSV.exists():
        pytest.skip("Source CSV files not found")

    temp_db_path = tmp_path / "constraints_test.duckdb"
    con = load_duckdb(
        db_path=temp_db_path,
        clean_csv=DEFAULT_CLEAN_CSV,
        selected_csv=DEFAULT_SELECTED_CSV,
    )

    try:
        # 1. Negative price in commodity_prices
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                "INSERT INTO commodity_prices VALUES ('2026-10-21', 'Fish', 'Test Tilapia', 'kg', -25.0);"
            )

        # 2. Zero price in commodity_prices
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                "INSERT INTO commodity_prices VALUES ('2026-10-21', 'Fish', 'Test Tilapia', 'kg', 0.0);"
            )

        # 3. Duplicate PK (date, commodity) in commodity_prices
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                "INSERT INTO commodity_prices VALUES ('2026-10-20', 'Fish', 'Tilapia', 'kg', 160.0);"
            )

        # 4. is_synthetic = TRUE in audit_prices
        with pytest.raises(duckdb.ConstraintException):
            con.execute("""
                INSERT INTO audit_prices VALUES (
                    '2026-10-21', 'Fish', 'Test Tilapia', 'kg', 160.0,
                    'NCR Wet Markets', 'da_amas', TRUE, 1, 'test.pdf', 'https://example.com'
                );
            """)

        # 5. raw_count < 0 in audit_prices
        with pytest.raises(duckdb.ConstraintException):
            con.execute("""
                INSERT INTO audit_prices VALUES (
                    '2026-10-21', 'Fish', 'Test Tilapia', 'kg', 160.0,
                    'NCR Wet Markets', 'da_amas', FALSE, -1, 'test.pdf', 'https://example.com'
                );
            """)

    finally:
        con.close()


def test_production_database_integrity():
    """
    Validate that the production commodities.duckdb on disk passes all integrity checks.
    """
    if not DEFAULT_DB_PATH.exists():
        pytest.skip("Production commodities.duckdb not found")

    con = duckdb.connect(str(DEFAULT_DB_PATH), read_only=True)
    try:
        metrics = validate_database_integrity(con)
        assert metrics["total_rows"] == 21648
        assert metrics["n_dates"] == 451
        assert metrics["n_commodities"] == 48
        assert metrics["n_groups"] == 7
        assert metrics["null_prices"] == 42
        assert metrics["pigue_nulls"] == 42
        assert metrics["v_analytics_rows"] == 21648
        assert metrics["v_snapshot_rows"] == 48
        assert metrics["v_index_rows"] == 451 * 7
    finally:
        con.close()
