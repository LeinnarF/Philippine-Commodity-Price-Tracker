"""
Stage 5: DuckDB Analytical Warehouse Loader for Philippine Commodity Price Tracker.

Loads clean and selected Department of Agriculture price monitoring CSV datasets into an embedded DuckDB database
(data/commodities.duckdb). Replaces legacy tables (e.g., fetch_log), builds strictly-typed
Silver layer tables (commodity_prices, audit_prices), and creates Gold layer analytical SQL views
(v_commodity_analytics, v_latest_snapshot, v_category_indices) for instant dashboard queries
and predictive modeling.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import time
from typing import Any
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("load_duckdb")

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "commodities.duckdb"
DEFAULT_CLEAN_CSV = Path(__file__).resolve().parent.parent / "data" / "clean_da_amas_prices.csv"
DEFAULT_SELECTED_CSV = Path(__file__).resolve().parent.parent / "data" / "selected_da_amas_prices.csv"

# SQL DDL Definitions

SQL_DROP_LEGACY_AND_EXISTING = """
DROP VIEW IF EXISTS v_latest_snapshot CASCADE;
DROP VIEW IF EXISTS v_category_indices CASCADE;
DROP VIEW IF EXISTS v_commodity_analytics CASCADE;
DROP TABLE IF EXISTS fetch_log CASCADE;
DROP TABLE IF EXISTS audit_prices CASCADE;
DROP TABLE IF EXISTS commodity_prices CASCADE;
"""

SQL_CREATE_COMMODITY_PRICES = """
CREATE OR REPLACE TABLE commodity_prices (
    date DATE NOT NULL,
    commodity_group VARCHAR NOT NULL,
    commodity VARCHAR NOT NULL,
    unit VARCHAR NOT NULL,
    price_prevailing DOUBLE CHECK (price_prevailing IS NULL OR price_prevailing > 0),
    PRIMARY KEY (date, commodity)
);
"""

SQL_INSERT_COMMODITY_PRICES = """
INSERT INTO commodity_prices (date, commodity_group, commodity, unit, price_prevailing)
SELECT
    date::DATE,
    commodity_group::VARCHAR,
    commodity::VARCHAR,
    unit::VARCHAR,
    price_prevailing::DOUBLE
FROM read_csv(
    ?,
    header=True,
    columns={
        'date': 'DATE',
        'commodity_group': 'VARCHAR',
        'commodity': 'VARCHAR',
        'unit': 'VARCHAR',
        'price_prevailing': 'DOUBLE'
    }
);
"""

SQL_CREATE_AUDIT_PRICES = """
CREATE OR REPLACE TABLE audit_prices (
    date DATE NOT NULL,
    commodity_group VARCHAR NOT NULL,
    commodity VARCHAR NOT NULL,
    unit VARCHAR NOT NULL,
    price_prevailing DOUBLE CHECK (price_prevailing IS NULL OR price_prevailing > 0),
    market_scope VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    is_synthetic BOOLEAN NOT NULL CHECK (is_synthetic = FALSE),
    raw_count INTEGER NOT NULL CHECK (raw_count >= 0),
    raw_file VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    PRIMARY KEY (date, commodity)
);
"""

SQL_INSERT_AUDIT_PRICES = """
INSERT INTO audit_prices (
    date,
    commodity_group,
    commodity,
    unit,
    price_prevailing,
    market_scope,
    source,
    is_synthetic,
    raw_count,
    raw_file,
    source_url
)
SELECT
    date::DATE,
    commodity_group::VARCHAR,
    commodity::VARCHAR,
    unit::VARCHAR,
    price_prevailing::DOUBLE,
    market_scope::VARCHAR,
    source::VARCHAR,
    is_synthetic::BOOLEAN,
    raw_count::INTEGER,
    raw_file::VARCHAR,
    source_url::VARCHAR
FROM read_csv(
    ?,
    header=True,
    columns={
        'date': 'DATE',
        'commodity_group': 'VARCHAR',
        'commodity': 'VARCHAR',
        'unit': 'VARCHAR',
        'price_prevailing': 'DOUBLE',
        'market_scope': 'VARCHAR',
        'source': 'VARCHAR',
        'is_synthetic': 'BOOLEAN',
        'raw_count': 'INTEGER',
        'raw_file': 'VARCHAR',
        'source_url': 'VARCHAR'
    }
);
"""

SQL_CREATE_VIEW_COMMODITY_ANALYTICS = """
CREATE OR REPLACE VIEW v_commodity_analytics AS
WITH lagged AS (
    SELECT
        date,
        commodity_group,
        commodity,
        unit,
        price_prevailing,
        AVG(price_prevailing) OVER (
            PARTITION BY commodity ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sma_7d,
        AVG(price_prevailing) OVER (
            PARTITION BY commodity ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30d,
        STDDEV_SAMP(price_prevailing) OVER (
            PARTITION BY commodity ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        LAG(price_prevailing, 1) OVER (
            PARTITION BY commodity ORDER BY date
        ) AS price_prev_1d,
        LAG(price_prevailing, 7) OVER (
            PARTITION BY commodity ORDER BY date
        ) AS price_prev_7d,
        LAG(price_prevailing, 30) OVER (
            PARTITION BY commodity ORDER BY date
        ) AS price_prev_30d
    FROM commodity_prices
)
SELECT
    date,
    commodity_group,
    commodity,
    unit,
    price_prevailing,
    sma_7d,
    sma_30d,
    volatility_30d,
    volatility_30d / NULLIF(sma_30d, 0) AS cv_30d,
    price_prev_1d,
    price_prev_7d,
    price_prev_30d,
    (price_prevailing - price_prev_1d) AS dod_abs,
    ((price_prevailing - price_prev_1d) / NULLIF(price_prev_1d, 0)) * 100.0 AS dod_pct,
    (price_prevailing - price_prev_7d) AS wow_abs,
    ((price_prevailing - price_prev_7d) / NULLIF(price_prev_7d, 0)) * 100.0 AS wow_pct,
    (price_prevailing - price_prev_30d) AS mom_abs,
    ((price_prevailing - price_prev_30d) / NULLIF(price_prev_30d, 0)) * 100.0 AS mom_pct
FROM lagged;
"""

SQL_CREATE_VIEW_LATEST_SNAPSHOT = """
CREATE OR REPLACE VIEW v_latest_snapshot AS
SELECT *
FROM v_commodity_analytics
WHERE date = (SELECT MAX(date) FROM commodity_prices)
ORDER BY dod_pct DESC NULLS LAST;
"""

SQL_CREATE_VIEW_CATEGORY_INDICES = """
CREATE OR REPLACE VIEW v_category_indices AS
WITH base_prices AS (
    SELECT
        commodity,
        commodity_group,
        ARG_MIN(price_prevailing, date) FILTER (WHERE price_prevailing IS NOT NULL) AS base_price
    FROM commodity_prices
    GROUP BY commodity, commodity_group
),
commodity_relatives AS (
    SELECT
        cp.date,
        cp.commodity_group,
        cp.commodity,
        cp.price_prevailing,
        bp.base_price,
        (cp.price_prevailing / NULLIF(bp.base_price, 0)) * 100.0 AS price_relative
    FROM commodity_prices cp
    JOIN base_prices bp ON cp.commodity = bp.commodity
)
SELECT
    date,
    commodity_group,
    AVG(price_relative) AS index_base100,
    AVG(price_relative) AS index_value,
    COUNT(price_prevailing) AS observed_commodities,
    COUNT(*) AS total_commodities
FROM commodity_relatives
GROUP BY date, commodity_group
ORDER BY date, commodity_group;
"""


def validate_database_integrity(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """
    Validates panel grid dimensions, constraints, views, and data integrity.

    Raises
    ------
    ValueError
        If any integrity assertion fails.
    """
    # 1. Check legacy tables are dropped
    tables = [t[0] for t in con.execute("SHOW TABLES;").fetchall()]
    if "fetch_log" in tables:
        raise ValueError("Legacy table 'fetch_log' still exists in DuckDB!")

    if "commodity_prices" not in tables or "audit_prices" not in tables:
        raise ValueError("Missing core silver tables in DuckDB!")

    # 2. Check commodity_prices dimensions and completeness
    cp_metrics = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT date) AS n_dates,
            COUNT(DISTINCT commodity) AS n_commodities,
            COUNT(DISTINCT commodity_group) AS n_groups,
            COUNT(*) - COUNT(price_prevailing) AS null_prices,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM commodity_prices;
    """).fetchone()

    total_rows, n_dates, n_commodities, n_groups, null_prices, min_date, max_date = cp_metrics

    if n_commodities != 48:
        raise ValueError(f"Expected 48 distinct commodities, found {n_commodities}.")

    if n_groups != 7:
        raise ValueError(f"Expected 7 distinct commodity groups, found {n_groups}.")

    expected_rows = n_dates * n_commodities
    if total_rows != expected_rows:
        raise ValueError(
            f"Panel grid is not rectangular: {total_rows} total rows != {n_dates} dates * {n_commodities} commodities."
        )

    # 3. Check null prices: only Pork Pigue leading gap is allowed
    pigue_nulls = con.execute("""
        SELECT COUNT(*) - COUNT(price_prevailing)
        FROM commodity_prices
        WHERE commodity = 'Pork Pigue (Hind Leg / Ham)';
    """).fetchone()[0]

    other_nulls = con.execute("""
        SELECT COUNT(*) - COUNT(price_prevailing)
        FROM commodity_prices
        WHERE commodity != 'Pork Pigue (Hind Leg / Ham)';
    """).fetchone()[0]

    if other_nulls > 0:
        raise ValueError(
            f"Integrity check failed: {other_nulls} unexpected blank prices outside Pork Pigue leading gap."
        )

    if pigue_nulls != 42 or null_prices != 42:
        raise ValueError(
            f"Expected exactly 42 Pork Pigue leading blank prices, got {pigue_nulls} (total nulls: {null_prices})."
        )

    # 4. Check observed prices > 0
    invalid_prices = con.execute("""
        SELECT COUNT(*)
        FROM commodity_prices
        WHERE price_prevailing IS NOT NULL AND price_prevailing <= 0;
    """).fetchone()[0]

    if invalid_prices > 0:
        raise ValueError(f"Integrity check failed: {invalid_prices} rows have price_prevailing <= 0.")

    # 5. Check audit_prices table matches commodity_prices exactly
    audit_metrics = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT date) AS n_dates,
            COUNT(DISTINCT commodity) AS n_commodities,
            SUM(CASE WHEN is_synthetic THEN 1 ELSE 0 END) AS synthetic_count,
            MIN(raw_count) AS min_raw_count
        FROM audit_prices;
    """).fetchone()

    audit_total, audit_dates, audit_commodities, synthetic_count, min_raw_count = audit_metrics

    if audit_total != total_rows or audit_dates != n_dates or audit_commodities != n_commodities:
        raise ValueError(
            f"Audit table dimensions ({audit_total} rows, {audit_dates} dates, {audit_commodities} commodities) "
            f"do not match commodity_prices ({total_rows} rows, {n_dates} dates, {n_commodities} commodities)."
        )

    if synthetic_count > 0:
        raise ValueError(f"Integrity check failed: {synthetic_count} synthetic rows detected in audit_prices.")

    if min_raw_count < 0:
        raise ValueError(f"Integrity check failed: negative raw_count ({min_raw_count}) detected in audit_prices.")

    # 6. Check Gold Views
    v_analytics_rows = con.execute("SELECT COUNT(*) FROM v_commodity_analytics;").fetchone()[0]
    if v_analytics_rows != total_rows:
        raise ValueError(f"v_commodity_analytics row count ({v_analytics_rows}) != commodity_prices ({total_rows}).")

    v_snapshot_rows = con.execute("SELECT COUNT(*) FROM v_latest_snapshot;").fetchone()[0]
    if v_snapshot_rows != n_commodities:
        raise ValueError(f"v_latest_snapshot row count ({v_snapshot_rows}) != n_commodities ({n_commodities}).")

    v_index_rows = con.execute("SELECT COUNT(*) FROM v_category_indices;").fetchone()[0]
    expected_index_rows = n_dates * n_groups
    if v_index_rows != expected_index_rows:
        raise ValueError(
            f"v_category_indices row count ({v_index_rows}) != {n_dates} dates * {n_groups} groups ({expected_index_rows})."
        )

    # Check baseline index values on start date
    start_index_deviations = con.execute("""
        SELECT COUNT(*)
        FROM v_category_indices
        WHERE date = ? AND ABS(index_base100 - 100.0) > 0.0001;
    """, [min_date]).fetchone()[0]

    if start_index_deviations > 0:
        raise ValueError(f"Base-100 category index on start date ({min_date}) does not equal 100.0 for all groups.")

    return {
        "total_rows": total_rows,
        "n_dates": n_dates,
        "n_commodities": n_commodities,
        "n_groups": n_groups,
        "null_prices": null_prices,
        "pigue_nulls": pigue_nulls,
        "min_date": str(min_date),
        "max_date": str(max_date),
        "v_analytics_rows": v_analytics_rows,
        "v_snapshot_rows": v_snapshot_rows,
        "v_index_rows": v_index_rows,
    }


def load_duckdb(
    db_path: str | Path = DEFAULT_DB_PATH,
    clean_csv: str | Path = DEFAULT_CLEAN_CSV,
    selected_csv: str | Path = DEFAULT_SELECTED_CSV,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Builds the DuckDB database from clean and selected CSVs.

    Parameters
    ----------
    db_path : str | Path
        Path to DuckDB database file (e.g. data/commodities.duckdb).
    clean_csv : str | Path
        Path to data/clean_da_amas_prices.csv.
    selected_csv : str | Path
        Path to data/selected_da_amas_prices.csv.
    con : duckdb.DuckDBPyConnection | None
        Optional existing connection (e.g., for in-memory or custom connections).
        If None, connects to db_path.

    Returns
    -------
    duckdb.DuckDBPyConnection
        The open DuckDB connection containing the loaded tables and views.
    """
    start_time = time.time()
    clean_path = Path(clean_csv)
    selected_path = Path(selected_csv)

    if not clean_path.exists():
        raise FileNotFoundError(f"Clean CSV file not found: {clean_path}")
    if not selected_path.exists():
        raise FileNotFoundError(f"Selected CSV file not found: {selected_path}")

    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: DuckDB Loader (Stage 5)")
    logger.info("Clean CSV Source   : %s", clean_path.resolve())
    logger.info("Selected CSV Source: %s", selected_path.resolve())
    logger.info("Target DuckDB File : %s", Path(db_path).resolve() if con is None else "Custom Connection")
    logger.info("==================================================")

    if con is None:
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))

    # 1. Clean up legacy tables and views
    logger.info("Removing legacy tables and existing views...")
    con.execute(SQL_DROP_LEGACY_AND_EXISTING)

    # 2. Build Silver Table: commodity_prices
    logger.info("Creating Silver table 'commodity_prices'...")
    con.execute(SQL_CREATE_COMMODITY_PRICES)
    logger.info("Ingesting clean CSV data into 'commodity_prices'...")
    con.execute(SQL_INSERT_COMMODITY_PRICES, [str(clean_path.resolve())])

    # 3. Build Silver Table: audit_prices
    logger.info("Creating Silver table 'audit_prices'...")
    con.execute(SQL_CREATE_AUDIT_PRICES)
    logger.info("Ingesting selected CSV provenance data into 'audit_prices'...")
    con.execute(SQL_INSERT_AUDIT_PRICES, [str(selected_path.resolve())])

    # 4. Build Gold View: v_commodity_analytics
    logger.info("Creating Gold view 'v_commodity_analytics'...")
    con.execute(SQL_CREATE_VIEW_COMMODITY_ANALYTICS)

    # 5. Build Gold View: v_latest_snapshot
    logger.info("Creating Gold view 'v_latest_snapshot'...")
    con.execute(SQL_CREATE_VIEW_LATEST_SNAPSHOT)

    # 6. Build Gold View: v_category_indices
    logger.info("Creating Gold view 'v_category_indices'...")
    con.execute(SQL_CREATE_VIEW_CATEGORY_INDICES)

    # 7. Validate Data Integrity
    logger.info("Validating database constraints and panel integrity...")
    metrics = validate_database_integrity(con)

    elapsed = time.time() - start_time
    logger.info("DuckDB build completed in %.4f seconds!", elapsed)
    logger.info("  Total Rows (Silver)  : %d", metrics["total_rows"])
    logger.info("  Observation Dates    : %d (%s to %s)", metrics["n_dates"], metrics["min_date"], metrics["max_date"])
    logger.info("  Panel Commodities    : %d across %d groups", metrics["n_commodities"], metrics["n_groups"])
    logger.info("  Observed Prices      : %d", metrics["total_rows"] - metrics["null_prices"])
    logger.info("  Pork Pigue Nulls     : %d (strictly leading unmonitored period)", metrics["pigue_nulls"])
    logger.info("  Gold Analytics Rows  : %d", metrics["v_analytics_rows"])
    logger.info("  Gold Snapshot Rows   : %d", metrics["v_snapshot_rows"])
    logger.info("  Gold Category Indices: %d", metrics["v_index_rows"])
    logger.info("==================================================")

    # Category distribution log
    summary_df = con.execute("""
        SELECT
            commodity_group,
            COUNT(DISTINCT commodity) AS commodities,
            COUNT(*) AS total_rows,
            COUNT(price_prevailing) AS observed_rows,
            COUNT(*) - COUNT(price_prevailing) AS null_rows,
            ROUND(MIN(price_prevailing), 2) AS min_price,
            ROUND(MAX(price_prevailing), 2) AS max_price
        FROM commodity_prices
        GROUP BY commodity_group
        ORDER BY commodity_group;
    """).df()
    logger.info("Silver Category Distribution:\n%s", summary_df.to_string(index=False))

    return con


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build DuckDB warehouse tables and gold views from CSV sources."
    )
    parser.add_argument(
        "--db",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to output DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--clean-csv",
        type=str,
        default=str(DEFAULT_CLEAN_CSV),
        help=f"Path to clean CSV (default: {DEFAULT_CLEAN_CSV})",
    )
    parser.add_argument(
        "--selected-csv",
        type=str,
        default=str(DEFAULT_SELECTED_CSV),
        help=f"Path to selected CSV (default: {DEFAULT_SELECTED_CSV})",
    )

    args = parser.parse_args()
    connection = load_duckdb(
        db_path=args.db,
        clean_csv=args.clean_csv,
        selected_csv=args.selected_csv,
    )
    connection.close()
