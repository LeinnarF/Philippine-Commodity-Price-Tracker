"""
Final Clean Dataset Pipeline for Philippine Commodity Price Tracker.

Reads the intermediate selected panel dataset (data/selected_da_amas_prices.csv),
strips all technical/provenance metadata columns (market_scope, source, is_synthetic,
raw_count, raw_file, source_url) for a clean presentation and modeling schema,
and writes the final 5-column dataset to data/clean_da_amas_prices.csv.

Architectural Milestones:
1. data/raw_da_amas_prices.csv:
   Raw extracted PDF survey observations (66,291 rows across 451 daily bulletins).
2. data/selected_da_amas_prices.csv:
   Filtered, normalized 48-commodity panel grid with audit metadata and forward-filled
   sporadic gaps (21,648 rows, 11 columns). Preserved intact for transparency.
3. data/clean_da_amas_prices.csv:
   Final production dataset with strictly 5 core business columns:
   ['date', 'commodity_group', 'commodity', 'unit', 'price_prevailing'].
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import time
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("clean_dataset")

DEFAULT_SELECTED_CSV = Path(__file__).resolve().parent.parent / "data" / "selected_da_amas_prices.csv"
DEFAULT_CLEAN_CSV = Path(__file__).resolve().parent.parent / "data" / "clean_da_amas_prices.csv"

CLEAN_COLUMNS = [
    "date",
    "commodity_group",
    "commodity",
    "unit",
    "price_prevailing",
]

METADATA_COLUMNS = [
    "market_scope",
    "source",
    "is_synthetic",
    "raw_count",
    "raw_file",
    "source_url",
]


def clean_dataset(
    input_csv: str | Path = DEFAULT_SELECTED_CSV,
    output_csv: str | Path = DEFAULT_CLEAN_CSV,
) -> pd.DataFrame:
    """
    Load the selected dataset, strip all metadata columns, validate panel integrity,
    and save the final clean dataset without modifying the input file.

    Parameters
    ----------
    input_csv : str | Path
        Path to data/selected_da_amas_prices.csv.
    output_csv : str | Path
        Path where data/clean_da_amas_prices.csv will be saved.

    Returns
    -------
    pd.DataFrame
        The cleaned panel dataframe with strictly the 5 presentation columns.
    """
    start_time = time.time()
    input_path = Path(input_csv)
    output_path = Path(output_csv)

    if not input_path.exists():
        raise FileNotFoundError(f"Input selected dataset not found: {input_path}")

    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: Clean Dataset Generator")
    logger.info("Input Selected CSV : %s", input_path.resolve())
    logger.info("Output Clean CSV   : %s", output_path.resolve())
    logger.info("==================================================")

    # 1. Read intermediate selected dataset
    logger.info("Loading selected panel dataset...")
    df = pd.read_csv(input_path, dtype={"date": str, "unit": str})
    logger.info("Loaded %d rows with columns: %s", len(df), list(df.columns))

    # 2. Check for required business columns
    missing_req = [col for col in CLEAN_COLUMNS if col not in df.columns]
    if missing_req:
        raise ValueError(f"Input dataset is missing required columns: {missing_req}")

    # 3. Strip all metadata columns and keep strictly the 5 presentation columns
    clean_df = df[CLEAN_COLUMNS].copy()

    # Ensure canonical sort order
    clean_df = clean_df.sort_values(
        by=["date", "commodity_group", "commodity"]
    ).reset_index(drop=True)

    # 4. Rigorous integrity validation
    total_rows = len(clean_df)
    unique_dates = clean_df["date"].nunique()
    unique_commodities = clean_df["commodity"].nunique()
    unique_groups = clean_df["commodity_group"].nunique()

    # Deduplication assertion: strictly 1 row per (date, commodity)
    dup_count = clean_df.duplicated(subset=["date", "commodity"]).sum()
    if dup_count > 0:
        raise ValueError(f"Integrity check failed: {dup_count} duplicate (date, commodity) rows found.")

    # Panel dimensions: 451 dates x 48 commodities = 21,648 rows
    expected_rows = unique_dates * unique_commodities
    if total_rows != expected_rows:
        raise ValueError(
            f"Panel grid is not rectangular: {total_rows} rows != {unique_dates} dates * {unique_commodities} commodities."
        )

    # Validate missing prices:
    # Only the leading structural gap for Pork Pigue (Hind Leg / Ham) is required (>= 42 dates)
    null_prices = clean_df["price_prevailing"].isnull().sum()
    pigue_nulls = clean_df[clean_df["commodity"] == "Pork Pigue (Hind Leg / Ham)"]["price_prevailing"].isnull().sum()
    other_nulls = clean_df[clean_df["commodity"] != "Pork Pigue (Hind Leg / Ham)"]["price_prevailing"].isnull().sum()

    if pigue_nulls < 42:
        raise ValueError(f"Integrity check failed: expected at least 42 leading nulls for Pork Pigue, got {pigue_nulls}.")

    if other_nulls > 0:
        logger.warning(
            "Notice: %d unobserved/blank prices detected outside Pork Pigue leading gap. Tolerating missing values.",
            other_nulls,
        )

    # Validate observed prices are strictly positive
    observed_mask = clean_df["price_prevailing"].notnull()
    if not (clean_df.loc[observed_mask, "price_prevailing"] > 0).all():
        raise ValueError("Integrity check failed: observed prevailing prices must be > 0.")

    # 5. Write to output CSV (leaving unobserved prices as empty string "")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_csv(output_path, index=False, encoding="utf-8")

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    elapsed = time.time() - start_time

    logger.info("Clean dataset written successfully in %.2f seconds!", elapsed)
    logger.info("  Total Rows           : %d", total_rows)
    logger.info("  Total Dates          : %d (%s to %s)", unique_dates, clean_df["date"].min(), clean_df["date"].max())
    logger.info("  Total Commodities    : %d across %d groups", unique_commodities, unique_groups)
    logger.info("  Observed Prices      : %d", total_rows - null_prices)
    logger.info("  Blank (Unobserved)   : %d (strictly leading Pork Pigue dates)", null_prices)
    logger.info("  Columns (%d)          : %s", len(clean_df.columns), list(clean_df.columns))
    logger.info("  Output File Size     : %.2f MB at %s", file_size_mb, output_path.resolve())
    logger.info("==================================================")

    # Summary by group
    summary = clean_df.groupby("commodity_group").agg(
        Commodity_Count=("commodity", "nunique"),
        Total_Rows=("date", "count"),
        Observed_Rows=("price_prevailing", lambda s: s.notnull().sum()),
        Blank_Rows=("price_prevailing", lambda s: s.isnull().sum()),
        Min_Price=("price_prevailing", "min"),
        Max_Price=("price_prevailing", "max"),
    ).reset_index()
    logger.info("Clean Category Distribution:\n%s", summary.to_string(index=False))

    return clean_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Strip metadata columns and generate final clean panel dataset."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default=str(DEFAULT_SELECTED_CSV),
        help=f"Path to input selected CSV (default: {DEFAULT_SELECTED_CSV})",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=str(DEFAULT_CLEAN_CSV),
        help=f"Path to output clean CSV (default: {DEFAULT_CLEAN_CSV})",
    )

    args = parser.parse_args()
    clean_dataset(input_csv=args.input, output_csv=args.output)
