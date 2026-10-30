"""
Fetch Missing Bulletin Dates for Philippine Commodity Price Tracker.

Fetches and extracts Department of Agriculture Daily Price Index bulletins for observation dates
that are present in data/server_daily_bulletins.csv but missing from data/cleaned_dataset.csv
(e.g., historical Sunday survey bulletins and previously omitted dates).

Downloads PDFs directly in-memory (no local raw PDF files created), extracts the 48 standardized
commodity prices matching the cleaned dataset schema, and merges them directly into
data/cleaned_dataset.csv into a unified, chronological, forward-filled panel.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import io
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import httpx
import numpy as np
import pandas as pd

from pipeline.clean_dataset import CLEAN_COLUMNS, DEFAULT_CLEAN_CSV
from pipeline.download_pdfs import PROJECT_USER_AGENT
from pipeline.load_duckdb import DEFAULT_DB_PATH, DEFAULT_LOGS_CSV, DEFAULT_SELECTED_CSV, load_duckdb
from pipeline.pdf_parser import parse_da_amas_pdf
from pipeline.select_commodities import DATA_ANOMALY_CORRECTIONS, get_commodity_definitions

DEFAULT_SERVER_CSV = PROJECT_ROOT / "data" / "server_daily_bulletins.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_missing_bulletins")


def find_missing_bulletin_dates(
    clean_csv: Path = DEFAULT_CLEAN_CSV,
    server_csv: Path = DEFAULT_SERVER_CSV,
) -> list[tuple[str, str]]:
    """
    Compares observation dates in data/server_daily_bulletins.csv against data/cleaned_dataset.csv.
    Returns a sorted list of (date_str, pdf_url) pairs that need to be fetched.
    """
    if not server_csv.exists():
        raise FileNotFoundError(f"Server bulletins CSV not found at: {server_csv}")

    server_df = pd.read_csv(server_csv)
    if "date" not in server_df.columns or "pdf_url" not in server_df.columns:
        raise ValueError(f"Malformed server bulletins CSV: {server_csv}")

    existing_dates: set[str] = set()
    if clean_csv.exists():
        clean_df = pd.read_csv(clean_csv, usecols=["date"])
        if "date" in clean_df.columns:
            existing_dates = set(clean_df["date"].dropna().astype(str).unique())

    server_records = server_df.dropna(subset=["date", "pdf_url"]).to_dict(orient="records")
    missing_items = [
        (str(r["date"]).strip(), str(r["pdf_url"]).strip())
        for r in server_records
        if str(r["date"]).strip() not in existing_dates
    ]
    missing_items.sort(key=lambda x: x[0])
    return missing_items


def extract_date_panel_records(df_raw: pd.DataFrame, target_date: str, source_url: str = "") -> list[dict[str, Any]]:
    """
    Maps raw PDF parsed survey rows to strictly the 48 standardized panel commodities.
    Handles column shifts, deduplication (e.g. favoring Local Rice over Imported Rice),
    and sets audit metadata columns.
    """
    # 1. Deduplicate raw records: strictly date, commodity_group, commodity, variety
    dedup_cols = ["date", "commodity_group", "commodity", "variety"]
    available_dedup = [c for c in dedup_cols if c in df_raw.columns]
    if available_dedup:
        df_raw = df_raw.drop_duplicates(subset=available_dedup, keep="last")

    # 2. Check if commodity column is numeric (indicator for the 104 shifted dates)
    is_num = df_raw["commodity"].astype(str).str.isdigit()

    definitions = get_commodity_definitions(egg_category="Basic Goods", include_corn=True)
    records: list[dict[str, Any]] = []

    for item in definitions:
        mask = item["matcher"](df_raw, is_num)
        sub = df_raw[mask]

        if not sub.empty:
            price = sub["price_prevailing"].mean()
            price_val = round(float(price), 2) if pd.notna(price) else np.nan
            raw_count = int(sub["price_prevailing"].count())
        else:
            price_val = np.nan
            raw_count = 0

        records.append({
            "date": target_date,
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": price_val,
            "market_scope": "NCR Wet Markets",
            "source": "da_amas",
            "is_synthetic": False,
            "raw_count": raw_count,
            "raw_file": f"{target_date}.pdf",
            "source_url": source_url or "https://www.da.gov.ph/price-monitoring/",
        })

    return records


def fetch_and_parse_bulletin_memory(
    date_str: str,
    pdf_url: str,
    client: httpx.Client,
    max_retries: int = 3,
) -> list[dict[str, Any]] | None:
    """
    Downloads a bulletin PDF into memory and extracts the 48 standardized commodity records.
    Returns None if download fails or content is not a valid PDF.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.get(pdf_url, timeout=30.0)
            if resp.status_code == 404:
                logger.warning("[%s] HTTP 404 Not Found at %s", date_str, pdf_url)
                return None
            elif resp.status_code != 200:
                time.sleep(0.5 * attempt)
                continue

            content = resp.content
            if not content.startswith(b"%PDF-") or len(content) < 5000:
                logger.warning("[%s] Response is not a valid PDF (%d bytes)", date_str, len(content))
                return None

            # Parse in-memory via pdfplumber
            df_raw = parse_da_amas_pdf(
                pdf_source=io.BytesIO(content),
                target_date=date_str,
                source_url=pdf_url,
            )

            if df_raw.empty:
                logger.warning("[%s] PDF parsed into 0 records.", date_str)
                return None

            panel_records = extract_date_panel_records(df_raw, date_str, source_url=pdf_url)
            return panel_records

        except (httpx.ConnectError, httpx.TimeoutException) as conn_err:
            if attempt == max_retries:
                logger.error("[%s] Network error after %d retries: %s", date_str, max_retries, conn_err)
                return None
            time.sleep(1.0 * attempt)
        except Exception as exc:
            logger.error("[%s] Unexpected error processing bulletin: %s", date_str, exc)
            return None

    return None


def fetch_and_merge_missing_bulletins(
    clean_csv: Path = DEFAULT_CLEAN_CSV,
    server_csv: Path = DEFAULT_SERVER_CSV,
    output_csv: Path = DEFAULT_CLEAN_CSV,
    workers: int = 8,
    limit: int | None = None,
    dry_run: bool = False,
    rebuild: bool = False,
) -> pd.DataFrame:
    """
    Fetches missing bulletin dates, extracts 48 standardized commodities,
    and merges them directly into the cleaned dataset.
    """
    start_time = time.time()
    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: Missing Bulletins Ingestion")
    logger.info("Clean Dataset Source : %s", clean_csv)
    logger.info("Server Bulletins CSV : %s", server_csv)
    logger.info("Destination Output   : %s", output_csv)
    logger.info("Concurrent Workers   : %d", workers)
    logger.info("Dry-Run Mode         : %s", dry_run)
    logger.info("==================================================")

    # 1. Identify missing dates
    missing_items = find_missing_bulletin_dates(clean_csv=clean_csv, server_csv=server_csv)
    total_missing = len(missing_items)

    logger.info("Discovered %d missing bulletin dates in %s", total_missing, server_csv.name)
    if not missing_items:
        logger.info("All server bulletins are already ingested in %s. Nothing to do!", clean_csv.name)
        if clean_csv.exists():
            return pd.read_csv(clean_csv)
        return pd.DataFrame(columns=CLEAN_COLUMNS)

    if limit is not None and limit > 0:
        logger.info("Applying limit: Fetching first %d of %d missing dates.", limit, total_missing)
        missing_items = missing_items[:limit]

    logger.info(
        "Date range to fetch (%d dates): %s to %s",
        len(missing_items),
        missing_items[0][0],
        missing_items[-1][0],
    )

    if dry_run:
        logger.info("DRY-RUN: Would fetch %d bulletins. Exiting without changes.", len(missing_items))
        if clean_csv.exists():
            return pd.read_csv(clean_csv)
        return pd.DataFrame(columns=CLEAN_COLUMNS)

    # 2. Parallel in-memory download and extraction
    extracted_rows: list[dict[str, Any]] = []
    completed_count = 0
    success_count = 0
    failure_count = 0
    lock = threading.Lock()

    client = httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": PROJECT_USER_AGENT},
        limits=httpx.Limits(max_connections=workers * 2, max_keepalive_connections=workers),
    )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_date = {
                executor.submit(fetch_and_parse_bulletin_memory, d_str, url, client): d_str
                for d_str, url in missing_items
            }

            for future in concurrent.futures.as_completed(future_to_date):
                d_str = future_to_date[future]
                try:
                    result = future.result()
                except Exception as exc:
                    logger.error("[%s] Worker exception: %s", d_str, exc)
                    result = None

                with lock:
                    completed_count += 1
                    if result:
                        extracted_rows.extend(result)
                        success_count += 1
                    else:
                        failure_count += 1

                    if completed_count % 10 == 0 or completed_count == len(missing_items):
                        pct = (completed_count / len(missing_items)) * 100
                        logger.info(
                            "Extraction Progress: [%d/%d] (%.1f%%) | Success: %d | Failed: %d",
                            completed_count,
                            len(missing_items),
                            pct,
                            success_count,
                            failure_count,
                        )
    finally:
        client.close()

    if not extracted_rows:
        logger.warning("No records could be extracted from any of the target bulletins.")
        if clean_csv.exists():
            return pd.read_csv(clean_csv)
        return pd.DataFrame(columns=CLEAN_COLUMNS)

    logger.info("Extracted %d commodity rows across %d successful bulletins.", len(extracted_rows), success_count)

    # 3. Load existing selected dataset and merge audit rows
    selected_cols = [
        "date",
        "commodity_group",
        "commodity",
        "unit",
        "price_prevailing",
        "market_scope",
        "source",
        "is_synthetic",
        "raw_count",
        "raw_file",
        "source_url",
    ]
    if DEFAULT_SELECTED_CSV.exists():
        existing_selected_df = pd.read_csv(DEFAULT_SELECTED_CSV, dtype={"date": str, "unit": str})
        logger.info("Loaded %d existing rows across %d dates from %s", len(existing_selected_df), existing_selected_df["date"].nunique(), DEFAULT_SELECTED_CSV.name)
    else:
        existing_selected_df = pd.DataFrame(columns=selected_cols)

    new_selected_df = pd.DataFrame(extracted_rows)
    for col in selected_cols:
        if col not in new_selected_df.columns:
            new_selected_df[col] = None

    combined_selected_df = pd.concat([existing_selected_df, new_selected_df[selected_cols]], ignore_index=True)

    # Deduplicate strictly on date, commodity
    combined_selected_df = combined_selected_df.drop_duplicates(subset=["date", "commodity"], keep="last")

    # Apply verified data anomaly corrections
    for (d, comm), (corrected_price, rationale) in DATA_ANOMALY_CORRECTIONS.items():
        mask = (combined_selected_df["date"] == d) & (combined_selected_df["commodity"] == comm)
        if mask.any():
            old_price = combined_selected_df.loc[mask, "price_prevailing"].iloc[0]
            if pd.notna(old_price) and old_price != corrected_price:
                combined_selected_df.loc[mask, "price_prevailing"] = corrected_price
                logger.info("Applied anomaly correction for '%s' on %s: ₱%.2f -> ₱%.2f (%s)", comm, d, old_price, corrected_price, rationale)
            elif pd.isna(old_price):
                combined_selected_df.loc[mask, "price_prevailing"] = corrected_price

    # 4. Apply forward fill (ffill) across the complete chronological timeline
    logger.info("Applying chronological forward-fill (ffill) for sporadic gaps...")
    combined_selected_df = combined_selected_df.sort_values(by=["commodity", "date"]).reset_index(drop=True)
    non_leading_mask = combined_selected_df["commodity"] != "Pork Pigue (Hind Leg / Ham)"
    combined_selected_df.loc[non_leading_mask, "price_prevailing"] = (
        combined_selected_df[non_leading_mask]
        .groupby("commodity")["price_prevailing"]
        .ffill()
    )

    # 5. Final canonical sort and schema validation
    combined_selected_df = combined_selected_df[selected_cols].sort_values(
        by=["date", "commodity_group", "commodity"]
    ).reset_index(drop=True)

    # Save synchronized audit selected dataset
    DEFAULT_SELECTED_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined_selected_df.to_csv(DEFAULT_SELECTED_CSV, index=False)
    logger.info("Saved %d rows to synchronized %s", len(combined_selected_df), DEFAULT_SELECTED_CSV.name)

    # Production clean dataset (strictly 5 columns)
    combined_df = combined_selected_df[CLEAN_COLUMNS].copy()

    # Rigorous validation checks
    total_dates = combined_df["date"].nunique()
    total_rows = len(combined_df)
    expected_rows = total_dates * 48

    if total_rows != expected_rows:
        logger.error("Panel matrix size mismatch: %d rows found, expected %d (%d dates x 48 items)", total_rows, expected_rows, total_dates)
        raise ValueError(f"Panel integrity failure: expected {expected_rows} rows, got {total_rows}")

    dup_count = combined_df.duplicated(subset=["date", "commodity"]).sum()
    if dup_count > 0:
        logger.error("Integrity check failed: %d duplicate (date, commodity) rows found.", dup_count)
        raise ValueError(f"Integrity check failed: {dup_count} duplicate rows found.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_csv, index=False)
    elapsed = time.time() - start_time

    logger.info("==================================================")
    logger.info("Direct Ingestion Complete in %.1f seconds!", elapsed)
    logger.info("  Cleaned Dataset     : %s", output_csv.resolve())
    logger.info("  Total Dates Tracked : %d days (+%d newly ingested)", total_dates, success_count)
    logger.info("  Total Panel Rows    : %d rows (%d dates x 48 commodities)", len(combined_df), total_dates)
    logger.info("  Observed Prices     : %d rows", combined_df["price_prevailing"].notnull().sum())
    logger.info("  Unobserved (Pigue)  : %d leading rows", combined_df["price_prevailing"].isnull().sum())
    logger.info("==================================================")

    # 6. Optional warehouse rebuild
    if rebuild:
        logger.info("Rebuilding DuckDB analytical warehouse...")
        load_duckdb(
            db_path=DEFAULT_DB_PATH,
            clean_csv=output_csv,
            selected_csv=DEFAULT_SELECTED_CSV,
            logs_csv=DEFAULT_LOGS_CSV,
        )
        try:
            from pipeline.build_dashboard import build_dashboard
            build_dashboard(db_path=DEFAULT_DB_PATH)
            logger.info("Static dashboard rebuild complete.")
        except Exception as d_err:
            logger.warning("Could not rebuild dashboard: %s", d_err)

    return combined_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch and directly ingest missing DA Daily Price Index bulletins into cleaned_dataset.csv."
    )
    parser.add_argument(
        "--clean-csv",
        type=Path,
        default=DEFAULT_CLEAN_CSV,
        help=f"Path to cleaned dataset CSV (default: {DEFAULT_CLEAN_CSV})",
    )
    parser.add_argument(
        "--server-csv",
        type=Path,
        default=DEFAULT_SERVER_CSV,
        help=f"Path to server daily bulletins CSV (default: {DEFAULT_SERVER_CSV})",
    )
    parser.add_argument(
        "--output-csv",
        "-o",
        type=Path,
        default=DEFAULT_CLEAN_CSV,
        help=f"Path to output cleaned dataset CSV (default: {DEFAULT_CLEAN_CSV})",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=8,
        help="Number of concurrent network download workers (default: 8)",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="Limit number of missing bulletins to fetch (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover missing dates without fetching or saving data",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild DuckDB warehouse and static dashboard after merging",
    )

    args = parser.parse_args()

    fetch_and_merge_missing_bulletins(
        clean_csv=args.clean_csv,
        server_csv=args.server_csv,
        output_csv=args.output_csv,
        workers=args.workers,
        limit=args.limit,
        dry_run=args.dry_run,
        rebuild=args.rebuild,
    )
