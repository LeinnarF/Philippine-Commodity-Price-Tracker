"""
Daily Incremental Update Orchestrator for Philippine Commodity Price Tracker.

Discovers, downloads, parses, and ingests newly published Department of Agriculture
(DA AMAS) Daily Price Index bulletins. Updates the Git-tracked panel datasets,
logs execution status to the audit trail, and rebuilds the analytical warehouse and dashboard.

Designed for automated headless execution inside GitHub Actions or local CLI runs.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any

import httpx
import pandas as pd

from pipeline.clean_dataset import DEFAULT_CLEAN_CSV, clean_dataset
from pipeline.download_pdfs import (
    MONTH_MAP,
    PROJECT_USER_AGENT,
    crawl_da_catalog,
    download_single_pdf,
    generate_candidate_urls,
    is_valid_pdf,
)
from pipeline.load_duckdb import (
    DEFAULT_DB_PATH,
    DEFAULT_LOGS_CSV,
    DEFAULT_SELECTED_CSV,
    load_duckdb,
)
from pipeline.pdf_parser import parse_da_amas_pdf
from pipeline.select_commodities import DEFAULT_RAW_CSV, select_commodities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("daily_update")


def get_latest_tracked_date(clean_csv_path: Path = DEFAULT_CLEAN_CSV) -> datetime.date:
    """Reads the maximum observation date currently tracked in the clean CSV dataset."""
    if not clean_csv_path.exists():
        raise FileNotFoundError(f"Clean CSV dataset not found at {clean_csv_path}")

    # Read strictly the 'date' column for speed and minimal memory
    df = pd.read_csv(clean_csv_path, usecols=["date"])
    if df.empty or "date" not in df.columns:
        raise ValueError(f"Clean CSV at {clean_csv_path} contains no valid 'date' column.")

    max_date_str = df["date"].max()
    return datetime.date.fromisoformat(str(max_date_str))


def append_execution_log(
    logs_csv_path: Path,
    run_id: str,
    executed_at: str,
    trigger: str,
    status: str,
    latest_bulletin_date: str | None,
    bulletins_checked: int,
    bulletins_downloaded: int,
    records_extracted: int,
    duration_seconds: float,
    message: str,
) -> None:
    """Appends an execution audit record to the tracked CSV log file."""
    logs_csv_path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "run_id",
        "executed_at",
        "trigger",
        "status",
        "latest_bulletin_date",
        "bulletins_checked",
        "bulletins_downloaded",
        "records_extracted",
        "duration_seconds",
        "message",
    ]

    new_row = {
        "run_id": run_id,
        "executed_at": executed_at,
        "trigger": trigger,
        "status": status,
        "latest_bulletin_date": latest_bulletin_date or "",
        "bulletins_checked": bulletins_checked,
        "bulletins_downloaded": bulletins_downloaded,
        "records_extracted": records_extracted,
        "duration_seconds": round(duration_seconds, 2),
        "message": message.replace(",", ";"),  # Sanitize for plain CSV safety
    }

    if not logs_csv_path.exists():
        df_new = pd.DataFrame([new_row], columns=header)
        df_new.to_csv(logs_csv_path, index=False)
    else:
        df_existing = pd.read_csv(logs_csv_path)
        # Check if run_id already exists to prevent duplicate rows on retry
        if run_id in df_existing["run_id"].astype(str).values:
            df_existing.loc[df_existing["run_id"].astype(str) == run_id] = list(new_row.values())
            df_existing.to_csv(logs_csv_path, index=False)
        else:
            df_new = pd.DataFrame([new_row], columns=header)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_csv(logs_csv_path, index=False)


def run_daily_update(
    raw_csv_path: Path = DEFAULT_RAW_CSV,
    selected_csv_path: Path = DEFAULT_SELECTED_CSV,
    clean_csv_path: Path = DEFAULT_CLEAN_CSV,
    logs_csv_path: Path = DEFAULT_LOGS_CSV,
    db_path: Path = DEFAULT_DB_PATH,
    target_date: datetime.date | None = None,
    trigger: str = "cli",
    run_id: str | None = None,
    rebuild: bool = True,
    dry_run: bool = False,
    temp_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Executes the incremental daily update pipeline.
    """
    start_time = time.time()
    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    resolved_run_id = run_id or f"run_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: Daily Incremental Update")
    logger.info("Run ID          : %s", resolved_run_id)
    logger.info("Trigger Source  : %s", trigger)
    logger.info("Execution Time  : %s UTC", now_utc)
    logger.info("==================================================")

    # 1. Determine date range
    latest_tracked = get_latest_tracked_date(clean_csv_path)
    today = target_date or datetime.date.today()
    logger.info("Current Max Tracked Date : %s", latest_tracked)
    logger.info("Target Evaluation Date   : %s", today)

    if latest_tracked >= today:
        msg = f"Dataset is already up to date with latest observation ({latest_tracked})."
        logger.info(msg)
        elapsed = time.time() - start_time
        if not dry_run:
            append_execution_log(
                logs_csv_path=logs_csv_path,
                run_id=resolved_run_id,
                executed_at=now_utc,
                trigger=trigger,
                status="NO_NEW_DATA",
                latest_bulletin_date=str(latest_tracked),
                bulletins_checked=0,
                bulletins_downloaded=0,
                records_extracted=0,
                duration_seconds=elapsed,
                message=msg,
            )
            if rebuild:
                load_duckdb(db_path=db_path, clean_csv=clean_csv_path, selected_csv=selected_csv_path, logs_csv=logs_csv_path)
        return {
            "status": "NO_NEW_DATA",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": 0,
            "records_extracted": 0,
            "duration": elapsed,
        }

    # Generate dates to evaluate: from latest_tracked + 1 day to today (excluding Sundays)
    dates_to_check: list[datetime.date] = []
    curr = latest_tracked + datetime.timedelta(days=1)
    while curr <= today:
        if curr.weekday() != 6:  # Exclude Sundays
            dates_to_check.append(curr)
        curr += datetime.timedelta(days=1)

    if not dates_to_check:
        msg = f"All target dates between {latest_tracked} and {today} are non-survey dates (e.g. Sundays)."
        logger.info(msg)
        elapsed = time.time() - start_time
        if not dry_run:
            append_execution_log(
                logs_csv_path=logs_csv_path,
                run_id=resolved_run_id,
                executed_at=now_utc,
                trigger=trigger,
                status="NO_NEW_DATA",
                latest_bulletin_date=str(latest_tracked),
                bulletins_checked=0,
                bulletins_downloaded=0,
                records_extracted=0,
                duration_seconds=elapsed,
                message=msg,
            )
        return {
            "status": "NO_NEW_DATA",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": 0,
            "records_extracted": 0,
            "duration": elapsed,
        }

    logger.info("Dates to evaluate (%d dates): %s to %s", len(dates_to_check), dates_to_check[0], dates_to_check[-1])

    # 2. Discover available bulletins
    catalog: dict[str, str] = {}
    try:
        catalog = crawl_da_catalog(use_cache=False)
    except Exception as exc:
        logger.warning("Could not crawl DA portal catalog directly (%s). Will attempt direct URL patterns.", exc)

    # Prepare storage directory for newly downloaded PDFs
    target_raw_dir = temp_dir or (raw_csv_path.parent / "raw" / "da_amas")
    target_raw_dir.mkdir(parents=True, exist_ok=True)

    newly_downloaded: list[tuple[str, Path]] = []
    headers = {"User-Agent": PROJECT_USER_AGENT}

    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        for d in dates_to_check:
            d_str = d.strftime("%Y-%m-%d")
            dest_pdf = target_raw_dir / f"{d_str}.pdf"

            # Check if PDF already exists and is valid
            if is_valid_pdf(dest_pdf):
                logger.info("Found existing local bulletin for %s", d_str)
                newly_downloaded.append((d_str, dest_pdf))
                continue

            # Check catalog URL and fallback candidate URL patterns
            catalog_url = catalog.get(d_str)
            logger.info("Checking bulletin for %s (catalog match: %s) ...", d_str, bool(catalog_url))
            date_res, status_res, size_res = download_single_pdf(
                date_str=d_str,
                pdf_url=catalog_url,
                client=client,
                max_retries=2,
                dest_dir=target_raw_dir,
            )
            if status_res == "DOWNLOADED" and is_valid_pdf(dest_pdf):
                logger.info("Successfully downloaded new bulletin for %s (%.1f KB)", d_str, size_res / 1024)
                newly_downloaded.append((d_str, dest_pdf))
            else:
                logger.info("No published bulletin found for %s (Holiday, weekend, or not yet uploaded).", d_str)

    # 3. Handle outcome if no new bulletins were found
    if not newly_downloaded:
        msg = f"Evaluated {len(dates_to_check)} candidate dates ({dates_to_check[0]} to {dates_to_check[-1]}). No new bulletins published on DA portal yet."
        logger.info(msg)
        elapsed = time.time() - start_time
        if not dry_run:
            append_execution_log(
                logs_csv_path=logs_csv_path,
                run_id=resolved_run_id,
                executed_at=now_utc,
                trigger=trigger,
                status="NO_NEW_DATA",
                latest_bulletin_date=str(latest_tracked),
                bulletins_checked=len(dates_to_check),
                bulletins_downloaded=0,
                records_extracted=0,
                duration_seconds=elapsed,
                message=msg,
            )
            if rebuild:
                load_duckdb(db_path=db_path, clean_csv=clean_csv_path, selected_csv=selected_csv_path, logs_csv=logs_csv_path)
        return {
            "status": "NO_NEW_DATA",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": 0,
            "records_extracted": 0,
            "duration": elapsed,
        }

    # 4. Parse records from new bulletins
    logger.info("Extracting price records from %d newly downloaded bulletin(s)...", len(newly_downloaded))
    extracted_records: list[dict[str, Any]] = []

    for d_str, pdf_path in newly_downloaded:
        try:
            pdf_df = parse_da_amas_pdf(pdf_path, d_str)
            if pdf_df is not None and not pdf_df.empty:
                records = pdf_df.to_dict(orient="records")
                for r in records:
                    r["raw_file"] = pdf_path.name
                extracted_records.extend(records)
                logger.info("Parsed %d records from %s", len(records), pdf_path.name)
            else:
                logger.warning("No records extracted from bulletin %s", pdf_path.name)
        except Exception as parse_err:
            logger.error("Failed to parse bulletin %s: %s", pdf_path.name, parse_err)

    if not extracted_records:
        msg = f"Downloaded {len(newly_downloaded)} bulletins but failed to parse price records."
        logger.warning(msg)
        elapsed = time.time() - start_time
        if not dry_run:
            append_execution_log(
                logs_csv_path=logs_csv_path,
                run_id=resolved_run_id,
                executed_at=now_utc,
                trigger=trigger,
                status="FAILED",
                latest_bulletin_date=str(latest_tracked),
                bulletins_checked=len(dates_to_check),
                bulletins_downloaded=len(newly_downloaded),
                records_extracted=0,
                duration_seconds=elapsed,
                message=msg,
            )
        return {
            "status": "FAILED",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": len(newly_downloaded),
            "records_extracted": 0,
            "duration": elapsed,
        }

    if dry_run:
        logger.info("DRY-RUN: Would merge %d new records into %s", len(extracted_records), raw_csv_path)
        return {
            "status": "DRY_RUN_SUCCESS",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": len(newly_downloaded),
            "records_extracted": len(extracted_records),
            "duration": time.time() - start_time,
        }

    # 5. Merge incrementally into data/raw_da_amas_prices.csv
    logger.info("Merging %d new records into %s ...", len(extracted_records), raw_csv_path)
    existing_raw_df = pd.read_csv(raw_csv_path) if raw_csv_path.exists() else pd.DataFrame()
    new_raw_df = pd.DataFrame(extracted_records)

    combined_raw_df = pd.concat([existing_raw_df, new_raw_df], ignore_index=True)
    # Deduplicate strictly on date, commodity_group, commodity, variety
    dedup_cols = ["date", "commodity_group", "commodity", "variety"]
    combined_raw_df = combined_raw_df.drop_duplicates(subset=dedup_cols, keep="last")
    combined_raw_df = combined_raw_df.sort_values(by=dedup_cols, ascending=[True, True, True, True])

    raw_cols = [
        "date",
        "commodity_group",
        "commodity",
        "variety",
        "unit",
        "price_prevailing",
        "market_scope",
        "source",
        "is_synthetic",
        "source_url",
        "raw_file",
    ]
    existing_cols = [c for c in raw_cols if c in combined_raw_df.columns]
    combined_raw_df = combined_raw_df[existing_cols]
    combined_raw_df.to_csv(raw_csv_path, index=False)
    logger.info("Successfully updated raw dataset: %d total rows in %s", len(combined_raw_df), raw_csv_path.name)

    # 6. Re-run Stage 3 (select_commodities) and Stage 4 (clean_dataset)
    logger.info("Standardizing 48 commodities panel grid (Stage 3)...")
    select_commodities(input_csv=raw_csv_path, output_csv=selected_csv_path)

    logger.info("Exporting clean presentation dataset (Stage 4)...")
    clean_dataset(input_csv=selected_csv_path, output_csv=clean_csv_path)

    new_latest_date = get_latest_tracked_date(clean_csv_path)
    elapsed = time.time() - start_time

    # 7. Record Audit Log Entry
    status_msg = (
        f"Successfully ingested {len(newly_downloaded)} new bulletin(s) with {len(extracted_records)} "
        f"records. Panel updated to {new_latest_date}."
    )
    logger.info(status_msg)
    append_execution_log(
        logs_csv_path=logs_csv_path,
        run_id=resolved_run_id,
        executed_at=now_utc,
        trigger=trigger,
        status="SUCCESS",
        latest_bulletin_date=str(new_latest_date),
        bulletins_checked=len(dates_to_check),
        bulletins_downloaded=len(newly_downloaded),
        records_extracted=len(extracted_records),
        duration_seconds=elapsed,
        message=status_msg,
    )

    # 8. Rebuild Warehouse & Static Dashboard
    if rebuild:
        logger.info("Rebuilding DuckDB analytical warehouse (Stage 5)...")
        load_duckdb(
            db_path=db_path,
            clean_csv=clean_csv_path,
            selected_csv=selected_csv_path,
            logs_csv=logs_csv_path,
        )

        logger.info("Rebuilding static dashboard bundle (Stage 6)...")
        from pipeline.build_dashboard import build_dashboard
        build_dashboard(db_path=db_path)

    return {
        "status": "SUCCESS",
        "latest_date": str(new_latest_date),
        "bulletins_downloaded": len(newly_downloaded),
        "records_extracted": len(extracted_records),
        "duration": elapsed,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Daily incremental update pipeline for DA AMAS retail price bulletins."
    )
    parser.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="Target date up to which to evaluate bulletins (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--trigger",
        type=str,
        default="cli",
        help="Trigger source identifier (e.g., github_actions_cron, workflow_dispatch, cli)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional unique run execution identifier",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        default=True,
        help="Rebuild DuckDB warehouse and static dashboard bundle (default: True)",
    )
    parser.add_argument(
        "--no-rebuild",
        action="store_false",
        dest="rebuild",
        help="Skip rebuilding DuckDB and the static dashboard",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check and discover bulletins without saving changes to disk",
    )

    args = parser.parse_args()

    target_d = datetime.date.fromisoformat(args.target_date) if args.target_date else None
    res = run_daily_update(
        target_date=target_d,
        trigger=args.trigger,
        run_id=args.run_id,
        rebuild=args.rebuild,
        dry_run=args.dry_run,
    )
    logger.info("Daily update finished with result: %s", res)
