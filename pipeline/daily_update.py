"""
Daily Incremental Update Orchestrator for Philippine Commodity Price Tracker.

Discovers, downloads, parses, and ingests newly published Department of Agriculture
(DA AMAS) Daily Price Index bulletins in-memory. Updates the Git-tracked cleaned_dataset.csv,
logs execution status to the audit trail, and rebuilds the analytical warehouse and dashboard.

Designed for automated headless execution inside GitHub Actions or local CLI runs.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any

import httpx
import pandas as pd

from pipeline.commodity_schema import (
    CLEAN_COLUMNS,
    DEFAULT_CLEAN_CSV,
    standardize_bulletin_records,
    validate_cleaned_panel,
)
from pipeline.download_pdfs import (
    PROJECT_USER_AGENT,
    crawl_da_catalog,
    generate_candidate_urls,
)
from pipeline.load_duckdb import (
    DEFAULT_BULLETINS_CSV,
    DEFAULT_DB_PATH,
    DEFAULT_LOGS_CSV,
    load_duckdb,
)
from pipeline.pdf_parser import parse_da_amas_pdf

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
        "message": message.replace(",", ";"),
    }

    if not logs_csv_path.exists():
        df_new = pd.DataFrame([new_row], columns=header)
        df_new.to_csv(logs_csv_path, index=False)
    else:
        df_existing = pd.read_csv(logs_csv_path)
        if run_id in df_existing["run_id"].astype(str).values:
            df_existing.loc[df_existing["run_id"].astype(str) == run_id] = list(new_row.values())
            df_existing.to_csv(logs_csv_path, index=False)
        else:
            df_new = pd.DataFrame([new_row], columns=header)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_csv(logs_csv_path, index=False)


def fetch_bulletin_bytes_in_memory(
    date_str: str,
    catalog_url: str | None,
    client: httpx.Client,
    max_retries: int = 2,
) -> tuple[bytes | None, str | None]:
    """
    Attempts to download bulletin PDF bytes into memory.
    Tries the catalog URL followed by standard candidate naming patterns.
    Returns (pdf_bytes_or_None, final_url_or_None).
    """
    urls_to_try: list[str] = []
    if catalog_url:
        urls_to_try.append(catalog_url)

    try:
        d = datetime.date.fromisoformat(date_str)
        for cand in generate_candidate_urls(d):
            if cand not in urls_to_try:
                urls_to_try.append(cand)
    except Exception:
        pass

    for current_url in urls_to_try:
        for attempt in range(1, max_retries + 1):
            try:
                resp = client.get(current_url)
                if resp.status_code == 404:
                    break
                elif resp.status_code != 200:
                    time.sleep(0.5 * attempt)
                    continue

                content = resp.content
                if not content.startswith(b"%PDF-") or len(content) < 5000:
                    break

                return content, current_url
            except Exception:
                time.sleep(0.5 * attempt)

    return None, None


def run_daily_update(
    clean_csv_path: Path = DEFAULT_CLEAN_CSV,
    bulletins_csv_path: Path = DEFAULT_BULLETINS_CSV,
    logs_csv_path: Path = DEFAULT_LOGS_CSV,
    db_path: Path = DEFAULT_DB_PATH,
    target_date: datetime.date | None = None,
    trigger: str = "cli",
    run_id: str | None = None,
    rebuild: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Executes the streamlined, in-memory daily incremental update pipeline.
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
                load_duckdb(db_path=db_path, clean_csv=clean_csv_path, logs_csv=logs_csv_path)
        return {
            "status": "NO_NEW_DATA",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": 0,
            "records_extracted": 0,
            "duration": elapsed,
        }

    # Evaluate all calendar days (no day-of-week skipping; ingest whenever available)
    dates_to_check: list[datetime.date] = []
    curr = latest_tracked + datetime.timedelta(days=1)
    while curr <= today:
        dates_to_check.append(curr)
        curr += datetime.timedelta(days=1)

    logger.info("Dates to evaluate (%d dates): %s to %s", len(dates_to_check), dates_to_check[0], dates_to_check[-1])

    # 2. Discover available bulletins
    catalog: dict[str, str] = {}
    try:
        catalog = crawl_da_catalog(use_cache=False)
    except Exception as exc:
        logger.warning("Could not crawl DA portal catalog directly (%s). Will attempt direct URL patterns.", exc)

    new_bulletins: list[tuple[str, str, bytes]] = []  # (date_str, source_url, pdf_bytes)
    headers = {"User-Agent": PROJECT_USER_AGENT}

    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        for d in dates_to_check:
            d_str = d.strftime("%Y-%m-%d")
            catalog_url = catalog.get(d_str)

            pdf_bytes, final_url = fetch_bulletin_bytes_in_memory(
                date_str=d_str,
                catalog_url=catalog_url,
                client=client,
            )
            if pdf_bytes and final_url:
                logger.info("Successfully fetched new bulletin for %s in-memory (%.1f KB)", d_str, len(pdf_bytes) / 1024)
                new_bulletins.append((d_str, final_url, pdf_bytes))
            else:
                logger.info("No published bulletin found for %s (Holiday, weekend, or not yet uploaded).", d_str)

    # 3. Handle outcome if no new bulletins were found
    if not new_bulletins:
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
                load_duckdb(db_path=db_path, clean_csv=clean_csv_path, logs_csv=logs_csv_path)
        return {
            "status": "NO_NEW_DATA",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": 0,
            "records_extracted": 0,
            "duration": elapsed,
        }

    # 4. Parse records directly in-memory and standardize without forward filling
    logger.info("Extracting price records from %d newly discovered bulletin(s)...", len(new_bulletins))
    extracted_records: list[dict[str, Any]] = []

    for d_str, source_url, pdf_bytes in new_bulletins:
        try:
            pdf_df = parse_da_amas_pdf(
                pdf_source=io.BytesIO(pdf_bytes),
                target_date=d_str,
                source_url=source_url,
            )
            if pdf_df is not None and not pdf_df.empty:
                panel_records = standardize_bulletin_records(
                    df_raw=pdf_df,
                    target_date=d_str,
                    source_url=source_url,
                )
                extracted_records.extend(panel_records)
                logger.info("Extracted %d panel commodity records for %s", len(panel_records), d_str)
            else:
                logger.warning("No records extracted from bulletin %s", d_str)
        except Exception as parse_err:
            logger.error("Failed to parse bulletin %s: %s", d_str, parse_err)

    if not extracted_records:
        msg = f"Discovered {len(new_bulletins)} bulletins but failed to parse price records."
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
                bulletins_downloaded=len(new_bulletins),
                records_extracted=0,
                duration_seconds=elapsed,
                message=msg,
            )
        return {
            "status": "FAILED",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": len(new_bulletins),
            "records_extracted": 0,
            "duration": elapsed,
        }

    if dry_run:
        logger.info("DRY-RUN: Would merge %d new records into %s", len(extracted_records), clean_csv_path)
        return {
            "status": "DRY_RUN_SUCCESS",
            "latest_date": str(latest_tracked),
            "bulletins_downloaded": len(new_bulletins),
            "records_extracted": len(extracted_records),
            "duration": time.time() - start_time,
        }

    # 5. Direct merge into data/cleaned_dataset.csv
    logger.info("Merging %d new records into %s ...", len(extracted_records), clean_csv_path.name)
    existing_clean_df = pd.read_csv(clean_csv_path) if clean_csv_path.exists() else pd.DataFrame(columns=CLEAN_COLUMNS)
    new_clean_df = pd.DataFrame(extracted_records)

    combined_clean_df = pd.concat([existing_clean_df, new_clean_df], ignore_index=True)
    combined_clean_df = combined_clean_df.drop_duplicates(subset=["date", "commodity"], keep="last")
    combined_clean_df = combined_clean_df[CLEAN_COLUMNS].sort_values(
        by=["date", "commodity_group", "commodity"]
    ).reset_index(drop=True)

    validate_cleaned_panel(combined_clean_df)

    # Atomic write to CSV
    tmp_clean = clean_csv_path.with_suffix(".csv.tmp")
    combined_clean_df.to_csv(tmp_clean, index=False)
    tmp_clean.replace(clean_csv_path)
    logger.info("Successfully updated clean dataset: %d total rows in %s", len(combined_clean_df), clean_csv_path.name)

    # Also update server_daily_bulletins.csv if tracked
    if bulletins_csv_path.exists():
        server_df = pd.read_csv(bulletins_csv_path)
        new_bulletin_rows = [
            {"date": b[0], "pdf_url": b[1]}
            for b in new_bulletins
            if b[0] not in server_df["date"].values
        ]
        if new_bulletin_rows:
            server_df = pd.concat([server_df, pd.DataFrame(new_bulletin_rows)], ignore_index=True)
            server_df = server_df.sort_values(by="date").reset_index(drop=True)
            server_df.to_csv(bulletins_csv_path, index=False)
            logger.info("Updated %s with %d new bulletin entries.", bulletins_csv_path.name, len(new_bulletin_rows))

    new_latest_date = get_latest_tracked_date(clean_csv_path)
    elapsed = time.time() - start_time

    # 6. Record Audit Log Entry
    status_msg = (
        f"Successfully ingested {len(new_bulletins)} new bulletin(s) with {len(extracted_records)} "
        f"records in-memory. Panel updated to {new_latest_date}."
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
        bulletins_downloaded=len(new_bulletins),
        records_extracted=len(extracted_records),
        duration_seconds=elapsed,
        message=status_msg,
    )

    # 7. Rebuild Warehouse & Static Dashboard
    if rebuild:
        logger.info("Rebuilding DuckDB analytical warehouse (Stage 5)...")
        con = load_duckdb(
            db_path=db_path,
            clean_csv=clean_csv_path,
            logs_csv=logs_csv_path,
        )
        con.close()

        logger.info("Rebuilding static dashboard bundle (Stage 6)...")
        from pipeline.build_dashboard import build_dashboard
        build_dashboard(db_path=db_path)

    return {
        "status": "SUCCESS",
        "latest_date": str(new_latest_date),
        "bulletins_downloaded": len(new_bulletins),
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
