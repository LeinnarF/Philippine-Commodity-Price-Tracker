"""
Isolated In-Memory Salt Price Extraction & Testing Script for Philippine Commodity Price Tracker.

Extracts 'Rock Salt' and 'Iodized Salt' prevailing retail prices from Department of Agriculture
(DA AMAS) Daily Price Index bulletins 100% headless in-memory (no local PDF files created).

Streams PDF bytes directly into memory via httpx and parses them in-memory using pdfplumber/io.BytesIO.

Usage:
    # 1. Extract salt prices in-memory for a specific bulletin date:
    python scripts/extract_salt_prices.py --date 2026-11-03

    # 2. Test in-memory extraction on the 5 most recent published bulletins:
    python scripts/extract_salt_prices.py --limit 5

    # 3. Scan a specific historical date range in-memory:
    python scripts/extract_salt_prices.py --start-date 2026-10-01 --end-date 2026-11-03

    # 4. Scan all historical bulletins registered in data/server_daily_bulletins.csv:
    python scripts/extract_salt_prices.py --all

    # 5. Extract in-memory and merge/apply discovered prices into data/cleaned_dataset.csv:
    python scripts/extract_salt_prices.py --limit 10 --apply
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import io
import logging
import os
import sys
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
import pdfplumber

from pipeline.pdf_parser import parse_da_amas_pdf
from pipeline.download_pdfs import PROJECT_USER_AGENT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("extract_salt_prices")

DEFAULT_SERVER_BULLETINS_CSV = PROJECT_ROOT / "data" / "server_daily_bulletins.csv"
DEFAULT_CLEAN_CSV = PROJECT_ROOT / "data" / "cleaned_dataset.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "data" / "extracted_salt_prices.csv"


def extract_salt_from_dataframe(df_raw: pd.DataFrame, target_date: str, source_url: str = "") -> list[dict[str, Any]]:
    """
    Extracts Rock Salt and Iodized Salt prices from a raw parsed bulletin DataFrame.
    """
    records: list[dict[str, Any]] = []

    if df_raw.empty:
        return records

    # Column shift detection (104 dates have numeric commodity IDs)
    is_num = df_raw["commodity"].astype(str).str.isdigit()

    # Matchers
    mask_rock = (
        (is_num & ((df_raw["variety"].str.contains(r"rock", case=False, na=False) & df_raw["variety"].str.contains(r"salt", case=False, na=False)) | df_raw["variety"].isin(["Salt (Rock)", "Rock Salt", "Salt, Rock", "Rock"])))
        | (~is_num & (
            (df_raw["commodity"].str.contains(r"rock", case=False, na=False) & df_raw["commodity"].str.contains(r"salt", case=False, na=False))
            | (df_raw["commodity"].str.contains(r"salt", case=False, na=False) & df_raw["variety"].str.contains(r"rock", case=False, na=False))
            | df_raw["commodity"].isin(["Salt (Rock)", "Rock Salt", "Salt, Rock"])
        ))
    )

    mask_iodized = (
        (is_num & ((df_raw["variety"].str.contains(r"iodized", case=False, na=False) & df_raw["variety"].str.contains(r"salt", case=False, na=False)) | df_raw["variety"].isin(["Salt (Iodized)", "Iodized Salt", "Salt, Iodized", "Iodized"])))
        | (~is_num & (
            (df_raw["commodity"].str.contains(r"iodized", case=False, na=False) & df_raw["commodity"].str.contains(r"salt", case=False, na=False))
            | (df_raw["commodity"].str.contains(r"salt", case=False, na=False) & df_raw["variety"].str.contains(r"iodized", case=False, na=False))
            | df_raw["commodity"].isin(["Salt (Iodized)", "Iodized Salt", "Salt, Iodized"])
        ))
    )

    for salt_name, mask in [("Rock Salt", mask_rock), ("Iodized Salt", mask_iodized)]:
        sub = df_raw[mask]
        if not sub.empty:
            valid_prices = sub["price_prevailing"].dropna()
            price_val = round(float(valid_prices.mean()), 2) if not valid_prices.empty else np.nan
            raw_match = sub.iloc[0].to_dict()
        else:
            price_val = np.nan
            raw_match = {}

        records.append({
            "date": target_date,
            "commodity_group": "Basic Goods",
            "commodity": salt_name,
            "unit": "kg",
            "price_prevailing": price_val,
            "source_url": source_url,
            "raw_commodity": raw_match.get("commodity", ""),
            "raw_variety": raw_match.get("variety", ""),
        })

    return records


def extract_salt_from_in_memory_bytes(pdf_bytes: bytes, target_date: str, source_url: str = "") -> list[dict[str, Any]]:
    """
    Extracts salt records completely in-memory from PDF raw bytes (zero files written to disk).
    Uses parse_da_amas_pdf with fallback direct in-memory table cell inspection.
    """
    records: list[dict[str, Any]] = []

    # 1. Primary: Standard parsed dataframe via parse_da_amas_pdf
    try:
        df_raw = parse_da_amas_pdf(pdf_bytes, target_date, source_url)
        records = extract_salt_from_dataframe(df_raw, target_date, source_url)

        # Check if both found valid prices
        found_prices = {r["commodity"]: r["price_prevailing"] for r in records if pd.notna(r["price_prevailing"])}
        if len(found_prices) == 2:
            return records
    except Exception as e:
        logger.debug("parse_da_amas_pdf in-memory failed on date %s: %s", target_date, e)
        records = []

    # 2. Fallback: Direct in-memory table scan across all pages via BytesIO
    try:
        direct_matches: dict[str, float] = {}
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        non_empty = [c.replace("\n", " ").strip() for c in row if c and c.strip()]
                        if not non_empty:
                            continue
                        line_text = " ".join(non_empty).lower()
                        if "salt" in line_text or "asin" in line_text:
                            # Extract price
                            p_val = None
                            for cell in reversed(non_empty):
                                try:
                                    p = float(cell.replace(",", ""))
                                    if p > 0:
                                        p_val = p
                                        break
                                except ValueError:
                                    pass

                            if p_val is not None:
                                if "rock" in line_text and "Rock Salt" not in direct_matches:
                                    direct_matches["Rock Salt"] = p_val
                                elif "iodized" in line_text and "Iodized Salt" not in direct_matches:
                                    direct_matches["Iodized Salt"] = p_val

        if direct_matches:
            records = [
                {
                    "date": target_date,
                    "commodity_group": "Basic Goods",
                    "commodity": comm,
                    "unit": "kg",
                    "price_prevailing": direct_matches.get(comm, np.nan),
                    "source_url": source_url,
                    "raw_commodity": f"In-Memory Table Scan ({comm})",
                    "raw_variety": "Standard",
                }
                for comm in ["Rock Salt", "Iodized Salt"]
            ]
    except Exception as fallback_err:
        logger.error("In-memory table fallback failed on %s: %s", target_date, fallback_err)

    return records


def download_pdf_bytes_in_memory(url: str, client: httpx.Client, max_retries: int = 3) -> bytes | None:
    """Streams PDF bytes directly into memory with retry logic (zero disk writes)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.get(url, timeout=25.0)
            if resp.status_code == 200 and resp.content.startswith(b"%PDF"):
                return resp.content
            elif resp.status_code == 404:
                return None
        except Exception as e:
            if attempt == max_retries:
                logger.warning("Failed to stream %s in-memory after %d attempts: %s", url, max_retries, e)
            time.sleep(0.5 * attempt)
    return None


def process_in_memory_bulletin(item: tuple[str, str], client: httpx.Client) -> list[dict[str, Any]]:
    """Worker task to stream and extract salt for a given (date, url) item 100% in-memory."""
    d_str, url = item
    pdf_bytes = download_pdf_bytes_in_memory(url, client)
    if pdf_bytes is None:
        return [
            {
                "date": d_str,
                "commodity_group": "Basic Goods",
                "commodity": comm,
                "unit": "kg",
                "price_prevailing": np.nan,
                "source_url": url,
                "raw_commodity": "Stream Failed / Not Found",
                "raw_variety": "",
            }
            for comm in ["Rock Salt", "Iodized Salt"]
        ]
    return extract_salt_from_in_memory_bytes(pdf_bytes, d_str, url)


def apply_extracted_prices_to_clean_dataset(
    df_salt: pd.DataFrame,
    clean_csv_path: Path = DEFAULT_CLEAN_CSV,
) -> None:
    """
    Safely merges extracted salt prices into data/cleaned_dataset.csv,
    preserving all existing records and rectangular Cartesian matrix constraints.
    """
    if not clean_csv_path.exists():
        raise FileNotFoundError(f"Clean dataset not found at {clean_csv_path}")

    logger.info("Applying extracted salt prices to %s...", clean_csv_path)
    clean_df = pd.read_csv(clean_csv_path)

    # Filter to valid prices only
    observed_salt = df_salt[df_salt["price_prevailing"].notna()]
    if observed_salt.empty:
        logger.warning("No observed prices in extracted salt data to apply.")
        return

    update_count = 0
    # Map (date, commodity) -> price
    salt_lookup = {(r["date"], r["commodity"]): r["price_prevailing"] for _, r in observed_salt.iterrows()}

    for (d, comm), price in salt_lookup.items():
        mask = (clean_df["date"] == d) & (clean_df["commodity"] == comm)
        if mask.any():
            clean_df.loc[mask, "price_prevailing"] = price
            update_count += 1

    clean_df.to_csv(clean_csv_path, index=False, encoding="utf-8")
    logger.info("Successfully populated %d observed price cells in %s!", update_count, clean_csv_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Isolated In-Memory Salt Price Extraction Tool for Philippine Commodity Price Tracker."
    )
    parser.add_argument("--date", type=str, help="Specific date to stream and extract (YYYY-MM-DD).")
    parser.add_argument("--start-date", type=str, help="Filter start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, help="Filter end date (YYYY-MM-DD).")
    parser.add_argument("--limit", type=int, help="Limit number of bulletins to process (e.g. 5, 20).")
    parser.add_argument("--all", action="store_true", help="Process all registered server bulletins.")
    parser.add_argument("--output", "-o", type=str, default=str(DEFAULT_OUTPUT_CSV), help="Output CSV path.")
    parser.add_argument("--resume", action="store_true", default=True, help="Resume from existing extracted records (default: True).")
    parser.add_argument("--force", action="store_true", help="Force re-extraction of all targeted dates.")
    parser.add_argument("--ffill", action="store_true", default=True, help="Forward fill sporadic gaps after initial observation (default: True).")
    parser.add_argument("--no-ffill", dest="ffill", action="store_false", help="Disable forward fill of sporadic gaps.")
    parser.add_argument("--bfill", action="store_true", default=True, help="Backfill leading unmonitored dates to March 1, 2025 (default: True).")
    parser.add_argument("--no-bfill", dest="bfill", action="store_false", help="Disable backfilling of leading unmonitored dates.")
    parser.add_argument("--apply", action="store_true", help="Merge extracted prices into data/cleaned_dataset.csv.")
    parser.add_argument("--workers", type=int, default=6, help="Parallel workers for downloading (default: 6).")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging.")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not DEFAULT_SERVER_BULLETINS_CSV.exists():
        logger.error("Server daily bulletins registry not found at %s", DEFAULT_SERVER_BULLETINS_CSV)
        sys.exit(1)

    server_df = pd.read_csv(DEFAULT_SERVER_BULLETINS_CSV).dropna(subset=["date", "pdf_url"])
    server_df["date"] = server_df["date"].astype(str).str.strip()

    # Filter dates
    if args.date:
        server_df = server_df[server_df["date"] == args.date]
    if args.start_date:
        server_df = server_df[server_df["date"] >= args.start_date]
    if args.end_date:
        server_df = server_df[server_df["date"] <= args.end_date]

    # Sort descending by date to prioritize most recent bulletins
    server_df = server_df.sort_values(by="date", ascending=False)

    if args.limit and args.limit > 0:
        server_df = server_df.head(args.limit)

    targets = list(zip(server_df["date"], server_df["pdf_url"]))

    if not targets:
        logger.warning("No bulletin URLs matched the specified date filters.")
        return

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_records: list[dict[str, Any]] = []
    already_extracted_dates: set[str] = set()

    # Resume support: load existing records if present
    if output_path.exists() and not args.force:
        try:
            df_existing = pd.read_csv(output_path)
            if not df_existing.empty and "date" in df_existing.columns:
                already_extracted_dates = set(df_existing["date"].dropna().astype(str).unique())
                all_records = df_existing.to_dict(orient="records")
                logger.info("Found %d already extracted dates in %s (will resume).", len(already_extracted_dates), output_path)
        except Exception as read_err:
            logger.warning("Could not load existing extracted CSV (%s). Starting clean.", read_err)

    # Filter out already extracted targets
    pending_targets = [t for t in targets if t[0] not in already_extracted_dates]

    logger.info("==================================================")
    logger.info("DA In-Memory Salt Price Extractor (Zero Local PDFs)")
    logger.info("Total Registered Bulletins : %d dates", len(targets))
    logger.info("Already Extracted          : %d dates", len(already_extracted_dates))
    logger.info("Pending Bulletins to Stream: %d dates", len(pending_targets))
    if pending_targets:
        logger.info("Date Range                 : %s to %s", pending_targets[-1][0], pending_targets[0][0])
    logger.info("Parallel Workers           : %d in-memory threads", args.workers)
    logger.info("Output CSV                 : %s", output_path)
    logger.info("==================================================")

    start_time = time.time()

    if pending_targets:
        client_headers = {
            "User-Agent": PROJECT_USER_AGENT,
            "Accept": "application/pdf,*/*",
        }
        limits = httpx.Limits(max_connections=12, max_keepalive_connections=6)

        # Parallel in-memory stream and extract with checkpointing
        with httpx.Client(headers=client_headers, limits=limits, verify=False) as client:
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_item = {executor.submit(process_in_memory_bulletin, item, client): item for item in pending_targets}

                for idx, future in enumerate(concurrent.futures.as_completed(future_to_item), 1):
                    item = future_to_item[future]
                    try:
                        records = future.result()
                        all_records.extend(records)
                    except Exception as exc:
                        logger.error("Error processing bulletin %s: %s", item[0], exc)

                    if idx % 10 == 0 or idx == len(pending_targets):
                        logger.info("Progress: [%d/%d] bulletins streamed (%.1f%%)", idx, len(pending_targets), (idx / len(pending_targets)) * 100)

                    # Incremental checkpoint every 25 bulletins
                    if idx % 25 == 0:
                        df_chk = pd.DataFrame(all_records).drop_duplicates(subset=["date", "commodity"], keep="last")
                        df_chk = df_chk.sort_values(by=["date", "commodity"]).reset_index(drop=True)
                        df_chk.to_csv(output_path, index=False, encoding="utf-8")
                        logger.debug("Checkpoint saved (%d rows) to %s", len(df_chk), output_path)

    elapsed = time.time() - start_time
    df_all = pd.DataFrame(all_records).drop_duplicates(subset=["date", "commodity"], keep="last")

    # Forward fill sporadic gaps if enabled (preserves leading unmonitored gap naturally)
    if args.ffill and not df_all.empty:
        logger.info("Applying forward fill (ffill) to sporadic gaps after first observation date...")
        df_all = df_all.sort_values(by=["commodity", "date"]).reset_index(drop=True)
        df_all["price_prevailing"] = df_all.groupby("commodity")["price_prevailing"].ffill()

    # Backfill leading unmonitored gaps if enabled
    if args.bfill and not df_all.empty:
        logger.info("Applying backward fill (bfill) to leading dates prior to initial observation...")
        df_all = df_all.sort_values(by=["commodity", "date"]).reset_index(drop=True)
        df_all["price_prevailing"] = df_all.groupby("commodity")["price_prevailing"].bfill()

    df_all = df_all.sort_values(by=["date", "commodity"]).reset_index(drop=True)

    # Save to output CSV
    df_all.to_csv(output_path, index=False, encoding="utf-8")

    # Metrics Summary
    rock = df_all[df_all["commodity"] == "Rock Salt"]
    iodized = df_all[df_all["commodity"] == "Iodized Salt"]

    rock_obs = rock[rock["price_prevailing"].notna()]
    iod_obs = iodized[iodized["price_prevailing"].notna()]

    logger.info("==================================================")
    logger.info("In-Memory Extraction Completed in %.2f seconds!", elapsed)
    logger.info("Total Dates in Dataset     : %d", df_all["date"].nunique())
    logger.info("Rock Salt Observed Dates   : %d / %d (%.1f%%)", len(rock_obs), df_all["date"].nunique(), (len(rock_obs) / df_all["date"].nunique()) * 100 if not df_all.empty else 0)
    if not rock_obs.empty:
        logger.info("  First Rock Salt Date     : %s (₱%.2f/kg)", rock_obs["date"].min(), rock_obs.loc[rock_obs["date"] == rock_obs["date"].min(), "price_prevailing"].iloc[0])
        logger.info("  Latest Rock Salt Date    : %s (₱%.2f/kg)", rock_obs["date"].max(), rock_obs.loc[rock_obs["date"] == rock_obs["date"].max(), "price_prevailing"].iloc[0])
        logger.info("  Rock Salt Prices (₱/kg)  : Min=%.2f | Mean=%.2f | Median=%.2f | Max=%.2f", rock_obs["price_prevailing"].min(), rock_obs["price_prevailing"].mean(), rock_obs["price_prevailing"].median(), rock_obs["price_prevailing"].max())

    logger.info("Iodized Salt Observed Dates: %d / %d (%.1f%%)", len(iod_obs), df_all["date"].nunique(), (len(iod_obs) / df_all["date"].nunique()) * 100 if not df_all.empty else 0)
    if not iod_obs.empty:
        logger.info("  First Iodized Salt Date  : %s (₱%.2f/kg)", iod_obs["date"].min(), iod_obs.loc[iod_obs["date"] == iod_obs["date"].min(), "price_prevailing"].iloc[0])
        logger.info("  Latest Iodized Salt Date : %s (₱%.2f/kg)", iod_obs["date"].max(), iod_obs.loc[iod_obs["date"] == iod_obs["date"].max(), "price_prevailing"].iloc[0])
        logger.info("  Iodized Salt Prices (₱/kg): Min=%.2f | Mean=%.2f | Median=%.2f | Max=%.2f", iod_obs["price_prevailing"].min(), iod_obs["price_prevailing"].mean(), iod_obs["price_prevailing"].median(), iod_obs["price_prevailing"].max())

    logger.info("Saved Extracted Data To    : %s (%d rows)", output_path, len(df_all))
    logger.info("==================================================")

    # Show preview
    print("\n=== LATEST EXTRACTED SAMPLES ===")
    preview = df_all.dropna(subset=["price_prevailing"]).tail(10)
    if not preview.empty:
        print(preview[["date", "commodity", "unit", "price_prevailing", "raw_commodity"]].to_string(index=False))
    else:
        print("No observed price records found in evaluated bulletins.")

    # Apply if requested
    if args.apply:
        apply_extracted_prices_to_clean_dataset(df_all)


if __name__ == "__main__":
    main()
