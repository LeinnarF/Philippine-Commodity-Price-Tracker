"""
Batch PDF-to-CSV Extractor for Philippine Commodity Price Tracker (Stage 2).
Parses all locally downloaded DA AMAS Daily Price Index PDFs into a consolidated raw CSV file.
Runs 100% offline at CPU speed using multi-process parallelism.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.pdf_parser import parse_da_amas_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("extract_raw")

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "da_amas"
DEFAULT_OUTPUT_CSV = Path(__file__).resolve().parent.parent / "data" / "raw_da_amas_prices.csv"


def _parse_worker(pdf_path_str: str) -> list[dict[str, Any]]:
    """Worker function for multiprocessing pool: parses a single PDF into dict records."""
    pdf_path = Path(pdf_path_str)
    m = re.match(r"(\d{4}-\d{2}-\d{2})\.pdf", pdf_path.name)
    if not m:
        return []
    date_str = m.group(1)

    try:
        df = parse_da_amas_pdf(pdf_path, date_str)
        if df.empty:
            return []
        records = df.to_dict(orient="records")
        for r in records:
            r["raw_file"] = pdf_path.name
        return records
    except Exception as err:
        return []


def run_batch_extraction(
    input_dir: str | Path = RAW_DATA_DIR,
    output_path: str | Path = DEFAULT_OUTPUT_CSV,
    workers: int | None = None,
    limit: int | None = None,
) -> int:
    """
    Extracts all local PDFs in input_dir and writes them to output_path CSV.
    """
    input_dir = Path(input_dir)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        logger.error("Input directory does not exist: %s", input_dir)
        return 0

    pdf_files = sorted(list(input_dir.glob("*.pdf")))
    logger.info("Found %d local PDF files in %s", len(pdf_files), input_dir)

    if limit is not None and limit > 0:
        logger.info("Limit specified: Parsing %d files only.", limit)
        pdf_files = pdf_files[:limit]

    if not pdf_files:
        logger.warning("No PDF files to process in %s", input_dir)
        return 0

    cpu_count = os.cpu_count() or 4
    if workers is None:
        workers = min(cpu_count, 8)

    logger.info("==================================================")
    logger.info("DA AMAS Batch PDF-to-CSV Extractor (Stage 2)")
    logger.info("Input PDF Directory : %s (%d files)", input_dir, len(pdf_files))
    logger.info("Output CSV File     : %s", output_path)
    logger.info("Parallel Workers    : %d CPU processes", workers)
    logger.info("==================================================")

    all_records = []
    completed_files = 0
    start_time = time.time()

    pdf_path_strs = [str(p) for p in pdf_files]

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_parse_worker, p_str): p_str for p_str in pdf_path_strs}

        for future in concurrent.futures.as_completed(futures):
            completed_files += 1
            res = future.result()
            if res:
                all_records.extend(res)

            if completed_files % 50 == 0 or completed_files == len(pdf_files):
                pct = (completed_files / len(pdf_files)) * 100
                logger.info(
                    "Progress: [%d/%d] (%.1f%%) | Extracted: %d total records so far",
                    completed_files,
                    len(pdf_files),
                    pct,
                    len(all_records),
                )

    if not all_records:
        logger.warning("No records could be extracted from the PDF archive.")
        return 0

    # Build DataFrame
    logger.info("Consolidating and sorting %d total extracted records...", len(all_records))
    df = pd.DataFrame(all_records)

    # Sort deterministically
    df = df.sort_values(
        by=["date", "commodity_group", "commodity", "variety"],
        ascending=[True, True, True, True],
    )

    # Reorder columns
    cols = [
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
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols]

    # Write to CSV
    df.to_csv(output_path, index=False, encoding="utf-8")
    elapsed = time.time() - start_time
    csv_size_mb = output_path.stat().st_size / (1024 * 1024)

    logger.info("==================================================")
    logger.info("Extraction Complete!")
    logger.info("  Time Elapsed       : %.1f seconds", elapsed)
    logger.info("  Total Bulletins    : %d PDFs", completed_files)
    logger.info("  Total Records      : %d rows", len(df))
    logger.info("  Unique Dates       : %d days", df["date"].nunique())
    logger.info("  Unique Commodities : %d items", df["commodity"].nunique())
    logger.info("  Earliest Date      : %s", df["date"].min())
    logger.info("  Latest Date        : %s", df["date"].max())
    logger.info("  Consolidated CSV   : %s (%.2f MB)", output_path.resolve(), csv_size_mb)
    logger.info("==================================================")

    return len(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage 2: Batch PDF-to-CSV Extractor (Offline Multiprocessing)"
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        type=str,
        default=str(RAW_DATA_DIR),
        help=f"Directory containing downloaded PDF bulletins (default: {RAW_DATA_DIR})",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=str(DEFAULT_OUTPUT_CSV),
        help=f"Output consolidated CSV file path (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=None,
        help="Number of CPU worker processes (defaults to available CPU cores)",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="Limit number of PDFs to parse (useful for quick testing)",
    )
    args = parser.parse_args()

    run_batch_extraction(
        input_dir=args.input_dir,
        output_path=args.output,
        workers=args.workers,
        limit=args.limit,
    )
