"""
Public Web Server PDF Scanner for Philippine Commodity Price Tracker.

Scans the Department of Agriculture public web server (WordPress uploads and portal archives)
for Daily Price Index (DPI) bulletin PDFs across specified date ranges, and outputs
a CSV containing the observation date and direct download link to each PDF.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime
import json
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

from pipeline.download_pdfs import (
    CATALOG_CACHE_FILE,
    PROJECT_USER_AGENT,
    crawl_da_catalog,
    generate_candidate_urls,
)

DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "data" / "server_daily_bulletins.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scan_server_bulletins")


def check_candidate_urls_for_date(
    date_str: str,
    known_url: str | None,
    client: httpx.Client,
) -> tuple[str, str | None]:
    """
    Checks if a valid PDF bulletin exists on the public server for a given date.
    Performs lightweight HEAD / range GET checks across known WordPress upload patterns.
    Returns (date_str, direct_pdf_url_or_None).
    """
    urls_to_try: list[str] = []
    if known_url:
        urls_to_try.append(known_url)

    try:
        d = datetime.date.fromisoformat(date_str)
        for cand in generate_candidate_urls(d):
            if cand not in urls_to_try:
                urls_to_try.append(cand)
    except Exception:
        pass

    for url in urls_to_try:
        try:
            # First attempt a fast HEAD request
            resp = client.head(url, timeout=10.0)
            if resp.status_code == 200:
                content_type = resp.headers.get("content-type", "").lower()
                content_length = int(resp.headers.get("content-length", 0))
                # Validate it's a PDF of substantial size (not an HTML error page)
                if ("pdf" in content_type or url.lower().endswith(".pdf")) and (content_length == 0 or content_length > 5000):
                    return date_str, url

            # Some servers reject HEAD or return 403/405 for HEAD; try lightweight Range GET
            if resp.status_code in (403, 405, 501):
                resp = client.get(url, headers={"Range": "bytes=0-1024"}, timeout=10.0)
                if resp.status_code in (200, 206):
                    if resp.content.startswith(b"%PDF-"):
                        return date_str, url

        except Exception:
            continue

    return date_str, None


def scan_and_export_server_bulletins(
    start_date: datetime.date,
    end_date: datetime.date,
    output_csv: Path = DEFAULT_OUTPUT_CSV,
    workers: int = 10,
    use_catalog: bool = True,
) -> list[dict[str, str]]:
    """
    Scans the public server across start_date to end_date, maps every valid
    daily PDF URL, and writes the results to CSV.
    """
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # 1. Load catalog links if available
    catalog: dict[str, str] = {}
    if use_catalog:
        try:
            catalog = crawl_da_catalog(use_cache=True)
            logger.info("Loaded %d links from catalog archive.", len(catalog))
        except Exception as exc:
            logger.warning("Could not load catalog archive: %s", exc)

    # 2. Build list of calendar dates to verify
    target_dates: list[str] = []
    curr = start_date
    while curr <= end_date:
        target_dates.append(curr.strftime("%Y-%m-%d"))
        curr += datetime.timedelta(days=1)

    target_dates.sort(reverse=True)
    logger.info("Scanning %d dates from %s to %s ...", len(target_dates), start_date, end_date)

    discovered: dict[str, str] = {}
    pending_probe: list[tuple[str, str | None]] = []

    for d_str in target_dates:
        if d_str in catalog:
            # Already mapped and verified in catalog
            discovered[d_str] = catalog[d_str]
        else:
            pending_probe.append((d_str, None))

    logger.info("Known from catalog: %d | Probing public server for: %d dates", len(discovered), len(pending_probe))

    # 3. Parallel probing of uncataloged dates
    if pending_probe:
        client = httpx.Client(
            follow_redirects=True,
            headers={"User-Agent": PROJECT_USER_AGENT},
            limits=httpx.Limits(max_connections=workers * 2, max_keepalive_connections=workers),
        )
        lock = threading.Lock()
        completed = 0

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(check_candidate_urls_for_date, d_str, url, client): d_str
                    for d_str, url in pending_probe
                }
                for future in concurrent.futures.as_completed(futures):
                    d_str, found_url = future.result()
                    with lock:
                        completed += 1
                        if found_url:
                            discovered[d_str] = found_url
                            logger.info("[FOUND] %s -> %s", d_str, found_url)
                        if completed % 50 == 0 or completed == len(pending_probe):
                            logger.info("Probing progress: %d/%d (%.1f%%)", completed, len(pending_probe), (completed / len(pending_probe)) * 100)
        finally:
            client.close()

    # 4. Sort results chronologically and write to CSV
    sorted_records = [
        {"date": d, "pdf_url": discovered[d]}
        for d in sorted(discovered.keys())
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "pdf_url"])
        writer.writeheader()
        writer.writerows(sorted_records)

    logger.info("==================================================")
    logger.info("Public Server Scan Complete!")
    logger.info("Total Daily Bulletins Found: %d", len(sorted_records))
    logger.info("Exported to CSV            : %s", output_csv)
    logger.info("==================================================")

    return sorted_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan the DA public web server for Daily Price Index PDF bulletins and save to CSV."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-01-01",
        help="Start date to scan (YYYY-MM-DD). Default: 2024-01-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date to scan (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV filepath (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=10,
        help="Number of concurrent network workers (default: 10)",
    )
    parser.add_argument(
        "--no-catalog",
        action="store_true",
        help="Do not use cached catalog; probe all dates directly against the server.",
    )
    args = parser.parse_args()

    s_date = datetime.date.fromisoformat(args.start_date)
    e_date = datetime.date.fromisoformat(args.end_date) if args.end_date else datetime.date.today()

    scan_and_export_server_bulletins(
        start_date=s_date,
        end_date=e_date,
        output_csv=args.output,
        workers=args.workers,
        use_catalog=not args.no_catalog,
    )
