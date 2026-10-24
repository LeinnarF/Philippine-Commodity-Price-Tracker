"""
Parallel Raw PDF Downloader for Philippine Commodity Price Tracker.
Crawls and downloads up to 2 years of official Department of Agriculture price monitoring
Daily Price Index bulletins in parallel into data/raw/da_amas/.

NOTE: This script ONLY downloads and archives raw PDF files. No parsing or extraction is performed.

Features:
- Bounded Parallelism: Uses ThreadPoolExecutor (default: 5 workers) to avoid overwhelming the server.
- Resumable: Automatically skips already downloaded and valid PDFs.
- Integrity Check: Validates that downloaded files are authentic PDFs (%PDF- header).
- Graceful Error Handling: Retries with backoff; records missing/holiday 404s without crashing.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("download_pdfs")

DA_PRICE_MONITORING_URL = "https://www.da.gov.ph/price-monitoring/"
PROJECT_USER_AGENT = "PH-Commodity-Tracker/1.0 (+https://github.com/ph-commodity-tracker; open-source research)"

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "da_amas"
CATALOG_CACHE_FILE = RAW_DATA_DIR / "catalog.json"
MISSING_LOG_FILE = RAW_DATA_DIR / "missing_dates.txt"

MONTH_MAP = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def crawl_da_catalog(use_cache: bool = True) -> dict[str, str]:
    """
    Crawls https://www.da.gov.ph/price-monitoring/ for all Daily Price Index PDF links.
    Caches the catalog to data/raw/da_amas/catalog.json.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if use_cache and CATALOG_CACHE_FILE.exists():
        try:
            with open(CATALOG_CACHE_FILE, "r") as f:
                data = json.load(f)
                if data and isinstance(data, dict):
                    logger.info("Loaded %d bulletin links from cached catalog: %s", len(data), CATALOG_CACHE_FILE.name)
                    return data
        except Exception as e:
            logger.warning("Failed to read cached catalog (%s). Re-fetching from DA...", e)

    headers = {"User-Agent": PROJECT_USER_AGENT}
    logger.info("Connecting to DA portal to discover available PDF bulletins...")

    try:
        with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
            resp = client.get(DA_PRICE_MONITORING_URL)
            if resp.status_code != 200:
                logger.error("DA landing page returned HTTP %d", resp.status_code)
                return {}

            soup = BeautifulSoup(resp.text, "html.parser")
            date_to_url: dict[str, str] = {}

            for a in soup.find_all("a"):
                href = a.get("href") or ""
                if not href.lower().endswith(".pdf") or "daily-price-index" not in href.lower():
                    continue

                filename = href.split("/")[-1]
                m = re.search(
                    r"Daily-Price-Index-([A-Za-z]+)-([0-9]{1,2})-([0-9]{4})",
                    filename,
                    re.IGNORECASE,
                )
                if m:
                    month_str, day_str, year_str = m.groups()
                    month_num = MONTH_MAP.get(month_str.lower())
                    if month_num:
                        try:
                            d = datetime.date(int(year_str), month_num, int(day_str))
                            date_to_url[d.strftime("%Y-%m-%d")] = href
                        except ValueError:
                            continue

            logger.info("Found %d official daily PDF bulletins in Department of Agriculture price monitoring portal.", len(date_to_url))

            # Save catalog to disk
            with open(CATALOG_CACHE_FILE, "w") as f:
                json.dump(date_to_url, f, indent=2)

            return date_to_url

    except Exception as err:
        logger.error("Error crawling DA catalog: %s", err)
        return {}


def is_valid_pdf(file_path: Path) -> bool:
    """Verifies that the file exists, is non-empty, and has a valid %PDF- magic header."""
    if not file_path.exists() or file_path.stat().st_size < 5000:
        return False
    try:
        with open(file_path, "rb") as f:
            header = f.read(5)
            return header.startswith(b"%PDF-")
    except Exception:
        return False


def download_single_pdf(
    date_str: str,
    pdf_url: str | None,
    client: httpx.Client,
    max_retries: int = 3,
    force: bool = False,
) -> tuple[str, str, int]:
    """
    Downloads a single bulletin PDF with retries.
    Returns (date_str, status, file_size).
    Statuses: 'ALREADY_CACHED', 'DOWNLOADED', 'NOT_FOUND_404', 'FAILED'
    """
    dest_path = RAW_DATA_DIR / f"{date_str}.pdf"

    if not force and is_valid_pdf(dest_path):
        return date_str, "ALREADY_CACHED", dest_path.stat().st_size

    # Fallback URL pattern if not in catalog
    if not pdf_url:
        date_obj = datetime.date.fromisoformat(date_str)
        month_name = date_obj.strftime("%B")
        day_num = date_obj.day
        year_num = date_obj.year
        pdf_url = (
            f"https://www.da.gov.ph/wp-content/uploads/{year_num}/{date_obj.month:02d}/"
            f"Daily-Price-Index-{month_name}-{day_num}-{year_num}.pdf"
        )

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.get(pdf_url)
            if resp.status_code == 404:
                return date_str, "NOT_FOUND_404", 0
            elif resp.status_code != 200:
                time.sleep(1.0 * attempt)
                continue

            content = resp.content
            # Verify magic bytes
            if not content.startswith(b"%PDF-") or len(content) < 5000:
                return date_str, "NOT_FOUND_404", 0

            RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(content)
            return date_str, "DOWNLOADED", len(content)

        except (httpx.ConnectError, httpx.TimeoutException):
            if attempt == max_retries:
                return date_str, "FAILED", 0
            time.sleep(2.0 * attempt)
        except Exception:
            return date_str, "FAILED", 0

    return date_str, "FAILED", 0


def run_parallel_downloader(
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    workers: int = 5,
    limit: int | None = None,
    force: bool = False,
    refresh_catalog: bool = False,
) -> dict[str, int]:
    """
    Coordinates multi-threaded downloading of Department of Agriculture price monitoring PDF bulletins.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(days=730)  # ~2 years

    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: PDF Downloader")
    logger.info("Target Range    : %s to %s", start_date, end_date)
    logger.info("Destination Dir : %s", RAW_DATA_DIR)
    logger.info("Parallel Workers: %d", workers)
    logger.info("==================================================")

    catalog = crawl_da_catalog(use_cache=not refresh_catalog)
    if not catalog:
        logger.error("No bulletins discovered. Check network connection.")
        return {"downloaded": 0, "cached": 0, "not_found": 0, "failed": 0}

    # Filter target dates within range (skipping Sundays)
    target_dates = []
    curr = start_date
    while curr <= end_date:
        if curr.weekday() != 6:  # Exclude Sundays
            target_dates.append(curr.strftime("%Y-%m-%d"))
        curr += datetime.timedelta(days=1)

    target_dates.sort(reverse=True)  # Most recent first

    # Check already downloaded files for instant resume
    pending_tasks = []
    already_cached_count = 0

    for d in target_dates:
        pdf_path = RAW_DATA_DIR / f"{d}.pdf"
        if not force and is_valid_pdf(pdf_path):
            already_cached_count += 1
        else:
            pending_tasks.append((d, catalog.get(d)))

    if limit is not None and limit > 0:
        logger.info("Limit flag set: Restricting pending downloads to %d files.", limit)
        pending_tasks = pending_tasks[:limit]

    total_target = len(pending_tasks) + already_cached_count
    logger.info(
        "Target Status: %d total dates | %d already cached | %d pending download.",
        total_target,
        already_cached_count,
        len(pending_tasks),
    )

    if not pending_tasks:
        logger.info("All target bulletins are already downloaded in %s! Nothing to fetch.", RAW_DATA_DIR)
        return {"downloaded": 0, "cached": already_cached_count, "not_found": 0, "failed": 0}

    downloaded_count = 0
    not_found_count = 0
    failed_count = 0
    total_bytes = 0

    lock = threading.Lock()
    completed_tasks = 0
    start_time = time.time()

    # Reusable HTTP client with connection pooling
    client = httpx.Client(
        timeout=25.0,
        follow_redirects=True,
        headers={"User-Agent": PROJECT_USER_AGENT},
        limits=httpx.Limits(max_connections=workers * 2, max_keepalive_connections=workers),
    )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    download_single_pdf,
                    date_str,
                    pdf_url,
                    client,
                    force=force,
                ): date_str
                for date_str, pdf_url in pending_tasks
            }

            for future in concurrent.futures.as_completed(futures):
                date_str, status, file_size = future.result()

                with lock:
                    completed_tasks += 1
                    pct = (completed_tasks / len(pending_tasks)) * 100

                    if status == "DOWNLOADED":
                        downloaded_count += 1
                        total_bytes += file_size
                        kb = file_size / 1024
                        logger.info(
                            "[%d/%d] (%.1f%%) DOWNLOADED: %s.pdf (%.0f KB)",
                            completed_tasks,
                            len(pending_tasks),
                            pct,
                            date_str,
                            kb,
                        )
                    elif status == "ALREADY_CACHED":
                        already_cached_count += 1
                        logger.info(
                            "[%d/%d] (%.1f%%) CACHED: %s.pdf",
                            completed_tasks,
                            len(pending_tasks),
                            pct,
                            date_str,
                        )
                    elif status == "NOT_FOUND_404":
                        not_found_count += 1
                        with open(MISSING_LOG_FILE, "a") as mf:
                            mf.write(f"{date_str}\n")
                        logger.info(
                            "[%d/%d] (%.1f%%) NOT FOUND (404/Holiday): %s",
                            completed_tasks,
                            len(pending_tasks),
                            pct,
                            date_str,
                        )
                    else:  # FAILED
                        failed_count += 1
                        logger.warning(
                            "[%d/%d] (%.1f%%) FAILED: %s",
                            completed_tasks,
                            len(pending_tasks),
                            pct,
                            date_str,
                        )

    except KeyboardInterrupt:
        logger.warning("\n[STOPPED] Download cancelled by user. All completed PDFs are safely saved on disk!")

    finally:
        client.close()

    elapsed = time.time() - start_time
    mb_downloaded = total_bytes / (1024 * 1024)

    logger.info("==================================================")
    logger.info("Download Summary:")
    logger.info("  Time Elapsed   : %.1f seconds", elapsed)
    logger.info("  New Downloads  : %d files (%.1f MB)", downloaded_count, mb_downloaded)
    logger.info("  Already Cached : %d files", already_cached_count)
    logger.info("  404/Holidays   : %d dates (logged in %s)", not_found_count, MISSING_LOG_FILE.name)
    logger.info("  Failed Retries : %d files", failed_count)
    logger.info("  Archive Folder : %s", RAW_DATA_DIR)
    logger.info("==================================================")

    return {
        "downloaded": downloaded_count,
        "cached": already_cached_count,
        "not_found": not_found_count,
        "failed": failed_count,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parallel Downloader for Official Department of Agriculture Price Monitoring PDF Bulletins (Downloads ONLY, no extraction)"
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=5,
        help="Number of concurrent download threads (default: 5)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Earliest date to download (YYYY-MM-DD). Defaults to 2 years ago.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Latest date to download (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="Limit number of pending downloads to process (useful for test runs)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if PDF already exists locally",
    )
    parser.add_argument(
        "--refresh-catalog",
        action="store_true",
        help="Re-crawl DA portal index instead of using cached catalog.json",
    )
    args = parser.parse_args()

    s_date = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    e_date = datetime.date.fromisoformat(args.end_date) if args.end_date else None

    run_parallel_downloader(
        start_date=s_date,
        end_date=e_date,
        workers=args.workers,
        limit=args.limit,
        force=args.force,
        refresh_catalog=args.refresh_catalog,
    )
