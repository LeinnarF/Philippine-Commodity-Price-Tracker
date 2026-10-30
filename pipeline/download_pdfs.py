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

# Keywords that indicate general price monitoring, non-food commodities (cigarettes), or non-daily bulletins.
# Daily Price Index bulletins must NOT match any of these.
DPI_EXCLUSION_KEYWORDS = (
    "price-monitoring",
    "price_monitoring",
    "pricemonitoring",
    "cigarette",
    "cigarettes",
    "tobacco",
    "cigar",
    "cigars",
    "nicotine",
    "vape",
    "smoking",
    "weekly",
    "fertilizer",
)


def is_valid_dpi_bulletin_url(url: str, anchor_text: str = "") -> bool:
    """
    Validates that a URL points to an official DA Daily Price Index (DPI) bulletin.
    Strictly excludes general 'Price Monitoring', cigarette/tobacco, fertilizer,
    or weekly report files.
    """
    if not url:
        return False

    url_lower = url.lower()
    anchor_lower = (anchor_text or "").lower()

    # Reject if any exclusion keyword is present in url or anchor text
    for kw in DPI_EXCLUSION_KEYWORDS:
        if kw in url_lower or kw in anchor_lower:
            return False

    # Extract filename
    filename = url_lower.split("/")[-1].split("?")[0]
    if not filename.endswith(".pdf"):
        return False

    # Must contain "daily-price-index" or "dpi" as a distinct token/part
    if "daily-price-index" in filename or "daily_price_index" in filename:
        return True

    # Check for dpi pattern: e.g. -dpi-, -dpi., _dpi_, _dpi., -dpi-afc
    if re.search(r"(^|[-_])dpi([-_.]|$)", filename):
        return True

    return False

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
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def extract_date_from_bulletin_url(url: str) -> datetime.date | None:
    """
    Extracts a date from a DA price bulletin URL or filename using regex pattern matching.
    Supports historical and new naming schemes used on the DA portal:
      - 'October-24-2026-DPI-AFC.pdf' (New convention introduced late Oct 2026)
      - 'Daily-Price-Index-October-23-2026.pdf' (Historical standard)
      - 'Revised-Daily-Price-Index-October-7-2026.pdf' (Revisions)
      - 'Daily-Price-Index-October-8-2026-1.pdf' (Numbered upload duplicates)
      - '24-October-2026-DPI.pdf' (Day-first variant)
      - '2026-10-24-DPI.pdf' or '2026_10_24.pdf' (ISO format)
    """
    filename = url.split("/")[-1].split("?")[0]
    clean_name = filename[:-4] if filename.lower().endswith(".pdf") else filename

    # Pattern 1: [Month Name]-[Day]-[Year] (e.g. October-24-2026-DPI-AFC or Daily-Price-Index-October-23-2026)
    m1 = re.search(r"([A-Za-z]+)[-_ ]+([0-9]{1,2})[-_ ]+([0-9]{4})", clean_name)
    if m1:
        m_str, d_str, y_str = m1.groups()
        m_num = MONTH_MAP.get(m_str.lower())
        if m_num:
            try:
                return datetime.date(int(y_str), m_num, int(d_str))
            except ValueError:
                pass

    # Pattern 2: [Day]-[Month Name]-[Year] (e.g. 24-October-2026-DPI)
    m2 = re.search(r"([0-9]{1,2})[-_ ]+([A-Za-z]+)[-_ ]+([0-9]{4})", clean_name)
    if m2:
        d_str, m_str, y_str = m2.groups()
        m_num = MONTH_MAP.get(m_str.lower())
        if m_num:
            try:
                return datetime.date(int(y_str), m_num, int(d_str))
            except ValueError:
                pass

    # Pattern 3: ISO numeric [Year]-[Month]-[Day] (e.g. 2026-10-24 or 2026_10_24)
    m3 = re.search(r"\b([0-9]{4})[-_]([0-9]{1,2})[-_]([0-9]{1,2})\b", clean_name)
    if m3:
        y_str, m_str, d_str = m3.groups()
        try:
            return datetime.date(int(y_str), int(m_str), int(d_str))
        except ValueError:
            pass

    return None


def generate_candidate_urls(target_date: datetime.date | str) -> list[str]:
    """
    Generates all known URL variations for a DA price bulletin on a given date.
    Covers new '*-DPI-AFC.pdf', classic 'Daily-Price-Index-*', shorthand '*-DPI.pdf',
    revised versions, and padded/unpadded day digits.
    """
    if isinstance(target_date, str):
        target_date = datetime.date.fromisoformat(target_date)

    d = target_date
    month_name = d.strftime("%B")
    month_short = d.strftime("%b")
    base = f"https://www.da.gov.ph/wp-content/uploads/{d.year}/{d.month:02d}"

    patterns = [
        # New DA naming convention (Oct 2026 onwards): Month-Day-Year-DPI-AFC.pdf
        f"{base}/{month_name}-{d.day}-{d.year}-DPI-AFC.pdf",
        f"{base}/{month_name}-{d.day:02d}-{d.year}-DPI-AFC.pdf",
        # Historical standard: Daily-Price-Index-Month-Day-Year.pdf
        f"{base}/Daily-Price-Index-{month_name}-{d.day}-{d.year}.pdf",
        f"{base}/Daily-Price-Index-{month_name}-{d.day:02d}-{d.year}.pdf",
        # Shorthand DPI variants
        f"{base}/{month_name}-{d.day}-{d.year}-DPI.pdf",
        f"{base}/{month_name}-{d.day:02d}-{d.year}-DPI.pdf",
        # Duplicates/revisions uploaded with '-1' suffix
        f"{base}/{month_name}-{d.day}-{d.year}-DPI-AFC-1.pdf",
        f"{base}/Daily-Price-Index-{month_name}-{d.day}-{d.year}-1.pdf",
        f"{base}/Revised-Daily-Price-Index-{month_name}-{d.day}-{d.year}.pdf",
        # Abbreviated month names
        f"{base}/{month_short}-{d.day}-{d.year}-DPI-AFC.pdf",
        f"{base}/Daily-Price-Index-{month_short}-{d.day}-{d.year}.pdf",
        # Lowercase variants
        f"{base}/{month_name.lower()}-{d.day}-{d.year}-dpi-afc.pdf",
        f"{base}/daily-price-index-{month_name.lower()}-{d.day}-{d.year}.pdf",
    ]

    # End-of-month bulletins are occasionally uploaded in the following month's folder
    if d.day >= 25:
        if d.month == 12:
            next_year, next_month = d.year + 1, 1
        else:
            next_year, next_month = d.year, d.month + 1
        base_next = f"https://www.da.gov.ph/wp-content/uploads/{next_year}/{next_month:02d}"

        patterns.extend([
            f"{base_next}/{month_name}-{d.day}-{d.year}-DPI-AFC.pdf",
            f"{base_next}/{month_name}-{d.day:02d}-{d.year}-DPI-AFC.pdf",
            f"{base_next}/Daily-Price-Index-{month_name}-{d.day}-{d.year}.pdf",
            f"{base_next}/Daily-Price-Index-{month_name}-{d.day:02d}-{d.year}.pdf",
            f"{base_next}/Revised-Daily-Price-Index-{month_name}-{d.day}-{d.year}.pdf",
            f"{base_next}/{month_name}-{d.day}-{d.year}-DPI.pdf",
        ])

    seen: set[str] = set()
    return [p for p in patterns if is_valid_dpi_bulletin_url(p) and not (p in seen or seen.add(p))]


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
                href = (a.get("href") or "").strip()
                if not href.lower().endswith(".pdf"):
                    continue

                anchor_text = a.get_text().strip()
                # Strict DPI filter: rejects Price-Monitoring, cigarettes, weekly bulletins, etc.
                if not is_valid_dpi_bulletin_url(href, anchor_text):
                    continue

                # Extract date from URL filename or anchor text
                d = extract_date_from_bulletin_url(href)
                if not d and anchor_text:
                    d = extract_date_from_bulletin_url(anchor_text)

                if d:
                    d_key = d.strftime("%Y-%m-%d")
                    # Do not overwrite unless current is missing or new link is an official revision
                    if d_key not in date_to_url:
                        date_to_url[d_key] = href
                    elif "revised" in href.lower() and "revised" not in date_to_url[d_key].lower():
                        logger.info("Replacing bulletin for %s with revised version: %s", d_key, href)
                        date_to_url[d_key] = href

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


def probe_dpi_url_for_date(
    date_str: str,
    client: httpx.Client,
    catalog_url: str | None = None,
) -> str | None:
    """
    Lightweight probe for a valid Daily Price Index bulletin on the DA server for a given date.
    Tries generate_candidate_urls() first via HEAD / Range GET requests, then falls back
    to catalog_url ONLY if it passes is_valid_dpi_bulletin_url().
    Returns the verified direct PDF URL if found, else None.
    """
    urls_to_try: list[str] = []

    try:
        d = datetime.date.fromisoformat(date_str)
        for cand in generate_candidate_urls(d):
            if cand not in urls_to_try and is_valid_dpi_bulletin_url(cand):
                urls_to_try.append(cand)
    except Exception:
        pass

    if catalog_url and is_valid_dpi_bulletin_url(catalog_url) and catalog_url not in urls_to_try:
        if "revised" in catalog_url.lower():
            urls_to_try.insert(0, catalog_url)
        else:
            urls_to_try.append(catalog_url)

    for url in urls_to_try:
        try:
            resp = client.head(url, timeout=10.0)
            if resp.status_code == 200:
                content_type = resp.headers.get("content-type", "").lower()
                content_length = int(resp.headers.get("content-length", 0))
                if ("pdf" in content_type or url.lower().endswith(".pdf")) and (content_length == 0 or content_length > 5000):
                    return url
            elif resp.status_code in (403, 405, 501):
                resp = client.get(url, headers={"Range": "bytes=0-1024"}, timeout=10.0)
                if resp.status_code in (200, 206) and resp.content.startswith(b"%PDF-"):
                    return url
        except Exception:
            continue

    return None


def download_single_pdf(
    date_str: str,
    pdf_url: str | None,
    client: httpx.Client,
    max_retries: int = 3,
    force: bool = False,
    dest_dir: Path | None = None,
) -> tuple[str, str, int]:
    """
    Downloads a single bulletin PDF with retries and fallback candidate URL patterns.
    Returns (date_str, status, file_size).
    Statuses: 'ALREADY_CACHED', 'DOWNLOADED', 'NOT_FOUND_404', 'FAILED'
    """
    target_dir = dest_dir or RAW_DATA_DIR
    dest_path = target_dir / f"{date_str}.pdf"

    if not force and is_valid_pdf(dest_path):
        return date_str, "ALREADY_CACHED", dest_path.stat().st_size

    # Build candidate URLs to try:
    # 1. Catalog / explicit URL (if provided, must be valid DPI; otherwise reject immediately)
    # 2. Known URL pattern variations for this date as fallback
    urls_to_try: list[str] = []
    if pdf_url is not None:
        if not is_valid_dpi_bulletin_url(pdf_url):
            logger.warning("Rejected non-DPI URL for date %s: %s", date_str, pdf_url)
            return date_str, "INVALID_URL", 0
        urls_to_try.append(pdf_url)

    try:
        d = datetime.date.fromisoformat(date_str)
        for cand in generate_candidate_urls(d):
            if cand not in urls_to_try and is_valid_dpi_bulletin_url(cand):
                urls_to_try.append(cand)
    except Exception:
        pass

    if not urls_to_try:
        return date_str, "FAILED", 0

    had_connection_error = False

    for current_url in urls_to_try:
        for attempt in range(1, max_retries + 1):
            try:
                resp = client.get(current_url)
                if resp.status_code == 404:
                    # 404 at this URL candidate, try next pattern
                    break
                elif resp.status_code != 200:
                    time.sleep(0.5 * attempt)
                    continue

                content = resp.content
                # Verify magic bytes
                if not content.startswith(b"%PDF-") or len(content) < 5000:
                    # Not a real PDF (e.g. 200 OK HTML error page)
                    break

                target_dir.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(content)
                return date_str, "DOWNLOADED", len(content)

            except (httpx.ConnectError, httpx.TimeoutException):
                had_connection_error = True
                if attempt == max_retries:
                    break
                time.sleep(1.0 * attempt)
            except Exception:
                break

    if had_connection_error:
        return date_str, "FAILED", 0

    return date_str, "NOT_FOUND_404", 0


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

    # Target dates within range (including Sundays)
    target_dates = []
    curr = start_date
    while curr <= end_date:
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
