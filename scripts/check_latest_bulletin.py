"""
Check Latest Bulletin Date for Philippine Commodity Price Tracker.

Discovers the date of the latest Department of Agriculture (DA AMAS)
Daily Price Index bulletin WITHOUT downloading the PDF file by default.
Silently excludes cigarette and tobacco items from the scan.

Options:
- --check-missing: Compares portal dates against data/cleaned_dataset.csv
  to identify uningested bulletins.
- --download-date YYYY-MM-DD: Downloads the specific bulletin PDF for that date.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path when executed directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from pipeline.download_pdfs import (
    CATALOG_CACHE_FILE,
    DA_PRICE_MONITORING_URL,
    PROJECT_USER_AGENT,
    RAW_DATA_DIR,
    download_single_pdf,
    extract_date_from_bulletin_url,
)

DEFAULT_CLEAN_CSV = PROJECT_ROOT / "data" / "cleaned_dataset.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("check_latest_bulletin")

# Terms identifying cigarette, tobacco, or nicotine products to exclude from scan
CIGARETTE_EXCLUSION_KEYWORDS = (
    "cigarette",
    "cigarettes",
    "tobacco",
    "cigar",
    "cigars",
    "nicotine",
    "vape",
    "smoking",
)


def is_cigarette_commodity(name_or_text: str) -> bool:
    """Returns True if the link or commodity refers to cigarette or tobacco items."""
    if not name_or_text:
        return False
    lower = name_or_text.lower()
    return any(keyword in lower for keyword in CIGARETTE_EXCLUSION_KEYWORDS)


def extract_dates_from_clean_csv(csv_path: Path = DEFAULT_CLEAN_CSV) -> set[str]:
    """
    Extracts all unique observation dates recorded in data/cleaned_dataset.csv.
    Returns an empty set if the CSV file does not exist.
    """
    if not csv_path.exists():
        logger.warning("Clean CSV dataset not found at %s", csv_path)
        return set()

    try:
        df = pd.read_csv(csv_path, usecols=["date"])
        if "date" in df.columns:
            return set(df["date"].dropna().astype(str).unique())
    except Exception as exc:
        logger.error("Error reading dates from clean CSV (%s): %s", csv_path, exc)

    return set()


def fetch_latest_bulletin_info(
    use_cache: bool = False,
    timeout: float = 30.0,
    check_csv_diff: bool = False,
    clean_csv_path: Path = DEFAULT_CLEAN_CSV,
) -> dict[str, Any] | None:
    """
    Checks the DA Price Monitoring portal (or cached catalog if offline/cached)
    for available price bulletins and determines the date of the latest bulletin.
    Does NOT download or store the PDF.

    Silently ignores and does not include any cigarette/tobacco entries in the scan.

    If check_csv_diff is True, extracts dates from clean_csv_path and identifies
    which portal dates are missing from the clean dataset.
    """
    date_to_url: dict[datetime.date, str] = {}
    source = "live_portal"

    headers = {"User-Agent": PROJECT_USER_AGENT}
    logger.info("Scanning DA price monitoring portal for latest bulletin...")

    portal_succeeded = False
    if not use_cache:
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
                resp = client.get(DA_PRICE_MONITORING_URL)
                if resp.status_code == 200:
                    portal_succeeded = True
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for a in soup.find_all("a"):
                        href = (a.get("href") or "").strip()
                        anchor_text = a.get_text().strip()

                        # Do not include cigarette/tobacco data in scan
                        if is_cigarette_commodity(href) or is_cigarette_commodity(anchor_text):
                            continue

                        if not href.lower().endswith(".pdf"):
                            continue

                        # Filter for price bulletins
                        if not any(k in href.lower() for k in ("price", "dpi", "index", "daily", "afc", "amas")):
                            continue

                        # Parse date
                        d = extract_date_from_bulletin_url(href)
                        if not d and anchor_text:
                            d = extract_date_from_bulletin_url(anchor_text)

                        if d:
                            date_to_url[d] = href
        except Exception as exc:
            logger.warning("Could not reach DA portal (%s). Falling back to local catalog cache...", exc)

    # Fallback to catalog cache if portal failed or use_cache=True
    if not portal_succeeded:
        source = "cached_catalog"
        if CATALOG_CACHE_FILE.exists():
            try:
                with open(CATALOG_CACHE_FILE, "r") as f:
                    cached_data = json.load(f)
                for date_str, url in cached_data.items():
                    # Do not include cigarette/tobacco data in scan
                    if is_cigarette_commodity(date_str) or is_cigarette_commodity(url):
                        continue
                    try:
                        d = datetime.date.fromisoformat(date_str)
                        date_to_url[d] = url
                    except ValueError:
                        continue
            except Exception as exc:
                logger.error("Failed to read local catalog cache: %s", exc)

    if not date_to_url:
        logger.error("No valid bulletins discovered.")
        return None

    # Identify latest bulletin date
    latest_date = max(date_to_url.keys())
    latest_url = date_to_url[latest_date]

    output: dict[str, Any] = {
        "latest_date": latest_date.isoformat(),
        "url": latest_url,
        "source": source,
        "total_bulletins_scanned": len(date_to_url),
        "all_bulletins": {d.isoformat(): url for d, url in date_to_url.items()},
    }

    # Optional clause: check portal dates against clean CSV
    if check_csv_diff:
        csv_dates = extract_dates_from_clean_csv(clean_csv_path)
        portal_date_strings = {d.isoformat() for d in date_to_url.keys()}
        missing_from_csv = sorted(portal_date_strings - csv_dates)

        output["clean_csv_dates_count"] = len(csv_dates)
        output["missing_from_csv_count"] = len(missing_from_csv)
        output["missing_from_csv_dates"] = missing_from_csv

    return output


def download_bulletin_for_date(
    target_date_str: str,
    known_url: str | None = None,
    dest_dir: Path = RAW_DATA_DIR,
    force: bool = False,
) -> tuple[str, str, int]:
    """
    Downloads the bulletin PDF for a specific date (YYYY-MM-DD).
    Uses connection pooling and fallback candidate URLs.
    """
    with httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        headers={"User-Agent": PROJECT_USER_AGENT},
    ) as client:
        return download_single_pdf(
            date_str=target_date_str,
            pdf_url=known_url,
            client=client,
            force=force,
            dest_dir=dest_dir,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve the latest DA price bulletin date without downloading the PDF file."
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached catalog.json directly instead of fetching online.",
    )
    parser.add_argument(
        "--check-missing",
        action="store_true",
        help="Compare portal dates against data/cleaned_dataset.csv and show dates present in portal but missing from CSV.",
    )
    parser.add_argument(
        "--clean-csv",
        type=Path,
        default=DEFAULT_CLEAN_CSV,
        help=f"Path to clean CSV file (default: {DEFAULT_CLEAN_CSV})",
    )
    parser.add_argument(
        "--download-date",
        type=str,
        default=None,
        help="Download the PDF bulletin for a specific date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--dest-dir",
        type=Path,
        default=RAW_DATA_DIR,
        help=f"Destination directory for downloaded PDF (default: {RAW_DATA_DIR})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if PDF already exists locally.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON.",
    )
    args = parser.parse_args()

    result = fetch_latest_bulletin_info(
        use_cache=args.use_cache,
        check_csv_diff=args.check_missing,
        clean_csv_path=args.clean_csv,
    )

    if not result:
        sys.exit(1)

    # Optional: download a specific date if requested
    download_result = None
    if args.download_date:
        target_d = args.download_date.strip()
        bulletins_map = result.get("all_bulletins", {})
        known_url = bulletins_map.get(target_d)
        logger.info("Downloading PDF for date: %s ...", target_d)
        d_str, status, size = download_bulletin_for_date(
            target_date_str=target_d,
            known_url=known_url,
            dest_dir=args.dest_dir,
            force=args.force,
        )
        download_result = {
            "date": d_str,
            "status": status,
            "file_size_bytes": size,
            "saved_to": str(args.dest_dir / f"{d_str}.pdf") if status in ("DOWNLOADED", "ALREADY_CACHED") else None,
        }
        result["download"] = download_result

    if args.json:
        # Don't pollute JSON with raw all_bulletins dict unless requested
        if "all_bulletins" in result and not args.download_date:
            del result["all_bulletins"]
        print(json.dumps(result, indent=2))
    else:
        print("==================================================")
        print("Philippine Commodity Price Tracker - Latest Bulletin")
        print(f"Latest Date : {result['latest_date']}")
        print(f"Bulletin URL: {result['url']}")
        print(f"Source      : {result['source']}")
        print(f"Scanned     : {result['total_bulletins_scanned']} bulletins")

        if args.check_missing:
            print("--------------------------------------------------")
            print("Clean CSV Comparison:")
            print(f"CSV Total Dates : {result['clean_csv_dates_count']}")
            print(f"Missing from CSV: {result['missing_from_csv_count']} bulletins")
            if result["missing_from_csv_dates"]:
                preview = result["missing_from_csv_dates"][:10]
                more = len(result["missing_from_csv_dates"]) - len(preview)
                print(f"Sample Missing  : {', '.join(preview)}{f' ... (+{more} more)' if more > 0 else ''}")

        if download_result:
            print("--------------------------------------------------")
            print(f"Download Result for {download_result['date']}:")
            print(f"Status    : {download_result['status']}")
            if download_result["saved_to"]:
                print(f"Saved to  : {download_result['saved_to']} ({download_result['file_size_bytes'] / 1024:.1f} KB)")
        print("==================================================")
