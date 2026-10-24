"""
Universal Department of Agriculture Price Monitoring PDF Bulletin Parser for Philippine Commodity Price Tracker.
Extracts daily retail commodity price records from official Department of Agriculture
price monitoring "Daily Price Index" PDF bulletins.
Handles structural layout variations across 2025 and 2026 bulletins.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber

logger = logging.getLogger("pipeline.pdf_parser")


def normalize_commodity_group(header_text: str) -> str:
    """Maps raw PDF table header strings to standardized commodity categories."""
    up = header_text.upper().strip()
    if "RICE" in up or "CORN" in up:
        return "Rice & Cereals"
    elif "FISH" in up:
        return "Fish"
    elif any(term in up for term in ["BEEF", "PORK", "POULTRY", "MEAT", "LIVESTOCK"]):
        return "Meat & Poultry"
    elif "LOWLAND" in up:
        return "Lowland Vegetables"
    elif "HIGHLAND" in up:
        return "Highland Vegetables"
    elif "VEGETABLE" in up:
        return "Vegetables"
    elif "SPICE" in up or "CONDIMENT" in up:
        return "Spices"
    elif "FRUIT" in up:
        return "Fruits"
    elif any(term in up for term in ["SUGAR", "OIL", "OTHER"]):
        return "Other Basic Goods"
    return "Other"


def infer_unit(commodity: str, spec: str) -> str:
    """Infers standard measurement unit from commodity name and specification."""
    c_lower = commodity.lower()
    s_lower = spec.lower()

    if "1 liter" in s_lower or "1l" in s_lower:
        return "bottle (1L)"
    elif "350 ml" in s_lower or "350ml" in s_lower:
        return "bottle (350ml)"
    elif "egg" in c_lower or "pc" in s_lower or "pcs" in s_lower or "piece" in s_lower:
        return "piece"
    elif "bundle" in s_lower:
        return "bundle"
    return "kg"


def parse_da_amas_pdf(
    pdf_path: str | Path,
    target_date: str,
    source_url: str | None = None,
) -> pd.DataFrame:
    """
    Parses an official Department of Agriculture price monitoring "Daily Price Index" PDF bulletin across all pages.
    Handles both 2025 (4-column with UNIT) and 2026 (3-column) layout variations.
    Includes commodities with valid numeric prices as well as rows with missing/unreported
    prices (e.g. 'n/a', '-', or blank).
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF bulletin not found at: {pdf_path}")

    records = []
    current_group = "Other"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for raw_row in table:
                    non_empty = [
                        c.replace("\n", " ").strip()
                        for c in raw_row
                        if c is not None and c.strip()
                    ]
                    if not non_empty:
                        continue

                    # Skip table header lines
                    full_line = " ".join(non_empty).upper()
                    if any(
                        h in full_line
                        for h in [
                            "COMMODITY",
                            "PREVAILING",
                            "PRICE PER UNIT",
                            "(P/UNIT)",
                            "RETAIL PRICE",
                        ]
                    ):
                        continue

                    # Group category header (single cell or header row without prices)
                    if len(non_empty) == 1:
                        current_group = normalize_commodity_group(non_empty[0])
                        continue

                    # Category header with letter prefix (e.g. ["A", "IMPORTED COMMERCIAL RICE"])
                    if len(non_empty) == 2 and len(non_empty[0]) <= 2 and not non_empty[0].isdigit():
                        current_group = normalize_commodity_group(non_empty[1])
                        continue

                    # If this is a 3-column table where the price cell (column 2) is completely blank
                    if len(raw_row) == 3 and (raw_row[2] is None or not raw_row[2].strip()):
                        commodity = raw_row[0].replace("\n", " ").strip()
                        spec = raw_row[1].replace("\n", " ").strip() or "Standard"
                        unit = infer_unit(commodity, spec)
                        records.append(
                            {
                                "date": target_date,
                                "commodity_group": current_group,
                                "commodity": commodity,
                                "variety": spec,
                                "market_scope": "NCR Wet Markets",
                                "unit": unit,
                                "price_prevailing": None,
                                "price_low": None,
                                "price_high": None,
                                "source": "da_amas",
                                "is_synthetic": False,
                                "source_url": source_url or "https://www.da.gov.ph/price-monitoring/",
                            }
                        )
                        continue

                    clean = list(non_empty)

                    # Strip leading line number in numbered tables (e.g. 2025 bulletins)
                    if len(clean) >= 3 and (clean[0].isdigit() or clean[0].lower() in ["n/a", "na", "-"]):
                        clean = clean[1:]

                    # Check if the last cell is a valid numeric price or a missing price marker
                    price_str = clean[-1].replace(",", "").strip()
                    price_val = None
                    try:
                        p = float(price_str)
                        if p > 0:
                            price_val = p
                    except ValueError:
                        pass

                    is_missing_price = price_str.lower() in [
                        "n/a",
                        "na",
                        "-",
                        "--",
                        "none",
                        "#div/0!",
                        "",
                    ]

                    # If price_val is None and not a recognized missing price marker, skip non-commodity row
                    if price_val is None and not is_missing_price:
                        continue

                    commodity = clean[0]
                    spec = "Standard"
                    unit = "kg"

                    if len(clean) >= 4:
                        # 2025 layout: [COMMODITY, SPECIFICATION, UNIT, PRICE]
                        spec = clean[1]
                        unit = clean[2]
                    elif len(clean) == 3:
                        # Could be [COMMODITY, UNIT, PRICE] or [COMMODITY, SPEC, PRICE]
                        if clean[1].lower() in ["kg", "pc", "piece", "bundle", "bottle"]:
                            unit = clean[1]
                        else:
                            spec = clean[1]
                            unit = infer_unit(commodity, spec)
                    elif len(clean) == 2:
                        # [COMMODITY, PRICE]
                        spec = "Standard"
                        unit = infer_unit(commodity, "")

                    records.append(
                        {
                            "date": target_date,
                            "commodity_group": current_group,
                            "commodity": commodity,
                            "variety": spec,
                            "market_scope": "NCR Wet Markets",
                            "unit": unit,
                            "price_prevailing": price_val,
                            "price_low": price_val,
                            "price_high": price_val,
                            "source": "da_amas",
                            "is_synthetic": False,
                            "source_url": source_url or "https://www.da.gov.ph/price-monitoring/",
                        }
                    )

    df = pd.DataFrame(records)
    logger.info(
        "Parsed %d commodity records from Department of Agriculture price monitoring PDF for date %s",
        len(df),
        target_date,
    )
    return df
