"""
Unit tests for the missing bulletins fetcher and direct ingestion script (scripts.fetch_missing_bulletins).
"""

import pandas as pd
import pytest

from pipeline.clean_dataset import CLEAN_COLUMNS
from scripts.fetch_missing_bulletins import (
    extract_date_panel_records,
    find_missing_bulletin_dates,
)


def test_find_missing_bulletin_dates(tmp_path):
    """Verify detection of missing dates between server CSV and clean CSV."""
    clean_csv = tmp_path / "cleaned_dataset.csv"
    server_csv = tmp_path / "server_daily_bulletins.csv"

    # Server has 3 dates
    server_df = pd.DataFrame({
        "date": ["2025-03-01", "2025-03-02", "2025-03-03"],
        "pdf_url": [
            "https://example.com/2025-03-01.pdf",
            "https://example.com/2025-03-02.pdf",
            "https://example.com/2025-03-03.pdf",
        ],
    })
    server_df.to_csv(server_csv, index=False)

    # Clean dataset only has 2025-03-01
    clean_df = pd.DataFrame({
        "date": ["2025-03-01"],
        "commodity_group": ["Rice"],
        "commodity": ["Premium Rice"],
        "unit": ["kg"],
        "price_prevailing": [50.0],
    })
    clean_df.to_csv(clean_csv, index=False)

    missing = find_missing_bulletin_dates(clean_csv=clean_csv, server_csv=server_csv)
    assert len(missing) == 2
    assert missing[0] == ("2025-03-02", "https://example.com/2025-03-02.pdf")
    assert missing[1] == ("2025-03-03", "https://example.com/2025-03-03.pdf")


def test_extract_date_panel_records_shape():
    """Verify that extract_date_panel_records produces strictly 48 rows matching expected commodities."""
    # Dummy raw records
    df_raw = pd.DataFrame([
        {
            "date": "2025-03-02",
            "commodity_group": "Rice & Cereals",
            "commodity": "Premium",
            "variety": "5% broken",
            "unit": "kg",
            "price_prevailing": 52.0,
        },
        {
            "date": "2025-03-02",
            "commodity_group": "Meat & Poultry",
            "commodity": "Whole Chicken",
            "variety": "Fully dressed",
            "unit": "kg",
            "price_prevailing": 180.0,
        },
    ])

    records = extract_date_panel_records(df_raw, "2025-03-02", "https://example.com/2025-03-02.pdf")
    assert len(records) == 48

    df_out = pd.DataFrame(records)
    assert df_out["commodity"].nunique() == 48
    assert df_out["commodity_group"].nunique() == 7

    # Check observed vs unobserved
    prem_rice = df_out[df_out["commodity"] == "Premium Rice"].iloc[0]
    assert prem_rice["price_prevailing"] == 52.0
    assert prem_rice["raw_count"] == 1

    chicken = df_out[df_out["commodity"] == "Whole Chicken"].iloc[0]
    assert chicken["price_prevailing"] == 180.0
    assert chicken["raw_count"] == 1

    unobserved = df_out[df_out["commodity"] == "White Potato"].iloc[0]
    assert pd.isna(unobserved["price_prevailing"])
    assert unobserved["raw_count"] == 0
