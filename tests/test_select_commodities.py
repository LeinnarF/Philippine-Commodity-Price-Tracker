"""
Unit tests for the selected commodities pipeline (pipeline.select_commodities).
"""

from pathlib import Path
import pandas as pd
import pytest

from pipeline.select_commodities import (
    select_commodities,
    get_commodity_definitions,
    DEFAULT_RAW_CSV,
    DEFAULT_SELECTED_CSV,
)


def test_commodity_definitions_structure():
    """Verify that commodity definitions cover the expected 7 categories and 48 items."""
    defs = get_commodity_definitions(egg_category="Basic Goods", include_corn=True)
    assert len(defs) == 48

    categories = {d["category"] for d in defs}
    expected_categories = {
        "Rice",
        "Fish",
        "Meat and Poultry",
        "Vegetables",
        "Spices and Condiments",
        "Fruits",
        "Basic Goods",
    }
    assert categories == expected_categories


def test_select_commodities_execution(tmp_path):
    """Verify that select_commodities runs on raw CSV and outputs a complete panel dataset."""
    if not DEFAULT_RAW_CSV.exists():
        pytest.skip("Raw Department of Agriculture price monitoring prices CSV not found")

    output_csv = tmp_path / "selected_sample.csv"
    selected_df = select_commodities(
        input_csv=DEFAULT_RAW_CSV,
        output_csv=output_csv,
        egg_category="Basic Goods",
        include_corn=True,
    )

    assert output_csv.exists()
    assert not selected_df.empty
    # Exactly 451 dates x 48 commodities = 21,648 rows
    assert len(selected_df) == 21648

    # Required columns (no min or max)
    expected_cols = [
        "date",
        "commodity_group",
        "commodity",
        "unit",
        "price_prevailing",
        "market_scope",
        "source",
        "is_synthetic",
        "raw_count",
        "raw_file",
        "source_url",
    ]
    assert list(selected_df.columns) == expected_cols
    assert "price_min" not in selected_df.columns
    assert "price_max" not in selected_df.columns

    # Dimension assertions
    assert selected_df["date"].nunique() == 451
    assert selected_df["commodity"].nunique() == 48
    assert selected_df["commodity_group"].nunique() == 7
    assert (selected_df["is_synthetic"] == False).all()

    # Deduplication assertion: exactly 1 row per (date, commodity)
    assert selected_df.duplicated(subset=["date", "commodity"]).sum() == 0

    # Unobserved values: sporadic gaps are forward-filled, leading gap for Pork Pigue (42 dates) is blank
    missing_count = selected_df["price_prevailing"].isnull().sum()
    assert missing_count == 42
    assert (selected_df[selected_df["commodity"] != "Pork Pigue (Hind Leg / Ham)"]["price_prevailing"].notnull()).all()
    assert (selected_df["price_prevailing"].dropna() > 0).all()

    # Specific commodity verifications
    egg_row = selected_df[selected_df["commodity"] == "Chicken Egg (Medium White)"].iloc[0]
    assert egg_row["commodity_group"] == "Basic Goods"
    assert egg_row["unit"] == "piece"

    # Verify anomaly correction: Chicken Egg on 2026-09-10 should be 8.13, not the bulletin typo 200.00
    egg_sept10 = selected_df[
        (selected_df["date"] == "2026-09-10")
        & (selected_df["commodity"] == "Chicken Egg (Medium White)")
    ]
    assert len(egg_sept10) == 1
    assert egg_sept10.iloc[0]["price_prevailing"] == 8.13

    corn_row = selected_df[selected_df["commodity"] == "Yellow Corn"].iloc[0]
    assert corn_row["commodity_group"] == "Vegetables"
