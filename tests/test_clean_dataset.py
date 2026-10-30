"""
Unit tests for the clean dataset pipeline (pipeline.clean_dataset).
"""

import hashlib
from pathlib import Path
import pandas as pd
import pytest

from pipeline.clean_dataset import (
    clean_dataset,
    DEFAULT_SELECTED_CSV,
    DEFAULT_CLEAN_CSV,
    CLEAN_COLUMNS,
    METADATA_COLUMNS,
)


def _file_hash(path: Path) -> str:
    """Helper to compute sha256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def test_clean_dataset_execution(tmp_path):
    """
    Verify that clean_dataset produces a 6-column dataset with zero metadata,
    exact panel shape, preserves unobserved gaps, and leaves the input CSV completely intact.
    """
    if DEFAULT_SELECTED_CSV.exists():
        input_csv = DEFAULT_SELECTED_CSV
    else:
        # Build synthetic test panel with metadata columns
        from pipeline.commodity_schema import get_commodity_definitions
        input_csv = tmp_path / "mock_selected.csv"
        records = []
        for d in ["2026-10-25", "2026-10-26"]:
            for item in get_commodity_definitions():
                price = None if item["commodity"] == "Pork Pigue (Hind Leg / Ham)" else 150.0
                records.append({
                    "date": d,
                    "commodity_group": item["category"],
                    "commodity": item["commodity"],
                    "unit": item["unit"],
                    "price_prevailing": price,
                    "market_scope": "NCR Wet Markets",
                    "source": "da_amas",
                    "is_synthetic": False,
                    "raw_count": 0 if price is None else 1,
                    "raw_file": f"{d}.pdf",
                    "source_url": f"https://example.com/{d}.pdf",
                })
        pd.DataFrame(records).to_csv(input_csv, index=False)

    # Record hash of input file before running
    input_hash_before = _file_hash(input_csv)

    temp_output_csv = tmp_path / "clean_sample.csv"
    clean_df = clean_dataset(
        input_csv=input_csv,
        output_csv=temp_output_csv,
    )

    # 1. Input file preservation check
    input_hash_after = _file_hash(input_csv)
    assert input_hash_before == input_hash_after, "Input selected CSV was modified!"

    # Also verify input still has metadata columns
    input_df = pd.read_csv(input_csv, nrows=5)
    for meta_col in METADATA_COLUMNS:
        assert meta_col in input_df.columns

    # 2. Output file existence and shape
    assert temp_output_csv.exists()
    assert not clean_df.empty
    n_dates = clean_df["date"].nunique()
    assert len(clean_df) == n_dates * 48

    # 3. Strictly 6 clean columns (no metadata)
    assert list(clean_df.columns) == CLEAN_COLUMNS
    for meta_col in METADATA_COLUMNS:
        assert meta_col not in clean_df.columns
    assert "price_min" not in clean_df.columns
    assert "price_max" not in clean_df.columns

    # 4. Panel dimensions
    assert clean_df["commodity"].nunique() == 48
    assert clean_df["commodity_group"].nunique() == 7

    # 5. Deduplication: strictly 1 row per (date, commodity)
    assert clean_df.duplicated(subset=["date", "commodity"]).sum() == 0

    # 6. Valid observed prices (> 0)
    observed = clean_df.loc[clean_df["price_prevailing"].notnull(), "price_prevailing"]
    assert len(observed) > 0
    assert (observed > 0).all()


def test_production_clean_csv_integrity():
    """
    Validate that data/cleaned_dataset.csv on disk meets all final requirements.
    """
    if not DEFAULT_CLEAN_CSV.exists():
        pytest.skip("Production clean Department of Agriculture price monitoring prices CSV not found")

    df = pd.read_csv(DEFAULT_CLEAN_CSV)
    assert list(df.columns) == CLEAN_COLUMNS
    n_dates = df["date"].nunique()
    assert n_dates >= 451
    assert len(df) == n_dates * 48
    assert df["commodity"].nunique() == 48
    assert df["commodity_group"].nunique() == 7
    pigue_nulls = df[df["commodity"] == "Pork Pigue (Hind Leg / Ham)"]["price_prevailing"].isnull().sum()
    assert pigue_nulls >= 42
    observed = df.loc[df["price_prevailing"].notnull(), "price_prevailing"]
    assert len(observed) > 0
    assert (observed > 0).all()
