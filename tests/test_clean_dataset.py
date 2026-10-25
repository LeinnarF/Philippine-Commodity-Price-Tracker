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
    Verify that clean_dataset produces a 5-column dataset with zero metadata,
    exact panel shape (21,648 rows), exactly 42 blank prices for Pork Pigue,
    and leaves the input selected_da_amas_prices.csv completely intact.
    """
    if not DEFAULT_SELECTED_CSV.exists():
        pytest.skip("Selected Department of Agriculture price monitoring prices CSV not found")

    # Record hash of input file before running
    input_hash_before = _file_hash(DEFAULT_SELECTED_CSV)

    temp_output_csv = tmp_path / "clean_sample.csv"
    clean_df = clean_dataset(
        input_csv=DEFAULT_SELECTED_CSV,
        output_csv=temp_output_csv,
    )

    # 1. Input file preservation check
    input_hash_after = _file_hash(DEFAULT_SELECTED_CSV)
    assert input_hash_before == input_hash_after, "Input selected CSV was modified!"

    # Also verify input still has 11 columns
    input_df = pd.read_csv(DEFAULT_SELECTED_CSV, nrows=5)
    assert len(input_df.columns) == 11
    for meta_col in METADATA_COLUMNS:
        assert meta_col in input_df.columns

    # 2. Output file existence and shape
    assert temp_output_csv.exists()
    assert not clean_df.empty
    n_dates = clean_df["date"].nunique()
    assert n_dates >= 451
    assert len(clean_df) == n_dates * 48

    # 3. Strictly 5 presentation columns (no metadata)
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

    # 6. Unobserved price rules:
    # At least 42 blank prices for Pork Pigue leading gap (2025-03-01 to 2025-04-08)
    pigue_df = clean_df[clean_df["commodity"] == "Pork Pigue (Hind Leg / Ham)"]
    assert pigue_df["price_prevailing"].isnull().sum() >= 42

    # 7. Valid observed prices (> 0)
    observed = clean_df.loc[clean_df["price_prevailing"].notnull(), "price_prevailing"]
    assert len(observed) > 0
    assert (observed > 0).all()


def test_production_clean_csv_integrity():
    """
    Validate that data/clean_da_amas_prices.csv on disk meets all final requirements.
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
