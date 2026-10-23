"""
Unit tests for the DA AMAS PDF bulletin parser (pipeline.pdf_parser).
"""

from pathlib import Path
import pytest

from pipeline.pdf_parser import (
    infer_unit,
    normalize_commodity_group,
    parse_da_amas_pdf,
)


def test_normalize_commodity_group():
    """Verify header normalization maps raw strings to standard categories."""
    assert normalize_commodity_group("IMPORTED COMMERCIAL RICE") == "Rice & Cereals"
    assert normalize_commodity_group("FISH PRODUCTS") == "Fish"
    assert normalize_commodity_group("LIVESTOCK & POULTRY PRODUCTS") == "Meat & Poultry"
    assert normalize_commodity_group("LOWLAND VEGETABLES") == "Lowland Vegetables"
    assert normalize_commodity_group("HIGHLAND VEGETABLES") == "Highland Vegetables"
    assert normalize_commodity_group("SPICES") == "Spices"
    assert normalize_commodity_group("FRUITS") == "Fruits"
    assert normalize_commodity_group("OTHER BASIC COMMODITIES") == "Other Basic Goods"


def test_infer_unit():
    """Verify unit inference from commodity and specification strings."""
    assert infer_unit("Cooking Oil (Palm)", "1 Liter/bottle") == "bottle (1L)"
    assert infer_unit("Cooking Oil (Palm)", "350 ml/bottle") == "bottle (350ml)"
    assert infer_unit("Chicken Egg (White, Medium)", "Medium") == "piece"
    assert infer_unit("Pechay (Native)", "1 bundle") == "bundle"
    assert infer_unit("Pork Belly", "Local") == "kg"


def test_real_pdf_parser_on_fixture():
    """Verify DA AMAS PDF parser extracts genuine records from archived bulletin."""
    fixture_path = Path("data/raw/da_amas/2026-10-20.pdf")
    if not fixture_path.exists():
        pytest.skip("Local test fixture PDF not present")

    df = parse_da_amas_pdf(fixture_path, "2026-10-20")
    assert not df.empty
    assert len(df) > 50

    # Ensure all parsed rows are marked real (is_synthetic == False)
    assert (df["is_synthetic"] == False).all()
    assert (df["source"] == "da_amas").all()
    # Non-null prices must be positive
    assert (df["price_prevailing"].dropna() > 0).all()
    # Missing prices ('n/a', blank) are also retained
    assert df["price_prevailing"].isna().any()

    # Check key staple presence
    commodities = df["commodity"].str.lower().tolist()
    assert any("rice" in c for c in commodities)
    assert any("pork" in c or "chicken" in c or "beef" in c for c in commodities)
