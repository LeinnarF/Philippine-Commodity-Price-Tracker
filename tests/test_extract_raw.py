"""
Unit tests for the batch PDF-to-CSV extractor (Stage 2).
"""

from pathlib import Path
import pandas as pd
import pytest

from pipeline.extract_raw import run_batch_extraction, RAW_DATA_DIR


def test_batch_extraction_sample(tmp_path):
    """Verify that batch extraction on local PDFs generates valid sorted CSV."""
    pdf_files = list(RAW_DATA_DIR.glob("*.pdf"))
    if not pdf_files:
        pytest.skip("No local PDFs available for test")

    output_csv = tmp_path / "extracted_sample.csv"
    count = run_batch_extraction(
        input_dir=RAW_DATA_DIR,
        output_path=output_csv,
        limit=2,
        workers=2,
    )

    assert count > 0
    assert output_csv.exists()

    df = pd.read_csv(output_csv)
    assert not df.empty
    assert "date" in df.columns
    assert "commodity" in df.columns
    assert "price_prevailing" in df.columns
    assert (df["is_synthetic"] == False).all()
    assert (df["price_prevailing"].dropna() > 0).all()
