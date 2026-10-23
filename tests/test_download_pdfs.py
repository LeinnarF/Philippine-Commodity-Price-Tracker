"""
Unit tests for parallel raw PDF downloader.
"""

import datetime
from pathlib import Path
import pytest

from pipeline.download_pdfs import (
    is_valid_pdf,
    download_single_pdf,
)


def test_is_valid_pdf(tmp_path):
    """Verify PDF magic bytes validation."""
    # 1. Valid PDF file
    valid_pdf = tmp_path / "valid.pdf"
    valid_pdf.write_bytes(b"%PDF-1.4\n" + b"0" * 6000)
    assert is_valid_pdf(valid_pdf) is True

    # 2. Invalid text file
    invalid_file = tmp_path / "invalid.pdf"
    invalid_file.write_bytes(b"<html>404 Not Found</html>" * 500)
    assert is_valid_pdf(invalid_file) is False

    # 3. Too small file
    small_file = tmp_path / "small.pdf"
    small_file.write_bytes(b"%PDF-1.4")
    assert is_valid_pdf(small_file) is False

    # 4. Non-existent file
    assert is_valid_pdf(tmp_path / "non_existent.pdf") is False


def test_downloader_skips_cached_without_network(tmp_path, monkeypatch):
    """Verify downloader immediately returns ALREADY_CACHED for existing valid PDFs without network."""
    # Create fake cached PDF in RAW_DATA_DIR
    import pipeline.download_pdfs as dl

    monkeypatch.setattr(dl, "RAW_DATA_DIR", tmp_path)

    test_date = "2026-10-15"
    fake_cached = tmp_path / f"{test_date}.pdf"
    fake_cached.write_bytes(b"%PDF-1.5\n" + b"1" * 8000)

    # Call download_single_pdf (client should not be touched)
    import httpx
    client = httpx.Client()
    try:
        date_str, status, size = download_single_pdf(
            date_str=test_date,
            pdf_url="https://fake-url.com/fake.pdf",
            client=client,
            force=False,
        )
        assert date_str == test_date
        assert status == "ALREADY_CACHED"
        assert size > 5000
    finally:
        client.close()
