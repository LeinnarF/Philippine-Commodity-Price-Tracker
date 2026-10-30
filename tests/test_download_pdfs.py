"""
Unit tests for parallel raw PDF downloader.
"""

import datetime
from pathlib import Path
import httpx
import pytest

from pipeline.download_pdfs import (
    is_valid_pdf,
    is_valid_dpi_bulletin_url,
    download_single_pdf,
    extract_date_from_bulletin_url,
    generate_candidate_urls,
    crawl_da_catalog,
    probe_dpi_url_for_date,
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


def test_extract_date_from_bulletin_url():
    """Verify regex pattern matching on multiple DA bulletin filename conventions."""
    # 1. Historical standard format
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/Daily-Price-Index-October-23-2026.pdf"
    ) == datetime.date(2026, 10, 23)

    # 2. New DA format introduced late October 2026
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/October-24-2026-DPI-AFC.pdf"
    ) == datetime.date(2026, 10, 24)

    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/October-25-2026-DPI-AFC.pdf"
    ) == datetime.date(2026, 10, 25)

    # 3. Revision prefix
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/Revised-Daily-Price-Index-October-7-2026.pdf"
    ) == datetime.date(2026, 10, 7)

    # 4. Duplicate upload suffix '-1'
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/Daily-Price-Index-October-8-2026-1.pdf"
    ) == datetime.date(2026, 10, 8)

    # 5. Day-first variant
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/24-October-2026-DPI.pdf"
    ) == datetime.date(2026, 10, 24)

    # 6. ISO numeric date variant
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/2026-10-24-DPI.pdf"
    ) == datetime.date(2026, 10, 24)

    # 7. Non-bulletin PDF should return None
    assert extract_date_from_bulletin_url(
        "https://www.da.gov.ph/wp-content/uploads/2026/10/annual_guidelines.pdf"
    ) is None


def test_generate_candidate_urls():
    """Verify that candidate generator produces both new DPI-AFC and legacy Daily-Price-Index URLs."""
    target_d = datetime.date(2026, 10, 24)
    candidates = generate_candidate_urls(target_d)

    assert any("October-24-2026-DPI-AFC.pdf" in url for url in candidates)
    assert any("Daily-Price-Index-October-24-2026.pdf" in url for url in candidates)
    assert any("October-24-2026-DPI.pdf" in url for url in candidates)
    # Ensure no duplicates
    assert len(candidates) == len(set(candidates))


def test_crawl_da_catalog_discovers_both_formats(monkeypatch, tmp_path):
    """Verify that crawl_da_catalog discovers both historical and new DPI-AFC naming formats."""
    import pipeline.download_pdfs as dl

    fake_html = """
    <html>
      <body>
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/Daily-Price-Index-October-23-2026.pdf">Oct 23</a>
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/October-24-2026-DPI-AFC.pdf">Oct 24</a>
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/October-25-2026-DPI-AFC.pdf">Oct 25</a>
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/random_irrelevant.pdf">Random PDF</a>
      </body>
    </html>
    """

    class FakeResponse:
        status_code = 200
        text = fake_html

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def get(self, url):
            return FakeResponse()

    monkeypatch.setattr(dl.httpx, "Client", FakeClient)
    monkeypatch.setattr(dl, "RAW_DATA_DIR", tmp_path)
    monkeypatch.setattr(dl, "CATALOG_CACHE_FILE", tmp_path / "catalog.json")

    catalog = crawl_da_catalog(use_cache=False)
    assert "2026-10-23" in catalog
    assert "2026-10-24" in catalog
    assert "2026-10-25" in catalog
    assert catalog["2026-10-24"] == "https://www.da.gov.ph/wp-content/uploads/2026/10/October-24-2026-DPI-AFC.pdf"
    assert "random_irrelevant.pdf" not in str(catalog.values())


def test_download_single_pdf_candidate_fallback(tmp_path):
    """Verify download_single_pdf falls back to candidate URLs when primary/old pattern 404s."""
    target_date = "2026-10-24"
    valid_pdf_content = b"%PDF-1.5\n" + b"X" * 6000

    class MockTransport(httpx.BaseTransport):
        def handle_request(self, request):
            url_str = str(request.url)
            # Old name returns 404
            if "Daily-Price-Index" in url_str:
                return httpx.Response(404, request=request)
            # New DPI-AFC format returns 200 with valid PDF
            elif "October-24-2026-DPI-AFC.pdf" in url_str:
                return httpx.Response(200, content=valid_pdf_content, request=request)
            return httpx.Response(404, request=request)

    client = httpx.Client(transport=MockTransport())
    try:
        # Pass pdf_url=None to trigger candidate search
        date_res, status_res, size_res = download_single_pdf(
            date_str=target_date,
            pdf_url=None,
            client=client,
            dest_dir=tmp_path,
        )
        assert date_res == target_date
        assert status_res == "DOWNLOADED"
        assert size_res == len(valid_pdf_content)
        assert (tmp_path / f"{target_date}.pdf").exists()
    finally:
        client.close()


def test_is_valid_dpi_bulletin_url():
    """Verify strict Daily Price Index URL and exclusion rules."""
    # Valid DPI patterns
    assert is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Daily-Price-Index-October-23-2026.pdf")
    assert is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/October-24-2026-DPI-AFC.pdf")
    assert is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/October-27-2026-DPI-AFC.pdf")
    assert is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Revised-Daily-Price-Index-October-7-2026.pdf")
    assert is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/24-October-2026-DPI.pdf")
    assert is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/2026-10-24-DPI.pdf")

    # Rejection of non-DPI monitoring tables (e.g. general Price-Monitoring tables)
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Price-Monitoring-October-28-2026.pdf")
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Price-Monitoring-October-29-2026.pdf")
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Price-Monitoring-October-30-2026.pdf")

    # Rejection of cigarette / tobacco / non-commodity bulletins
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Cigarette-Price-Monitoring-October-2026.pdf")
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/Tobacco-Monitoring-2026.pdf")
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/weekly-price-index.pdf")

    # Rejection of non-pdf or non-DPI generic files
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/wp-content/uploads/2026/10/annual_guidelines.pdf")
    assert not is_valid_dpi_bulletin_url("https://www.da.gov.ph/price-monitoring/")
    assert not is_valid_dpi_bulletin_url("")


def test_crawl_da_catalog_filters_price_monitoring(monkeypatch, tmp_path):
    """Verify crawler ignores Price-Monitoring files and prioritizes revised DPI files."""
    import pipeline.download_pdfs as dl

    html_content = """
    <html>
      <body>
        <!-- Legitimate DPI bulletin -->
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/October-28-2026-DPI-AFC.pdf">Oct 28 DPI</a>
        <!-- General price monitoring table (must be ignored) -->
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/Price-Monitoring-October-28-2026.pdf">Oct 28 General Monitoring</a>
        <!-- Revised DPI bulletin -->
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/Revised-Daily-Price-Index-October-29-2026.pdf">Oct 29 Revised DPI</a>
        <!-- Standard DPI bulletin for same date -->
        <a href="https://www.da.gov.ph/wp-content/uploads/2026/10/Daily-Price-Index-October-29-2026.pdf">Oct 29 DPI Standard</a>
      </body>
    </html>
    """

    class FakeResponse:
        status_code = 200
        text = html_content

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def get(self, url):
            return FakeResponse()

    monkeypatch.setattr(dl.httpx, "Client", FakeClient)
    monkeypatch.setattr(dl, "RAW_DATA_DIR", tmp_path)
    monkeypatch.setattr(dl, "CATALOG_CACHE_FILE", tmp_path / "catalog.json")

    catalog = crawl_da_catalog(use_cache=False)

    # 2026-10-28 should point to DPI-AFC, NOT Price-Monitoring
    assert catalog.get("2026-10-28") == "https://www.da.gov.ph/wp-content/uploads/2026/10/October-28-2026-DPI-AFC.pdf"
    assert "Price-Monitoring" not in str(catalog.values())

    # 2026-10-29 should retain the Revised DPI URL
    assert "Revised" in catalog.get("2026-10-29", "")


def test_download_single_pdf_rejects_invalid_dpi_url(tmp_path):
    """Verify download_single_pdf rejects non-DPI URLs before making network requests."""
    client = httpx.Client()
    try:
        date_res, status_res, size_res = download_single_pdf(
            date_str="2026-10-28",
            pdf_url="https://www.da.gov.ph/wp-content/uploads/2026/10/Price-Monitoring-October-28-2026.pdf",
            client=client,
            dest_dir=tmp_path,
        )
        assert status_res == "INVALID_URL"
        assert not (tmp_path / "2026-10-28.pdf").exists()
    finally:
        client.close()


def test_probe_dpi_url_for_date():
    """Verify probe_dpi_url_for_date finds valid candidate URLs with mocked transport."""
    class MockProbeTransport(httpx.BaseTransport):
        def handle_request(self, request):
            url = str(request.url)
            if "October-27-2026-DPI-AFC.pdf" in url:
                return httpx.Response(
                    200,
                    headers={"Content-Type": "application/pdf", "Content-Length": "60000"},
                    request=request,
                )
            return httpx.Response(404, request=request)

    client = httpx.Client(transport=MockProbeTransport())
    try:
        found_url = probe_dpi_url_for_date("2026-10-27", client=client)
        assert found_url is not None
        assert "October-27-2026-DPI-AFC.pdf" in found_url

        # Non-existent date
        nf_url = probe_dpi_url_for_date("2026-10-31", client=client)
        assert nf_url is None
    finally:
        client.close()
