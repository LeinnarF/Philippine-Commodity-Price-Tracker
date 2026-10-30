"""
Unit tests for the daily incremental update pipeline (pipeline.daily_update).
"""

import datetime
from pathlib import Path
import pandas as pd
import pytest

from pipeline.daily_update import (
    append_execution_log,
    get_latest_tracked_date,
    run_daily_update,
)
from pipeline.clean_dataset import DEFAULT_CLEAN_CSV
from pipeline.load_duckdb import DEFAULT_LOGS_CSV


def test_get_latest_tracked_date():
    """Verify that get_latest_tracked_date correctly identifies the newest observation date."""
    if not DEFAULT_CLEAN_CSV.exists():
        pytest.skip("cleaned_dataset.csv not found")

    latest_date = get_latest_tracked_date(DEFAULT_CLEAN_CSV)
    assert isinstance(latest_date, datetime.date)
    assert latest_date >= datetime.date(2026, 10, 20)
    assert latest_date <= datetime.date.today()


def test_append_execution_log_lifecycle(tmp_path):
    """Verify execution log file creation, row appending, and duplicate run_id updates."""
    log_csv = tmp_path / "test_pipeline_execution_logs.csv"

    # 1. Create first log entry
    append_execution_log(
        logs_csv_path=log_csv,
        run_id="run-001",
        executed_at="2026-10-25 10:00:00",
        trigger="github_actions_cron",
        status="NO_NEW_DATA",
        latest_bulletin_date="2026-10-20",
        bulletins_checked=5,
        bulletins_downloaded=0,
        records_extracted=0,
        duration_seconds=1.23,
        message="No new bulletin published",
    )

    assert log_csv.exists()
    df = pd.read_csv(log_csv)
    assert len(df) == 1
    assert df.iloc[0]["run_id"] == "run-001"
    assert df.iloc[0]["status"] == "NO_NEW_DATA"
    assert df.iloc[0]["bulletins_checked"] == 5

    # 2. Append second log entry
    append_execution_log(
        logs_csv_path=log_csv,
        run_id="run-002",
        executed_at="2026-10-26 10:00:00",
        trigger="github_actions_cron",
        status="SUCCESS",
        latest_bulletin_date="2026-10-26",
        bulletins_checked=1,
        bulletins_downloaded=1,
        records_extracted=62,
        duration_seconds=5.45,
        message="Ingested 1 new bulletin",
    )

    df2 = pd.read_csv(log_csv)
    assert len(df2) == 2
    assert df2.iloc[1]["run_id"] == "run-002"
    assert df2.iloc[1]["status"] == "SUCCESS"

    # 3. Update existing run_id (idempotency check)
    append_execution_log(
        logs_csv_path=log_csv,
        run_id="run-002",
        executed_at="2026-10-26 10:05:00",
        trigger="workflow_dispatch",
        status="SUCCESS",
        latest_bulletin_date="2026-10-26",
        bulletins_checked=1,
        bulletins_downloaded=1,
        records_extracted=62,
        duration_seconds=4.50,
        message="Re-ran successfully",
    )

    df3 = pd.read_csv(log_csv)
    assert len(df3) == 2  # Still 2 rows, not 3
    assert df3.iloc[1]["run_id"] == "run-002"
    assert df3.iloc[1]["trigger"] == "workflow_dispatch"


def test_run_daily_update_already_up_to_date(tmp_path):
    """Verify that when target_date <= latest_tracked_date, pipeline exits cleanly with NO_NEW_DATA."""
    clean_csv = tmp_path / "clean.csv"
    logs_csv = tmp_path / "logs.csv"

    # Create dummy clean CSV with max date 2026-10-20
    df = pd.DataFrame([
        {"date": "2026-10-19", "commodity_group": "Rice", "commodity": "Regular Milled Rice", "unit": "kg", "price_prevailing": 45.0},
        {"date": "2026-10-20", "commodity_group": "Rice", "commodity": "Regular Milled Rice", "unit": "kg", "price_prevailing": 45.0},
    ])
    df.to_csv(clean_csv, index=False)

    res = run_daily_update(
        clean_csv_path=clean_csv,
        logs_csv_path=logs_csv,
        target_date=datetime.date(2026, 10, 20),
        rebuild=False,
    )

    assert res["status"] == "NO_NEW_DATA"
    assert res["bulletins_downloaded"] == 0
    assert logs_csv.exists()
    logs_df = pd.read_csv(logs_csv)
    assert len(logs_df) == 1
    assert logs_df.iloc[0]["status"] == "NO_NEW_DATA"


def test_run_daily_update_successful_incremental_ingestion(monkeypatch, tmp_path):
    """Verify in-memory ingestion, panel standardization, bulletin registry, and log recording."""
    from pipeline.commodity_schema import get_commodity_definitions

    clean_csv = tmp_path / "cleaned_dataset.csv"
    logs_csv = tmp_path / "pipeline_execution_logs.csv"
    bulletins_csv = tmp_path / "server_daily_bulletins.csv"

    defs = get_commodity_definitions()

    # Pre-populate day 1 (2026-10-25) with all 48 commodities
    day1_records = []
    for item in defs:
        day1_records.append({
            "date": "2026-10-25",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 100.0,
            "source_url": "https://example.com/2026-10-25.pdf",
        })
    pd.DataFrame(day1_records).to_csv(clean_csv, index=False)

    # Pre-populate server bulletins
    pd.DataFrame([{"date": "2026-10-25", "pdf_url": "https://example.com/2026-10-25.pdf"}]).to_csv(
        bulletins_csv, index=False
    )

    # Mock catalog crawler & in-memory fetch
    monkeypatch.setattr(
        "pipeline.daily_update.crawl_da_catalog",
        lambda use_cache=False: {"2026-10-26": "https://example.com/2026-10-26.pdf"},
    )
    monkeypatch.setattr(
        "pipeline.daily_update.fetch_bulletin_bytes_in_memory",
        lambda date_str, catalog_url, client, **kwargs: (b"%PDF-1.4 mock", "https://example.com/2026-10-26.pdf"),
    )

    # Mock raw PDF parser
    monkeypatch.setattr(
        "pipeline.daily_update.parse_da_amas_pdf",
        lambda pdf_source, target_date, source_url: pd.DataFrame([{"dummy": 1}]),
    )

    # Mock standardize_bulletin_records to return 48 standardized panel items
    mock_std_records = []
    for item in defs:
        mock_std_records.append({
            "date": "2026-10-26",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 120.0,
            "source_url": "https://example.com/2026-10-26.pdf",
        })

    monkeypatch.setattr(
        "pipeline.daily_update.standardize_bulletin_records",
        lambda df_raw, target_date, source_url: mock_std_records,
    )

    # Run updater targeting 2026-10-26
    res = run_daily_update(
        clean_csv_path=clean_csv,
        logs_csv_path=logs_csv,
        bulletins_csv_path=bulletins_csv,
        target_date=datetime.date(2026, 10, 26),
        rebuild=False,
    )

    assert res["status"] == "SUCCESS"
    assert res["bulletins_downloaded"] == 1
    assert res["records_extracted"] == 48

    # Verify updated clean dataset
    updated_df = pd.read_csv(clean_csv)
    assert len(updated_df) == 96
    assert updated_df["date"].nunique() == 2
    assert (updated_df[updated_df["date"] == "2026-10-26"]["price_prevailing"] == 120.0).all()

    # Verify server bulletins registry updated
    updated_bulletins = pd.read_csv(bulletins_csv)
    assert len(updated_bulletins) == 2
    assert "2026-10-26" in updated_bulletins["date"].values

    # Verify log entry
    logs_df = pd.read_csv(logs_csv)
    assert len(logs_df) == 1
    assert logs_df.iloc[0]["status"] == "SUCCESS"
    assert logs_df.iloc[0]["bulletins_downloaded"] == 1
    assert logs_df.iloc[0]["records_extracted"] == 48


def test_run_daily_update_rejects_low_observed_commodities(monkeypatch, tmp_path):
    """Verify that Gate 3 rejects bulletins where observed panel commodity count is below threshold."""
    from pipeline.commodity_schema import get_commodity_definitions

    clean_csv = tmp_path / "cleaned_dataset.csv"
    logs_csv = tmp_path / "pipeline_execution_logs.csv"
    bulletins_csv = tmp_path / "server_daily_bulletins.csv"

    defs = get_commodity_definitions()

    # Pre-populate day 1 (2026-10-25) with all 48 commodities
    day1_records = [
        {
            "date": "2026-10-25",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 100.0,
            "source_url": "https://example.com/2026-10-25.pdf",
        }
        for item in defs
    ]
    pd.DataFrame(day1_records).to_csv(clean_csv, index=False)
    pd.DataFrame([{"date": "2026-10-25", "pdf_url": "https://example.com/2026-10-25.pdf"}]).to_csv(
        bulletins_csv, index=False
    )

    # Mock catalog & fetch
    monkeypatch.setattr(
        "pipeline.daily_update.crawl_da_catalog",
        lambda use_cache=False: {"2026-10-26": "https://example.com/2026-10-26.pdf"},
    )
    monkeypatch.setattr(
        "pipeline.daily_update.fetch_bulletin_bytes_in_memory",
        lambda date_str, catalog_url, client, **kwargs: (b"%PDF-1.4 mock", "https://example.com/2026-10-26.pdf"),
    )
    monkeypatch.setattr(
        "pipeline.daily_update.parse_da_amas_pdf",
        lambda pdf_source, target_date, source_url: pd.DataFrame([{"commodity": "Some Item"}]),
    )

    # Return only 5 observed items (below MIN_OBSERVED_PANEL_COMMODITIES = 30)
    low_obs_records = []
    for i, item in enumerate(defs):
        low_obs_records.append({
            "date": "2026-10-26",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 100.0 if i < 5 else None,  # only 5 observed
            "source_url": "https://example.com/2026-10-26.pdf",
        })

    monkeypatch.setattr(
        "pipeline.daily_update.standardize_bulletin_records",
        lambda df_raw, target_date, source_url: low_obs_records,
    )

    res = run_daily_update(
        clean_csv_path=clean_csv,
        logs_csv_path=logs_csv,
        bulletins_csv_path=bulletins_csv,
        target_date=datetime.date(2026, 10, 26),
        rebuild=False,
    )

    # Must be marked FAILED
    assert res["status"] == "FAILED"
    assert res["records_extracted"] == 0

    # Dataset must not have been corrupted / modified
    clean_df = pd.read_csv(clean_csv)
    assert len(clean_df) == 48
    assert clean_df["date"].nunique() == 1

    # Server bulletins must not record the invalid bulletin
    bulletins_df = pd.read_csv(bulletins_csv)
    assert len(bulletins_df) == 1
    assert "2026-10-26" not in bulletins_df["date"].values

    # Logs must document the rejection
    log_df = pd.read_csv(logs_csv)
    assert len(log_df) == 1
    assert log_df.iloc[0]["status"] == "FAILED"
    assert "content and coverage gate" in log_df.iloc[0]["message"]


def test_run_daily_update_rejects_prohibited_content(monkeypatch, tmp_path):
    """Verify that Gate 3 rejects bulletins containing prohibited non-food commodities (e.g. tobacco)."""
    from pipeline.commodity_schema import get_commodity_definitions

    clean_csv = tmp_path / "cleaned_dataset.csv"
    logs_csv = tmp_path / "pipeline_execution_logs.csv"
    bulletins_csv = tmp_path / "server_daily_bulletins.csv"

    defs = get_commodity_definitions()

    # Pre-populate day 1
    day1_records = [
        {
            "date": "2026-10-25",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 100.0,
            "source_url": "https://example.com/2026-10-25.pdf",
        }
        for item in defs
    ]
    pd.DataFrame(day1_records).to_csv(clean_csv, index=False)
    pd.DataFrame([{"date": "2026-10-25", "pdf_url": "https://example.com/2026-10-25.pdf"}]).to_csv(
        bulletins_csv, index=False
    )

    # Mock catalog & fetch
    monkeypatch.setattr(
        "pipeline.daily_update.crawl_da_catalog",
        lambda use_cache=False: {"2026-10-26": "https://example.com/2026-10-26.pdf"},
    )
    monkeypatch.setattr(
        "pipeline.daily_update.fetch_bulletin_bytes_in_memory",
        lambda date_str, catalog_url, client, **kwargs: (b"%PDF-1.4 mock", "https://example.com/2026-10-26.pdf"),
    )
    # Parser returned prohibited commodity
    monkeypatch.setattr(
        "pipeline.daily_update.parse_da_amas_pdf",
        lambda pdf_source, target_date, source_url: pd.DataFrame([
            {"commodity": "Marlboro Red Cigarettes"}
        ]),
    )

    all_obs_records = [
        {
            "date": "2026-10-26",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 100.0,
            "source_url": "https://example.com/2026-10-26.pdf",
        }
        for item in defs
    ]
    monkeypatch.setattr(
        "pipeline.daily_update.standardize_bulletin_records",
        lambda df_raw, target_date, source_url: all_obs_records,
    )

    res = run_daily_update(
        clean_csv_path=clean_csv,
        logs_csv_path=logs_csv,
        bulletins_csv_path=bulletins_csv,
        target_date=datetime.date(2026, 10, 26),
        rebuild=False,
    )

    assert res["status"] == "FAILED"
    clean_df = pd.read_csv(clean_csv)
    assert len(clean_df) == 48


def test_run_daily_update_uses_known_server_bulletins_url(monkeypatch, tmp_path):
    """Verify that run_daily_update utilizes verified URLs registered in server_daily_bulletins.csv."""
    from pipeline.commodity_schema import get_commodity_definitions

    clean_csv = tmp_path / "cleaned_dataset.csv"
    logs_csv = tmp_path / "pipeline_execution_logs.csv"
    bulletins_csv = tmp_path / "server_daily_bulletins.csv"

    defs = get_commodity_definitions()

    # Pre-populate day 1 in clean CSV
    day1_records = [
        {
            "date": "2026-10-25",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 100.0,
            "source_url": "https://www.da.gov.ph/wp-content/uploads/2026/10/October-25-2026-DPI-AFC.pdf",
        }
        for item in defs
    ]
    pd.DataFrame(day1_records).to_csv(clean_csv, index=False)

    # In server_daily_bulletins.csv, date 2026-10-26 is already pre-recorded with a verified DPI URL
    known_target_url = "https://www.da.gov.ph/wp-content/uploads/2026/10/October-26-2026-DPI-AFC.pdf"
    pd.DataFrame([
        {"date": "2026-10-25", "pdf_url": "https://www.da.gov.ph/wp-content/uploads/2026/10/October-25-2026-DPI-AFC.pdf"},
        {"date": "2026-10-26", "pdf_url": known_target_url},
    ]).to_csv(bulletins_csv, index=False)

    received_known_url = None

    def mock_fetch(date_str, catalog_url, client, known_url=None, **kwargs):
        nonlocal received_known_url
        received_known_url = known_url
        return b"%PDF-1.4 mock", known_url

    monkeypatch.setattr("pipeline.daily_update.crawl_da_catalog", lambda use_cache=False: {})
    monkeypatch.setattr("pipeline.daily_update.fetch_bulletin_bytes_in_memory", mock_fetch)
    monkeypatch.setattr(
        "pipeline.daily_update.parse_da_amas_pdf",
        lambda pdf_source, target_date, source_url: pd.DataFrame([{"commodity": "Rice"}]),
    )

    day2_records = [
        {
            "date": "2026-10-26",
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": 110.0,
            "source_url": known_target_url,
        }
        for item in defs
    ]
    monkeypatch.setattr(
        "pipeline.daily_update.standardize_bulletin_records",
        lambda df_raw, target_date, source_url: day2_records,
    )

    rebuild_calls = []
    monkeypatch.setattr(
        "pipeline.daily_update._rebuild_warehouse_and_dashboard",
        lambda db_path, clean_csv_path, logs_csv_path: rebuild_calls.append((db_path, clean_csv_path, logs_csv_path)),
    )

    res = run_daily_update(
        clean_csv_path=clean_csv,
        logs_csv_path=logs_csv,
        bulletins_csv_path=bulletins_csv,
        target_date=datetime.date(2026, 10, 26),
        rebuild=True,
    )

    assert res["status"] == "SUCCESS"
    assert len(rebuild_calls) == 1
    assert received_known_url == known_target_url
    clean_df = pd.read_csv(clean_csv)
    assert len(clean_df) == 96
    assert clean_df["date"].nunique() == 2


def test_run_daily_update_rebuild_invoked_on_no_new_data(monkeypatch, tmp_path):
    """Verify --rebuild always builds DuckDB and public/ dashboard even when status is NO_NEW_DATA."""
    clean_csv = tmp_path / "clean.csv"
    logs_csv = tmp_path / "logs.csv"

    df = pd.DataFrame([
        {"date": "2026-10-20", "commodity_group": "Rice", "commodity": "Regular Milled Rice", "unit": "kg", "price_prevailing": 45.0},
    ])
    df.to_csv(clean_csv, index=False)

    rebuild_calls = []
    monkeypatch.setattr(
        "pipeline.daily_update._rebuild_warehouse_and_dashboard",
        lambda db_path, clean_csv_path, logs_csv_path: rebuild_calls.append((db_path, clean_csv_path, logs_csv_path)),
    )

    res = run_daily_update(
        clean_csv_path=clean_csv,
        logs_csv_path=logs_csv,
        target_date=datetime.date(2026, 10, 20),
        rebuild=True,
    )

    assert res["status"] == "NO_NEW_DATA"
    assert len(rebuild_calls) == 1
