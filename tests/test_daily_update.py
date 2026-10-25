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
        pytest.skip("clean_da_amas_prices.csv not found")

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
