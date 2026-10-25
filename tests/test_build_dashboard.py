"""
Unit tests for the Modular Static Dashboard Generator & Live Dev Server (pipeline.build_dashboard).
"""

import json
from pathlib import Path
import threading
import time
import urllib.request

import duckdb
import pytest

from pipeline.build_dashboard import (
    DEFAULT_DB_PATH,
    DEFAULT_TEMPLATE_DIR,
    build_dashboard,
    change_badge_filter,
    currency_filter,
    fetch_audit_info,
    fetch_category_indices,
    fetch_census_history,
    fetch_commodity_timeseries,
    fetch_kpi_summary,
    fetch_latest_snapshot,
    fetch_top_movers,
    format_date_filter,
    format_num_filter,
    load_all_dashboard_data,
    percent_filter,
    render_dashboard_html,
    slugify_filter,
)
from pipeline.dashboard.dev_server import collect_watch_mtimes, run_dev_watcher


@pytest.fixture(scope="module")
def duckdb_con():
    """Provides a read-only DuckDB connection for testing queries."""
    if not DEFAULT_DB_PATH.exists():
        pytest.skip(f"DuckDB database not found at {DEFAULT_DB_PATH}")
    con = duckdb.connect(str(DEFAULT_DB_PATH), read_only=True)
    yield con
    con.close()


# =====================================================================
# 1. Jinja Filter Unit Tests
# =====================================================================

def test_currency_filter():
    assert currency_filter(123.456) == "₱123.46"
    assert currency_filter(0) == "₱0.00"
    assert currency_filter(None) == "—"
    assert currency_filter("invalid") == "—"


def test_percent_filter():
    assert percent_filter(5.25) == "+5.25%"
    assert percent_filter(-3.14) == "-3.14%"
    assert percent_filter(0.0) == "0.00%"
    assert percent_filter(5.25, show_sign=False) == "5.25%"
    assert percent_filter(None) == "—"
    assert percent_filter("invalid") == "—"


def test_change_badge_filter():
    pos = str(change_badge_filter(2.5))
    assert "badge-increase" in pos
    assert "▲" in pos
    assert "+2.50%" in pos

    neg = str(change_badge_filter(-1.8))
    assert "badge-decrease" in neg
    assert "▼" in neg
    assert "-1.80%" in neg

    zero = str(change_badge_filter(0.0))
    assert "badge-neutral" in zero
    assert "0.00%" in zero

    none_badge = str(change_badge_filter(None))
    assert "badge-neutral" in none_badge
    assert "—" in none_badge


def test_format_date_filter():
    assert format_date_filter("2026-10-20") == "October 20, 2026"
    assert format_date_filter(None) == "—"


def test_format_num_filter():
    assert format_num_filter(1234567) == "1,234,567"
    assert format_num_filter(1234.5678, decimals=2) == "1,234.57"
    assert format_num_filter(None) == "—"


def test_slugify_filter():
    assert slugify_filter("Pork Pigue (Hind Leg / Ham)") == "pork-pigue-hind-leg-ham"
    assert slugify_filter("Regular Milled Rice") == "regular-milled-rice"
    assert slugify_filter(None) == ""


# =====================================================================
# 2. Database Fetchers Unit Tests
# =====================================================================

def test_fetch_kpi_summary(duckdb_con):
    kpis = fetch_kpi_summary(duckdb_con)
    assert kpis["total_commodities"] == 48
    assert kpis["total_groups"] == 7
    assert kpis["total_dates"] == 451
    assert kpis["observed_today"] == 48
    assert kpis["count_gainers"] + kpis["count_decliners"] + kpis["count_unchanged"] == 48
    assert "latest_date_formatted" in kpis
    assert "top_gainer" in kpis
    assert "top_decliner" in kpis
    assert "top_volatile" in kpis
    assert kpis["top_gainer"]["dod_pct"] > 0


def test_fetch_top_movers(duckdb_con):
    movers = fetch_top_movers(duckdb_con, limit=5)
    assert len(movers["gainers"]) == 5
    assert len(movers["decliners"]) == 5
    assert movers["gainers"][0]["dod_pct"] >= movers["gainers"][1]["dod_pct"]
    assert movers["decliners"][0]["dod_pct"] <= movers["decliners"][1]["dod_pct"]


def test_fetch_category_indices(duckdb_con):
    summary_rows, chart_json = fetch_category_indices(duckdb_con)
    assert len(summary_rows) == 7
    for row in summary_rows:
        assert "commodity_group" in row
        assert "index_val" in row
        assert row["total_commodities"] > 0
        assert "count_up" in row and "count_flat" in row and "count_down" in row
        assert row["count_up"] + row["count_flat"] + row["count_down"] == row["total_commodities"]
        assert "spark_points" in row and "spark_area" in row
        assert "min_index" in row and "max_index" in row and "range_pct" in row
        assert row["min_index"] <= row["index_val"] <= row["max_index"]
        assert 0.0 <= row["range_pct"] <= 100.0
        assert "horizons" in row and set(row["horizons"].keys()) == {"30D", "90D", "1Y", "ALL"}

    chart_data = json.loads(chart_json)
    assert "data" in chart_data
    assert "layout" in chart_data
    assert len(chart_data["data"]) == 7


def test_fetch_commodity_timeseries(duckdb_con):
    ts_json, meta_json, groups_dict, default_comm = fetch_commodity_timeseries(duckdb_con)

    ts = json.loads(ts_json)
    meta = json.loads(meta_json)

    assert len(ts) == 48
    assert len(meta) == 48
    assert default_comm in ts
    assert len(groups_dict) == 7

    # Verify sidebar tree metadata enrichment
    first_group_items = next(iter(groups_dict.values()))
    assert "slug" in first_group_items[0]
    assert "price" in first_group_items[0]
    assert "dod_pct" in first_group_items[0]

    # Verify basic price statistics in metadata
    assert "mean_price" in meta[default_comm]
    assert "median_price" in meta[default_comm]
    assert "min_price" in meta[default_comm]
    assert "min_date" in meta[default_comm]
    assert "max_price" in meta[default_comm]
    assert "max_date" in meta[default_comm]
    assert meta[default_comm]["min_price"] <= meta[default_comm]["mean_price"] <= meta[default_comm]["max_price"]
    assert meta[default_comm]["min_date"] is not None
    assert meta[default_comm]["max_date"] is not None

    sample_comm = default_comm
    assert len(ts[sample_comm]["dates"]) == 451
    assert len(ts[sample_comm]["prices"]) == 451
    assert len(ts[sample_comm]["sma_7d"]) == 451
    assert len(ts[sample_comm]["sma_30d"]) == 451
    assert len(ts[sample_comm]["dod_pct"]) == 451
    assert len(ts[sample_comm]["wow_pct"]) == 451
    assert len(ts[sample_comm]["mom_pct"]) == 451


def test_fetch_latest_snapshot(duckdb_con):
    rows, counts = fetch_latest_snapshot(duckdb_con)
    assert len(rows) == 48
    assert sum(counts.values()) == 48
    assert len(counts) == 7
    assert counts["Vegetables"] == 14
    assert counts["Rice"] == 3


def test_fetch_census_history(duckdb_con):
    census_by_date_json, dates = fetch_census_history(duckdb_con)
    by_date = json.loads(census_by_date_json)

    assert len(dates) == 451
    assert len(by_date) == 451
    assert dates[0] == "2026-10-20"
    assert dates[-1] == "2025-03-01"

    # Each date entry must have all 48 commodity rows with 8 metrics each
    latest_rows = by_date[dates[0]]
    assert len(latest_rows) == 48
    assert len(latest_rows[0]) == 8  # [price, dod, wow, mom, s7, s30, vol, cv]


def test_fetch_audit_info(duckdb_con):
    audit = fetch_audit_info(duckdb_con)
    assert audit["total_rows"] == 21648
    assert audit["total_bulletins"] == 451
    assert audit["synthetic_count"] == 0
    assert audit["market_scope"] == "NCR Wet Markets"
    assert audit["source_agency"] == "da_amas"


# =====================================================================
# 3. Modular Jinja Rendering & Asset Build Tests
# =====================================================================

def test_render_dashboard_html(duckdb_con):
    html = render_dashboard_html(duckdb_con, template_dir=DEFAULT_TEMPLATE_DIR)
    assert "<!DOCTYPE html>" in html
    assert "Philippine Commodity Price Tracker" in html
    assert "Bantay Presyo" in html
    assert "dashboard-sidebar" in html
    assert "sidebar-commodity-tree" in html
    assert "view-overview" in html
    assert "view-commodity" in html
    assert "view-census" in html
    assert "view-governance" in html
    assert "category-index-plot" in html
    assert "commodity-trend-plot" in html
    assert "master-price-table" in html
    assert "{{" not in html
    assert "{%" not in html
    assert "/__dev_poll__" not in html


def test_render_dashboard_html_dev_mode(duckdb_con):
    cached = load_all_dashboard_data(duckdb_con)
    html = render_dashboard_html(
        template_dir=DEFAULT_TEMPLATE_DIR,
        cached_data=cached,
        dev_mode=True,
        build_id="test_build_999",
    )
    assert "/__dev_poll__" in html
    assert "test_build_999" in html


def test_build_dashboard_end_to_end(tmp_path, duckdb_con):
    out_file = tmp_path / "public" / "index.html"
    res_path = build_dashboard(
        template_dir=DEFAULT_TEMPLATE_DIR,
        output_path=out_file,
        con=duckdb_con,
    )
    assert res_path == out_file
    assert out_file.exists()
    assert out_file.stat().st_size > 50_000

    # Verify modular static assets exist and are populated
    css_file = tmp_path / "public" / "assets" / "css" / "dashboard.css"
    js_file = tmp_path / "public" / "assets" / "js" / "dashboard.js"
    data_file = tmp_path / "public" / "assets" / "data" / "commodities.js"
    nojekyll_file = tmp_path / "public" / ".nojekyll"

    assert nojekyll_file.exists()
    assert css_file.exists() and css_file.stat().st_size > 5_000
    assert js_file.exists() and js_file.stat().st_size > 5_000
    assert data_file.exists() and data_file.stat().st_size > 500_000
    data_content = data_file.read_text(encoding="utf-8")
    assert "window.DASHBOARD_DATA =" in data_content
    assert "dates:" in data_content
    assert "timeseries:" in data_content
    assert "censusDates:" in data_content

    html_content = out_file.read_text(encoding="utf-8")
    assert "census-date-select" in html_content
    assert "btn-census-date-prev" in html_content
    assert "btn-census-date-next" in html_content
    assert "commodity-history-table" in html_content
    assert "commodity-history-pagination" in html_content


def test_build_dashboard_missing_db(tmp_path):
    fake_db = tmp_path / "non_existent.duckdb"
    with pytest.raises(FileNotFoundError):
        build_dashboard(db_path=fake_db)


def test_render_dashboard_missing_template(tmp_path, duckdb_con):
    fake_templates = tmp_path / "fake_templates"
    with pytest.raises(FileNotFoundError):
        render_dashboard_html(con=duckdb_con, template_dir=fake_templates)


# =====================================================================
# 4. Live Development Watcher & Auto-Reload Tests
# =====================================================================

def test_collect_watch_mtimes():
    tpl_mtimes, db_mtime = collect_watch_mtimes(DEFAULT_TEMPLATE_DIR, DEFAULT_DB_PATH)
    assert len(tpl_mtimes) >= 9  # base.html, dashboard.html, components/*, views/*, assets/*
    assert db_mtime > 0


def test_dev_watcher_rebuild_cycle(tmp_path):
    if not DEFAULT_DB_PATH.exists():
        pytest.skip("DuckDB database not found")

    out_file = tmp_path / "public" / "index.html"
    run_dev_watcher(
        db_path=DEFAULT_DB_PATH,
        template_dir=DEFAULT_TEMPLATE_DIR,
        output_path=out_file,
        build_fn=build_dashboard,
        host="127.0.0.1",
        port=0,  # OS assigns an ephemeral free port
        poll_interval=0.05,
        max_cycles=1,
    )
    assert out_file.exists()
    assert "/__dev_poll__" in out_file.read_text(encoding="utf-8")
