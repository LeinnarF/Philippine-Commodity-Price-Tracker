"""
Modular dashboard package for the Philippine Commodity Price Tracker.

Contains SQL query extractors, Jinja2 custom filters, and the live development
watcher and HTTP server.
"""

from pipeline.dashboard.filters import (
    change_badge_filter,
    currency_filter,
    format_date_filter,
    format_num_filter,
    percent_filter,
    slugify_filter,
)
from pipeline.dashboard.queries import (
    CATEGORY_PALETTE,
    fetch_audit_info,
    fetch_category_indices,
    fetch_commodity_timeseries,
    fetch_kpi_summary,
    fetch_latest_snapshot,
    fetch_top_movers,
    load_all_dashboard_data,
)

__all__ = [
    "CATEGORY_PALETTE",
    "change_badge_filter",
    "currency_filter",
    "fetch_audit_info",
    "fetch_category_indices",
    "fetch_commodity_timeseries",
    "fetch_kpi_summary",
    "fetch_latest_snapshot",
    "fetch_top_movers",
    "format_date_filter",
    "format_num_filter",
    "load_all_dashboard_data",
    "percent_filter",
    "slugify_filter",
]
