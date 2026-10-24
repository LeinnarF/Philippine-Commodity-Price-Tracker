"""
DuckDB SQL analytical queries and Plotly figure builders for the dashboard.
"""

from __future__ import annotations

from datetime import date
import json
from typing import Any

import duckdb
import plotly.graph_objects as go

from pipeline.dashboard.filters import slugify_filter

CATEGORY_PALETTE = {
    "Rice": "#15803d",                  # DA Emerald Green
    "Meat and Poultry": "#b91c1c",      # Crimson / Deep Red
    "Fish": "#0284c7",                  # Ocean Blue / Teal
    "Vegetables": "#16a34a",            # Agriculture Leaf Green
    "Fruits": "#d97706",                # Harvest Amber
    "Spices and Condiments": "#7c3aed",  # Spice Purple
    "Basic Goods": "#475569",           # Slate Grey
}


def fetch_kpi_summary(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """
    Queries overall portfolio KPIs, observation date ranges, breadth, and top movers.
    """
    sql_summary = """
    SELECT
        (SELECT MAX(date) FROM commodity_prices) AS latest_date,
        (SELECT MIN(date) FROM commodity_prices) AS earliest_date,
        (SELECT COUNT(DISTINCT date) FROM commodity_prices) AS total_dates,
        (SELECT COUNT(DISTINCT commodity) FROM commodity_prices) AS total_commodities,
        (SELECT COUNT(DISTINCT commodity_group) FROM commodity_prices) AS total_groups,
        (SELECT COUNT(*) FROM commodity_prices WHERE date = (SELECT MAX(date) FROM commodity_prices) AND price_prevailing IS NOT NULL) AS observed_today,
        (SELECT COUNT(*) FROM v_latest_snapshot WHERE dod_pct > 0) AS count_gainers,
        (SELECT COUNT(*) FROM v_latest_snapshot WHERE dod_pct < 0) AS count_decliners,
        (SELECT COUNT(*) FROM v_latest_snapshot WHERE dod_pct = 0) AS count_unchanged,
        (SELECT ROUND(AVG(dod_pct), 2) FROM v_latest_snapshot) AS avg_dod_pct,
        (SELECT ROUND(AVG(index_base100), 2) FROM v_category_indices WHERE date = (SELECT MAX(date) FROM commodity_prices)) AS avg_category_index
    """
    row = con.execute(sql_summary).df().to_dict(orient="records")[0]

    dt_latest = row["latest_date"]
    dt_earliest = row["earliest_date"]

    if hasattr(dt_latest, "strftime"):
        latest_str = dt_latest.strftime("%A, %B %d, %Y")
        latest_short = dt_latest.strftime("%b %d, %Y")
    else:
        d = date.fromisoformat(str(dt_latest)[:10])
        latest_str = d.strftime("%A, %B %d, %Y")
        latest_short = d.strftime("%b %d, %Y")

    if hasattr(dt_earliest, "strftime"):
        earliest_str = dt_earliest.strftime("%A, %B %d, %Y")
        earliest_short = dt_earliest.strftime("%b %d, %Y")
    else:
        d = date.fromisoformat(str(dt_earliest)[:10])
        earliest_str = d.strftime("%A, %B %d, %Y")
        earliest_short = d.strftime("%b %d, %Y")

    top_gainer = con.execute("""
        SELECT commodity, commodity_group, price_prevailing, unit, dod_abs, dod_pct
        FROM v_latest_snapshot
        WHERE dod_pct IS NOT NULL
        ORDER BY dod_pct DESC
        LIMIT 1;
    """).df().to_dict(orient="records")[0]

    top_decliner = con.execute("""
        SELECT commodity, commodity_group, price_prevailing, unit, dod_abs, dod_pct
        FROM v_latest_snapshot
        WHERE dod_pct IS NOT NULL
        ORDER BY dod_pct ASC
        LIMIT 1;
    """).df().to_dict(orient="records")[0]

    top_volatile = con.execute("""
        SELECT commodity, commodity_group, price_prevailing, unit, volatility_30d, cv_30d
        FROM v_latest_snapshot
        ORDER BY cv_30d DESC NULLS LAST
        LIMIT 1;
    """).df().to_dict(orient="records")[0]

    return {
        "latest_date": str(dt_latest)[:10],
        "earliest_date": str(dt_earliest)[:10],
        "latest_date_formatted": latest_str,
        "earliest_date_formatted": earliest_str,
        "latest_date_short": latest_short,
        "earliest_date_short": earliest_short,
        "total_dates": int(row["total_dates"]),
        "total_commodities": int(row["total_commodities"]),
        "total_groups": int(row["total_groups"]),
        "observed_today": int(row["observed_today"]),
        "count_gainers": int(row["count_gainers"]),
        "count_decliners": int(row["count_decliners"]),
        "count_unchanged": int(row["count_unchanged"]),
        "avg_dod_pct": float(row["avg_dod_pct"]) if row["avg_dod_pct"] is not None else 0.0,
        "avg_category_index": float(row["avg_category_index"]) if row["avg_category_index"] is not None else 100.0,
        "top_gainer": top_gainer,
        "top_decliner": top_decliner,
        "top_volatile": top_volatile,
    }


def fetch_top_movers(con: duckdb.DuckDBPyConnection, limit: int = 5) -> dict[str, list[dict[str, Any]]]:
    """
    Returns top N price increases and decreases on the latest snapshot date.
    """
    gainers = con.execute("""
        SELECT commodity, commodity_group, price_prevailing, unit, dod_abs, dod_pct, wow_pct, mom_pct
        FROM v_latest_snapshot
        WHERE dod_pct IS NOT NULL
        ORDER BY dod_pct DESC
        LIMIT ?;
    """, [limit]).df().to_dict(orient="records")

    decliners = con.execute("""
        SELECT commodity, commodity_group, price_prevailing, unit, dod_abs, dod_pct, wow_pct, mom_pct
        FROM v_latest_snapshot
        WHERE dod_pct IS NOT NULL
        ORDER BY dod_pct ASC
        LIMIT ?;
    """, [limit]).df().to_dict(orient="records")

    return {
        "gainers": gainers,
        "decliners": decliners,
    }


def fetch_category_indices(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], str]:
    """
    Fetches category summary metrics and serializes the interactive Plotly time-series chart.
    """
    sql_summary = """
    WITH lagged AS (
        SELECT
            date,
            commodity_group,
            index_base100,
            LAG(index_base100, 1) OVER (PARTITION BY commodity_group ORDER BY date) AS idx_prev_1d,
            LAG(index_base100, 7) OVER (PARTITION BY commodity_group ORDER BY date) AS idx_prev_7d,
            LAG(index_base100, 30) OVER (PARTITION BY commodity_group ORDER BY date) AS idx_prev_30d
        FROM v_category_indices
    ),
    cat_vol AS (
        SELECT
            commodity_group,
            COUNT(*) AS total_commodities,
            ROUND(AVG(volatility_30d), 2) AS avg_vol_30d,
            ROUND(AVG(cv_30d) * 100, 2) AS avg_cv_30d
        FROM v_latest_snapshot
        GROUP BY commodity_group
    )
    SELECT
        l.commodity_group,
        v.total_commodities,
        ROUND(l.index_base100, 2) AS index_val,
        ROUND(l.index_base100 - l.idx_prev_1d, 2) AS dod_pts,
        ROUND(((l.index_base100 - l.idx_prev_1d) / l.idx_prev_1d) * 100, 2) AS dod_pct,
        ROUND(l.index_base100 - l.idx_prev_7d, 2) AS wow_pts,
        ROUND(((l.index_base100 - l.idx_prev_7d) / l.idx_prev_7d) * 100, 2) AS wow_pct,
        ROUND(l.index_base100 - l.idx_prev_30d, 2) AS mom_pts,
        ROUND(((l.index_base100 - l.idx_prev_30d) / l.idx_prev_30d) * 100, 2) AS mom_pct,
        v.avg_vol_30d,
        v.avg_cv_30d
    FROM lagged l
    JOIN cat_vol v ON l.commodity_group = v.commodity_group
    WHERE l.date = (SELECT MAX(date) FROM commodity_prices)
    ORDER BY l.commodity_group;
    """
    summary_rows = con.execute(sql_summary).df().to_dict(orient="records")

    ts_df = con.execute("""
        SELECT
            date::VARCHAR AS date_str,
            commodity_group,
            ROUND(index_base100, 2) AS index_base100
        FROM v_category_indices
        ORDER BY date, commodity_group;
    """).df()

    fig = go.Figure()
    for grp in sorted(ts_df["commodity_group"].unique()):
        sub = ts_df[ts_df["commodity_group"] == grp]
        fig.add_trace(go.Scatter(
            x=sub["date_str"],
            y=sub["index_base100"],
            mode="lines",
            name=grp,
            line=dict(width=2.2, color=CATEGORY_PALETTE.get(grp, "#064e3b")),
            hovertemplate="%{x|%b %d, %Y}<br><b>" + grp + "</b>: %{y:.2f}<extra></extra>",
        ))

    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=55, r=20, t=30, b=40),
        xaxis=dict(
            title="",
            showgrid=True,
            gridcolor="#f1f5f9",
            zeroline=False,
        ),
        yaxis=dict(
            title="Price Index (Base 100)",
            showgrid=True,
            gridcolor="#f1f5f9",
            zeroline=True,
            zerolinecolor="#cbd5e1",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11),
        ),
    )

    chart_json = fig.to_json()
    return summary_rows, chart_json


def fetch_commodity_timeseries(
    con: duckdb.DuckDBPyConnection,
) -> tuple[str, str, dict[str, list[dict[str, Any]]], str]:
    """
    Loads complete historical time-series for all 48 commodities and precomputes
    metadata lookups for zero-latency client-side Plotly rendering.
    """
    rows = con.execute("""
        SELECT
            commodity,
            date::VARCHAR AS date_str,
            ROUND(price_prevailing, 2) AS price,
            ROUND(sma_7d, 2) AS sma_7d,
            ROUND(sma_30d, 2) AS sma_30d
        FROM v_commodity_analytics
        ORDER BY commodity, date;
    """).fetchall()

    ts_dict: dict[str, dict[str, list[Any]]] = {}
    for comm, dt, pr, s7, s30 in rows:
        if comm not in ts_dict:
            ts_dict[comm] = {"dates": [], "prices": [], "sma_7d": [], "sma_30d": []}
        ts_dict[comm]["dates"].append(dt)
        ts_dict[comm]["prices"].append(pr)
        ts_dict[comm]["sma_7d"].append(s7)
        ts_dict[comm]["sma_30d"].append(s30)

    snapshot_records = con.execute("""
        SELECT
            commodity,
            commodity_group,
            unit,
            ROUND(price_prevailing, 2) AS price,
            ROUND(dod_abs, 2) AS dod_abs,
            ROUND(dod_pct, 2) AS dod_pct,
            ROUND(wow_abs, 2) AS wow_abs,
            ROUND(wow_pct, 2) AS wow_pct,
            ROUND(mom_abs, 2) AS mom_abs,
            ROUND(mom_pct, 2) AS mom_pct,
            ROUND(sma_7d, 2) AS sma_7d,
            ROUND(sma_30d, 2) AS sma_30d,
            ROUND(volatility_30d, 2) AS volatility_30d,
            ROUND(cv_30d, 4) AS cv_30d
        FROM v_latest_snapshot
        ORDER BY commodity_group, commodity;
    """).df().to_dict(orient="records")

    meta_dict: dict[str, dict[str, Any]] = {}
    commodities_by_group: dict[str, list[dict[str, Any]]] = {}

    for rec in snapshot_records:
        comm = rec["commodity"]
        grp = rec["commodity_group"]
        slug = slugify_filter(comm)
        rec["slug"] = slug
        meta_dict[comm] = rec
        if grp not in commodities_by_group:
            commodities_by_group[grp] = []
        commodities_by_group[grp].append({
            "commodity": comm,
            "unit": rec["unit"],
            "price": rec["price"],
            "dod_pct": rec["dod_pct"],
            "slug": slug,
        })

    default_commodity = "Regular Milled Rice"
    if default_commodity not in ts_dict and ts_dict:
        default_commodity = next(iter(ts_dict.keys()))

    return (
        json.dumps(ts_dict),
        json.dumps(meta_dict),
        commodities_by_group,
        default_commodity,
    )


def fetch_latest_snapshot(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Fetches all 48 commodity rows from v_latest_snapshot and computes category counts.
    """
    rows = con.execute("""
        SELECT
            commodity_group,
            commodity,
            unit,
            ROUND(price_prevailing, 2) AS price_prevailing,
            ROUND(sma_7d, 2) AS sma_7d,
            ROUND(sma_30d, 2) AS sma_30d,
            ROUND(volatility_30d, 2) AS volatility_30d,
            ROUND(cv_30d, 4) AS cv_30d,
            ROUND(dod_abs, 2) AS dod_abs,
            ROUND(dod_pct, 2) AS dod_pct,
            ROUND(wow_abs, 2) AS wow_abs,
            ROUND(wow_pct, 2) AS wow_pct,
            ROUND(mom_abs, 2) AS mom_abs,
            ROUND(mom_pct, 2) AS mom_pct
        FROM v_latest_snapshot
        ORDER BY commodity_group, commodity;
    """).df().to_dict(orient="records")

    counts: dict[str, int] = {}
    for r in rows:
        r["slug"] = slugify_filter(r["commodity"])
        grp = r["commodity_group"]
        counts[grp] = counts.get(grp, 0) + 1

    return rows, counts


def fetch_audit_info(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """
    Extracts warehouse provenance metrics and verification assertions.
    """
    res = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT raw_file) AS total_bulletins,
            MIN(date)::VARCHAR AS start_date,
            MAX(date)::VARCHAR AS end_date,
            SUM(CASE WHEN is_synthetic THEN 1 ELSE 0 END) AS synthetic_count,
            MAX(market_scope) AS market_scope,
            MAX(source) AS source_agency
        FROM audit_prices;
    """).df().to_dict(orient="records")[0]

    return {
        "total_rows": int(res["total_rows"]),
        "total_bulletins": int(res["total_bulletins"]),
        "start_date": res["start_date"],
        "end_date": res["end_date"],
        "synthetic_count": int(res["synthetic_count"]),
        "market_scope": res["market_scope"] or "NCR Wet Markets",
        "source_agency": res["source_agency"] or "da_amas",
    }


def load_all_dashboard_data(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """
    Loads all analytical models from DuckDB into a single reusable context dictionary.
    """
    kpis = fetch_kpi_summary(con)
    top_movers = fetch_top_movers(con, limit=5)
    category_summary, category_chart_json = fetch_category_indices(con)
    (
        timeseries_json,
        commodity_meta_json,
        commodities_by_group,
        default_commodity,
    ) = fetch_commodity_timeseries(con)
    snapshot_rows, group_counts = fetch_latest_snapshot(con)
    audit_info = fetch_audit_info(con)

    return {
        "kpis": kpis,
        "top_movers": top_movers,
        "category_summary": category_summary,
        "category_indices_chart_json": category_chart_json,
        "timeseries_json": timeseries_json,
        "commodity_meta_json": commodity_meta_json,
        "commodities_by_group": commodities_by_group,
        "default_commodity": default_commodity,
        "snapshot_rows": snapshot_rows,
        "group_counts": group_counts,
        "audit_info": audit_info,
    }
