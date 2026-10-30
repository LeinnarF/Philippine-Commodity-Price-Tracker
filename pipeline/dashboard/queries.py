"""
DuckDB SQL analytical queries and Plotly figure builders for the dashboard.
"""

from __future__ import annotations

from datetime import date
import json
from typing import Any

import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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


def _build_sparkline_horizon(values: list[float]) -> dict[str, Any]:
    """
    Computes SVG sparkline polyline/area coordinates and min/max range metrics
    for a given slice of category index values.
    """
    if not values:
        return {
            "min_val": 100.0,
            "max_val": 100.0,
            "range_pct": 50.0,
            "points": "4.0,18.0 132.0,18.0",
            "area": "4.0,31.0 4.0,18.0 132.0,18.0 132.0,31.0",
            "last_x": 132.0,
            "last_y": 18.0,
            "base100_y": 18.0,
        }

    min_val = float(min(values))
    max_val = float(max(values))
    cur_val = float(values[-1])
    span = max_val - min_val
    range_pct = round(((cur_val - min_val) / span) * 100.0, 1) if span > 1e-6 else 50.0
    range_pct = max(0.0, min(100.0, range_pct))

    # Downsample to at most 48 points while preserving start and end
    n = len(values)
    max_pts = 48
    if n <= max_pts:
        sampled = values
    else:
        idxs = [round(i * (n - 1) / (max_pts - 1)) for i in range(max_pts)]
        sampled = [values[idx] for idx in idxs]

    pad_x = 4.0
    pad_y = 5.0
    w = 128.0
    h = 26.0
    m = len(sampled)

    pts: list[tuple[float, float]] = []
    for i, val in enumerate(sampled):
        x = pad_x + (i / (m - 1)) * w if m > 1 else pad_x + w
        y = pad_y + h - ((val - min_val) / span) * h if span > 1e-6 else pad_y + h / 2.0
        pts.append((round(x, 1), round(y, 1)))

    points_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area_str = f"{pts[0][0]:.1f},{pad_y + h:.1f} {points_str} {pts[-1][0]:.1f},{pad_y + h:.1f}"

    base100_y: float | None = None
    if span > 1e-6 and min_val <= 100.0 <= max_val:
        base100_y = round(pad_y + h - ((100.0 - min_val) / span) * h, 1)

    return {
        "min_val": round(min_val, 2),
        "max_val": round(max_val, 2),
        "range_pct": range_pct,
        "points": points_str,
        "area": area_str,
        "last_x": pts[-1][0],
        "last_y": pts[-1][1],
        "base100_y": base100_y,
    }


def fetch_category_indices(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], str]:
    """
    Fetches category summary metrics (including inline sparkline geometry, historical
    range positioning, and daily breadth) and serializes the category chart payload.
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
            SUM(CASE WHEN dod_pct > 0.0001 THEN 1 ELSE 0 END) AS count_up,
            SUM(CASE WHEN dod_pct < -0.0001 THEN 1 ELSE 0 END) AS count_down,
            SUM(CASE WHEN dod_pct IS NULL OR ABS(dod_pct) <= 0.0001 THEN 1 ELSE 0 END) AS count_flat,
            ROUND(AVG(volatility_30d), 2) AS avg_vol_30d,
            ROUND(AVG(cv_30d) * 100, 2) AS avg_cv_30d
        FROM v_latest_snapshot
        GROUP BY commodity_group
    )
    SELECT
        l.commodity_group,
        v.total_commodities,
        v.count_up,
        v.count_flat,
        v.count_down,
        ROUND(l.index_base100, 2) AS index_val,
        ROUND(l.index_base100 - 100.0, 2) AS base_diff_pts,
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

    # Enrich each category summary row with palette color, breadth percentages,
    # and multi-horizon sparkline + range metrics (30D, 90D, 1Y, ALL)
    for row in summary_rows:
        grp = row["commodity_group"]
        total = max(int(row["total_commodities"]), 1)
        up = int(row.get("count_up") or 0)
        flat = int(row.get("count_flat") or 0)
        down = int(row.get("count_down") or 0)
        row["count_up"] = up
        row["count_flat"] = flat
        row["count_down"] = down
        row["pct_up"] = round((up / total) * 100.0, 1)
        row["pct_flat"] = round((flat / total) * 100.0, 1)
        row["pct_down"] = round((down / total) * 100.0, 1)
        row["color"] = CATEGORY_PALETTE.get(grp, "#15803d")
        row["slug"] = slugify_filter(grp)

        grp_vals = ts_df[ts_df["commodity_group"] == grp]["index_base100"].tolist()
        horizons = {
            "30D": _build_sparkline_horizon(grp_vals[-30:]),
            "90D": _build_sparkline_horizon(grp_vals[-90:]),
            "1Y": _build_sparkline_horizon(grp_vals[-365:]),
            "ALL": _build_sparkline_horizon(grp_vals),
        }
        row["horizons"] = horizons
        row["horizons_json"] = json.dumps(horizons, separators=(",", ":"))
        # Default display horizon: ALL (full baseline trajectory since March 1, 2025)
        default_h = horizons["ALL"]
        row["min_index"] = default_h["min_val"]
        row["max_index"] = default_h["max_val"]
        row["range_pct"] = default_h["range_pct"]
        row["spark_points"] = default_h["points"]
        row["spark_area"] = default_h["area"]
        row["spark_last_x"] = default_h["last_x"]
        row["spark_last_y"] = default_h["last_y"]
        row["spark_base100_y"] = default_h["base100_y"]

    groups = sorted(ts_df["commodity_group"].unique())
    n_groups = len(groups)
    cols = 4
    rows = (n_groups + cols - 1) // cols

    # Subplot titles with clean typography
    subplot_titles = [f"<b>{grp}</b>" for grp in groups]

    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.045,
        vertical_spacing=0.18,
    )

    for i, grp in enumerate(groups):
        r = (i // cols) + 1
        c = (i % cols) + 1
        sub = ts_df[ts_df["commodity_group"] == grp]
        color = CATEGORY_PALETTE.get(grp, "#064e3b")

        # Subtle Base 100 baseline reference
        fig.add_hline(
            y=100,
            line_dash="dot",
            line_width=1,
            line_color="#cbd5e1",
            row=r,
            col=c,
        )

        fig.add_trace(
            go.Scatter(
                x=sub["date_str"],
                y=sub["index_base100"],
                mode="lines",
                name=grp,
                line=dict(width=2.0, color=color),
                showlegend=False,
                hovertemplate="%{x|%b %d, %Y}<br>" + grp + ": <b>%{y:.2f}</b><extra></extra>",
            ),
            row=r,
            col=c,
        )

    # Style subplot titles
    for annotation in fig["layout"]["annotations"]:
        annotation["font"] = dict(size=12, color="#0f172a", family="Inter, system-ui, sans-serif")

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=35, r=20, t=35, b=30),
        height=480,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="#f8fafc",
        tickfont=dict(size=9, color="#94a3b8"),
        showticklabels=True,
        nticks=4,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="#f1f5f9",
        tickfont=dict(size=9, color="#64748b"),
        zeroline=False,
        nticks=4,
    )

    chart_json = json.dumps(json.loads(fig.to_json()), separators=(",", ":"))
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
            ROUND(sma_30d, 2) AS sma_30d,
            ROUND(dod_pct, 2) AS dod_pct,
            ROUND(wow_pct, 2) AS wow_pct,
            ROUND(mom_pct, 2) AS mom_pct,
            ROUND(volatility_30d, 2) AS vol,
            ROUND(cv_30d * 100, 2) AS cv
        FROM v_commodity_analytics
        ORDER BY commodity_group, commodity, date;
    """).fetchall()

    ts_dict: dict[str, dict[str, list[Any]]] = {}
    for comm, dt, pr, s7, s30, dod, wow, mom, vol, cv in rows:
        if comm not in ts_dict:
            ts_dict[comm] = {
                "dates": [],
                "prices": [],
                "sma_7d": [],
                "sma_30d": [],
                "dod_pct": [],
                "wow_pct": [],
                "mom_pct": [],
                "vol": [],
                "cv": [],
            }
        ts_dict[comm]["dates"].append(dt)
        ts_dict[comm]["prices"].append(pr)
        ts_dict[comm]["sma_7d"].append(s7)
        ts_dict[comm]["sma_30d"].append(s30)
        ts_dict[comm]["dod_pct"].append(dod)
        ts_dict[comm]["wow_pct"].append(wow)
        ts_dict[comm]["mom_pct"].append(mom)
        ts_dict[comm]["vol"].append(vol)
        ts_dict[comm]["cv"].append(cv)

    snapshot_records = con.execute("""
        SELECT
            s.commodity,
            s.commodity_group,
            s.unit,
            ROUND(s.price_prevailing, 2) AS price,
            ROUND(s.dod_abs, 2) AS dod_abs,
            ROUND(s.dod_pct, 2) AS dod_pct,
            ROUND(s.wow_abs, 2) AS wow_abs,
            ROUND(s.wow_pct, 2) AS wow_pct,
            ROUND(s.mom_abs, 2) AS mom_abs,
            ROUND(s.mom_pct, 2) AS mom_pct,
            ROUND(s.sma_7d, 2) AS sma_7d,
            ROUND(s.sma_30d, 2) AS sma_30d,
            ROUND(s.volatility_30d, 2) AS volatility_30d,
            ROUND(s.cv_30d, 4) AS cv_30d,
            ROUND(st.mean_price, 2) AS mean_price,
            ROUND(st.median_price, 2) AS median_price,
            ROUND(st.min_price, 2) AS min_price,
            st.min_date AS min_date,
            ROUND(st.max_price, 2) AS max_price,
            st.max_date AS max_date
        FROM v_latest_snapshot s
        LEFT JOIN (
            SELECT
                commodity,
                AVG(price_prevailing) AS mean_price,
                MEDIAN(price_prevailing) AS median_price,
                MIN(price_prevailing) AS min_price,
                ARG_MIN(date, price_prevailing)::VARCHAR AS min_date,
                MAX(price_prevailing) AS max_price,
                ARG_MAX(date, price_prevailing)::VARCHAR AS max_date
            FROM commodity_prices
            GROUP BY commodity
        ) st ON s.commodity = st.commodity
        ORDER BY s.commodity_group, s.commodity;
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
        json.dumps(ts_dict, separators=(",", ":")),
        json.dumps(meta_dict, separators=(",", ":")),
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


def fetch_census_history(con: duckdb.DuckDBPyConnection) -> tuple[str, list[str]]:
    """
    Fetches full panel history across all 48 commodities and all observation dates
    for client-side dynamic date filtering of the census table.
    Returns:
        (census_by_date_json, sorted_census_dates_list)
    """
    rows = con.execute("""
        SELECT
            date::VARCHAR AS date,
            ROUND(price_prevailing, 2) AS price,
            ROUND(dod_pct, 2) AS dod_pct,
            ROUND(wow_pct, 2) AS wow_pct,
            ROUND(mom_pct, 2) AS mom_pct,
            ROUND(sma_7d, 2) AS sma_7d,
            ROUND(sma_30d, 2) AS sma_30d,
            ROUND(volatility_30d, 2) AS vol,
            ROUND(cv_30d * 100, 2) AS cv
        FROM v_commodity_analytics
        ORDER BY date DESC, commodity_group, commodity;
    """).fetchall()

    by_date: dict[str, list[list[Any]]] = {}
    for dt, pr, dod, wow, mom, s7, s30, vol, cv in rows:
        if dt not in by_date:
            by_date[dt] = []
        by_date[dt].append([pr, dod, wow, mom, s7, s30, vol, cv])

    dates = sorted(by_date.keys(), reverse=True)
    return json.dumps(by_date, separators=(",", ":")), dates


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

    # Fetch recent pipeline execution logs if table exists
    tables = [t[0] for t in con.execute("SHOW TABLES;").fetchall()]
    execution_logs: list[dict[str, Any]] = []
    latest_run: dict[str, Any] | None = None

    if "pipeline_execution_logs" in tables:
        try:
            log_df = con.execute("""
                SELECT
                    run_id,
                    executed_at::VARCHAR AS executed_at,
                    trigger,
                    status,
                    latest_bulletin_date::VARCHAR AS latest_bulletin_date,
                    bulletins_checked,
                    bulletins_downloaded,
                    records_extracted,
                    duration_seconds,
                    message
                FROM pipeline_execution_logs
                ORDER BY executed_at DESC
                LIMIT 15;
            """).df()
            if not log_df.empty:
                execution_logs = log_df.to_dict(orient="records")
                latest_run = execution_logs[0]
        except Exception:
            execution_logs = []
            latest_run = None

    # Fetch all bulletin provenance records grouped by date
    bulletin_rows: list[dict[str, Any]] = []
    available_months: list[dict[str, str]] = []
    try:
        bulletin_df = con.execute("""
            SELECT
                date::VARCHAR AS date_str,
                strftime(date, '%Y-%m') AS ym,
                strftime(date, '%B %Y') AS ym_label,
                MAX(raw_file) AS raw_file,
                MAX(source_url) AS source_url,
                COUNT(price_prevailing)::INTEGER AS recorded_items,
                COUNT(*)::INTEGER AS total_items
            FROM audit_prices
            GROUP BY date
            ORDER BY date DESC;
        """).df()
        if not bulletin_df.empty:
            bulletin_rows = bulletin_df.to_dict(orient="records")
            seen_ym = set()
            for r in bulletin_rows:
                ym = r["ym"]
                if ym not in seen_ym:
                    seen_ym.add(ym)
                    available_months.append({"ym": ym, "label": r["ym_label"]})
    except Exception as exc:
        logger.warning("Failed to fetch bulletin provenance register: %s", exc)
        bulletin_rows = []
        available_months = []

    return {
        "total_rows": int(res["total_rows"]),
        "total_bulletins": int(res["total_bulletins"]),
        "start_date": res["start_date"],
        "end_date": res["end_date"],
        "synthetic_count": int(res["synthetic_count"]),
        "market_scope": res["market_scope"] or "NCR Wet Markets",
        "source_agency": res["source_agency"] or "da_amas",
        "execution_logs": execution_logs,
        "latest_run": latest_run,
        "bulletins": bulletin_rows,
        "available_months": available_months,
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
    census_dates = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT date::VARCHAR AS d FROM commodity_prices ORDER BY d DESC;"
        ).fetchall()
    ]
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
        "census_dates": census_dates,
        "audit_info": audit_info,
    }
