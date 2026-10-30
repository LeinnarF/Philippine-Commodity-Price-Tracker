"""
Stage 6: Modular Static Analytical Dashboard Generator & Live Development Server.

Extracts data from the DuckDB analytical warehouse (commodities.duckdb), computes
macro category indices, item-level moving averages, price velocity metrics, and
renders a modular static dashboard via Jinja2 into public/ (index.html +
assets/css/dashboard.css + assets/js/dashboard.js + assets/data/commodities.js).

Includes a built-in live development watcher and HTTP server (--watch) that
rebuilds on template/CSS/JS/DuckDB changes and auto-reloads connected browsers.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import shutil
import time
from typing import Any

import duckdb
import jinja2

from pipeline.dashboard.dev_server import run_dev_watcher
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
    fetch_census_history,
    fetch_commodity_timeseries,
    fetch_kpi_summary,
    fetch_latest_snapshot,
    fetch_top_movers,
    load_all_dashboard_data,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_dashboard")

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "commodities.duckdb"
DEFAULT_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"
DEFAULT_OUTPUT_HTML = Path(__file__).resolve().parent.parent / "public" / "index.html"


def create_jinja_env(template_dir: str | Path = DEFAULT_TEMPLATE_DIR) -> jinja2.Environment:
    """
    Configures the Jinja2 Environment with custom filters and template loader.
    """
    template_path = Path(template_dir)
    if not template_path.exists():
        raise FileNotFoundError(f"Template directory does not exist: {template_path}")

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(template_path)),
        autoescape=jinja2.select_autoescape(["html", "xml"]),
    )
    env.filters["currency"] = currency_filter
    env.filters["percent"] = percent_filter
    env.filters["badge"] = change_badge_filter
    env.filters["format_date"] = format_date_filter
    env.filters["format_num"] = format_num_filter
    env.filters["slugify"] = slugify_filter
    return env


def render_dashboard_html(
    con: duckdb.DuckDBPyConnection | None = None,
    template_dir: str | Path = DEFAULT_TEMPLATE_DIR,
    cached_data: dict[str, Any] | None = None,
    dev_mode: bool = False,
    build_id: str | None = None,
) -> str:
    """
    Loads data models from DuckDB (or reuses cached_data), configures Jinja2
    environment, and returns rendered HTML string.
    """
    env = create_jinja_env(template_dir)

    if cached_data is None:
        if con is None:
            raise ValueError("Either 'con' or 'cached_data' must be provided.")
        data_ctx = load_all_dashboard_data(con)
    else:
        data_ctx = cached_data

    resolved_build_id = build_id or str(int(time.time()))

    template = env.get_template("dashboard.html")
    rendered = template.render(
        **data_ctx,
        dev_mode=dev_mode,
        build_id=resolved_build_id,
    )
    return rendered


def write_modular_assets(
    template_dir: Path,
    public_dir: Path,
    data_ctx: dict[str, Any],
) -> dict[str, Path]:
    """
    Writes modular CSS, JS, and precomputed data payload files into public/assets/.
    """
    css_dir = public_dir / "assets" / "css"
    js_dir = public_dir / "assets" / "js"
    data_dir = public_dir / "assets" / "data"

    css_dir.mkdir(parents=True, exist_ok=True)
    js_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Copy standalone stylesheet
    src_css = template_dir / "assets" / "dashboard.css"
    dest_css = css_dir / "dashboard.css"
    if src_css.exists():
        shutil.copyfile(src_css, dest_css)

    # 2. Copy standalone interactive controller JS
    src_js = template_dir / "assets" / "dashboard.js"
    dest_js = js_dir / "dashboard.js"
    if src_js.exists():
        shutil.copyfile(src_js, dest_js)

    # 3. Copy image assets and favicon if present
    src_img = template_dir / "assets" / "img"
    dest_img = public_dir / "assets" / "img"
    if src_img.exists():
        dest_img.mkdir(parents=True, exist_ok=True)
        for img_file in src_img.glob("*"):
            if img_file.is_file():
                shutil.copyfile(img_file, dest_img / img_file.name)

    src_favicon = template_dir / "favicon.ico"
    dest_favicon = public_dir / "favicon.ico"
    if src_favicon.exists():
        shutil.copyfile(src_favicon, dest_favicon)

    # 4. Emit precomputed data payload (window.DASHBOARD_DATA)
    # Deduplicate shared dates array across all 48 commodities to save ~300 KB
    raw_ts = json.loads(data_ctx["timeseries_json"])
    shared_dates: list[str] = []
    compact_series: dict[str, dict[str, list[Any]]] = {}
    for comm, series in raw_ts.items():
        if not shared_dates and "dates" in series:
            shared_dates = series["dates"]
        compact_series[comm] = {k: v for k, v in series.items() if k != "dates"}

    dest_data = data_dir / "commodities.js"
    data_js_content = (
        "// Auto-generated by pipeline.build_dashboard — Do not edit manually\n"
        "window.DASHBOARD_DATA = {\n"
        f"  categoryChart: {data_ctx['category_indices_chart_json']},\n"
        f"  dates: {json.dumps(shared_dates, separators=(',', ':'))},\n"
        f"  timeseries: {json.dumps(compact_series, separators=(',', ':'))},\n"
        f"  commodityMeta: {data_ctx['commodity_meta_json']},\n"
        f"  defaultCommodity: {json.dumps(data_ctx['default_commodity'])},\n"
        f"  censusDates: {json.dumps(data_ctx['census_dates'], separators=(',', ':'))},\n"
        f"  bulletins: {json.dumps(data_ctx.get('audit_info', {}).get('bulletins', []), separators=(',', ':'))}\n"
        "};\n"
    )
    dest_data.write_text(data_js_content, encoding="utf-8")

    # 5. Emit .nojekyll for seamless GitHub Pages deployment
    nojekyll_file = public_dir / ".nojekyll"
    nojekyll_file.touch(exist_ok=True)

    # 6. Emit search engine robots.txt
    robots_file = public_dir / "robots.txt"
    robots_file.write_text(
        "User-agent: *\n"
        "Allow: /\n\n"
        "Sitemap: https://leinnarf.github.io/Philippine-Commodity-Price-Tracker/sitemap.xml\n",
        encoding="utf-8",
    )

    # 7. Emit search engine sitemap.xml
    sitemap_file = public_dir / "sitemap.xml"
    latest_date_str = str(data_ctx.get("kpis", {}).get("latest_date", "2026-10-20"))
    sitemap_file.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        '  <url>\n'
        '    <loc>https://leinnarf.github.io/Philippine-Commodity-Price-Tracker/</loc>\n'
        f'    <lastmod>{latest_date_str}</lastmod>\n'
        '    <changefreq>daily</changefreq>\n'
        '    <priority>1.0</priority>\n'
        '  </url>\n'
        '</urlset>\n',
        encoding="utf-8",
    )

    return {
        "css": dest_css,
        "js": dest_js,
        "data": dest_data,
        "nojekyll": nojekyll_file,
        "robots": robots_file,
        "sitemap": sitemap_file,
    }


def build_dashboard(
    db_path: str | Path = DEFAULT_DB_PATH,
    template_dir: str | Path = DEFAULT_TEMPLATE_DIR,
    output_path: str | Path = DEFAULT_OUTPUT_HTML,
    con: duckdb.DuckDBPyConnection | None = None,
    cached_data: dict[str, Any] | None = None,
    dev_mode: bool = False,
    build_id: str | None = None,
) -> Path:
    """
    Generates the modular static dashboard HTML and assets from DuckDB.

    Parameters
    ----------
    db_path : str | Path
        Path to DuckDB database file (data/commodities.duckdb).
    template_dir : str | Path
        Path to Jinja2 template directory (templates/).
    output_path : str | Path
        Path to destination output HTML file (public/index.html).
    con : duckdb.DuckDBPyConnection | None
        Optional existing open DuckDB connection.
    cached_data : dict[str, Any] | None
        Optional pre-loaded data context (used by live watcher for fast rebuilds).
    dev_mode : bool
        If True, injects the live-reload client script into index.html.
    build_id : str | None
        Optional cache-busting build identifier.

    Returns
    -------
    Path
        Absolute path to the generated index.html file.
    """
    start_time = time.time()
    tpl_path = Path(template_dir)
    if not tpl_path.exists():
        raise FileNotFoundError(f"Template directory does not exist: {tpl_path}")

    dest = Path(output_path).resolve()
    public_dir = dest.parent
    public_dir.mkdir(parents=True, exist_ok=True)

    should_close = False
    if cached_data is None and con is None:
        db_file = Path(db_path)
        if not db_file.exists():
            raise FileNotFoundError(f"DuckDB database not found at {db_file}")
        con = duckdb.connect(str(db_file), read_only=True)
        should_close = True

    try:
        if not dev_mode:
            logger.info("==================================================")
            logger.info("Philippine Commodity Price Tracker: Dashboard Generator")
            logger.info("Source DuckDB : %s", Path(db_path).resolve() if should_close else "Custom/Cached")
            logger.info("Template Dir  : %s", tpl_path.resolve())
            logger.info("Target Output : %s", dest)
            logger.info("==================================================")

        data_ctx = cached_data if cached_data is not None else load_all_dashboard_data(con)
        resolved_build_id = build_id or str(int(time.time()))

        html_content = render_dashboard_html(
            con=con,
            template_dir=tpl_path,
            cached_data=data_ctx,
            dev_mode=dev_mode,
            build_id=resolved_build_id,
        )
        dest.write_text(html_content, encoding="utf-8")

        assets = write_modular_assets(
            template_dir=tpl_path.resolve(),
            public_dir=public_dir,
            data_ctx=data_ctx,
        )

        if not dev_mode:
            html_kb = dest.stat().st_size / 1024
            data_kb = assets["data"].stat().st_size / 1024 if assets["data"].exists() else 0.0
            elapsed = time.time() - start_time
            logger.info("Dashboard successfully generated in %.3f seconds!", elapsed)
            logger.info("  HTML Shell : %.1f KB (%s)", html_kb, dest)
            logger.info("  Data Asset : %.1f KB (%s)", data_kb, assets["data"])
            logger.info("  Stylesheet : %s", assets["css"])
            logger.info("  Controller : %s", assets["js"])
            logger.info("==================================================")

        return dest
    finally:
        if should_close and con is not None:
            con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate modular static HTML dashboard from DuckDB or run live dev watcher."
    )
    parser.add_argument(
        "--db",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--templates",
        type=str,
        default=str(DEFAULT_TEMPLATE_DIR),
        help=f"Path to templates directory (default: {DEFAULT_TEMPLATE_DIR})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_HTML),
        help=f"Output destination HTML path (default: {DEFAULT_OUTPUT_HTML})",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run live file watcher and local HTTP dev server with browser auto-reload.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host interface for live dev server (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for live dev server (default: 8000)",
    )

    args = parser.parse_args()

    if args.watch:
        run_dev_watcher(
            db_path=Path(args.db),
            template_dir=Path(args.templates),
            output_path=Path(args.output),
            build_fn=build_dashboard,
            host=args.host,
            port=args.port,
        )
    else:
        output_file = build_dashboard(
            db_path=args.db,
            template_dir=args.templates,
            output_path=args.output,
        )
        print(f"Generated dashboard at: {output_file}")
