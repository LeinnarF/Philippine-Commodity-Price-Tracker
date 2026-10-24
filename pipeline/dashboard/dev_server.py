"""
Live Development File Watcher & Local HTTP Server with Auto-Reload.

Monitors template files (.html, .css, .js) and the DuckDB database file.
Automatically rebuilds the static dashboard on save and notifies connected
browsers via the /__dev_poll__ endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
import threading
import time
from typing import Any, Callable

import duckdb

from pipeline.dashboard.queries import load_all_dashboard_data

logger = logging.getLogger("build_dashboard.watch")


@dataclass
class DashboardDevState:
    """Shared state between the file watcher thread and HTTP request handler."""

    build_id: str
    last_error: str | None = None


def collect_watch_mtimes(
    template_dir: Path,
    db_path: Path,
) -> tuple[dict[str, float], float]:
    """
    Snapshots modification timestamps for all template/asset files and the DuckDB file.
    """
    template_mtimes: dict[str, float] = {}
    if template_dir.exists():
        for path in sorted(template_dir.rglob("*")):
            if path.is_file() and path.suffix in {".html", ".css", ".js", ".json"}:
                try:
                    template_mtimes[str(path.resolve())] = path.stat().st_mtime
                except OSError:
                    continue

    db_mtime = 0.0
    if db_path.exists():
        try:
            db_mtime = db_path.stat().st_mtime
        except OSError:
            db_mtime = 0.0

    return template_mtimes, db_mtime


class DevServerRequestHandler(SimpleHTTPRequestHandler):
    """
    HTTP handler serving public/ static files and the /__dev_poll__ endpoint.
    """

    def __init__(
        self,
        *args: Any,
        dev_state: DashboardDevState,
        **kwargs: Any,
    ) -> None:
        self.dev_state = dev_state
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        if self.path.startswith("/__dev_poll__"):
            payload = json.dumps({
                "build_id": self.dev_state.build_id,
                "error": self.dev_state.last_error,
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        super().do_GET()

    def end_headers(self) -> None:
        # Disable caching during live development so CSS/JS edits appear immediately
        if not self.path.startswith("/__dev_poll__"):
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        super().end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        # Silence high-frequency /__dev_poll__ heartbeat logs
        if self.path.startswith("/__dev_poll__"):
            return
        logger.info("[HTTP] %s - %s", self.address_string(), format % args)


def run_dev_watcher(
    db_path: Path,
    template_dir: Path,
    output_path: Path,
    build_fn: Callable[..., Path],
    host: str = "127.0.0.1",
    port: int = 8000,
    poll_interval: float = 0.5,
    max_cycles: int | None = None,
) -> None:
    """
    Runs an initial build, starts a background HTTP server on host:port, and
    watches template_dir + db_path for changes to trigger instant rebuilds.
    """
    db_file = Path(db_path).resolve()
    tpl_dir = Path(template_dir).resolve()
    out_file = Path(output_path).resolve()
    serve_dir = out_file.parent

    # Cache DuckDB data in memory so template/CSS/JS edits rebuild in <50ms
    logger.info("Loading initial data cache from DuckDB: %s", db_file)
    with duckdb.connect(str(db_file), read_only=True) as con:
        cached_data = load_all_dashboard_data(con)

    initial_build_id = str(int(time.time() * 1000))
    build_fn(
        db_path=db_file,
        template_dir=tpl_dir,
        output_path=out_file,
        cached_data=cached_data,
        dev_mode=True,
        build_id=initial_build_id,
    )

    dev_state = DashboardDevState(build_id=initial_build_id)
    prev_tpl_mtimes, prev_db_mtime = collect_watch_mtimes(tpl_dir, db_file)

    handler_cls = partial(
        DevServerRequestHandler,
        directory=str(serve_dir),
        dev_state=dev_state,
    )
    httpd = ThreadingHTTPServer((host, port), handler_cls)
    actual_port = httpd.server_address[1]

    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()

    logger.info("==================================================")
    logger.info("Live Development Server & Watcher Active")
    logger.info("  Local URL     : http://%s:%d", host, actual_port)
    logger.info("  Watching Tpls : %s", tpl_dir)
    logger.info("  Watching DB   : %s", db_file)
    logger.info("  Press Ctrl+C to stop")
    logger.info("==================================================")

    cycles = 0
    try:
        while True:
            time.sleep(poll_interval)
            cycles += 1

            curr_tpl_mtimes, curr_db_mtime = collect_watch_mtimes(tpl_dir, db_file)
            db_changed = curr_db_mtime != prev_db_mtime
            tpl_changed = curr_tpl_mtimes != prev_tpl_mtimes

            if db_changed or tpl_changed:
                t0 = time.time()
                try:
                    if db_changed:
                        logger.info("DuckDB change detected -> refreshing warehouse cache...")
                        with duckdb.connect(str(db_file), read_only=True) as con:
                            cached_data = load_all_dashboard_data(con)
                    else:
                        changed_files = [
                            Path(k).name
                            for k, v in curr_tpl_mtimes.items()
                            if prev_tpl_mtimes.get(k) != v
                        ]
                        logger.info("Template/Asset change detected (%s) -> rebuilding...", ", ".join(changed_files) or "modified")

                    new_build_id = str(int(time.time() * 1000))
                    build_fn(
                        db_path=db_file,
                        template_dir=tpl_dir,
                        output_path=out_file,
                        cached_data=cached_data,
                        dev_mode=True,
                        build_id=new_build_id,
                    )
                    dev_state.build_id = new_build_id
                    dev_state.last_error = None
                    elapsed_ms = (time.time() - t0) * 1000
                    logger.info("Rebuild complete in %.1f ms (build_id=%s)", elapsed_ms, new_build_id)
                except Exception as exc:
                    dev_state.last_error = str(exc)
                    logger.error("Rebuild failed: %s", exc)

                prev_tpl_mtimes = curr_tpl_mtimes
                prev_db_mtime = curr_db_mtime

            if max_cycles is not None and cycles >= max_cycles:
                break
    except KeyboardInterrupt:
        logger.info("Shutting down live development server...")
    finally:
        httpd.shutdown()
        httpd.server_close()
