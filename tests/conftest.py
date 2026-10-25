import sys
from pathlib import Path
import pytest

# Ensure repo root is on sys.path for test discovery in CI
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


@pytest.fixture(scope="session", autouse=True)
def ensure_duckdb_exists():
    """
    On a fresh clone in CI (where commodities.duckdb is gitignored),
    automatically loads the embedded analytical warehouse from tracked CSVs (< 1s)
    so all query, snapshot, and dashboard tests execute and pass without skipping.
    """
    from pipeline.load_duckdb import (
        DEFAULT_CLEAN_CSV,
        DEFAULT_DB_PATH,
        DEFAULT_LOGS_CSV,
        DEFAULT_SELECTED_CSV,
        load_duckdb,
    )

    if not DEFAULT_DB_PATH.exists() and DEFAULT_CLEAN_CSV.exists() and DEFAULT_SELECTED_CSV.exists():
        con = load_duckdb(
            db_path=DEFAULT_DB_PATH,
            clean_csv=DEFAULT_CLEAN_CSV,
            selected_csv=DEFAULT_SELECTED_CSV,
            logs_csv=DEFAULT_LOGS_CSV,
        )
        con.close()
