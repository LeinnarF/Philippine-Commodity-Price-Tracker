#!/usr/bin/env bash
# Quick start script for the live development server

set -euo pipefail

# Navigate to project root directory (directory where this script lives)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Select Python executable (prefer .venv if present)
if [ -x ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
else
    echo "Error: Python was not found on your system." >&2
    exit 1
fi

# Ensure DuckDB warehouse exists; build if missing
if [ ! -f "data/commodities.duckdb" ]; then
    echo "Building DuckDB analytical warehouse (data/commodities.duckdb)..."
    "$PYTHON_BIN" -m pipeline.load_duckdb
fi

# Default port or pass-through arguments
PORT="${PORT:-8000}"

echo "Starting development server with live reload on http://127.0.0.1:${PORT}..."
exec "$PYTHON_BIN" -m pipeline.build_dashboard --watch --port "$PORT" "$@"
