import sys
from pathlib import Path

# Ensure repo root is on sys.path for test discovery in CI
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
