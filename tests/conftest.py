import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / 'src'
for path in (PROJECT_DIR, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
