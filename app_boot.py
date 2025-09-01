# app_boot.py (in C:\Projects\DataNavigator\app_boot.py)
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WEBAPP = ROOT / "webapp"

for p in (ROOT, WEBAPP):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)
