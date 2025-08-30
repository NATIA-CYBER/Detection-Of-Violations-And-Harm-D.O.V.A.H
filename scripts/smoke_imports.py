# scripts/smoke_imports.py
import importlib, traceback, sys
from pathlib import Path

# Make file-path runs work (add repo root so "src.*" is importable)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

mods = [
    "src.fusion.late_fusion",
    "src.models.anomaly.iforest",
    "src.models.anomaly.iforest_cli",
    "src.models.log_lm.score",
]

ok = True
for m in mods:
    try:
        importlib.import_module(m)
        print("OK:", m)
    except Exception:
        ok = False
        print("FAIL:", m)
        traceback.print_exc()

raise SystemExit(0 if ok else 1)
