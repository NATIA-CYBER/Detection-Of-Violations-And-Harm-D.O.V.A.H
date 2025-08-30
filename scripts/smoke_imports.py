# scripts/smoke_imports.py
import importlib
import traceback

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
