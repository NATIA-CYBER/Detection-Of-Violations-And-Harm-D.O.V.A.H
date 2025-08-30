# scripts/smoke_iforest.py
import sys
from pathlib import Path

# Ensure repo root is on sys.path so "src.*" imports work even when run by file path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.models.anomaly.iforest import IForestModel, IForestConfig  # noqa
import random, time

r = random.Random(0)

def fake_event(sid: int):
    return {
        "session_id": f"s{sid}",
        "ts": int(time.time()),
        "event_count": r.randint(1, 20),
        "unique_components": r.randint(1, 10),
        "error_ratio": r.random(),
        "template_entropy": r.random() * 3,
        "component_entropy": r.random() * 2,
    }

def main() -> int:
    train = [fake_event(i) for i in range(100)]
    model = IForestModel(IForestConfig())
    model.fit(train)
    pred = model.predict([fake_event(i) for i in range(5)])
    print("Scored:", pred)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
