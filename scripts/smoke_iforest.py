# scripts/smoke_iforest.py
from src.models.anomaly.iforest import IForestModel, IForestConfig
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
