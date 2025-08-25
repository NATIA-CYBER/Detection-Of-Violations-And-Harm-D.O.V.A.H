from __future__ import annotations
import argparse, json, os
from pathlib import Path
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from .iforest import IForestModel, IForestConfig

def _load_events(p: Path) -> List[Dict[str, Any]]:
    out = []
    with p.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                out.append(json.loads(line))
    return out

def train_cmd(train_files: List[str], model_out: str) -> int:
    events: List[Dict[str, Any]] = []
    for f in train_files:
        events.extend(_load_events(Path(f)))
    model = IForestModel(IForestConfig())
    model.fit(events)
    model.save(model_out)
    print(f"[iforest] saved -> {model_out} (n={len(events)})")
    return 0

def score_cmd(model_in: str, test_file: str, db_url: str) -> int:
    model = IForestModel.load(model_in)
    events = _load_events(Path(test_file))
    scores = model.predict(events)  # {session_id: {score, ts}}
    eng = create_engine(db_url)
    ins = text("""
        INSERT INTO detections (ts, session_id, window_id, score, source, model_version, created_at)
        VALUES (:ts, :session_id, 0, :score, 'iforest', 'v0', CURRENT_TIMESTAMP)
    """)
    with eng.begin() as cx:
        for sid, d in scores.items():
            cx.execute(ins, {"ts": d["ts"], "session_id": sid, "score": d["score"]})
    print(f"[iforest] wrote {len(scores)} detections")
    return 0

def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    tr = sub.add_parser("train")
    tr.add_argument("--model-out", required=True)
    tr.add_argument("--train", nargs="+", required=True)

    sc = sub.add_parser("score")
    sc.add_argument("--model-in", required=True)
    sc.add_argument("--test", required=True)
    sc.add_argument("--db", default=(os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""))

    args = ap.parse_args()
    if args.cmd == "train":
        return train_cmd(args.train, args.model_out)
    return score_cmd(args.model_in, args.test, args.db)

if __name__ == "__main__":
    raise SystemExit(main())
