from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import List, Dict, Any, Iterable, Iterator, Optional

from .iforest import IForestModel, IForestConfig


def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _load_events(files: Iterable[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for f in files:
        out.extend(_iter_jsonl(Path(f)))
    return out


def train_cmd(train_files: List[str], model_out: str) -> int:
    events = _load_events(train_files)
    model = IForestModel(IForestConfig())
    model.fit(events)
    Path(model_out).parent.mkdir(parents=True, exist_ok=True)
    model.save(model_out)
    print(f"[iforest] saved -> {model_out} (n={len(events)})")
    return 0


def score_cmd(model_in: str, test_file: str, db_url: Optional[str]) -> int:
    model = IForestModel.load(model_in)
    events = _iter_jsonl(Path(test_file))
    events_list = list(events)
    scores = model.predict(events_list)  # {session_id: {score, ts}}

    if db_url:
        try:
            from sqlalchemy import create_engine, text  # optional import
            eng = create_engine(db_url)
            ins = text(
                """
                INSERT INTO detections (ts, session_id, window_id, score, source, model_version, created_at)
                VALUES (:ts, :session_id, :window_id, :score, 'iforest', 'v0', CURRENT_TIMESTAMP)
                """
            )
            with eng.begin() as cx:
                for sid, d in scores.items():
                    cx.execute(
                        ins,
                        {"ts": d["ts"], "session_id": sid, "window_id": 0, "score": d["score"]},
                    )
            print(f"[iforest] wrote {len(scores)} detections to DB")
        except Exception as e:
            # Do not fail CI just because DB is unavailable
            print(f"[iforest] DB write skipped due to error: {e}")
    else:
        print(f"[iforest] scored {len(scores)} windows (no DB URL provided)")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m src.models.anomaly.iforest_cli")
    sub = ap.add_subparsers(dest="cmd", required=True)

    tr = sub.add_parser("train", help="Train and save model from JSONL files")
    tr.add_argument("--model-out", required=True)
    tr.add_argument("--train", nargs="+", required=True)

    sc = sub.add_parser("score", help="Score JSONL file with a saved model")
    sc.add_argument("--model-in", required=True)
    sc.add_argument("--test", required=True)
    sc.add_argument(
        "--db",
        default=(os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""),
        help="Optional SQLAlchemy DB URL; if empty, results are not persisted.",
    )

    args = ap.parse_args()
    if args.cmd == "train":
        return train_cmd(args.train, args.model_out)
    return score_cmd(args.model_in, args.test, args.db or None)


if __name__ == "__main__":
    raise SystemExit(main())
