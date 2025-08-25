from __future__ import annotations
import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

from sqlalchemy import create_engine, text

from .metrics import EvalMetrics, EvalConfig


@dataclass(frozen=True)
class WeekWindow:
    start: datetime
    end: datetime


def _floor_to_week_start(dt: datetime) -> datetime:
    # Monday=0 … Sunday=6; align start to Monday 00:00
    weekday = dt.weekday()
    day_start = datetime(dt.year, dt.month, dt.day)
    return day_start - timedelta(days=weekday)


def discover_week_windows(engine, needed_weeks: int = 5) -> Dict[int, WeekWindow]:
    """
    Build week windows (1..needed_weeks) based on MIN(ts) in window_features.
    Each window covers 7 days, contiguous, no shuffling.
    """
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MIN(ts) AS min_ts FROM window_features")).first()
        if not row or not row.min_ts:
            raise RuntimeError("window_features is empty; cannot compute week windows")
        anchor = _floor_to_week_start(row.min_ts if isinstance(row.min_ts, datetime) else row.min_ts.to_pydatetime())
    out: Dict[int, WeekWindow] = {}
    for i in range(needed_weeks):
        start = anchor + timedelta(days=7 * i)
        end = start + timedelta(days=7)
        out[i + 1] = WeekWindow(start=start, end=end)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Time-based split evaluation (weeks 1–3 train, 4 val, 5 test)")
    ap.add_argument("--weeks", type=int, default=5, help="number of contiguous weeks to materialize (default: 5)")
    ap.add_argument("--out-json", type=str, default="docs/metrics/split_eval.json", help="output metrics JSON")
    ap.add_argument("--print", dest="do_print", action="store_true", help="print metrics")
    args = ap.parse_args()

    cfg = EvalConfig()
    ev = EvalMetrics(cfg)
    engine = ev.engine

    weeks = discover_week_windows(engine, needed_weeks=args.weeks)
    if 5 not in weeks:
        raise RuntimeError("Need at least 5 weeks of data for this split.")

    # Train = weeks 1–3 (your training code runs elsewhere; this script evaluates val/test)
    train_w = (weeks[1], weeks[2], weeks[3])
    val_w = weeks[4]
    test_w = weeks[5]

    # Evaluate validation week
    val_metrics = ev.evaluate(val_w.start, val_w.end)

    # Evaluate test week
    test_metrics = ev.evaluate(test_w.start, test_w.end)

    result = {
        "train_weeks": [
            {"start": w.start.isoformat(), "end": w.end.isoformat()} for w in train_w
        ],
        "val_week": {"start": val_w.start.isoformat(), "end": val_w.end.isoformat()},
        "test_week": {"start": test_w.start.isoformat(), "end": test_w.end.isoformat()},
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
    }

    # Persist
    out_path = args.out_json
    from pathlib import Path
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        json.dump(result, fh, indent=2)

    if args.do_print:
        print(json.dumps(result, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
