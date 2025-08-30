# scripts/prepare_day4_data.py
from __future__ import annotations
import argparse, csv, json, math, sys
from pathlib import Path
from typing import List, Dict, Any

ID_KEYS = ("window_id", "session_id", "id")
LABEL_KEYS = ("label", "y", "is_attack", "target")
TS_KEYS = ("ts", "timestamp", "time", "event_time")

def read_jsonl(p: Path) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    if not p.exists(): return recs
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip(): continue
        try:
            recs.append(json.loads(line))
        except Exception:
            pass
    return recs

def read_csv(p: Path) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    with p.open(newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            recs.append(row)
    return recs

def to_number(x) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0

def pick_key(candidates, sample: Dict[str, Any]) -> str | None:
    for k in candidates:
        if k in sample: return k
    return None

def get_ts(rec: Dict[str, Any]) -> float:
    for k in TS_KEYS:
        if k in rec:
            try: return float(rec[k])
            except Exception: pass
    return math.nan

def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")

def main() -> int:
    ap = argparse.ArgumentParser(description="Prepare Day-4 val/test events+labels from a labeled dataset.")
    ap.add_argument("--source", required=True,
                    help="Path to labeled dataset (.csv or .jsonl) containing an id + label + fields for events.")
    ap.add_argument("--val-ratio", type=float, default=0.5,
                    help="Fraction for validation split (time-based if ts exists). Default 0.5.")
    ap.add_argument("--val-out", default="sample_data/hdfs/val.jsonl")
    ap.add_argument("--test-out", default="sample_data/hdfs/test.jsonl")
    ap.add_argument("--val-labels", default="data/val/labels.jsonl")
    ap.add_argument("--test-labels", default="data/test/labels.jsonl")
    args = ap.parse_args()

    src = Path(args.source)
    if not src.exists():
        print(f"❌ Source not found: {src}", file=sys.stderr)
        return 2

    if src.suffix.lower() == ".csv":
        rows = read_csv(src)
    else:
        rows = read_jsonl(src)

    if not rows:
        print(f"❌ No rows in {src}", file=sys.stderr)
        return 3

    id_key = pick_key(ID_KEYS, rows[0]) or "window_id"
    label_key = pick_key(LABEL_KEYS, rows[0]) or "label"

    # Time-aware split if possible
    have_ts = pick_key(TS_KEYS, rows[0]) is not None
    if have_ts:
        rows.sort(key=get_ts)  # oldest → newest
    else:
        # deterministic: keep order; user can re-run with different ratio
        pass

    n = len(rows)
    n_val = max(1, int(n * args.val_ratio))
    val_rows = rows[:n_val]
    test_rows = rows[n_val:]

    # Events JSONL = original rows minus label
    def strip_label(r):
        d = dict(r)
        if label_key in d: d.pop(label_key, None)
        return d

    val_events = [strip_label(r) for r in val_rows]
    test_events = [strip_label(r) for r in test_rows]

    # Labels JSONL = {id_key: <id>, label: int}
    def to_label(r):
        return {
            id_key: r.get(id_key),
            "label": to_number(r.get(label_key, 0)),
        }

    val_labels = [to_label(r) for r in val_rows if id_key in r]
    test_labels = [to_label(r) for r in test_rows if id_key in r]

    # Write
    write_jsonl(Path(args.val_out), val_events)
    write_jsonl(Path(args.test_out), test_events)
    write_jsonl(Path(args.val_labels), val_labels)
    write_jsonl(Path(args.test_labels), test_labels)

    print(f"✅ Wrote:")
    print(f"  {args.val_out}        ({len(val_events)} events)")
    print(f"  {args.test_out}       ({len(test_events)} events)")
    print(f"  {args.val_labels}     ({len(val_labels)} labels, id={id_key})")
    print(f"  {args.test_labels}    ({len(test_labels)} labels, id={id_key})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
