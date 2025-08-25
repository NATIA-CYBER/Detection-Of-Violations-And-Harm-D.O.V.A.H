from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

def _parse_ts(v: Any) -> datetime:
    if isinstance(v, (int, float)):
        # treat as seconds epoch
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        s = v.strip().replace(" ", "T")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    raise ValueError(f"Unparseable ts: {v!r}")

def _bounds(ts_list: List[datetime], parts: int) -> List[datetime]:
    mn, mx = min(ts_list), max(ts_list)
    if mx <= mn:
        return [mn]*(parts+1)
    span = (mx - mn) / parts
    return [mn + span*i for i in range(parts)] + [mx]

def split_jsonl_into_slices(input_file: Path, out_dir: Path, slices: int = 5) -> Dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_list: List[datetime] = []
    rows: List[Dict[str, Any]] = []
    with input_file.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            ts = _parse_ts(obj["ts"])
            ts_list.append(ts)
            rows.append({"ts": ts, "obj": obj})
    if not rows:
        raise RuntimeError("No records in input")

    bnds = _bounds(ts_list, slices)
    # open slice writers
    fhs = [ (out_dir / f"slice{i+1}.jsonl").open("w", encoding="utf-8") for i in range(slices) ]
    counts = { f"slice{i+1}": 0 for i in range(slices) }

    for r in rows:
        ts = r["ts"]
        # find bin
        idx = 0
        while idx < slices and not (bnds[idx] <= ts <= bnds[idx+1]):
            idx += 1
        if idx >= slices:
            idx = slices - 1
        fhs[idx].write(json.dumps(r["obj"]) + "\n")
        counts[f"slice{idx+1}"] += 1

    for fh in fhs:
        fh.close()
    return counts

def main() -> int:
    ap = argparse.ArgumentParser(description="Time-ordered N-slice split")
    ap.add_argument("--input-file", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--slices", type=int, default=5)
    args = ap.parse_args()
    counts = split_jsonl_into_slices(Path(args.input_file), Path(args.output_dir), slices=args.slices)
    for k in sorted(counts):
        print(f"{k}: {counts[k]}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
