# scripts/prep_day4.py
"""
Prepare Day-4 inputs (val/test events + labels) from either:
  • REAL parsed events JSONL (recommended), or
  • the small sample bundle (demo).

It writes exactly what the eval/fusion expects:
  sample_data/hdfs/val/events.jsonl
  sample_data/hdfs/test/events.jsonl
  data/val/labels.jsonl
  data/test/labels.jsonl

Split logic:
  1) If --val-end and --test-start are provided AND timestamps exist -> time-based split.
  2) Else -> deterministic hash split by id (session_id/window_id/dedup_key/id) with --val-ratio.

Labels:
  • If event already has one of {label|y|is_attack|target} -> use it.
  • Else derive: label=1 if (any CVE in KEV) or (max EPSS >= --epss-threshold), else 0.

No non-stdlib deps. Streams large files (line-by-line).
"""

from __future__ import annotations
import argparse, csv, json, re, sys, hashlib
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime

# ---- Heuristics ----
CVERE = re.compile(r"CVE-\d{4}-\d{4,7}", re.IGNORECASE)
ID_PREF = ("session_id", "window_id", "dedup_key", "id")
TS_KEYS = ("timestamp", "ts", "event_time", "time")
LABEL_PREF = ("label", "y", "is_attack", "target")

def parse_iso_to_epoch(ts: Any) -> Optional[float]:
    if ts is None:
        return None
    s = str(ts)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        try:
            return float(s)
        except Exception:
            return None

def pick_id_key(sample: Dict[str, Any]) -> str:
    for k in ID_PREF:
        if k in sample:
            return k
    return "id"

def pick_label_key(sample: Dict[str, Any]) -> Optional[str]:
    for k in LABEL_PREF:
        if k in sample:
            return k
    return None

def extract_cves(ev: Dict[str, Any]) -> list[str]:
    # Prefer explicit lists if present
    for k in ("cves", "cve_ids", "cve_list"):
        v = ev.get(k)
        if isinstance(v, list) and v:
            return [str(x).upper() for x in v]
    # Fallback: scan common text fields
    hay = " ".join(str(ev.get(k, "")) for k in ("message","msg","detail","raw","component"))
    return [m.upper() for m in CVERE.findall(hay)]

def load_epss_map(csv_path: Optional[Path]) -> dict[str, float]:
    if not csv_path or not csv_path.exists():
        return {}
    m: dict[str, float] = {}
    with csv_path.open(newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        cols = { (h or "").strip().lower(): h for h in (r.fieldnames or []) }
        cve_col  = cols.get("cve") or cols.get("cveid") or cols.get("cve_id") or "cve"
        epss_col = cols.get("epss") or cols.get("epss_score") or cols.get("likelihood") or "epss"
        for row in r:
            cve = (row.get(cve_col) or "").strip().upper()
            if not cve:
                continue
            try:
                m[cve] = float(row.get(epss_col, "0") or 0.0)
            except Exception:
                pass
    return m

def scan_json_for_cves(obj: Any, acc: set[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            scan_json_for_cves(v, acc)
    elif isinstance(obj, list):
        for v in obj:
            scan_json_for_cves(v, acc)
    elif isinstance(obj, str):
        for m in CVERE.findall(obj):
            acc.add(m.upper())

def load_kev_set(json_path: Optional[Path]) -> set[str]:
    s: set[str] = set()
    if not json_path or not json_path.exists():
        return s
    try:
        obj = json.loads(json_path.read_text(encoding="utf-8"))
        scan_json_for_cves(obj, s)
    except Exception:
        pass
    return s

def derive_label(ev: Dict[str, Any], kev: set[str], epss: dict[str, float], epss_thr: float) -> int:
    cves = extract_cves(ev)
    if not cves:
        return 0
    if any(c in kev for c in cves):
        return 1
    mx = max((epss.get(c, 0.0) for c in cves), default=0.0)
    return 1 if mx >= epss_thr else 0

def decide_bucket(ts: Optional[float],
                  val_end: Optional[float],
                  test_start: Optional[float],
                  hash_key: str,
                  hash_ratio: float) -> str:
    # Prefer time windows if provided and ts exists
    if val_end is not None and test_start is not None and ts is not None:
        if ts <= val_end:
            return "val"
        if ts >= test_start:
            return "test"
        # If between windows, fall back to hash for stability
    # Deterministic hash split
    h = int.from_bytes(hashlib.sha256(hash_key.encode("utf-8")).digest()[:8], "big")
    return "val" if (h / (1 << 64)) < hash_ratio else "test"

def open_jsonl_writer(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    return p.open("w", encoding="utf-8")

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Prepare Day-4 val/test events+labels from REAL or sample JSONL (+ EPSS/KEV)."
    )
    ap.add_argument("--events", default="sample_data/hdfs/sample.jsonl",
                    help="Path to parsed events JSONL (default: sample_data/hdfs/sample.jsonl)")
    ap.add_argument("--epss", default="sample_data/security/epss_scores.csv",
                    help="EPSS CSV (optional; default: sample_data/security/epss_scores.csv)")
    ap.add_argument("--kev",  default="sample_data/security/kev_entries.json",
                    help="KEV JSON (optional; default: sample_data/security/kev_entries.json)")

    # Time-based split (recommended for real data)
    ap.add_argument("--val-end", help="Validation period end (ISO, e.g., 2025-07-31T23:59:59Z).")
    ap.add_argument("--test-start", help="Test period start (ISO, e.g., 2025-08-01T00:00:00Z).")

    # Hash split fallback
    ap.add_argument("--val-ratio", type=float, default=0.5, help="Validation fraction for hash split (default 0.5).")

    ap.add_argument("--epss-threshold", type=float, default=0.6, help="Label if max EPSS ≥ threshold (default 0.6).")

    # Outputs (fixed paths expected by the rest of the pipeline)
    ap.add_argument("--val-events-out", default="sample_data/hdfs/val/events.jsonl")
    ap.add_argument("--test-events-out", default="sample_data/hdfs/test/events.jsonl")
    ap.add_argument("--val-labels-out", default="data/val/labels.jsonl")
    ap.add_argument("--test-labels-out", default="data/test/labels.jsonl")

    args = ap.parse_args()

    events_path = Path(args.events)
    if not events_path.exists():
        print(f"❌ Events not found: {events_path}", file=sys.stderr)
        return 2

    epss_map = load_epss_map(Path(args.epss)) if args.epss else {}
    kev_set  = load_kev_set(Path(args.kev))   if args.kev  else set()

    def parse_boundary(s: Optional[str]) -> Optional[float]:
        if not s:
            return None
        # re-use parse_iso_to_epoch for ISO/epoch inputs
        return parse_iso_to_epoch(s)

    val_end_epoch    = parse_boundary(args.val_end)
    test_start_epoch = parse_boundary(args.test_start)
    if (args.val_end and val_end_epoch is None) or (args.test_start and test_start_epoch is None):
        print("❌ Could not parse --val-end/--test-start; use ISO like 2025-08-01T00:00:00Z", file=sys.stderr)
        return 2

    # Open outputs
    val_events_f  = open_jsonl_writer(Path(args.val_events_out))
    test_events_f = open_jsonl_writer(Path(args.test_events_out))
    val_labels_f  = open_jsonl_writer(Path(args.val_labels_out))
    test_labels_f = open_jsonl_writer(Path(args.test_labels_out))

    # Streaming pass
    seen_first: Optional[Dict[str, Any]] = None
    id_key: Optional[str] = None
    label_key: Optional[str] = None
    val_n = test_n = 0

    with events_path.open(encoding="utf-8") as fh:
        for raw in fh:
            s = raw.strip()
            if not s:
                continue
            try:
                ev = json.loads(s)
            except Exception:
                continue

            if seen_first is None:
                seen_first = ev
                id_key = pick_id_key(ev)
                label_key = pick_label_key(ev)

            # bucket decision
            ts: Optional[float] = None
            for k in TS_KEYS:
                if k in ev:
                    ts = parse_iso_to_epoch(ev[k])
                    if ts is not None:
                        break
            _id = str(ev.get(id_key or "id", "")) or "NOID"
            bucket = decide_bucket(ts, val_end_epoch, test_start_epoch, _id, args.val_ratio)

            # label
            if label_key and label_key in ev:
                try:
                    y = int(ev[label_key])
                except Exception:
                    try:
                        y = int(float(ev[label_key]))
                    except Exception:
                        y = 0
            else:
                y = derive_label(ev, kev_set, epss_map, args.epss_threshold)

            # event without label field (don’t leak target downstream)
            ev_out = dict(ev)
            if label_key:
                ev_out.pop(label_key, None)

            # ensure id present in labels
            if not _id or _id == "NOID":
                _id = f"e{val_n + test_n + 1}"

            if bucket == "val":
                val_events_f.write(json.dumps(ev_out) + "\n")
                val_labels_f.write(json.dumps({(id_key or "id"): _id, "label": y}) + "\n")
                val_n += 1
            else:
                test_events_f.write(json.dumps(ev_out) + "\n")
                test_labels_f.write(json.dumps({(id_key or "id"): _id, "label": y}) + "\n")
                test_n += 1

    for f in (val_events_f, test_events_f, val_labels_f, test_labels_f):
        f.close()

    print("✅ Prepared Day-4 inputs")
    print(f"  {args.val_events_out}   ({val_n} events)")
    print(f"  {args.test_events_out}  ({test_n} events)")
    print(f"  {args.val_labels_out}   ({val_n} labels)")
    print(f"  {args.test_labels_out}  ({test_n} labels)")
    print(f"  split = {'time' if (val_end_epoch is not None and test_start_epoch is not None) else 'hash('+str(args.val_ratio)+')'}")
    print(f"  id field = {id_key or 'id'}; labels = {'existing' if label_key else 'derived (KEV/EPSS≥'+str(args.epss_threshold)+')'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
