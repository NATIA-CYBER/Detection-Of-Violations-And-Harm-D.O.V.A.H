# scripts/prep_day4.py
from __future__ import annotations
import argparse, csv, json, re, sys, math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# --- Heuristics & field preferences
CVERE = re.compile(r"CVE-\d{4}-\d{4,7}", re.IGNORECASE)
ID_PREF = ("session_id", "window_id", "dedup_key", "id")
TS_KEYS = ("timestamp", "ts", "event_time", "time")
LABEL_PREF = ("label", "y", "is_attack", "target")

@dataclass
class Args:
    events: Path
    epss_csv: Optional[Path]
    kev_json: Optional[Path]
    out_root: Path
    val_ratio: float
    epss_threshold: float
    min_events: int
    seed: int

# ---------- IO helpers ----------
def read_jsonl(p: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not p.exists():
        return out
    with p.open(encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except Exception:
                pass
    return out

def write_jsonl(p: Path, rows: Iterable[Dict[str, Any]]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")

def load_epss_map(csv_path: Optional[Path]) -> Dict[str, float]:
    if not csv_path or not csv_path.exists():
        return {}
    m: Dict[str, float] = {}
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

# ---------- Field selection ----------
def pick_label_key(sample: Dict[str, Any]) -> Optional[str]:
    for k in LABEL_PREF:
        if k in sample:
            return k
    return None

def pick_id_key(sample: Dict[str, Any]) -> str:
    for k in ID_PREF:
        if k in sample:
            return k
    return "id"

def parse_ts(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        try:
            return float(s)
        except Exception:
            return None

def ts_key(ev: Dict[str, Any]) -> Tuple[int, float]:
    for k in TS_KEYS:
        if k in ev:
            t = parse_ts(ev[k])
            if t is not None:
                return (1, t)
    return (0, 0.0)

def extract_cves(ev: Dict[str, Any]) -> List[str]:
    for k in ("cves", "cve_ids", "cve_list"):
        v = ev.get(k)
        if isinstance(v, list) and v:
            return [str(x).upper() for x in v]
    hay = " ".join(str(ev.get(k, "")) for k in ("message","msg","detail","raw","component"))
    return [m.upper() for m in CVERE.findall(hay)]

# ---------- Labeling ----------
def derive_label(ev: Dict[str, Any], kev: set[str], epss: Dict[str, float], epss_thr: float) -> int:
    cves = extract_cves(ev)
    if not cves:
        return 0
    if any(c in kev for c in cves):
        return 1
    mx = max((epss.get(c, 0.0) for c in cves), default=0.0)
    return 1 if mx >= epss_thr else 0

# ---------- Demo inflation (optional) ----------
def inflate_events(events: List[Dict[str, Any]], target: int, seed: int) -> List[Dict[str, Any]]:
    """For demos only: deterministically duplicate events to reach at least `target` rows."""
    if len(events) >= target:
        return events
    out = list(events)
    base_ts = None
    # try to capture a baseline timestamp
    for ev in events:
        for k in TS_KEYS:
            if k in ev:
                t = parse_ts(ev[k])
                if t is not None:
                    base_ts = t
                    break
        if base_ts is not None:
            break
    i = 0
    while len(out) < target:
        src = events[i % len(events)]
        dup = dict(src)
        # make a unique id
        if "session_id" in dup:
            dup["session_id"] = f"{dup['session_id']}_d{len(out)}"
        elif "window_id" in dup:
            dup["window_id"] = f"{dup['window_id']}_d{len(out)}"
        else:
            dup["id"] = f"e{len(out)}"
        # nudge timestamp to keep chronological order (1s steps)
        for k in TS_KEYS:
            if k in dup:
                t = parse_ts(dup[k])
                if t is None and base_ts is not None:
                    t = base_ts
                if t is not None:
                    t = t + (len(out) - len(events))
                    dup[k] = datetime.fromtimestamp(t, tz=timezone.utc).isoformat().replace("+00:00","Z")
                break
        out.append(dup)
        i += 1
    return out

# ---------- Main build ----------
def build(a: Args) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], str]:
    events = read_jsonl(a.events)
    if not events:
        return [], [], [], [], f"❌ No events found at {a.events}"

    # Optional inflation for demo-scale evaluation (not for production)
    if a.min_events > 0:
        events = inflate_events(events, a.min_events, a.seed)

    # Determine keys
    id_key = pick_id_key(events[0])
    label_key = pick_label_key(events[0])  # may be None

    kev = load_kev_set(a.kev_json)
    epss = load_epss_map(a.epss_csv)

    # Sort chronologically when possible
    events.sort(key=ts_key)

    n = len(events)
    n_val = max(1, int(n * a.val_ratio))
    val_ev = events[:n_val]
    test_ev = events[n_val:]

    def get_label(ev: Dict[str, Any]) -> int:
        if label_key and label_key in ev:
            try:
                return int(ev[label_key])
            except Exception:
                try:
                    return int(float(ev[label_key]))
                except Exception:
                    return 0
        return derive_label(ev, kev, epss, a.epss_threshold)

    def strip_label(ev: Dict[str, Any]) -> Dict[str, Any]:
        d = dict(ev)
        if label_key:
            d.pop(label_key, None)
        return d

    def ensure_id(ev: Dict[str, Any], idx: int) -> Any:
        if id_key in ev:
            return ev[id_key]
        return f"e{idx}"

    val_events_out = [strip_label(ev) for ev in val_ev]
    test_events_out = [strip_label(ev) for ev in test_ev]

    val_labels_out = [{id_key: ensure_id(ev, i), "label": get_label(ev)} for i, ev in enumerate(val_ev)]
    test_labels_out = [{id_key: ensure_id(ev, n_val+i), "label": get_label(ev)} for i, ev in enumerate(test_ev)]

    # Write
    val_events_path  = a.out_root / "val"  / "events.jsonl"
    test_events_path = a.out_root / "test" / "events.jsonl"
    val_labels_path  = Path("data/val/labels.jsonl")
    test_labels_path = Path("data/test/labels.jsonl")

    write_jsonl(val_events_path,  val_events_out)
    write_jsonl(test_events_path, test_events_out)
    write_jsonl(val_labels_path,  val_labels_out)
    write_jsonl(test_labels_path, test_labels_out)

    msg = (
        f"✅ Prepared Day-4 inputs\n"
        f"  {val_events_path}   ({len(val_events_out)} events)\n"
        f"  {test_events_path}  ({len(test_events_out)} events)\n"
        f"  {val_labels_path}   ({len(val_labels_out)} labels)\n"
        f"  {test_labels_path}  ({len(test_labels_out)} labels)\n"
        f"  id field = {id_key}; labels = {'existing' if label_key else 'derived (KEV/EPSS≥'+str(a.epss_threshold)+')'}"
    )
    return val_events_out, test_events_out, val_labels_out, test_labels_out, msg

def parse_args() -> Args:
    ap = argparse.ArgumentParser(
        description="Prepare Day-4 val/test events+labels from events JSONL (+optional EPSS/KEV)."
    )
    ap.add_argument("--events", default="sample_data/hdfs/sample.jsonl",
                    help="Events JSONL (default: sample_data/hdfs/sample.jsonl)")
    ap.add_argument("--epss", default="sample_data/security/epss_scores.csv",
                    help="EPSS CSV (optional; default: sample_data/security/epss_scores.csv)")
    ap.add_argument("--kev",  default="sample_data/security/kev_entries.json",
                    help="KEV JSON (optional; default: sample_data/security/kev_entries.json)")
    ap.add_argument("--out-root", default="sample_data/hdfs",
                    help="Where to write val/test/events.jsonl (default: sample_data/hdfs)")
    ap.add_argument("--val-ratio", type=float, default=0.5, help="Validation fraction (default 0.5)")
    ap.add_argument("--epss-threshold", type=float, default=0.6, help="Label if max EPSS ≥ threshold (default 0.6)")
    ap.add_argument("--min-events", type=int, default=0,
                    help="DEMO ONLY: inflate events to at least this many by duplicating (default 0 = no inflate)")
    ap.add_argument("--seed", type=int, default=0, help="Inflation determinism (default 0)")
    a = ap.parse_args()
    return Args(
        events=Path(a.events),
        epss_csv=Path(a.epss) if a.epss else None,
        kev_json=Path(a.kev) if a.kev else None,
        out_root=Path(a.out_root),
        val_ratio=float(a.val_ratio),
        epss_threshold=float(a.epss_threshold),
        min_events=int(a.min_events),
        seed=int(a.seed),
    )

def main() -> int:
    args = parse_args()
    if not args.events.exists():
        print(f"❌ Events not found: {args.events}", file=sys.stderr)
        return 2
    _, _, _, _, msg = build(args)
    print(msg)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
