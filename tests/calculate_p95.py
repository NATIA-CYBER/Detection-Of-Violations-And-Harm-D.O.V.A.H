# tests/calculate_p95.py
from __future__ import annotations
import argparse, json, sys, csv, math
from pathlib import Path

def percentile(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values)-1) * (p/100.0)
    f = math.floor(k); c = math.ceil(k)
    if f == c: 
        return float(values[int(k)])
    d0 = values[f] * (c-k)
    d1 = values[c] * (k-f)
    return float(d0 + d1)

def read_csv_latencies(p: Path) -> list[float]:
    out = []
    if not p.exists():
        return out
    with p.open(newline="") as fh:
        r = csv.DictReader(fh)
        for row in r:
            # Prefer features-only timers if present; fall back to totals.
            for key in ("features_ms","stage_features_ms","total_ms","wall_ms","lat_ms"):
                if key in row and row[key]:
                    try:
                        out.append(float(row[key]))
                        break
                    except Exception:
                        pass
    return out

def read_jsonl_latencies(p: Path) -> list[float]:
    out = []
    if not p.exists():
        return out
    with p.open() as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            for key in ("features_ms","stage_features_ms","total_ms","wall_ms","lat_ms"):
                if key in obj:
                    try:
                        out.append(float(obj[key]))
                        break
                    except Exception:
                        pass
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="latency.csv (preferred) or features.jsonl")
    ap.add_argument("--sla-ms", type=float, default=800.0)
    args = ap.parse_args()

    p = Path(args.path)
    # If we were given features.jsonl, try using sibling latency.csv instead.
    if p.suffix.lower() == ".csv":
        lat = read_csv_latencies(p)
    else:
        maybe_csv = p.with_name("latency.csv")
        lat = read_csv_latencies(maybe_csv) if maybe_csv.exists() else read_jsonl_latencies(p)

    # Drop obvious warmup and final flush if enough samples
    if len(lat) >= 6:
        lat = lat[3:-1]

    p95 = percentile(lat, 95) if lat else float("nan")
    ok = (p95 <= args.sla_ms) and not math.isnan(p95)

    print(f"windows={len(lat)}  p95={p95:.1f} ms  pass={ok}")

    # Emit p95 for artifact viewing
    outdir = Path("reports/phase3")
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "p95.txt").write_text(f"{p95:.1f}\n", encoding="utf-8")

    return 0 if ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
    
