
import argparse
import json
import sys
from pathlib import Path
import numpy as np

def main():
    ap = argparse.ArgumentParser(description="Check p95(lat_ms) against an SLA.")
    ap.add_argument("features", nargs="?", default="reports/day3/metrics/features.jsonl",
                    help="Path to features JSONL (default: reports/day3/metrics/features.jsonl)")
    ap.add_argument("--sla-ms", type=float, default=800, help="SLA threshold in ms (default: 800)")
    args = ap.parse_args()

    path = Path(args.features)
    if not path.exists():
        print(f"Error: {path} not found.")
        sys.exit(1)

    lats = []
    with path.open() as f:
        for line in f:
            try:
                val = json.loads(line).get("lat_ms")
                if val is not None:
                    lats.append(float(val))
            except Exception:
                
                pass

    if not lats:
        print("No latency data found.")
        sys.exit(1)

    p95 = float(np.percentile(lats, 95))
    ok = p95 < args.sla_ms
    print(f"windows={len(lats)}  p95={p95:.1f} ms  pass={ok}")
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
