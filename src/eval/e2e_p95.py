from __future__ import annotations
import argparse
import csv
import subprocess
from pathlib import Path
import numpy as np


def run_pipeline(phase: str) -> None:
    # Mirrors your CI call: avoids conda by overriding PY/SH
    cmd = ["make", f"{phase}-all", f"PHASE={phase}", "PY=python", 'SH=sh -c']
    print(f"+ Running pipeline: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def compute_p95_from_latency_csv(csv_path: Path) -> float:
    if not csv_path.exists():
        raise FileNotFoundError(f"latency file not found: {csv_path}")
    vals = []
    with csv_path.open() as fh:
        reader = csv.DictReader(fh)
        # Expect a column named 'lat_ms' (match your features writer)
        col = "lat_ms"
        if col not in reader.fieldnames:
            raise RuntimeError(f"Expected column '{col}' in {csv_path}, got {reader.fieldnames}")
        for row in reader:
            try:
                vals.append(float(row[col]))
            except Exception:
                continue
    if not vals:
        raise RuntimeError(f"No latency values parsed from {csv_path}")
    return float(np.percentile(vals, 95))


def main() -> int:
    ap = argparse.ArgumentParser(description="Run E2E pipeline and compute P95 latency")
    ap.add_argument("--phase", default="phase3", choices=["phase3", "phase4"], help="which Makefile phase to run")
    ap.add_argument("--metrics-dir", default=None, help="override metrics dir (default uses reports/{phase}/metrics)")
    args = ap.parse_args()

    # 1) Run the pipeline (replay -> features -> accept -> tests)
    run_pipeline(args.phase)

    # 2) Locate the latency CSV produced by the run
    metrics_dir = Path(args.metrics_dir) if args.metrics_dir else Path(f"reports/{args.phase}/metrics")
    latency_csv = metrics_dir / "latency.csv"

    # 3) Compute P95
    p95 = compute_p95_from_latency_csv(latency_csv)
    print(f"P95 end-to-end latency (ms): {p95:.2f}")

    # 4) Write a small stamp file alongside p95.txt, if you want
    out_txt = metrics_dir.parent / "p95.txt"
    out_txt.write_text(f"{p95:.2f}\n", encoding="utf-8")
    print(f"Wrote {out_txt}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
