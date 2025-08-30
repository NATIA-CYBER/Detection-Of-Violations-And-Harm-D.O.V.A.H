# scripts/calibrate_thresholds.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import sys
from typing import List, Dict, Any
import numpy as np

# Make file-path runs work (src layout)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def load_jsonl_scores(p: Path, score_key: str, label_key: str) -> tuple[np.ndarray, np.ndarray]:
    scores: List[float] = []
    labels: List[int] = []
    for line in p.read_text().splitlines():
        if not line.strip():
            continue
        try:
            obj: Dict[str, Any] = json.loads(line)
        except Exception:
            continue
        if score_key not in obj or label_key not in obj:
            continue
        try:
            s = float(obj[score_key])
            y = int(obj[label_key])
        except Exception:
            continue
        scores.append(s); labels.append(y)
    if not scores:
        raise SystemExit(f"No usable rows in {p} with keys '{score_key}' and '{label_key}'")
    return np.asarray(scores, dtype=float), np.asarray(labels, dtype=int)

def pick_threshold(scores: np.ndarray, labels: np.ndarray, fp1k_cap: float) -> dict:
    # unique thresholds from scores (sorted)
    grid = np.unique(np.round(scores, 6))
    if grid.size == 0:
        return {"t": 0.5, "precision": 0.0, "recall": 0.0, "fp_per_1k": float("inf")}
    best = None
    for t in grid:
        pred = (scores >= t).astype(int)
        tp = int(((pred == 1) & (labels == 1)).sum())
        fp = int(((pred == 1) & (labels == 0)).sum())
        fn = int(((pred == 0) & (labels == 1)).sum())
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        fp1k = 1000.0 * fp / len(labels)
        cand = {"t": float(t), "precision": prec, "recall": rec, "fp_per_1k": fp1k}
        # Keep the best under cap by (precision desc, recall desc, threshold asc)
        if fp1k <= fp1k_cap:
            if best is None or (prec, rec, -t) > (best["precision"], best["recall"], -best["t"]):
                best = cand
    # If nothing satisfies cap, take max precision overall
    if best is None:
        best = max(
            (
                {
                    "t": float(t),
                    "precision": (int(((scores >= t) & (labels == 1)).sum()))
                                 / max(1, int((scores >= t).sum())),
                    "recall": (int(((scores >= t) & (labels == 1)).sum()))
                              / max(1, int((labels == 1).sum())),
                    "fp_per_1k": 1000.0 * int(((scores >= t) & (labels == 0)).sum()) / len(labels),
                }
                for t in grid
            ),
            key=lambda d: (d["precision"], d["recall"], -d["t"])
        )
    return best

def main() -> int:
    ap = argparse.ArgumentParser(description="Calibrate threshold on validation set.")
    ap.add_argument("--pred", required=True, help="Validation JSONL with 'score' and 'label' per row")
    ap.add_argument("--model", default="fusion", help="Model name key to store in thresholds.json")
    ap.add_argument("--out", default="docs/metrics/thresholds.json", help="Output thresholds JSON")
    ap.add_argument("--score-key", default="score")
    ap.add_argument("--label-key", default="label")
    ap.add_argument("--fp1k-cap", type=float, default=5.0, help="Max FP per 1000 windows")
    args = ap.parse_args()

    scores, labels = load_jsonl_scores(Path(args.pred), args.score_key, args.label_key)
    best = pick_threshold(scores, labels, args.fp1k_cap)
    out_p = Path(args.out); out_p.parent.mkdir(parents=True, exist_ok=True)
    data = {}
    if out_p.exists():
        try:
            data = json.loads(out_p.read_text())
        except Exception:
            data = {}
    data[args.model] = best["t"]
    out_p.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"[calibrate] model={args.model} chosen_t={best['t']:.4f} "
          f"precision={best['precision']:.4f} recall={best['recall']:.4f} fp/1k={best['fp_per_1k']:.3f}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
