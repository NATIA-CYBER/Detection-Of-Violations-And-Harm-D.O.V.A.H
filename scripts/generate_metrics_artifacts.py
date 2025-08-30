# scripts/generate_metrics_artifacts.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import sys
import numpy as np

# Headless plotting for CI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from sklearn.metrics import precision_recall_curve, roc_curve, auc  # noqa: E402

# Make file-path runs work (src layout)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def load_jsonl_scores(p: Path, score_key: str, label_key: str) -> tuple[np.ndarray, np.ndarray]:
    scores, labels = [], []
    for line in p.read_text().splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if score_key not in obj or label_key not in obj:
            continue
        try:
            scores.append(float(obj[score_key]))
            labels.append(int(obj[label_key]))
        except Exception:
            continue
    if not scores:
        raise SystemExit(f"No usable rows in {p} with keys '{score_key}' and '{label_key}'")
    return np.asarray(scores, float), np.asarray(labels, int)

def main() -> int:
    ap = argparse.ArgumentParser(description="Generate PR/ROC PNGs and metrics CSV for one model.")
    ap.add_argument("--pred", required=True, help="Test JSONL with 'score' and 'label'")
    ap.add_argument("--model", default="fusion")
    ap.add_argument("--out-dir", default="docs/metrics")
    ap.add_argument("--thresholds", default="docs/metrics/thresholds.json")
    ap.add_argument("--score-key", default="score")
    ap.add_argument("--label-key", default="label")
    args = ap.parse_args()

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    scores, labels = load_jsonl_scores(Path(args.pred), args.score_key, args.label_key)

    # Curves
    prec, rec, _ = precision_recall_curve(labels, scores)
    fpr, tpr, _  = roc_curve(labels, scores)
    pr_auc  = auc(rec, prec)
    roc_auc = auc(fpr, tpr)

    # Plots
    plt.figure()
    plt.plot(rec, prec)
    plt.xlabel("Recall"); plt.ylabel("Precision")
    plt.title(f"{args.model} PR (AUC={pr_auc:.3f})")
    plt.savefig(out_dir / f"{args.model}_pr.png", dpi=160, bbox_inches="tight")
    plt.close()

    plt.figure()
    plt.plot(fpr, tpr)
    plt.plot([0,1], [0,1], "--", linewidth=1)
    plt.xlabel("FPR"); plt.ylabel("TPR")
    plt.title(f"{args.model} ROC (AUC={roc_auc:.3f})")
    plt.savefig(out_dir / f"{args.model}_roc.png", dpi=160, bbox_inches="tight")
    plt.close()

    # Threshold
    thr = 0.5
    tpath = Path(args.thresholds)
    if tpath.exists():
        try:
            thr = float(json.loads(tpath.read_text()).get(args.model, thr))
        except Exception:
            pass

    pred = (scores >= thr).astype(int)
    tp = int(((pred==1)&(labels==1)).sum())
    fp = int(((pred==1)&(labels==0)).sum())
    fn = int(((pred==0)&(labels==1)).sum())
    tn = int(((pred==0)&(labels==0)).sum())

    precision = tp/(tp+fp) if (tp+fp)>0 else 0.0
    recall    = tp/(tp+fn) if (tp+fn)>0 else 0.0
    fp1k      = 1000.0*fp/len(labels)

    # CSV (append if exists, else write header)
    csv_p = out_dir / "metrics.csv"
    row = f"{args.model},{thr:.4f},{precision:.4f},{recall:.4f},{fp1k:.3f},{pr_auc:.4f},{roc_auc:.4f},{len(labels)}\n"
    if csv_p.exists():
        with csv_p.open("a", encoding="utf-8") as fh:
            fh.write(row)
    else:
        csv_p.write_text(
            "model,threshold,precision,recall,fp_per_1k,pr_auc,roc_auc,windows\n" + row,
            encoding="utf-8"
        )

    print(f"[artifacts] wrote {out_dir}/{args.model}_pr.png, {out_dir}/{args.model}_roc.png, {csv_p}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
