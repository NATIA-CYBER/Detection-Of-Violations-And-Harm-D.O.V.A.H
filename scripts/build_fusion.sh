#!/usr/bin/env bash
# scripts/build_fusion.sh
# Day-4 builder:
#   1) run harness on val/test → outputs/{val,test}_pred.jsonl
#   2) join preds + labels → data/{val,test}/fusion.jsonl

set -euo pipefail

# ====== EDIT THESE 4 LINES IF YOUR PATHS DIFFER ======
: "${VAL_IN:=sample_data/hdfs/val/events.jsonl}"     # validation events JSONL
: "${TEST_IN:=sample_data/hdfs/test/events.jsonl}"   # test events JSONL
: "${VAL_LABELS:=data/val/labels.jsonl}"             # validation labels JSONL
: "${TEST_LABELS:=data/test/labels.jsonl}"           # test labels JSONL
# =====================================================

CONFIG_PATH="configs/eval/hdfs_phase4.json"
mkdir -p outputs artifacts data/val data/test

# Optional: use a trained IForest if you already have it
IFOREST_FLAG=""
[ -f artifacts/iforest.pkl ] && IFOREST_FLAG="--iforest-model-path artifacts/iforest.pkl"

# Sanity checks
[ -f "$VAL_IN" ]      || { echo "❌ Missing VAL input: $VAL_IN"; exit 2; }
[ -f "$TEST_IN" ]     || { echo "❌ Missing TEST input: $TEST_IN"; exit 2; }
[ -f "$VAL_LABELS" ]  || { echo "❌ Missing VAL labels: $VAL_LABELS"; exit 2; }
[ -f "$TEST_LABELS" ] || { echo "❌ Missing TEST labels: $TEST_LABELS"; exit 2; }
[ -f "$CONFIG_PATH" ] || { echo "❌ Missing config: $CONFIG_PATH"; exit 2; }

echo "▶ VAL → outputs/val_pred.jsonl"
conda run -n dovah python -m src.eval.run_harness \
  --config "$CONFIG_PATH" \
  --input "$VAL_IN" \
  $IFOREST_FLAG \
  --use-perplexity false \
  --out outputs/val_pred.jsonl

echo "▶ TEST → outputs/test_pred.jsonl"
conda run -n dovah python -m src.eval.run_harness \
  --config "$CONFIG_PATH" \
  --input "$TEST_IN" \
  $IFOREST_FLAG \
  --use-perplexity false \
  --out outputs/test_pred.jsonl

echo "▶ Join preds + labels → data/val|test/fusion.jsonl"
VAL_LABELS="$VAL_LABELS" TEST_LABELS="$TEST_LABELS" conda run -n dovah python - <<'PY'
from pathlib import Path; import json, os, sys
pairs = [
    ("outputs/val_pred.jsonl",  os.environ["VAL_LABELS"],  "data/val/fusion.jsonl"),
    ("outputs/test_pred.jsonl", os.environ["TEST_LABELS"], "data/test/fusion.jsonl"),
]
SCORE_KEYS=("score","final_score","fusion_score","anomaly_score")
ID_KEYS=("window_id","session_id","id"); LABEL_KEYS=("label","y","is_attack","target")

def load_jsonl(p):
    P=Path(p)
    if not P.exists(): return []
    return [json.loads(l) for l in P.read_text(encoding="utf-8").splitlines() if l.strip()]

def pick(keys, sample):
    for k in keys:
        if k in sample: return k
    return None

total=0
for pred_path, lab_path, out_path in pairs:
    preds, labs = load_jsonl(pred_path), load_jsonl(lab_path)
    if not preds: print(f"❌ No predictions in {pred_path}"); sys.exit(3)
    if not labs:  print(f"❌ No labels in {lab_path}"); sys.exit(3)
    sk  = pick(SCORE_KEYS, preds[0]) or "score"
    pid = pick(ID_KEYS,   preds[0]) or "window_id"
    lid = pick(ID_KEYS,   labs[0])  or "window_id"
    lk  = pick(LABEL_KEYS, labs[0]) or "label"
    lbl = {}
    for r in labs:
        if lid in r and lk in r:
            try: lbl[r[lid]] = int(r[lk])
            except: pass
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    n=0
    with open(out_path,"w",encoding="utf-8") as fh:
        for r in preds:
            if pid in r and sk in r and r[pid] in lbl:
                try:
                    fh.write(json.dumps({"score": float(r[sk]), "label": int(lbl[r[pid]])})+"\n"); n+=1
                except: pass
    print(f"➡️  {out_path}: {n} rows (id {pid}/{lid}, score {sk}, label {lk})")
    total += n
sys.exit(0 if total>0 else 4)
PY

echo "——— Summary ———"
ls -lh data/val/fusion.jsonl data/test/fusion.jsonl
head -n 2 data/val/fusion.jsonl || true
head -n 2 data/test/fusion.jsonl || true
