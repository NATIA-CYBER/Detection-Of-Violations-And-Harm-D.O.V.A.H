#!/usr/bin/env bash
set -euo pipefail

# Inputs (override via env or Make)
: "${VAL_IN:=sample_data/hdfs/val/events.jsonl}"
: "${TEST_IN:=sample_data/hdfs/test/events.jsonl}"
: "${VAL_LABELS:=data/val/labels.jsonl}"
: "${TEST_LABELS:=data/test/labels.jsonl}"
: "${CONFIG_PATH:=configs/eval/hdfs_phase4.json}"

mkdir -p outputs data/val data/test

echo "VAL_IN=${VAL_IN}"
echo "TEST_IN=${TEST_IN}"
echo "VAL_LABELS=${VAL_LABELS}"
echo "TEST_LABELS=${TEST_LABELS}"
echo "CONFIG_PATH=${CONFIG_PATH}"

[ -f "$VAL_IN" ]     || { echo "❌ Missing VAL events: $VAL_IN"; exit 2; }
[ -f "$TEST_IN" ]    || { echo "❌ Missing TEST events: $TEST_IN"; exit 2; }
[ -f "$VAL_LABELS" ] || { echo "❌ Missing VAL labels: $VAL_LABELS"; exit 2; }
[ -f "$TEST_LABELS" ]|| { echo "❌ Missing TEST labels: $TEST_LABELS"; exit 2; }
[ -f "$CONFIG_PATH" ]|| { echo "❌ Missing config: $CONFIG_PATH"; exit 2; }

IFOREST_FLAG=""
[ -f artifacts/iforest.pkl ] && IFOREST_FLAG="--iforest-model-path artifacts/iforest.pkl"

echo "▶ VAL predictions → outputs/val_pred.jsonl"
python -m src.eval.run_harness \
  --config "$CONFIG_PATH" \
  --input  "$VAL_IN" \
  $IFOREST_FLAG \
  --use-perplexity false \
  --out outputs/val_pred.jsonl

echo "▶ TEST predictions → outputs/test_pred.jsonl"
python -m src.eval.run_harness \
  --config "$CONFIG_PATH" \
  --input  "$TEST_IN" \
  $IFOREST_FLAG \
  --use-perplexity false \
  --out outputs/test_pred.jsonl

echo "▶ Join preds + labels → data/val|test/fusion.jsonl"
VAL_LABELS="$VAL_LABELS" TEST_LABELS="$TEST_LABELS" python - <<'PY'
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

echo "Done."
