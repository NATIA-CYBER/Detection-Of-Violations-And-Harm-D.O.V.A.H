# src/eval/run_harness.py
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.eval.metrics import EvalMetrics, EvalConfig
from src.models.anomaly.iforest import IForestModel
from src.models.log_lm.score import PerplexityScorer  # optional
from src.fusion.late_fusion import combine_scores

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("harness")


# ----------------------- DB helpers -----------------------
def _engine():
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5433/dovah"
    )
    return create_engine(url)


# ----------------------- Data fetch -----------------------
def fetch_features(engine, start: datetime, end: datetime) -> List[Dict]:
    """
    Pulls features; tolerates either 'start_ts' or 'ts' schema.
    """
    sql = text(
        """
        SELECT
          id,
          session_id,
          COALESCE(start_ts, ts) AS ts,
          label,
          event_count,
          unique_components,
          error_ratio,
          template_entropy,
          component_entropy
        FROM window_features
        WHERE COALESCE(start_ts, ts) >= :start AND COALESCE(start_ts, ts) < :end
        ORDER BY COALESCE(start_ts, ts)
        """
    )
    with engine.connect() as c:
        rows = c.execute(sql, {"start": start, "end": end})
        return [dict(r._mapping) for r in rows]


def _find_seq_column(engine) -> str | None:
    probe = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='window_features'
          AND column_name IN ('templates_seq','template_ids','templates')
        LIMIT 1
        """
    )
    with engine.connect() as c:
        r = c.execute(probe).fetchone()
        return r[0] if r else None


def fetch_sequences(engine, start: datetime, end: datetime) -> Dict[str, List[str]]:
    """
    Returns {window_id: [template_id, ...]} if a sequence column exists; otherwise {}.
    """
    col = _find_seq_column(engine)
    if not col:
        return {}
    sql = text(
        f"""
        SELECT id AS window_id, {col} AS seq
        FROM window_features
        WHERE COALESCE(start_ts, ts) >= :start AND COALESCE(start_ts, ts) < :end
        """
    )
    out: Dict[str, List[str]] = {}
    with engine.connect() as c:
        for row in c.execute(sql, {"start": start, "end": end}):
            seq = row.seq
            if seq is None:
                continue
            if isinstance(seq, str):
                try:
                    seq = json.loads(seq)  # JSON text in DB
                except Exception:
                    seq = []
            out[str(row.window_id)] = [str(t) for t in (seq or [])]
    return out


# ----------------------- Training -----------------------
def train_iforest(train_rows: List[Dict]) -> IForestModel:
    if not train_rows:
        raise RuntimeError("No training rows.")
    model = IForestModel()
    X = [
        {
            "event_count": r["event_count"],
            "unique_components": r["unique_components"],
            "error_ratio": r["error_ratio"],
            "template_entropy": r["template_entropy"],
            "component_entropy": r["component_entropy"],
        }
        for r in train_rows
    ]
    model.fit(X)
    log.info("IForest trained on %d windows.", len(train_rows))
    return model


# ----------------------- Scoring & persistence -----------------------
def score_range(engine, start: datetime, end: datetime, iforest: IForestModel, lm: PerplexityScorer | None) -> None:
    rows = fetch_features(engine, start, end)
    if not rows:
        log.warning("No windows to score in %s..%s", start, end)
        return

    X = [
        {
            "event_count": r["event_count"],
            "unique_components": r["unique_components"],
            "error_ratio": r["error_ratio"],
            "template_entropy": r["template_entropy"],
            "component_entropy": r["component_entropy"],
        }
        for r in rows
    ]
    if_scores = iforest.score(X)  # list[float], same order as rows

    lm_seqs = fetch_sequences(engine, start, end) if lm else {}

    Session = sessionmaker(bind=engine)
    with Session() as s:
        # Clear existing detections for these windows (idempotent runs)
        window_ids = [r["id"] for r in rows]
        s.execute(text("DELETE FROM detections WHERE window_id = ANY(:ids)"), {"ids": window_ids})
        s.commit()

        for i, r in enumerate(rows):
            wid = str(r["id"])
            sess = r["session_id"]
            ts   = r["ts"]

            if_score = float(if_scores[i])
            seq = lm_seqs.get(wid, [])
            lm_score = float(lm.score(seq)) if (lm and seq) else 0.0

            # Be tolerant to different combine_scores signatures across your edits
            try:
                combine_scores(
                    session=s,
                    window_id=wid,
                    lm_score=lm_score,
                    iforest_score=if_score,
                    epss_scores={},
                    kev_cves=[],
                )
            except TypeError:
                # Older/newer variant that expects ts/session_id too
                combine_scores(
                    session=s,
                    window_id=wid,
                    ts=ts,
                    session_id=sess,
                    lm_score=lm_score,
                    iforest_score=if_score,
                    epss_scores={},
                    kev_cves=[],
                )

        s.commit()


# ----------------------- Harness -----------------------
def _scores_labels_for_threshold(engine, start: datetime, end: datetime) -> Tuple[List[float], List[int]]:
    """
    Pull per-session max detection score + binary label for threshold picking.
    If there is no 'label' column, returns empty lists.
    """
    q_has_label = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='window_features' AND column_name='label'
        LIMIT 1
    """)
    with engine.connect() as c:
        if not c.execute(q_has_label).fetchone():
            return [], []

    q = text("""
        WITH per_session AS (
          SELECT
            w.session_id,
            MAX(d.score) AS score,
            BOOL_OR(w.label = 'malicious') AS is_malicious
          FROM window_features w
          JOIN detections d ON d.window_id = w.id
          WHERE COALESCE(w.start_ts, w.ts) >= :start AND COALESCE(w.start_ts, w.ts) < :end
          GROUP BY w.session_id
        )
        SELECT score, is_malicious FROM per_session
    """)
    with engine.connect() as c:
        rows = c.execute(q, {"start": start, "end": end}).fetchall()
    return [float(r.score or 0.0) for r in rows], [1 if r.is_malicious else 0 for r in rows]


def main() -> int:
    # 3-1-1 split anchored to a fixed date (reproducible)
    anchor = datetime(2025, 8, 15, 10, 0, 0)
    train_start, train_end = anchor, anchor + timedelta(weeks=3)
    val_start, val_end     = train_end, train_end + timedelta(weeks=1)
    test_start, test_end   = val_start + timedelta(weeks=1), val_start + timedelta(weeks=2)

    log.info("Train  : %s → %s", train_start, train_end)
    log.info("Valid  : %s → %s", val_start, val_end)
    log.info("Test   : %s → %s", test_start, test_end)

    engine = _engine()

    # Train IForest
    train_rows = fetch_features(engine, train_start, train_end)
    iforest = train_iforest(train_rows)

    # Optional LM
    train_seqs = fetch_sequences(engine, train_start, train_end)
    lm = None
    if train_seqs:
        lm = PerplexityScorer(n=3)
        lm.fit(list(train_seqs.values()))
        log.info("Perplexity LM trained on %d windows.", len(train_seqs))
    else:
        log.info("No sequence column in window_features; LM disabled.")

    # Score & store detections
    score_range(engine, val_start,  val_end,  iforest, lm)
    score_range(engine, test_start, test_end, iforest, lm)

    # Evaluate + artifacts
    evaluator = EvalMetrics(EvalConfig())
    v_metrics = evaluator.evaluate(val_start, val_end)
    t_metrics = evaluator.evaluate(test_start, test_end)
    log.info("Validation metrics: %s", v_metrics)
    log.info("Test metrics      : %s", t_metrics)

    out_dir = Path("docs/metrics"); out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "metrics_val.json").write_text(json.dumps(v_metrics, indent=2), encoding="utf-8")
    (out_dir / "metrics_test.json").write_text(json.dumps(t_metrics, indent=2), encoding="utf-8")

    # Curves (only saved if labels exist and sklearn/matplotlib available)
    evaluator.save_curves(val_start,  val_end,  out_dir / "roc_pr_val.png")
    evaluator.save_curves(test_start, test_end, out_dir / "roc_pr_test.png")

    # Pick threshold on validation (if labels exist)
    scores_val, labels_val = _scores_labels_for_threshold(engine, val_start, val_end)

    def pick_threshold(scores: List[float], labels: List[int]) -> float | None:
        if not scores or len(set(labels)) < 2:
            return None
        uniq = sorted(set(scores))
        best = (0.0, None)  # (f1, thr)
        for thr in uniq:
            preds = [1 if s >= thr else 0 for s in scores]
            tp = sum(1 for p, y in zip(preds, labels) if p == 1 and y == 1)
            fp = sum(1 for p, y in zip(preds, labels) if p == 1 and y == 0)
            fn = sum(1 for p, y in zip(preds, labels) if p == 0 and y == 1)
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            rec  = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1   = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
            if f1 > best[0]:
                best = (f1, thr)
        return best[1]

    thr = pick_threshold(scores_val, labels_val)
    (out_dir / "thresholds.json").write_text(json.dumps({"score_threshold": thr}, indent=2), encoding="utf-8")
    log.info("Saved artifacts to %s (threshold=%s)", out_dir, thr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
