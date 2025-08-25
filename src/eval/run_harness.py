# src/eval/run_harness.py
from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.models.anomaly.iforest import IForestModel
from src.models.log_lm.score import PerplexityScorer
from src.fusion.late_fusion import combine_scores
from src.eval.metrics import EvalMetrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def fetch_features(engine, start: datetime, end: datetime) -> List[Dict]:
    sql = text(
        """
        SELECT id, session_id, ts, label,
               event_count, unique_components, error_ratio,
               template_entropy, component_entropy
        FROM window_features
        WHERE ts BETWEEN :start AND :end
        ORDER BY ts
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
    Returns {window_id: [template_id, ...]} if a sequence column exists;
    otherwise returns {} and the LM path will be skipped.
    """
    col = _find_seq_column(engine)
    if not col:
        return {}

    sql = text(
        f"""
        SELECT id AS window_id, {col} AS seq
        FROM window_features
        WHERE ts BETWEEN :start AND :end
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
                    seq = json.loads(seq)
                except Exception:
                    seq = []
            out[str(row.window_id)] = [str(t) for t in (seq or [])]
    return out


def main() -> None:
    # 3-1-1 split (anchor is arbitrary; adjust to your data)
    anchor = datetime(2025, 8, 15, 10, 0, 0)
    train_start, train_end = anchor, anchor + timedelta(weeks=3)
    val_start, val_end = train_end, train_end + timedelta(weeks=1)
    test_start, test_end = val_start + timedelta(weeks=1), val_start + timedelta(weeks=2)

    db_url = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5433/dovah"
    )
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)

    # ---- Train IF on features
    train_rows = fetch_features(engine, train_start, train_end)
    if not train_rows:
        raise RuntimeError("No training rows in window_features for the train range.")
    iforest = IForestModel()
    iforest.fit(train_rows)

    # ---- Fit Log-LM if we have sequences
    train_seqs = fetch_sequences(engine, train_start, train_end)
    lm = None
    if train_seqs:
        lm = PerplexityScorer(n=3)
        lm.fit(list(train_seqs.values()))
        log.info("Perplexity LM trained on %d windows", len(train_seqs))
    else:
        log.info("No templates sequence column found; LM path disabled.")

    def score_and_store(start: datetime, end: datetime) -> None:
        rows = fetch_features(engine, start, end)
        if not rows:
            return

        # IF scores keyed by session_id
        if_scores_by_session = iforest.predict(rows)

        # LM sequences (if available)
        lm_seqs = fetch_sequences(engine, start, end) if lm else {}

        with Session() as s:
            # Clear any existing detections for these windows
            window_ids = [r["id"] for r in rows]
            s.execute(text("DELETE FROM detections WHERE window_id = ANY(:ids)"), {"ids": window_ids})
            s.commit()

            for r in rows:
                wid = str(r["id"])
                sess = r["session_id"]
                if_score = float(if_scores_by_session.get(sess, {}).get("score", 0.0))

                # Perplexity as lm_score (raw; combine_scores will min–max normalize)
                seq = lm_seqs.get(wid, [])
                lm_score = float(lm.score(seq)) if (lm and seq) else 0.0

                combine_scores(
                    session=s,
                    window_id=wid,
                    lm_score=lm_score,
                    iforest_score=if_score,
                    epss_scores={},  # add EPSS later
                    kev_cves=[],     # add KEV later
                )
            # combine_scores commits internally

    # Validation + Test
    score_and_store(val_start, val_end)
    score_and_store(test_start, test_end)

    # Metrics/plots to docs/metrics/
    evaluator = EvalMetrics()
    print("Validation:", evaluator.evaluate(val_start, val_end))
    print("Test:", evaluator.evaluate(test_start, test_end))


if __name__ == "__main__":
    main()
