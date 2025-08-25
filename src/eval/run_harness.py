# src/eval/run_harness.py
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.eval.metrics import EvalMetrics, EvalConfig
from src.models.anomaly.iforest import IForestModel, IForestConfig
from src.models.log_lm.score import PerplexityScorer
from src.fusion.late_fusion import combine_scores

# ----- logging ---------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("harness")


# ----- DB helpers ------------------------------------------------------------
def get_engine():
    """Pick DATABASE_URL, then POSTGRES_URL, else localhost default."""
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5432/dovah"
    )
    return create_engine(url)


def fetch_features(engine, start: datetime, end: datetime) -> List[Dict]:
    """Return window_features rows for [start, end]."""
    sql = text(
        """
        SELECT
            id, session_id, ts, label,
            event_count, unique_components, error_ratio,
            template_entropy, component_entropy
        FROM window_features
        WHERE ts BETWEEN :start AND :end
        ORDER BY ts ASC
        """
    )
    with engine.connect() as c:
        rows = c.execute(sql, {"start": start, "end": end})
        return [dict(r._mapping) for r in rows]


def _find_seq_column(engine) -> Optional[str]:
    """Detect an optional templates sequence column for the LM."""
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
    Return {window_id: [template_id,...]} if the table has a sequence column.
    If not present, return {} and we’ll skip LM scoring.
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


# ----- model helpers ---------------------------------------------------------
def _rows_to_events(rows: List[Dict]) -> List[Dict]:
    """
    Shape DB rows into the dicts expected by IForestModel:
    needs flat feature keys + session_id + ts
    """
    ev = []
    for r in rows:
        ev.append(
            {
                "session_id": r["session_id"],
                "ts": r["ts"],
                "event_count": r["event_count"],
                "unique_components": r["unique_components"],
                "error_ratio": r["error_ratio"],
                "template_entropy": r["template_entropy"],
                "component_entropy": r["component_entropy"],
            }
        )
    return ev


def train_iforest(train_rows: List[Dict]) -> IForestModel:
    if not train_rows:
        raise RuntimeError("No training rows for IForest.")
    model = IForestModel(IForestConfig())
    model.fit(_rows_to_events(train_rows))
    log.info("IForest trained on %d windows.", len(train_rows))
    return model


def score_and_store_span(
    engine,
    sess_maker,
    iforest: IForestModel,
    lm: Optional[PerplexityScorer],
    start: datetime,
    end: datetime,
) -> int:
    """
    Score rows in [start, end] and write detections via combine_scores().
    EPSS/KEV not wired yet → pass empty dicts.
    """
    rows = fetch_features(engine, start, end)
    if not rows:
        log.info("No rows in %s → %s", start, end)
        return 0

    events = _rows_to_events(rows)
    # iforest.predict returns {session_id: {"score": float, "ts": ...}}
    if_scores_by_session = iforest.predict(events)

    lm_seqs: Dict[str, List[str]] = {}
    if lm:
        lm_seqs = fetch_sequences(engine, start, end)

    with sess_maker() as s:
        # clear prior detections for these windows so repeat runs are clean
        window_ids = [r["id"] for r in rows]
        s.execute(text("DELETE FROM detections WHERE window_id = ANY(:ids)"), {"ids": window_ids})
        s.commit()

        for r in rows:
            wid = str(r["id"])
            sess = r["session_id"]

            if_score = float(if_scores_by_session.get(sess, {}).get("score", 0.0))
            lm_score = 0.0
            if lm:
                seq = lm_seqs.get(wid, [])
                lm_score = float(lm.score(seq)) if seq else 0.0

            # combine_scores writes into detections and COMMITs
            combine_scores(
                session=s,
                window_id=wid,
                lm_score=lm_score,
                iforest_score=if_score,
                epss_scores={},  # TODO: join EPSS
                kev_cves=[],     # TODO: join KEV
            )

    log.info("Stored detections for %d windows in %s → %s", len(rows), start, end)
    return len(rows)


# ----- main ------------------------------------------------------------------
def main() -> int:
    # 3-1-1 time split (anchor is arbitrary; align this to your data’s calendar)
    anchor = datetime(2025, 8, 15, 10, 0, 0)
    train_start, train_end = anchor, anchor + timedelta(weeks=3)
    val_start, val_end     = train_end, train_end + timedelta(weeks=1)
    test_start, test_end   = val_start + timedelta(weeks=1), val_start + timedelta(weeks=2)

    log.info("Train: %s → %s", train_start, train_end)
    log.info("Valid: %s → %s", val_start, val_end)
    log.info("Test:  %s → %s", test_start, test_end)

    engine = get_engine()
    Session = sessionmaker(bind=engine)

    # ---- train IForest
    train_rows = fetch_features(engine, train_start, train_end)
    iforest = train_iforest(train_rows)

    # ---- train LM (optional)
    lm = None
    train_seqs = fetch_sequences(engine, train_start, train_end)
    if train_seqs:
        lm = PerplexityScorer(n=3)
        lm.fit(list(train_seqs.values()))
        log.info("Perplexity LM trained on %d windows.", len(train_seqs))
    else:
        log.info("No template sequence column; LM path disabled.")

    # ---- score + store detections (validation & test)
    score_and_store_span(engine, Session, iforest, lm, val_start, val_end)
    score_and_store_span(engine, Session, iforest, lm, test_start, test_end)

    # ---- metrics & artifacts
    evaluator = EvalMetrics(EvalConfig())
    log.info("Validation metrics: %s", evaluator.evaluate(val_start, val_end))
    log.info("Test metrics: %s",        evaluator.evaluate(test_start, test_end))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
