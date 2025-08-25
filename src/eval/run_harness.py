# allow running this file by absolute path without -m
if __package__ is None or __package__ == "":
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from __future__ import annotations
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("harness")


def _engine():
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5432/dovah"
    )
    return create_engine(url)


def fetch_features(engine, start: datetime, end: datetime) -> List[Dict]:
    sql = text(
        """
        SELECT id, session_id, ts, label,
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


def _rows_to_events(rows: List[Dict]) -> List[Dict]:
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


def _find_seq_column(engine) -> Optional[str]:
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
    Returns {window_id: [template_id, ...]} when a sequence column exists,
    else {}. Accepts JSON text or array types.
    """
    col = _find_seq_column(engine)
    if not col:
        return {}

    sql = text(
        f"""
        SELECT id AS window_id, {col} AS seq
        FROM window_features
        WHERE ts BETWEEN :start AND :end
        ORDER BY ts ASC
        """
    )
    out: Dict[str, List[str]] = {}
    with engine.connect() as c:
        for row in c.execute(sql, {"start": start, "end": end}):
            seq = row.seq
            if seq is None:
                continue
            if isinstance(seq, str):
                # try JSON decode if text
                import json
                try:
                    seq = json.loads(seq)
                except Exception:
                    seq = []
            out[str(row.window_id)] = [str(t) for t in (seq or [])]
    return out


def _train_iforest(train_rows: List[Dict]) -> IForestModel:
    if not train_rows:
        raise RuntimeError("No training rows.")
    model = IForestModel(IForestConfig())
    model.fit(_rows_to_events(train_rows))
    log.info("IForest trained on %d windows", len(train_rows))
    return model


def _score_and_store(engine, rows: List[Dict], iforest: IForestModel, lm: Optional[PerplexityScorer]) -> None:
    if not rows:
        return

    events = _rows_to_events(rows)
    by_session = iforest.predict(events)  # {session_id: {"score": float, "ts": ...}}

    # Optionally compute LM perplexity per window if sequences exist
    lm_seqs = {}
    if lm:
        # derive window range from first/last ts in rows
        start_ts = rows[0]["ts"]
        end_ts = rows[-1]["ts"]
        lm_seqs = fetch_sequences(engine, start_ts, end_ts)

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as s:
        # Clear old detections for these windows
        window_ids = [r["id"] for r in rows]
        s.execute(text("DELETE FROM detections WHERE window_id = ANY(:ids)"), {"ids": window_ids})
        s.commit()

        for r in rows:
            wid = str(r["id"])
            sid = r["session_id"]
            if_score = float(by_session.get(sid, {}).get("score", 0.0))
            lm_score = 0.0
            if lm:
                seq = lm_seqs.get(wid, [])
                lm_score = float(lm.score(seq)) if seq else 0.0

            combine_scores(
                session=s,
                window_id=wid,
                lm_score=lm_score,
                iforest_score=if_score,
                epss_scores={},  # wire later
                kev_cves=[],     # wire later
            )
        # combine_scores commits internally


def main(argv=None) -> int:
    # 3-1-1 weekly split relative to an anchor (adjust to your dataset)
    anchor = datetime(2025, 8, 15, 10, 0, 0)
    train_start, train_end = anchor, anchor + timedelta(weeks=3)
    val_start, val_end = train_end, train_end + timedelta(weeks=1)
    test_start, test_end = val_start + timedelta(weeks=1), val_start + timedelta(weeks=2)

    log.info("Train: %s → %s", train_start, train_end)
    log.info("Valid: %s → %s", val_start, val_end)
    log.info("Test:  %s → %s", test_start, test_end)

    engine = _engine()

    # Train IF
    train_rows = fetch_features(engine, train_start, train_end)
    iforest = _train_iforest(train_rows)

    # Train LM if sequences present
    train_seqs = fetch_sequences(engine, train_start, train_end)
    lm = None
    if train_seqs:
        lm = PerplexityScorer(n=3)
        lm.fit(list(train_seqs.values()))
        log.info("Perplexity LM trained on %d windows", len(train_seqs))
    else:
        log.info("No sequence column in window_features; LM path disabled.")

    # Score + store on validation and test
    val_rows = fetch_features(engine, val_start, val_end)
    _score_and_store(engine, val_rows, iforest, lm)

    test_rows = fetch_features(engine, test_start, test_end)
    _score_and_store(engine, test_rows, iforest, lm)

    # Evaluate (writes metrics/plots via EvalMetrics)
    evaluator = EvalMetrics(EvalConfig())
    log.info("Validation metrics: %s", evaluator.evaluate(val_start, val_end))
    log.info("Test metrics: %s", evaluator.evaluate(test_start, test_end))

    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
