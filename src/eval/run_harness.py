from __future__ import annotations
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.eval.metrics import EvalMetrics, EvalConfig
from src.fusion.late_fusion import combine_scores
from src.models.anomaly.iforest import IForestModel, IForestConfig

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("harness")

# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------
def get_engine():
    # Prefer DATABASE_URL, fall back to POSTGRES_URL, then local default
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5432/dovah"
    )
    return create_engine(url)

def fetch_window_features(engine, start_time: datetime, end_time: datetime) -> List[Dict]:
    """Fetch rows from window_features in a time range."""
    q = text("""
        SELECT
            id, session_id, ts, label,
            event_count, unique_components, error_ratio,
            template_entropy, component_entropy
        FROM window_features
        WHERE ts BETWEEN :start AND :end
        ORDER BY ts ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"start": start_time, "end": end_time})
        return [dict(r._mapping) for r in rows]

def rows_to_events(rows: List[Dict]) -> List[Dict]:
    """Shape rows into events that IForestModel expects (flat keys + session_id + ts)."""
    ev = []
    for r in rows:
        ev.append({
            "session_id":        r["session_id"],
            "ts":                r["ts"],
            "event_count":       r["event_count"],
            "unique_components": r["unique_components"],
            "error_ratio":       r["error_ratio"],
            "template_entropy":  r["template_entropy"],
            "component_entropy": r["component_entropy"],
        })
    return ev

# ---------------------------------------------------------------------
# Training / scoring
# ---------------------------------------------------------------------
def train_iforest(train_rows: List[Dict]) -> IForestModel:
    if not train_rows:
        raise ValueError("No training rows.")
    model = IForestModel(IForestConfig())
    model.fit(rows_to_events(train_rows))
    log.info("IForest trained on %d windows.", len(train_rows))
    return model

def score_and_store_detections(engine, rows: List[Dict], iforest: IForestModel) -> None:
    """
    Scores rows with IForest and writes detections using combine_scores().
    We don't have LM/EPSS/KEV wired here yet → pass zeros/empty.
    """
    if not rows:
        log.warning("No rows to score.")
        return

    events = rows_to_events(rows)
    # IForestModel.predict returns {session_id: {"score": float, "ts": ...}}
    by_session = iforest.predict(events)

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        for r in rows:
            sid = r["session_id"]
            wid = r["id"]
            if_data = by_session.get(sid, {})
            if_score = float(if_data.get("score", 0.0))

            # combine_scores requires lm_score, iforest_score, epss_scores, kev_cves
            final, _parts = combine_scores(
                session=session,
                window_id=str(wid),
                lm_score=0.0,
                iforest_score=if_score,
                epss_scores={},        # not joined yet
                kev_cves=[],           # not joined yet
                scaler_params=None,
                weights=None,
            )
            log.debug("stored detection window=%s score=%.4f", wid, final)

        session.commit()
        log.info("Stored detections for %d windows.", len(rows))
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# ---------------------------------------------------------------------
# Main harness: 3-1-1 weekly split (relative to an anchor)
# ---------------------------------------------------------------------
def main() -> int:
    # Anchor (adjust to your dataset’s calendar). Using a fixed point for reproducibility:
    anchor = datetime(2025, 8, 15, 10, 0, 0)

    train_start, train_end         = anchor,              anchor + timedelta(weeks=3)
    valid_start, valid_end         = train_end,           train_end + timedelta(weeks=1)
    test_start,  test_end          = valid_end,           valid_end + timedelta(weeks=1)

    log.info("Train: %s → %s", train_start, train_end)
    log.info("Valid: %s → %s", valid_start, valid_end)
    log.info("Test:  %s → %s", test_start,  test_end)

    engine = get_engine()

    # Train
    train_rows = fetch_window_features(engine, train_start, train_end)
    model = train_iforest(train_rows)

    # Validate
    valid_rows = fetch_window_features(engine, valid_start, valid_end)
    score_and_store_detections(engine, valid_rows, model)
    ev = EvalMetrics(EvalConfig())
    v_metrics = ev.evaluate(valid_start, valid_end)
    log.info("Validation metrics: %s", v_metrics)

    # Test
    test_rows = fetch_window_features(engine, test_start, test_end)
    score_and_store_detections(engine, test_rows, model)
    t_metrics = ev.evaluate(test_start, test_end)
    log.info("Test metrics: %s", t_metrics)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
