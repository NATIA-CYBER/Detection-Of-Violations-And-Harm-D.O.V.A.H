# src/fusion/late_fusion.py
from __future__ import annotations
import math
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING, Any
import numpy as np

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

def _scale(x: float, p: Dict[str, float]) -> float:
    mn, mx = float(p.get("min", 0.0)), float(p.get("max", 1.0))
    if not math.isfinite(x) or mx <= mn:
        return 0.0
    return float(np.clip((x - mn) / (mx - mn), 0.0, 1.0))

def combine_scores(
    # keep compatibility with your harness
    session: "Session" = None,
    window_id: Any = None,
    lm_score: Optional[float] = None,
    iforest_score: Optional[float] = None,
    epss_scores: Optional[Dict[str, float]] = None,
    kev_cves: Optional[List[str]] = None,
    # sometimes provided by harness (fallback call)
    ts: Any = None,
    session_id: Optional[str] = None,
    # optional config
    scaler_params: Optional[Dict[str, Dict[str, float]]] = None,
    weights: Optional[Dict[str, float]] = None,
    # also accept db_session for future calls
    db_session: "Session" = None,
    **kwargs,
) -> Tuple[float, Dict[str, float]]:
    if scaler_params is None:
        scaler_params = {
            "lm_score": {"min": 0.0, "max": 20.0},
            "iforest_score": {"min": 0.0, "max": 1.0},
            "epss_score": {"min": 0.0, "max": 1.0},
        }
    if weights is None:
        weights = {"lm": 0.30, "iforest": 0.30, "epss": 0.20, "kev": 0.20}

    lm = 0.0 if lm_score is None or not math.isfinite(lm_score) else float(lm_score)
    iso = 0.0 if iforest_score is None or not math.isfinite(iforest_score) else float(iforest_score)
    epss_scores = epss_scores or {}
    kev_cves = kev_cves or []

    n_lm = _scale(lm, scaler_params["lm_score"])
    n_iso = _scale(iso, scaler_params["iforest_score"])
    max_epss = max(epss_scores.values()) if epss_scores else 0.0
    n_epss = _scale(max_epss, scaler_params["epss_score"])
    kev_hit = any(cve in kev_cves for cve in epss_scores.keys())
    n_kev = 1.0 if kev_hit else 0.0

    final_score = float(np.clip(
        weights.get("lm",0)*n_lm +
        weights.get("iforest",0)*n_iso +
        weights.get("epss",0)*n_epss +
        weights.get("kev",0)*n_kev,
        0.0, 1.0
    ))
    components = {"lm_score": n_lm, "iforest_score": n_iso, "epss_score": n_epss, "kev_score": n_kev}

    # Optional DB write (schema aligned with Alembic 002)
    sess = db_session or session
    if sess is not None and ts is not None and session_id:
        try:
            from sqlalchemy import text  # local import avoids hard dep when unused
            stmt = text("""
                INSERT INTO detections (ts, session_id, window_id, score, source, model_version, created_at)
                VALUES (:ts, :session_id, :window_id, :score, 'fusion', 'v0', CURRENT_TIMESTAMP)
                ON CONFLICT (ts, session_id) DO UPDATE
                  SET score = EXCLUDED.score,
                      window_id = EXCLUDED.window_id,
                      source = EXCLUDED.source,
                      model_version = EXCLUDED.model_version,
                      created_at = NOW()
            """)
            sess.execute(stmt, {"ts": ts, "session_id": session_id, "window_id": window_id, "score": final_score})
        except Exception:
            # don't fail scoring if DB is missing or schema differs
            pass
    return final_score, components
