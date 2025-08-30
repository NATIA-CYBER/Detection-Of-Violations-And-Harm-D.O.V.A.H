# src/eval/metrics.py
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# Optional deps (only used if labels exist and you call save_curves)
try:
    from sklearn.metrics import precision_recall_curve, average_precision_score, roc_curve, auc  # type: ignore
    _HAVE_SK = True
except Exception:
    _HAVE_SK = False

try:
    import matplotlib.pyplot as plt  # type: ignore
    _HAVE_PLT = True
except Exception:
    _HAVE_PLT = False

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class EvalConfig(BaseModel):
    """Evaluation configuration."""
    precision_k: List[int] = [10, 50, 100]    # for precision@k
    fp_window: int = 1000                     # scale for FP/1k
    latency_percentile: float = 95.0          # P95 latency
    score_threshold: float = 0.8              # threshold for binarization

    # Prefer DATABASE_URL, fall back to POSTGRES_URL, then local default
    db_url: str = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5433/dovah"
    )


class EvalMetrics:
    """Computes detection performance metrics from Postgres."""

    def __init__(self, config: Optional[EvalConfig] = None):
        self.config = config or EvalConfig()
        self.engine = create_engine(self.config.db_url)

    # ------------------------------ helpers ------------------------------
    def _has_label_column(self) -> bool:
        q = text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='window_features'
              AND column_name='label'
            LIMIT 1
        """)
        with self.engine.connect() as c:
            return c.execute(q).fetchone() is not None

    # ------------------------------ metrics ------------------------------
    def get_detection_latencies(self, start_time: datetime, end_time: datetime) -> List[float]:
        """
        P95 end-to-end: first window start in range -> first detection >= threshold in range, per session.
        Uses start_ts for windows and COALESCE(d.ts, d.created_at) for detections to be schema-tolerant.
        """
        q = text("""
            WITH per_session AS (
              SELECT
                w.session_id,
                MIN(w.start_ts) AS first_event_time,
                MIN(COALESCE(d.ts, d.created_at)) FILTER (WHERE d.score >= :th) AS detection_time
              FROM window_features w
              LEFT JOIN detections d ON d.window_id = w.id
              WHERE w.start_ts >= :start_time AND w.start_ts < :end_time
              GROUP BY w.session_id
            )
            SELECT EXTRACT(EPOCH FROM (detection_time - first_event_time)) * 1000 AS latency_ms
            FROM per_session
            WHERE detection_time IS NOT NULL
        """)
        try:
            with self.engine.connect() as c:
                rows = c.execute(q, {
                    "start_time": start_time,
                    "end_time": end_time,
                    "th": self.config.score_threshold,
                })
                return [float(r.latency_ms) for r in rows]
        except Exception as e:
            log.error("Error getting detection latencies: %s", e)
            return []

    def get_confusion_matrix(self, start_time: datetime, end_time: datetime) -> Tuple[int, int, int, int]:
        """
        Confusion matrix over sessions in the window, if window_features.label exists.
        Label is expected to be 'malicious' for positives; anything else counts as benign.
        """
        if not self._has_label_column():
            log.warning("No 'label' column in window_features; skipping supervised metrics (TP/FP/TN/FN).")
            return (0, 0, 0, 0)

        q = text("""
            WITH per_session AS (
              SELECT
                w.session_id,
                MAX(d.score) AS max_score,
                BOOL_OR(w.label = 'malicious') AS is_malicious
              FROM window_features w
              LEFT JOIN detections d ON d.window_id = w.id
              WHERE w.start_ts >= :start_time AND w.start_ts < :end_time
              GROUP BY w.session_id
            )
            SELECT
              COUNT(*) FILTER (WHERE max_score >= :th AND is_malicious)  AS tp,
              COUNT(*) FILTER (WHERE max_score >= :th AND NOT is_malicious) AS fp,
              COUNT(*) FILTER (WHERE max_score  < :th AND NOT is_malicious) AS tn,
              COUNT(*) FILTER (WHERE max_score  < :th AND is_malicious)  AS fn
            FROM per_session
        """)
        try:
            with self.engine.connect() as c:
                row = c.execute(q, {
                    "start_time": start_time,
                    "end_time": end_time,
                    "th": self.config.score_threshold,
                }).first()
                return (int(row.tp or 0), int(row.fp or 0), int(row.tn or 0), int(row.fn or 0))
        except Exception as e:
            log.error("Error getting confusion matrix: %s", e)
            return (0, 0, 0, 0)

    def get_precision_at_k(self, start_time: datetime, end_time: datetime) -> Dict[int, Optional[float]]:
        if not self._has_label_column():
            log.warning("No 'label' column; precision@k unavailable.")
            return {k: None for k in self.config.precision_k}

        q = text("""
            WITH per_session AS (
              SELECT
                w.session_id,
                MAX(d.score) AS score,
                BOOL_OR(w.label = 'malicious') AS is_malicious
              FROM window_features w
              LEFT JOIN detections d ON d.window_id = w.id
              WHERE w.start_ts >= :start_time AND w.start_ts < :end_time
              GROUP BY w.session_id
            ),
            ordered AS (
              SELECT
                session_id, score, is_malicious,
                ROW_NUMBER() OVER (ORDER BY score DESC NULLS LAST) AS rk
              FROM per_session
            )
            SELECT rk, is_malicious FROM ordered ORDER BY rk
        """)
        try:
            with self.engine.connect() as c:
                rows = list(c.execute(q, {"start_time": start_time, "end_time": end_time}))
            dets = [(int(r.rk), bool(r.is_malicious)) for r in rows]
            out: Dict[int, Optional[float]] = {}
            for k in self.config.precision_k:
                if k > len(dets):
                    out[k] = None
                else:
                    tp = sum(1 for _, m in dets[:k] if m)
                    out[k] = tp / k
            return out
        except Exception as e:
            log.error("Error calculating precision@k: %s", e)
            return {k: None for k in self.config.precision_k}

    def get_fp_rate(self, start_time: datetime, end_time: datetime) -> Optional[float]:
        """False positives per 1000 windows (only if label exists)."""
        if not self._has_label_column():
            return None
        q = text("""
            WITH per_session AS (
              SELECT
                w.session_id,
                MAX(d.score) AS max_score,
                BOOL_OR(w.label = 'malicious') AS is_malicious
              FROM window_features w
              LEFT JOIN detections d ON d.window_id = w.id
              WHERE w.start_ts >= :start_time AND w.start_ts < :end_time
              GROUP BY w.session_id
            ),
            counts AS (
              SELECT
                COUNT(*) FILTER (WHERE max_score >= :th AND NOT is_malicious) AS fp,
                (SELECT COUNT(*) FROM window_features
                   WHERE start_ts >= :start_time AND start_ts < :end_time) AS total_windows
            )
            SELECT CASE WHEN total_windows > 0
                        THEN (fp::float * :scale) / total_windows
                        ELSE NULL END AS fp_rate
            FROM counts
        """)
        try:
            with self.engine.connect() as c:
                row = c.execute(q, {
                    "start_time": start_time,
                    "end_time": end_time,
                    "th": self.config.score_threshold,
                    "scale": self.config.fp_window,
                }).first()
                return float(row.fp_rate) if row and row.fp_rate is not None else None
        except Exception as e:
            log.error("Error calculating FP/1k: %s", e)
            return None

    def evaluate(self, start_time: datetime, end_time: datetime) -> Dict[str, Optional[float]]:
        """Compute metrics for the period; supervised metrics are None if no labels."""
        tp, fp, tn, fn = self.get_confusion_matrix(start_time, end_time)

        precision = tp / (tp + fp) if (tp + fp) > 0 else None
        recall    = tp / (tp + fn) if (tp + fn) > 0 else None
        f1 = (2 * precision * recall / (precision + recall)) if (precision and recall and (precision + recall) > 0) else None

        prec_k = self.get_precision_at_k(start_time, end_time)
        fp_rate = self.get_fp_rate(start_time, end_time)

        latencies = self.get_detection_latencies(start_time, end_time)
        p95_latency = float(np.percentile(latencies, self.config.latency_percentile)) if latencies else None

        metrics: Dict[str, Optional[float]] = {
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "fp_per_1k": fp_rate,
            "p95_latency_ms": p95_latency,
        }
        for k, v in prec_k.items():
            metrics[f"precision@{k}"] = v
        return metrics

    # ------------------------------ curves (optional) ------------------------------
    def _scores_and_labels(self, start_time: datetime, end_time: datetime) -> Optional[Tuple[List[float], List[int]]]:
        if not self._has_label_column():
            return None
        q = text("""
            SELECT
                w.session_id,
                MAX(d.score) AS score,
                BOOL_OR(w.label = 'malicious') AS is_malicious
            FROM window_features w
            LEFT JOIN detections d ON d.window_id = w.id
            WHERE w.start_ts >= :start AND w.start_ts < :end
            GROUP BY w.session_id
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"start": start_time, "end": end_time}).fetchall()
        if not rows:
            return None
        scores = [float(r.score or 0.0) for r in rows]
        labels = [1 if bool(r.is_malicious) else 0 for r in rows]
        return scores, labels

    def save_curves(self, start_time: datetime, end_time: datetime, out_png: Path) -> Dict[str, Optional[float]]:
        """
        Saves PR/ROC curves to out_png and returns {"auc_roc": ..., "avg_precision": ...}.
        Only works if labels exist and sklearn/matplotlib are available.
        """
        out_png.parent.mkdir(parents=True, exist_ok=True)

        data = self._scores_and_labels(start_time, end_time)
        if not data or not _HAVE_SK or not _HAVE_PLT:
            return {"auc_roc": None, "avg_precision": None}

        scores, labels = data
        if len(set(labels)) < 2:
            return {"auc_roc": None, "avg_precision": None}

        precision, recall, _ = precision_recall_curve(labels, scores)
        ap = average_precision_score(labels, scores)
        fpr, tpr, _ = roc_curve(labels, scores)
        roc_auc = auc(fpr, tpr)

        plt.figure(figsize=(8, 4.5))
        plt.subplot(1, 2, 1)
        plt.plot(recall, precision)
        plt.xlabel("Recall"); plt.ylabel("Precision"); plt.title(f"PR (AP={ap:.3f})")
        plt.subplot(1, 2, 2)
        plt.plot(fpr, tpr); plt.plot([0, 1], [0, 1], "--")
        plt.xlabel("FPR"); plt.ylabel("TPR"); plt.title(f"ROC (AUC={roc_auc:.3f})")
        plt.tight_layout()
        plt.savefig(out_png)
        plt.close()

        return {"auc_roc": float(roc_auc), "avg_precision": float(ap)}

    # ------------------------------ artifact helper ------------------------------
    def export_metrics_json(self, start_time: datetime, end_time: datetime, out_path: Path) -> Dict[str, Optional[float]]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        metrics = self.evaluate(start_time, end_time)
        out_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        return metrics
