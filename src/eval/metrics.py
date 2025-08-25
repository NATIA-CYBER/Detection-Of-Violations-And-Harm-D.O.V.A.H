"""Detection pipeline evaluation metrics"""
from __future__ import annotations
import logging, os, json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from pydantic import BaseModel

# Optional plotting; CI tolerates headless
import matplotlib.pyplot as plt
from sklearn.metrics import auc, precision_recall_curve, roc_curve

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EvalConfig(BaseModel):
    precision_k: List[int] = [10, 50, 100]
    fp_window: int = 1000
    latency_percentile: float = 95.0
    score_threshold: float = 0.8
    # Prefer DATABASE_URL, then POSTGRES_URL, then local default
    db_url: str = (os.getenv("DATABASE_URL")
                   or os.getenv("POSTGRES_URL")
                   or "postgresql://dovah:dovah@localhost:5432/dovah")
    artifacts_dir: str = "docs/metrics"

class EvalMetrics:
    def __init__(self, config: Optional[EvalConfig] = None):
        self.config = config or EvalConfig()
        self.engine = create_engine(self.config.db_url)
        os.makedirs(self.config.artifacts_dir, exist_ok=True)

    def get_detection_latencies(self, start_time: datetime, end_time: datetime) -> List[float]:
        q = text("""
            WITH det AS (
                SELECT d.created_at AS detection_time, w.ts AS first_event_time
                FROM detections d
                JOIN window_features w ON w.id = d.window_id
                WHERE d.created_at BETWEEN :start AND :end
                  AND d.score >= :th
            )
            SELECT EXTRACT(EPOCH FROM (detection_time - first_event_time)) * 1000 AS latency_ms
            FROM det
        """)
        try:
            with self.engine.connect() as cx:
                rows = cx.execute(q, {"start": start_time, "end": end_time, "th": self.config.score_threshold})
                return [float(r.latency_ms) for r in rows]
        except Exception as e:
            logger.error(f"latency query failed: {e}")
            return []

    def get_confusion_matrix(self, start_time: datetime, end_time: datetime) -> Tuple[int,int,int,int]:
        q = text("""
            WITH scored AS (
                SELECT w.session_id,
                       COALESCE(MAX(d.score), 0.0) AS max_score,
                       BOOL_OR(w.label = 'malicious') AS is_malicious
                FROM window_features w
                LEFT JOIN detections d ON d.window_id = w.id
                WHERE w.ts BETWEEN :start AND :end
                GROUP BY w.session_id
            )
            SELECT
              COUNT(*) FILTER (WHERE max_score >= :th AND  is_malicious) AS tp,
              COUNT(*) FILTER (WHERE max_score >= :th AND NOT is_malicious) AS fp,
              COUNT(*) FILTER (WHERE max_score <  :th AND NOT is_malicious) AS tn,
              COUNT(*) FILTER (WHERE max_score <  :th AND  is_malicious) AS fn
            FROM scored
        """)
        try:
            with self.engine.connect() as cx:
                r = cx.execute(q, {"start": start_time, "end": end_time, "th": self.config.score_threshold}).first()
                return (r.tp or 0, r.fp or 0, r.tn or 0, r.fn or 0) if r else (0,0,0,0)
        except Exception as e:
            logger.error(f"confusion query failed: {e}")
            return (0,0,0,0)

    def get_precision_at_k(self, start_time: datetime, end_time: datetime) -> Dict[int, Optional[float]]:
        q = text("""
            WITH ranked AS (
                SELECT w.session_id,
                       d.score,
                       BOOL_OR(w.label = 'malicious') AS is_malicious,
                       ROW_NUMBER() OVER (ORDER BY d.score DESC) AS rnk
                FROM detections d
                JOIN window_features w ON w.id = d.window_id
                WHERE d.created_at BETWEEN :start AND :end
                GROUP BY w.session_id, d.score
            )
            SELECT rnk, is_malicious FROM ranked ORDER BY rnk
        """)
        out: Dict[int, Optional[float]] = {}
        try:
            with self.engine.connect() as cx:
                rows = list(cx.execute(q, {"start": start_time, "end": end_time}))
            pairs = [(int(r.rnk), bool(r.is_malicious)) for r in rows]
            for k in self.config.precision_k:
                if len(pairs) < k:
                    out[k] = None
                else:
                    topk = pairs[:k]
                    tp = sum(1 for _, m in topk if m)
                    out[k] = tp / k
            return out
        except Exception as e:
            logger.error(f"p@k query failed: {e}")
            return {k: None for k in self.config.precision_k}

    def get_fp_rate(self, start_time: datetime, end_time: datetime) -> Optional[float]:
        q = text("""
            WITH stats AS (
                SELECT COUNT(*) AS total_events,
                       COUNT(*) FILTER (WHERE d.score >= :th AND NOT (w.label = 'malicious')) AS fp
                FROM detections d
                JOIN window_features w ON w.id = d.window_id
                WHERE d.created_at BETWEEN :start AND :end
            )
            SELECT CASE WHEN total_events > 0
                        THEN (fp::float * :window) / total_events
                        ELSE NULL END AS fp_rate
            FROM stats
        """)
        try:
            with self.engine.connect() as cx:
                r = cx.execute(q, {"start": start_time, "end": end_time, "th": self.config.score_threshold, "window": self.config.fp_window}).first()
                return None if r is None else (None if r.fp_rate is None else float(r.fp_rate))
        except Exception as e:
            logger.error(f"fp/1k query failed: {e}")
            return None

    def get_scores_and_labels(self, start_time: datetime, end_time: datetime) -> Tuple[List[float], List[int]]:
        q = text("""
            SELECT d.score, BOOL_OR(w.label = 'malicious') AS is_malicious
            FROM detections d
            JOIN window_features w ON w.id = d.window_id
            WHERE d.created_at BETWEEN :start AND :end
            GROUP BY w.session_id, d.score
        """)
        try:
            with self.engine.connect() as cx:
                rows = list(cx.execute(q, {"start": start_time, "end": end_time}))
            if not rows:
                return [], []
            scores = [float(r.score) for r in rows]
            labels = [1 if r.is_malicious else 0 for r in rows]
            return scores, labels
        except Exception as e:
            logger.error(f"scores/labels query failed: {e}")
            return [], []

    def evaluate(self, start_time: datetime, end_time: datetime) -> Dict[str, Optional[float]]:
        tp, fp, tn, fn = self.get_confusion_matrix(start_time, end_time)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall    = tp / (tp + fn) if (tp + fn) else 0.0
        f1        = 2*precision*recall/(precision+recall) if (precision+recall) else 0.0
        p_at_k    = self.get_precision_at_k(start_time, end_time)
        fp_rate   = self.get_fp_rate(start_time, end_time)
        lats      = self.get_detection_latencies(start_time, end_time)
        p95       = float(np.percentile(lats, self.config.latency_percentile)) if lats else None

        scores, labels = self.get_scores_and_labels(start_time, end_time)
        roc_auc = None
        if labels and len(set(labels)) > 1:
            fpr, tpr, _ = roc_curve(labels, scores)
            roc_auc = float(auc(fpr, tpr))
            plt.figure()
            plt.plot(fpr, tpr, lw=2)
            plt.plot([0,1],[0,1], lw=1, linestyle="--")
            plt.xlabel("FPR"); plt.ylabel("TPR"); plt.title("ROC")
            plt.savefig(os.path.join(self.config.artifacts_dir, "roc_curve.png")); plt.close()

            prec, rec, _ = precision_recall_curve(labels, scores)
            plt.figure()
            plt.plot(rec, prec, lw=2)
            plt.xlabel("Recall"); plt.ylabel("Precision"); plt.title("PR")
            plt.savefig(os.path.join(self.config.artifacts_dir, "pr_curve.png")); plt.close()

        metrics = {
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "fp_per_1k": fp_rate,
            "p95_latency_ms": p95,
            "roc_auc": roc_auc,
        }
        for k, v in p_at_k.items():
            metrics[f"precision@{k}"] = v

        pd.DataFrame([metrics]).to_csv(os.path.join(self.config.artifacts_dir, "metrics.csv"), index=False)
        with open(os.path.join(self.config.artifacts_dir, "thresholds.json"), "w") as fh:
            json.dump({"score_threshold": self.config.score_threshold}, fh, indent=2)
        return metrics
