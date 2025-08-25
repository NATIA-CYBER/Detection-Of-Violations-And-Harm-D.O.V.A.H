"""Detection pipeline evaluation metrics"""
import logging
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import numpy as np
from sqlalchemy import create_engine, text
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EvalConfig(BaseModel):
    """Evaluation configuration."""
    precision_k: List[int] = [10, 50, 100]  # For precision@k
    fp_window: int = 1000                   # Events for FP/1k
    latency_percentile: float = 95.0        # For P95 calculation
    score_threshold: float = 0.8            # For binary classification
    # Prefer DATABASE_URL, fall back to POSTGRES_URL, then local default
    db_url: str = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://dovah:dovah@localhost:5432/dovah"
    )

class EvalMetrics:
    """Computes detection performance metrics."""

    def __init__(self, config: Optional[EvalConfig] = None):
        self.config = config or EvalConfig()
        self.engine = create_engine(self.config.db_url)

    def get_detection_latencies(self, start_time: datetime, end_time: datetime) -> List[float]:
        """Get detection latencies in milliseconds."""
        query = text("""
            WITH detection_times AS (
                SELECT
                    w.session_id,
                    d.created_at AS detection_time,
                    w.ts AS first_event_time
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.created_at BETWEEN :start_time AND :end_time
                  AND d.score >= :threshold
                GROUP BY w.session_id, d.created_at, w.ts
            )
            SELECT EXTRACT(EPOCH FROM (detection_time - first_event_time)) * 1000 AS latency_ms
            FROM detection_times
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        "start_time": start_time,
                        "end_time": end_time,
                        "threshold": self.config.score_threshold,
                    },
                )
                return [float(row.latency_ms) for row in result]
        except Exception as e:
            logger.error(f"Error getting detection latencies: {e}")
            return []

    def get_confusion_matrix(
        self, start_time: datetime, end_time: datetime
    ) -> Tuple[int, int, int, int]:
        """Get TP, FP, TN, FN counts."""
        query = text("""
            WITH scored_sessions AS (
                SELECT
                    w.session_id,
                    COALESCE(MAX(d.score), 0.0) AS max_score,
                    bool_or(w.label = 'malicious') AS is_malicious
                FROM window_features w
                LEFT JOIN detections d ON w.id = d.window_id
                WHERE w.ts BETWEEN :start_time AND :end_time
                GROUP BY w.session_id
            )
            SELECT
                COUNT(*) FILTER (WHERE max_score >= :threshold AND is_malicious)        AS tp,
                COUNT(*) FILTER (WHERE max_score >= :threshold AND NOT is_malicious)    AS fp,
                COUNT(*) FILTER (WHERE max_score <  :threshold AND NOT is_malicious)    AS tn,
                COUNT(*) FILTER (WHERE max_score <  :threshold AND is_malicious)        AS fn
            FROM scored_sessions
        """)
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    query,
                    {
                        "start_time": start_time,
                        "end_time": end_time,
                        "threshold": self.config.score_threshold,
                    },
                ).first()
                if row:
                    return (row.tp, row.fp, row.tn, row.fn)
                return (0, 0, 0, 0)
        except Exception as e:
            logger.error(f"Error getting confusion matrix: {e}")
            return (0, 0, 0, 0)

    def get_precision_at_k(
        self, start_time: datetime, end_time: datetime
    ) -> Dict[int, Optional[float]]:
        """Get precision@k for configured k values."""
        query = text("""
            WITH ranked_detections AS (
                SELECT
                    w.session_id,
                    d.score,
                    bool_or(w.label = 'malicious') AS is_malicious,
                    ROW_NUMBER() OVER (ORDER BY d.score DESC) AS rank
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.created_at BETWEEN :start_time AND :end_time
                GROUP BY w.session_id, d.score
            )
            SELECT rank, is_malicious
            FROM ranked_detections
            ORDER BY rank
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {"start_time": start_time, "end_time": end_time},
                )
                detections = [(row.rank, row.is_malicious) for row in result]
                precisions: Dict[int, Optional[float]] = {}
                for k in self.config.precision_k:
                    if k > len(detections):
                        precisions[k] = None
                        continue
                    top_k = detections[:k]
                    tp = sum(1 for _, is_mal in top_k if is_mal)
                    precisions[k] = tp / k
                return precisions
        except Exception as e:
            logger.error(f"Error calculating precision@k: {e}")
            return {k: None for k in self.config.precision_k}

    def get_fp_rate(
        self, start_time: datetime, end_time: datetime
    ) -> Optional[float]:
        """Get false positives per 1000 events."""
        query = text("""
            WITH stats AS (
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(*) FILTER (
                        WHERE d.score >= :threshold AND NOT w.label = 'malicious'
                    ) AS false_positives
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.created_at BETWEEN :start_time AND :end_time
            )
            SELECT (false_positives::float * :window / total_events) AS fp_rate
            FROM stats
            WHERE total_events > 0
        """)
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    query,
                    {
                        "start_time": start_time,
                        "end_time": end_time,
                        "threshold": self.config.score_threshold,
                        "window": self.config.fp_window,
                    },
                ).first()
                return float(row.fp_rate) if row else None
        except Exception as e:
            logger.error(f"Error calculating FP rate: {e}")
            return None

    def evaluate(self, start_time: datetime, end_time: datetime) -> Dict[str, Optional[float]]:
        """Run full evaluation for time period."""
        tp, fp, tn, fn = self.get_confusion_matrix(start_time, end_time)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1        = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

        prec_k    = self.get_precision_at_k(start_time, end_time)
        fp_rate   = self.get_fp_rate(start_time, end_time)

        latencies = self.get_detection_latencies(start_time, end_time)
        p95_latency = float(np.percentile(latencies, self.config.latency_percentile)) if latencies else None

        metrics: Dict[str, Optional[float]] = {
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "fp_per_1k": fp_rate,
            "p95_latency_ms": p95_latency,
        }
        for k, p in prec_k.items():
            metrics[f"precision@{k}"] = p
        return metrics
