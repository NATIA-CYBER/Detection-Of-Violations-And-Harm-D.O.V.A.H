"""Detection pipeline evaluation metrics"""
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import os
from collections import defaultdict

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EvalConfig(BaseModel):
    """Evaluation configuration."""
    precision_k: List[int] = [10, 50, 100]  # For precision@k
    fp_window: int = 1000  # Events for FP/1k
    latency_percentile: float = 95.0  # For P95 calculation
    score_threshold: float = 0.8  # For binary classification
    db_url: str = os.getenv('POSTGRES_URL', 'postgresql://dovah:dovah@localhost:5432/dovah')

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
                    d.session_id,
                    d.ts as detection_time,
                    MIN(e.ts) as first_event_time
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.ts BETWEEN :start_time AND :end_time
                AND d.score >= :threshold
                GROUP BY d.session_id, d.ts
            )
            SELECT 
                EXTRACT(EPOCH FROM (detection_time - first_event_time)) * 1000 as latency_ms
            FROM detection_times
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        'start_time': start_time,
                        'end_time': end_time,
                        'threshold': self.config.score_threshold
                    }
                )
                return [float(row.latency_ms) for row in result]
                
        except Exception as e:
            logger.error(f"Error getting detection latencies: {e}")
            return []
    
    def get_confusion_matrix(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[int, int, int, int]:
        """Get TP, FP, TN, FN counts."""
        query = text("""
            WITH scored_sessions AS (
                SELECT
                    d.session_id,
                    MAX(d.score) as max_score,
                    bool_or(e.label = 'malicious') as is_malicious
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.ts BETWEEN :start_time AND :end_time
                GROUP BY d.session_id
            )
            SELECT
                COUNT(*) FILTER (
                    WHERE max_score >= :threshold AND is_malicious
                ) as tp,
                COUNT(*) FILTER (
                    WHERE max_score >= :threshold AND NOT is_malicious
                ) as fp,
                COUNT(*) FILTER (
                    WHERE max_score < :threshold AND NOT is_malicious
                ) as tn,
                COUNT(*) FILTER (
                    WHERE max_score < :threshold AND is_malicious
                ) as fn
            FROM scored_sessions
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        'start_time': start_time,
                        'end_time': end_time,
                        'threshold': self.config.score_threshold
                    }
                )
                row = result.first()
                return (row.tp, row.fp, row.tn, row.fn)
                
        except Exception as e:
            logger.error(f"Error getting confusion matrix: {e}")
            return (0, 0, 0, 0)
    
    def get_precision_at_k(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[int, float]:
        """Get precision@k for configured k values."""
        query = text("""
            WITH ranked_detections AS (
                SELECT
                    d.session_id,
                    d.score,
                    bool_or(e.label = 'malicious') as is_malicious,
                    ROW_NUMBER() OVER (ORDER BY d.score DESC) as rank
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.ts BETWEEN :start_time AND :end_time
                GROUP BY d.session_id, d.score
            )
            SELECT
                rank,
                is_malicious
            FROM ranked_detections
            ORDER BY rank
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        'start_time': start_time,
                        'end_time': end_time
                    }
                )
                
                # Calculate precision@k for each k
                detections = [(row.rank, row.is_malicious) for row in result]
                precisions = {}
                
                for k in self.config.precision_k:
                    if k > len(detections):
                        precisions[k] = None
                        continue
                        
                    top_k = detections[:k]
                    true_positives = sum(1 for _, is_mal in top_k if is_mal)
                    precisions[k] = true_positives / k
                
                return precisions
                
        except Exception as e:
            logger.error(f"Error calculating precision@k: {e}")
            return {k: None for k in self.config.precision_k}
    
    def get_fp_rate(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Optional[float]:
        """Get false positives per 1000 events."""
        query = text("""
            WITH stats AS (
                SELECT
                    COUNT(*) as total_events,
                    COUNT(*) FILTER (
                        WHERE d.score >= :threshold AND NOT e.label = 'malicious'
                    ) as false_positives
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.ts BETWEEN :start_time AND :end_time
            )
            SELECT
                (false_positives::float * :window / total_events) as fp_rate
            FROM stats
            WHERE total_events > 0
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        'start_time': start_time,
                        'end_time': end_time,
                        'threshold': self.config.score_threshold,
                        'window': self.config.fp_window
                    }
                )
                row = result.first()
                return float(row.fp_rate) if row else None
                
        except Exception as e:
            logger.error(f"Error calculating FP rate: {e}")
            return None
    
    def evaluate(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """Run full evaluation for time period.
        
        Returns dict with metrics:
        - precision
        - recall
        - f1_score
        - precision@k (for each k)
        - fp_per_1k
        - p95_latency_ms
        """
        # Get confusion matrix
        tp, fp, tn, fn = self.get_confusion_matrix(start_time, end_time)
        
        # Calculate basic metrics
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        
        # Get precision@k
        prec_k = self.get_precision_at_k(start_time, end_time)
        
        # Get FP rate
        fp_rate = self.get_fp_rate(start_time, end_time)
        
        # Get P95 latency
        latencies = self.get_detection_latencies(start_time, end_time)
        p95_latency = np.percentile(latencies, self.config.latency_percentile) if latencies else None
        
        metrics = {
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'fp_per_1k': fp_rate,
            'p95_latency_ms': p95_latency
        }
        
        # Add precision@k metrics
        for k, p in prec_k.items():
            metrics[f'precision@{k}'] = p
        
        return metrics
