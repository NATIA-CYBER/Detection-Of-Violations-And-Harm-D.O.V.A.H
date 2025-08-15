"""Session scoring and mapping functionality.

Maps session IDs to scores and handles score aggregation.
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import os

import pandas as pd
from sqlalchemy import create_engine, text
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScoreConfig(BaseModel):
    """Score aggregation configuration."""
    window_size: int = 60  # seconds
    min_events: int = 5
    max_score_age: int = 3600  # 1 hour
    db_url: str = os.getenv('POSTGRES_URL', 'postgresql://dovah:dovah@localhost:5432/dovah')

class SessionScorer:
    """Maps and aggregates session scores."""
    
    def __init__(self, config: Optional[ScoreConfig] = None):
        self.config = config or ScoreConfig()
        self.engine = create_engine(self.config.db_url)
    
    def get_session_scores(self, start_time: datetime, end_time: datetime) -> Dict[str, float]:
        """Get aggregated scores for all sessions in time range.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Dict mapping session_id to aggregated score
        """
        query = text("""
            WITH session_windows AS (
                SELECT 
                    d.session_id,
                    d.ts,
                    d.score,
                    w.event_count,
                    w.unique_components,
                    w.error_ratio
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                WHERE d.ts BETWEEN :start_time AND :end_time
                AND d.source = 'iforest'
            )
            SELECT
                session_id,
                MAX(ts) as last_seen,
                AVG(score) as avg_score,
                MAX(score) as max_score,
                MIN(score) as min_score,
                SUM(event_count) as total_events,
                AVG(unique_components) as avg_unique_components,
                AVG(error_ratio) as avg_error_ratio
            FROM session_windows
            GROUP BY session_id
            HAVING MAX(event_count) >= :min_events
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        'start_time': start_time,
                        'end_time': end_time,
                        'min_events': self.config.min_events
                    }
                )
                
                scores = {}
                for row in result:
                    # Use max score as the session score
                    # Could be changed to avg or weighted avg if needed
                    scores[row.session_id] = float(row.max_score)
                    
                logger.info(f"Retrieved scores for {len(scores)} sessions")
                return scores
                
        except Exception as e:
            logger.error(f"Error getting session scores: {e}")
            return {}
    
    def get_active_sessions(self) -> List[str]:
        """Get list of currently active session IDs.
        
        A session is considered active if it has events within
        max_score_age seconds.
        """
        cutoff = datetime.utcnow() - timedelta(seconds=self.config.max_score_age)
        
        query = text("""
            SELECT DISTINCT d.session_id
            FROM detections d
            JOIN window_features w ON d.window_id = w.id
            WHERE d.ts >= :cutoff
            AND d.source = 'iforest'
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'cutoff': cutoff})
                sessions = [row.session_id for row in result]
                logger.info(f"Found {len(sessions)} active sessions")
                return sessions
                
        except Exception as e:
            logger.error(f"Error getting active sessions: {e}")
            return []
    
    def get_session_details(self, session_id: str) -> Dict:
        """Get all details for a session including window features.
        
        Args:
            session_id: Session ID to lookup
            - min_score
        """
        query = text("""
            SELECT
                MIN(d.ts) as first_seen,
                MAX(d.ts) as last_seen,
                SUM(w.event_count) as total_events,
                AVG(d.score) as avg_score,
                MAX(d.score) as max_score,
                MIN(d.score) as min_score,
                AVG(w.unique_components) as avg_unique_components,
                AVG(w.error_ratio) as avg_error_ratio,
                AVG(w.template_entropy) as avg_template_entropy,
                AVG(w.component_entropy) as avg_component_entropy
            FROM detections d
            JOIN window_features w ON d.window_id = w.id
            WHERE d.session_id = :session_id
            AND d.source = 'iforest'
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'session_id': session_id})
                row = result.first()
                
                if not row:
                    return None
                    
                return {
                    'host': row.host,
                    'first_seen': row.first_seen,
                    'last_seen': row.last_seen,
                    'event_count': row.event_count,
                    'avg_score': float(row.avg_score),
                    'max_score': float(row.max_score),
                    'min_score': float(row.min_score)
                }
                
        except Exception as e:
            logger.error(f"Error getting session details: {e}")
            return None
