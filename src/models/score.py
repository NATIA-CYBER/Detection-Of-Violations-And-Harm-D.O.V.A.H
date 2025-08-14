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
                    session_id,
                    host,
                    ts,
                    score,
                    COUNT(*) OVER (PARTITION BY session_id) as event_count
                FROM detections d
                WHERE ts BETWEEN :start_time AND :end_time
                AND source = 'iforest'
            )
            SELECT
                session_id,
                host,
                MAX(ts) as last_seen,
                AVG(score) as avg_score,
                MAX(score) as max_score,
                MIN(score) as min_score,
                MAX(event_count) as total_events
            FROM session_windows
            GROUP BY session_id, host
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
            SELECT DISTINCT session_id
            FROM detections
            WHERE ts >= :cutoff
            AND source = 'iforest'
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
    
    def get_session_details(self, session_id: str) -> Optional[Dict]:
        """Get detailed scoring info for a specific session.
        
        Returns:
            Dict with session details including:
            - host
            - first_seen
            - last_seen  
            - event_count
            - avg_score
            - max_score
            - min_score
        """
        query = text("""
            SELECT
                host,
                MIN(ts) as first_seen,
                MAX(ts) as last_seen,
                COUNT(*) as event_count,
                AVG(score) as avg_score,
                MAX(score) as max_score,
                MIN(score) as min_score
            FROM detections
            WHERE session_id = :session_id
            AND source = 'iforest'
            GROUP BY host
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
