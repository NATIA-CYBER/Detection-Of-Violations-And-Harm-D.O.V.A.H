"""Risk score fusion logic"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
import os

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from pydantic import BaseModel

from .score import SessionScorer, ScoreConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FusionConfig(BaseModel):
    weights: Dict[str, float] = {
        'anomaly_score': 0.6,
        'epss_score': 0.3,
        'kev_score': 0.1
    }
    min_confidence: float = 0.7
    db_url: str = os.getenv('POSTGRES_URL', 'postgresql://dovah:dovah@localhost:5432/dovah')

class LateFusion:
    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or FusionConfig()
        self.engine = create_engine(self.config.db_url)
        self.session_scorer = SessionScorer()
    
    def get_intel_scores(self, session_id: str) -> Dict[str, float]:
        """Get EPSS and KEV scores for a session."""
        query = text("""
            WITH session_cves AS (
                SELECT DISTINCT
                    d.session_id,
                    i.cve_id,
                    i.epss_score,
                    CASE WHEN k.cve_id IS NOT NULL THEN 1.0 ELSE 0.0 END as kev_score
                FROM detections d
                JOIN window_features w ON d.window_id = w.id
                LEFT JOIN epss e ON d.cve_id = e.cve_id
                LEFT JOIN kev k ON d.cve_id = k.cve_id
                WHERE d.session_id = :session_id
                AND d.source = 'iforest'
            )
            SELECT
                MAX(epss_score) as max_epss,
                MAX(kev_score) as max_kev
            FROM session_cves
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'session_id': session_id})
                row = result.first()
                
                if not row or row.max_epss is None:
                    return {'epss_score': 0.0, 'kev_score': 0.0}
                    
                return {
                    'epss_score': float(row.max_epss),
                    'kev_score': float(row.max_kev)
                }
                
        except Exception as e:
            logger.error(f"Error getting intel scores: {e}")
            return {'epss_score': 0.0, 'kev_score': 0.0}
    
    def fuse_scores(self, scores: Dict[str, float]) -> float:
        """Combine multiple scores using weighted average."""
        total_weight = 0.0
        weighted_sum = 0.0
        
        for signal, score in scores.items():
            if signal in self.config.weights:
                weight = self.config.weights[signal]
                weighted_sum += weight * score
                total_weight += weight
        
        if total_weight == 0:
            return 0.0
            
        return weighted_sum / total_weight
    
    def get_session_risk(self, session_id: str) -> Optional[Dict]:
        """Get fused risk score and details for a session.
        
        Returns:
            Dict with:
            - risk_score: final fused score
            - confidence: confidence in the score
            - signals: individual signal scores
            - session_details: from SessionScorer
        """
        # Get session details first
        session = self.session_scorer.get_session_details(session_id)
        if not session:
            return None
            
        # Get intel scores
        intel = self.get_intel_scores(session_id)
        
        # Combine scores
        scores = {
            'anomaly_score': session['max_score'],
            'epss_score': intel['epss_score'],
            'kev_score': intel['kev_score']
        }
        
        # Calculate confidence based on signal availability
        available_signals = sum(1 for s in scores.values() if s > 0)
        confidence = available_signals / len(self.config.weights)
        
        if confidence < self.config.min_confidence:
            logger.warning(f"Low confidence ({confidence:.2f}) for session {session_id}")
        
        return {
            'risk_score': self.fuse_scores(scores),
            'confidence': confidence,
            'signals': scores,
            'session_details': session
        }
    
    def get_high_risk_sessions(
        self,
        min_score: float = 0.8,
        min_confidence: float = 0.7
    ) -> List[Dict]:
        """Get all high-risk active sessions.
        
        Args:
            min_score: Minimum risk score threshold
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of session risk details
        """
        high_risk = []
        active_sessions = self.session_scorer.get_active_sessions()
        
        for session_id in active_sessions:
            risk = self.get_session_risk(session_id)
            if risk and risk['risk_score'] >= min_score and risk['confidence'] >= min_confidence:
                high_risk.append(risk)
        
        return sorted(
            high_risk,
            key=lambda x: x['risk_score'],
            reverse=True
        )
