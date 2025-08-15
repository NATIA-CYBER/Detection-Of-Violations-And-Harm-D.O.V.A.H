"""Late fusion module for combining multiple detection signals.

This module implements late fusion logic to combine:
1. Log-LM perplexity scores
2. IsolationForest anomaly scores
3. EPSS exploitation likelihood
4. KEV known exploitation status
"""

from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from ..models.baselines import LogLM, WindowIsolationForest

def combine_scores(
    session: Session,
    window_id: str,
    lm_score: float,
    iforest_score: float,
    epss_scores: Dict[str, float],
    kev_cves: List[str],
    thresholds: Optional[Dict[str, float]] = None
) -> Tuple[float, Dict[str, float]]:
    """Combine multiple detection signals into a final score.
    
    Args:
        session: Database session
        window_id: Window identifier
        lm_score: Log-LM perplexity score
        iforest_score: IsolationForest anomaly score
        epss_scores: Dict mapping CVE IDs to EPSS scores
        kev_cves: List of CVEs from KEV catalog
        thresholds: Optional score thresholds, defaults to:
            lm_threshold: 0.8
            iforest_threshold: 0.7
            epss_threshold: 0.6
            
    Returns:
        Tuple of (final_score, component_scores)
        where final_score is in [0,1] and component_scores has individual scores
    """
    if thresholds is None:
        thresholds = {
            "lm_threshold": 0.8,
            "iforest_threshold": 0.7, 
            "epss_threshold": 0.6
        }
        
    # Normalize component scores to [0,1]
    norm_lm = min(lm_score / thresholds["lm_threshold"], 1.0)
    norm_iforest = min(iforest_score / thresholds["iforest_threshold"], 1.0)
    
    # Get max EPSS score if any CVEs present
    max_epss = max(epss_scores.values()) if epss_scores else 0.0
    norm_epss = min(max_epss / thresholds["epss_threshold"], 1.0)
    
    # KEV presence is binary
    kev_score = 1.0 if any(cve in kev_cves for cve in epss_scores.keys()) else 0.0
    
    # Weighted combination (can be tuned)
    weights = {
        "lm": 0.3,
        "iforest": 0.3,
        "epss": 0.2,
        "kev": 0.2
    }
    
    final_score = (
        weights["lm"] * norm_lm +
        weights["iforest"] * norm_iforest +
        weights["epss"] * norm_epss +
        weights["kev"] * kev_score
    )
    
    component_scores = {
        "lm_score": norm_lm,
        "iforest_score": norm_iforest,
        "epss_score": norm_epss,
        "kev_score": kev_score
    }
    
    # Save scores to detections table
    stmt = text("""
        INSERT INTO detections (
            window_id,
            score,
            lm_score,
            iforest_score,
            epss_score,
            kev_score,
            created_at
        ) VALUES (
            :window_id,
            :score,
            :lm_score,
            :iforest_score,
            :epss_score,
            :kev_score,
            CURRENT_TIMESTAMP
        )
    """)
    
    session.execute(
        stmt,
        {
            "window_id": window_id,
            "score": final_score,
            **component_scores
        }
    )
    session.commit()
    
    return final_score, component_scores
