"""Tests for late fusion module."""
import pytest
from sqlalchemy.orm import Session
from src.fusion.late_fusion import combine_scores

def test_combine_scores(db_session: Session):
    """Test late fusion score combination."""
    # Test inputs
    window_id = "test_window_1"
    lm_score = 0.9  # High perplexity
    iforest_score = 0.8  # High anomaly
    epss_scores = {"CVE-2021-44228": 0.97}  # Log4Shell
    kev_cves = ["CVE-2021-44228"]
    
    # Default thresholds
    final_score, component_scores = combine_scores(
        session=db_session,
        window_id=window_id,
        lm_score=lm_score,
        iforest_score=iforest_score,
        epss_scores=epss_scores,
        kev_cves=kev_cves
    )
    
    # Verify score ranges
    assert 0 <= final_score <= 1
    for score in component_scores.values():
        assert 0 <= score <= 1
    
    # Verify component presence
    assert "lm_score" in component_scores
    assert "iforest_score" in component_scores
    assert "epss_score" in component_scores
    assert "kev_score" in component_scores
    
    # Verify KEV match impact
    assert component_scores["kev_score"] == 1.0
    
    # Test with custom thresholds
    thresholds = {
        "lm_threshold": 0.9,
        "iforest_threshold": 0.8,
        "epss_threshold": 0.95
    }
    
    final_score_custom, _ = combine_scores(
        session=db_session,
        window_id="test_window_2",
        lm_score=lm_score,
        iforest_score=iforest_score,
        epss_scores=epss_scores,
        kev_cves=kev_cves,
        thresholds=thresholds
    )
    
    # Score should be different with custom thresholds
    assert final_score != final_score_custom
