"""Tests for late fusion module."""
import numpy as np
import pytest
from src.fusion.late_fusion import combine_scores

def test_score_combination():
    """Test score combination with weighted sum."""
    # Test scores from different models
    lm_scores = np.array([0.1, 0.8, 0.3])
    iforest_scores = np.array([0.2, 0.9, 0.4])
    
    # Default weights (0.5 each)
    combined = combine_scores(
        scores=[lm_scores, iforest_scores],
        weights=None
    )
    
    assert len(combined) == len(lm_scores)
    assert all(0 <= s <= 1 for s in combined)
    
    # Custom weights
    weights = [0.7, 0.3]  # More weight on LM
    combined = combine_scores(
        scores=[lm_scores, iforest_scores],
        weights=weights
    )
    
    assert len(combined) == len(lm_scores)
    assert all(0 <= s <= 1 for s in combined)
    
    # Verify weighted sum calculation
    expected = 0.7 * lm_scores[0] + 0.3 * iforest_scores[0]
    assert abs(combined[0] - expected) < 1e-6
