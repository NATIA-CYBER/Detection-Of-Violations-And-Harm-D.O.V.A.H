"""Tests for baseline models."""
import numpy as np
import pytest
from src.models.baselines import LogLM, WindowIsolationForest

def test_loglm_perplexity():
    """Test LogLM perplexity calculation."""
    model = LogLM(n=2)
    sequences = [
        ["login", "read", "write", "logout"],
        ["login", "read", "read", "logout"],
        ["login", "write", "logout"]
    ]
    
    # Train model
    model.fit(sequences)
    
    # Test sequence
    test_seq = ["login", "read", "logout"]
    perplexity = model.perplexity(test_seq)
    
    assert isinstance(perplexity, float)
    assert perplexity >= 0

def test_iforest_scoring():
    """Test IsolationForest scoring."""
    model = WindowIsolationForest(n_estimators=10, random_state=42)
    
    # Generate synthetic data
    X = np.random.randn(100, 5)  # 100 samples, 5 features
    
    # Fit and score
    model.fit(X)
    scores = model.predict_score(X)
    
    assert len(scores) == len(X)
    assert all(0 <= s <= 1 for s in scores)  # Scores normalized to [0,1]
