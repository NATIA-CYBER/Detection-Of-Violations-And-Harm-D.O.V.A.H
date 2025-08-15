"""Tests for distribution drift detection."""
import numpy as np
import pandas as pd
from src.eval.psi import calculate_psi, detect_distribution_drift

def test_psi_calculation():
    # Create synthetic distributions
    np.random.seed(42)
    baseline = np.random.normal(0, 1, 1000)
    similar = np.random.normal(0, 1.1, 1000)  # Similar distribution
    different = np.random.normal(2, 2, 1000)  # Different distribution
    
    # Test similar distributions
    psi_similar, _, _ = calculate_psi(baseline, similar)
    assert psi_similar < 0.2  # Should be below drift threshold
    
    # Test different distributions
    psi_different, _, _ = calculate_psi(baseline, different)
    assert psi_different > 0.2  # Should indicate drift

def test_drift_detection():
    # Create test dataframes
    baseline = pd.DataFrame({
        'numeric': np.random.normal(0, 1, 100),
        'categorical': np.random.choice(['A', 'B', 'C'], 100)
    })
    
    # Similar distribution
    current_similar = pd.DataFrame({
        'numeric': np.random.normal(0, 1.1, 100),
        'categorical': np.random.choice(['A', 'B', 'C'], 100)
    })
    
    results = detect_distribution_drift(
        baseline,
        current_similar,
        features=['numeric']
    )
    
    assert 'numeric' in results
    assert 'psi' in results['numeric']
    assert 'drift_detected' in results['numeric']
    assert results['numeric']['psi'] < 0.2

def test_drift_edge_cases():
    # Test with empty data
    empty_df = pd.DataFrame({'col': []})
    baseline = pd.DataFrame({'col': [1, 2, 3]})
    
    results = detect_distribution_drift(baseline, empty_df, ['col'])
    assert results == {}
    
    # Test with constant values
    constant_df = pd.DataFrame({'col': [1, 1, 1]})
    results = detect_distribution_drift(baseline, constant_df, ['col'])
    assert 'col' in results
    
    # Test with missing features
    results = detect_distribution_drift(baseline, baseline, ['missing_col'])
    assert results == {}
