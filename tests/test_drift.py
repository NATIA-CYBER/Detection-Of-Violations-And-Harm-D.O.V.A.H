"""Tests for distribution drift detection."""
import json
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
from src.eval.drift import calculate_psi, calculate_ks_test, analyze_drift

def test_psi_calculation():
    # Create synthetic distributions
    baseline = np.array([1, 1, 1, 2, 2, 2, 3, 3, 3])
    similar = np.array([1, 1, 2, 2, 2, 2, 3, 3, 3])  # Similar distribution
    different = np.array([7, 7, 7, 8, 8, 8, 9, 9, 9])  # Very different distribution
    
    # Test similar distributions
    psi_similar = calculate_psi(baseline, similar)
    assert psi_similar < 0.1  # Small drift
    
    # Test different distributions
    psi_different = calculate_psi(baseline, different)
    assert psi_different > 1.0  # Large drift

def test_drift_analysis():
    # Create test dataframes with timestamps
    np.random.seed(42)
    n_samples = 100
    baseline_df = pd.DataFrame({
        'timestamp': pd.date_range('2025-08-01', periods=n_samples, freq='h'),
        'level': np.random.choice(['INFO', 'WARN', 'ERROR'], n_samples),
        'component': np.random.choice(['DataNode', 'NameNode'], n_samples),
        'template_id': np.random.choice(['T1', 'T2', 'T3'], n_samples),
        'msg_len': np.random.normal(50, 10, n_samples)
    })
    
    current_df = pd.DataFrame({
        'timestamp': pd.date_range('2025-08-08', periods=n_samples, freq='h'),
        'level': np.random.choice(['INFO', 'WARN', 'ERROR'], n_samples),
        'component': np.random.choice(['DataNode', 'NameNode'], n_samples),
        'template_id': np.random.choice(['T1', 'T2', 'T3'], n_samples),
        'msg_len': np.random.normal(60, 15, n_samples)  # Drift in message length
    })
    
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        results = analyze_drift(
            baseline_df,
            current_df,
            features=['level', 'component', 'template_id', 'msg_len'],
            out_dir=out_dir
        )
        
        # Check PSI results
        assert 'psi' in results
        assert all(f in results['psi'] for f in ['level', 'component', 'template_id'])
        
        # Check KS test results
        assert 'ks_test' in results
        assert 'msg_len' in results['ks_test']
        assert 'statistic' in results['ks_test']['msg_len']
        assert 'pvalue' in results['ks_test']['msg_len']
        
        # Check output files
        assert (out_dir / 'psi_table.csv').exists()
        assert (out_dir / 'psi_summary.json').exists()
        assert (out_dir / 'ks_summary.json').exists()

def test_ks_test():
    # Create synthetic distributions
    np.random.seed(42)
    baseline = np.random.normal(0, 1, 1000)
    similar = np.random.normal(0, 1.1, 1000)  # Similar distribution
    different = np.random.normal(2, 2, 1000)  # Different distribution
    
    # Test similar distributions
    statistic, pvalue = calculate_ks_test(baseline, similar)
    assert statistic < 0.1  # Small difference
    assert pvalue > 0.05  # Not statistically significant
    
    # Test different distributions
    statistic, pvalue = calculate_ks_test(baseline, different)
    assert statistic > 0.1  # Large difference
    assert pvalue < 0.05  # Statistically significant

def test_edge_cases():
    # Test with empty data
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        baseline_df = pd.DataFrame({
            'timestamp': pd.date_range('2025-08-01', periods=1, freq='h'),
            'level': ['INFO'],
            'component': ['DataNode'],
            'msg_len': [50]
        })
        
        # Test with single sample
        current_df = pd.DataFrame({
            'timestamp': pd.date_range('2025-08-02', periods=1, freq='h'),
            'level': ['WARN'],
            'component': ['NameNode'], 
            'msg_len': [60]
        })
        
        results = analyze_drift(baseline_df, current_df, ['level', 'msg_len'], out_dir)
        assert 'summary' in results
        assert results['summary']['total_current'] == 1
        assert 'psi' in results
        assert 'level' in results['psi']
        assert results['psi']['level'] > 0  # Should show drift for different levels
