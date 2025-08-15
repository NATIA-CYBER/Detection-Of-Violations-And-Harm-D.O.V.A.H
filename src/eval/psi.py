"""Population Stability Index (PSI) calculation utilities."""

import numpy as np
import pandas as pd
from typing import Tuple, List, Union, Dict
from scipy import stats

def calculate_psi(expected: pd.Series, actual: pd.Series, bins: int = 10, bin_type: str = 'quantile') -> float:
    """Calculate Population Stability Index between two distributions.
    
    Args:
        expected: Baseline distribution
        actual: Current distribution
        bins: Number of bins for comparison
        bin_type: 'quantile' or 'uniform' binning
        
    Returns:
        Tuple of (PSI value, expected percentages, actual percentages)
    """
    if isinstance(expected, pd.Series):
        expected = expected.values
    if isinstance(actual, pd.Series):
        actual = actual.values
        
    # Define bin edges
    if bin_type == 'quantile':
        edges = np.percentile(
            expected,
            np.linspace(0, 100, bins + 1)
        )
    else:
        edges = np.linspace(
            min(expected.min(), actual.min()),
            max(expected.max(), actual.max()),
            bins + 1
        )
    
    # Calculate distributions
    e_counts, _ = np.histogram(expected, bins=edges)
    a_counts, _ = np.histogram(actual, bins=edges)
    
    # Convert to percentages with small epsilon
    eps = 1e-6
    e_pct = np.where(e_counts == 0, eps, e_counts / e_counts.sum())
    a_pct = np.where(a_counts == 0, eps, a_counts / a_counts.sum())
    
    # Calculate PSI
    psi_value = float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))
    
    return psi_value, e_pct, a_pct

def detect_distribution_drift(
    baseline: pd.DataFrame,
    current: pd.DataFrame,
    features: List[str],
    psi_threshold: float = 0.2,
    ks_alpha: float = 0.05
) -> Dict[str, Dict]:
    """Detect distribution drift using PSI and KS test.
    
    Args:
        baseline: Baseline data
        current: Current data
        features: Features to monitor
        psi_threshold: PSI threshold for drift
        ks_alpha: Significance level for KS test
        
    Returns:
        Dict with drift metrics per feature
    """
    results = {}
    
    for feature in features:
        # Skip if feature missing
        if feature not in baseline or feature not in current:
            continue
            
        # Get feature values
        base_vals = baseline[feature].dropna()
        curr_vals = current[feature].dropna()
        
        if len(base_vals) < 2 or len(curr_vals) < 2:
            continue
            
        # Calculate PSI
        psi_val, e_pct, a_pct = calculate_psi(base_vals, curr_vals)
        
        # Run KS test
        ks_stat, p_val = stats.ks_2samp(base_vals, curr_vals)
        
        # Determine drift
        has_drift = psi_val > psi_threshold or p_val < ks_alpha
        
        results[feature] = {
            'psi': round(psi_val, 3),
            'ks_statistic': round(ks_stat, 3),
            'ks_p_value': round(p_val, 3),
            'drift_detected': has_drift,
            'baseline_mean': float(base_vals.mean()),
            'current_mean': float(curr_vals.mean()),
            'baseline_std': float(base_vals.std()),
            'current_std': float(curr_vals.std())
        }
    
    return results

def psi_by_feature(expected_df: pd.DataFrame,
                  actual_df: pd.DataFrame,
                  features: List[str],
                  bins: int = 10) -> pd.Series:
    """Calculate PSI for multiple features.
    
    Args:
        expected_df: Reference dataframe
        actual_df: New dataframe
        features: List of feature columns
        bins: Number of bins for quantile bucketing
        
    Returns:
        Series of PSI values per feature
    """
    return pd.Series({
        feat: calculate_psi(expected_df[feat], actual_df[feat], bins)
        for feat in features
    })

def psi_contribution(expected: pd.Series,
                    actual: pd.Series,
                    bins: int = 10) -> pd.DataFrame:
    """Calculate per-bin PSI contributions.
    
    Args:
        expected: Reference distribution
        actual: Actual distribution
        bins: Number of bins
        
    Returns:
        DataFrame with bin edges and PSI contribution
    """
    quantiles = np.linspace(0, 1, bins+1)
    cut = expected.quantile(quantiles).values
    cut[0], cut[-1] = -np.inf, np.inf
    
    e_counts, edges = np.histogram(expected, bins=cut)
    a_counts, _ = np.histogram(actual, bins=cut)
    
    e_pct = np.where(e_counts==0, 1e-6, e_counts/e_counts.sum())
    a_pct = np.where(a_counts==0, 1e-6, a_counts/a_counts.sum())
    
    contributions = (a_pct - e_pct) * np.log(a_pct/e_pct)
    
    return pd.DataFrame({
        'bin_start': edges[:-1],
        'bin_end': edges[1:],
        'expected_pct': e_pct,
        'actual_pct': a_pct,
        'psi_contribution': contributions
    })

def calculate_ks(expected: pd.Series, actual: pd.Series) -> Dict[str, float]:
    """Calculate Kolmogorov-Smirnov test between two distributions.
    
    Args:
        expected: Reference/expected distribution
        actual: Actual/new distribution
        
    Returns:
        Dictionary with KS statistic and p-value
    """
    statistic, pvalue = stats.ks_2samp(expected, actual)
    return {
        'statistic': float(statistic),
        'pvalue': float(pvalue)
    }
