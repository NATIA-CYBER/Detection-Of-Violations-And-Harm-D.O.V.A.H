"""Drift detection using PSI and KS tests."""
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

def calculate_psi(expected: np.ndarray, actual: np.ndarray, eps: float = 1e-6) -> float:
    """Calculate Population Stability Index.
    
    PSI = sum((A - E) * ln(A/E)) where A and E are actual and expected probabilities.
    Values greater than 0.2 indicate significant drift.
    
    Args:
        expected: Baseline distribution
        actual: Current distribution to compare against baseline
        eps: Small value to avoid division by zero
        
    Returns:
        PSI value, higher values indicate more drift
    """
    # Get unique values and their counts
    unique_vals = np.unique(np.concatenate([expected, actual]))
    expected_counts = np.array([np.sum(expected == val) for val in unique_vals])
    actual_counts = np.array([np.sum(actual == val) for val in unique_vals])
    
    # Handle single-sample case
    if len(unique_vals) == 2 and (np.all(expected_counts == 0) or np.all(actual_counts == 0)):
        return 1.0  # Maximum drift for completely different distributions
    
    # Add small epsilon to avoid division by zero
    expected_counts = expected_counts + eps
    actual_counts = actual_counts + eps
    
    # Normalize to get probabilities
    expected_prob = expected_counts / expected_counts.sum()
    actual_prob = actual_counts / actual_counts.sum()
    
    # Calculate PSI
    psi = np.sum((actual_prob - expected_prob) * np.log(actual_prob / expected_prob))
    return float(np.abs(psi))

def calculate_ks_test(baseline: np.ndarray, current: np.ndarray) -> Tuple[float, float]:
    """Run Kolmogorov-Smirnov test."""
    statistic, pvalue = stats.ks_2samp(baseline, current)
    return float(statistic), float(pvalue)

def analyze_drift(baseline_df: pd.DataFrame, current_df: pd.DataFrame, 
                 features: List[str], out_dir: Path) -> Dict:
    """Analyze drift between two time windows."""
    results = {
        "psi": {},
        "ks_test": {},
        "summary": {
            "total_baseline": len(baseline_df),
            "total_current": len(current_df),
            "time_range_baseline": [
                baseline_df["timestamp"].min(),
                baseline_df["timestamp"].max()
            ],
            "time_range_current": [
                current_df["timestamp"].min(),
                current_df["timestamp"].max()
            ]
        }
    }
    
    # Calculate PSI for categorical features
    categorical_features = ["level", "component", "template_id"]
    psi_rows = []
    
    for feature in categorical_features:
        if feature in features:
            # Get unique categories and their counts
            baseline_cats = set(baseline_df[feature].unique())
            current_cats = set(current_df[feature].unique())
            
            # For single samples with different categories, set max drift
            if len(baseline_df) == 1 and len(current_df) == 1 and baseline_cats != current_cats:
                results["psi"][feature] = 1.0
                continue
            
            # Normal PSI calculation
            all_categories = sorted(baseline_cats | current_cats)
            baseline_counts = np.array([np.sum(baseline_df[feature] == cat) for cat in all_categories])
            current_counts = np.array([np.sum(current_df[feature] == cat) for cat in all_categories])
            
            # Calculate PSI
            psi = calculate_psi(baseline_counts, current_counts)
            results["psi"][feature] = psi
            
            # Add to PSI table
            for cat, base_count, curr_count in zip(all_categories, baseline_counts, current_counts):
                psi_rows.append({
                    "feature": feature,
                    "category": cat,
                    "baseline_count": int(base_count),
                    "current_count": int(curr_count),
                    "psi": psi
                })
    
    # Calculate KS test for numeric features
    numeric_features = ["msg_len"]
    for feature in numeric_features:
        if feature in features:
            statistic, pvalue = calculate_ks_test(
                baseline_df[feature].values,
                current_df[feature].values
            )
            results["ks_test"][feature] = {
                "statistic": statistic,
                "pvalue": pvalue
            }
    
    # Save results
    psi_table = pd.DataFrame(psi_rows)
    psi_table.to_csv(out_dir / "psi_table.csv", index=False)
    
    with open(out_dir / "psi_summary.json", "w") as f:
        json.dump({"psi": results["psi"]}, f, indent=2)
        
    with open(out_dir / "ks_summary.json", "w") as f:
        json.dump({"ks_test": results["ks_test"]}, f, indent=2)
    
    return results
