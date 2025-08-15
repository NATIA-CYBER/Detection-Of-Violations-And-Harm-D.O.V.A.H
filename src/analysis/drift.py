"""HDFS log drift analysis.

Analyzes HDFS logs for:
- Template frequency/entropy
- Class imbalance
- Spike detection (3σ)
- PSI/KS week-over-week
"""
import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
from collections import Counter
import json
from datetime import datetime, timedelta
from drain3 import TemplateMiner
import logging

logger = logging.getLogger(__name__)

def analyze_templates(df: pd.DataFrame) -> dict:
    """Analyze template distribution and entropy."""
    # Extract templates
    miner = TemplateMiner()
    templates = {}
    for msg in df['message']:
        result = miner.add_log_message(msg)
        templates[msg] = result.cluster_id

    df['template_id'] = df['message'].map(templates)

    # Calculate frequencies
    template_counts = df['template_id'].value_counts()

    # Calculate entropy
    probs = template_counts / len(df)
    entropy = stats.entropy(probs)
    logger.info(f'Template entropy: {entropy:.2f} bits')

    # Return stats
    return {
        'entropy': float(entropy),
        'unique_templates': len(template_counts),
        'top_templates': template_counts.head(10).to_dict()
    }

def analyze_class_imbalance(df: pd.DataFrame) -> dict:
    """Check distribution of log levels and components."""
    return {
        'level_counts': df['level'].value_counts().to_dict(),
        'component_counts': df['component'].value_counts().head(20).to_dict()
    }

def detect_spikes(df: pd.DataFrame) -> dict:
    """Detect anomalous spikes in log volume."""
    # Resample to 5-minute buckets
    ts_counts = df.set_index('ts').resample('5T').size()

    # Calculate mean and std
    mean = ts_counts.mean()
    std = ts_counts.std()
    threshold = mean + 3*std

    # Find spikes
    spikes = ts_counts[ts_counts > threshold]
    logger.info(f'Found {len(spikes)} spikes above 3σ threshold')

    return {
        'mean_volume': float(mean),
        'std_volume': float(std),
        'threshold': float(threshold),
        'spike_count': len(spikes),
        'spike_times': [str(t) for t in spikes.index]
    }

def calculate_psi(expected, actual):
    """Calculate Population Stability Index."""
    # Convert to probabilities
    e_probs = expected / expected.sum()
    a_probs = actual / actual.sum()
    
    # Handle zero probabilities
    e_probs = e_probs.replace(0, 1e-6)
    a_probs = a_probs.replace(0, 1e-6)
    
    # Calculate PSI
    psi = ((a_probs - e_probs) * np.log(a_probs / e_probs)).sum()
    return psi

def analyze_drift(df: pd.DataFrame) -> list:
    """Check for distribution drift between weeks."""
    # Split into weeks
    df['week'] = df['ts'].dt.isocalendar().week

    # Get template distributions by week
    weekly_dists = {}
    for week in df['week'].unique():
        weekly_dists[week] = df[df['week'] == week]['template_id'].value_counts()

    # Calculate PSI and KS test for each week pair
    drift_stats = []
    weeks = sorted(weekly_dists.keys())
    for i in range(len(weeks)-1):
        week1, week2 = weeks[i], weeks[i+1]
        dist1, dist2 = weekly_dists[week1], weekly_dists[week2]
        
        # PSI
        psi = calculate_psi(dist1, dist2)
        
        # KS test
        ks_stat, p_val = stats.ks_2samp(
            np.repeat(dist1.index, dist1.values),
            np.repeat(dist2.index, dist2.values)
        )
        
        drift_stats.append({
            'week1': int(week1),
            'week2': int(week2),
            'psi': float(psi),
            'ks_stat': float(ks_stat),
            'ks_pvalue': float(p_val)
        })
        
        logger.info(f'Week {week1} vs {week2}:')
        logger.info(f'  PSI: {psi:.3f}')
        logger.info(f'  KS stat: {ks_stat:.3f} (p={p_val:.3e})')

    return drift_stats

def analyze_logs(df: pd.DataFrame) -> dict:
    """Run full analysis suite on HDFS logs."""
    return {
        'templates': analyze_templates(df),
        'distributions': analyze_class_imbalance(df),
        'spikes': detect_spikes(df),
        'drift': analyze_drift(df)
    }
