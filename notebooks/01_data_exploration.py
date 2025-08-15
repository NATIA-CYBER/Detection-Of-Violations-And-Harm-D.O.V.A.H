"""DOVAH Data Exploration.

This script demonstrates data exploration and feature analysis for the DOVAH security ML pipeline,
focusing on EPSS and KEV data analysis.
"""

import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine
import json
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from drain3 import TemplateMiner

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def explore_epss():
    """Explore EPSS dataset."""
    epss_path = Path(__file__).parent.parent / "tests/data/epss/sample.csv"
    epss_df = pd.read_csv(epss_path)
    print("\nEPSS Dataset:")
    print(f"Total CVEs: {len(epss_df)}")
    print("\nSample EPSS scores:")
    print(epss_df.sort_values("epss", ascending=False).head())
    return epss_df

def explore_kev():
    """Explore KEV dataset."""
    kev_path = Path(__file__).parent.parent / "tests/data/kev/sample.json"
    with open(kev_path) as f:
        kev_data = json.load(f)
    
    print("\nKEV Catalog:")
    print(f"Version: {kev_data['catalogVersion']}")
    print(f"Total vulnerabilities: {kev_data['count']}")
    
    kev_df = pd.DataFrame(kev_data["vulnerabilities"])
    print("\nKEV Vendors:")
    print(kev_df["vendorProject"].value_counts())
    return kev_df

def calculate_psi(expected, actual, buckets=10, bucket_type='bins'):
    """Calculate Population Stability Index.
    
    Args:
        expected: numpy array of expected (baseline) values
        actual: numpy array of actual values
        buckets: number of buckets for binning
        bucket_type: 'bins' for equal-width or 'quantiles' for equal-population
    
    Returns:
        float: PSI value (<0.1 insignificant, <0.2 minor, >0.2 significant)
    """
    if bucket_type == 'quantiles':
        breakpoints = np.percentile(expected, np.linspace(0, 100, buckets + 1))
    else:
        breakpoints = np.linspace(start=min(min(expected), min(actual)), 
                                 stop=max(max(expected), max(actual)),
                                 num=buckets + 1)
    
    expected_percents = np.histogram(expected, breakpoints)[0] / len(expected)
    actual_percents = np.histogram(actual, breakpoints)[0] / len(actual)
    
    # Add small epsilon to avoid division by zero
    eps = 1e-10
    psi_value = sum(actual_percents[i] * np.log((actual_percents[i] + eps)/(expected_percents[i] + eps))
                    for i in range(buckets))
    
    return psi_value

def detect_drift(epss_df, window_size='7D'):
    """Detect distribution drift in EPSS scores.
    
    Args:
        epss_df: DataFrame with EPSS data
        window_size: Size of sliding window for drift detection
    """
    epss_df['date'] = pd.to_datetime(epss_df['date'])
    epss_df = epss_df.sort_values('date')
    
    # Create windows
    windows = [g for n, g in epss_df.groupby(pd.Grouper(key='date', freq=window_size))]
    if len(windows) < 2:
        logger.warning("Not enough data for drift detection")
        return
    
    baseline = windows[0]['epss'].values
    drift_detected = False
    
    print("\nDrift Analysis:")
    for i, window in enumerate(windows[1:], 1):
        current = window['epss'].values
        if len(current) < 10:
            continue
            
        # Calculate PSI
        psi = calculate_psi(baseline, current)
        
        # Perform KS test
        ks_stat, p_value = stats.ks_2samp(baseline, current)
        
        print(f"\nWindow {i} ({window['date'].min().date()} to {window['date'].max().date()})")
        print(f"PSI: {psi:.3f} ({'significant' if psi > 0.2 else 'minor' if psi > 0.1 else 'insignificant'})")
        print(f"KS test p-value: {p_value:.3f}")
        
        if psi > 0.2 or p_value < 0.05:
            drift_detected = True
            print("⚠️ Drift detected!")
            
    return drift_detected

def cross_reference(epss_df, kev_df):
    """Cross-reference EPSS and KEV data."""
    kev_cves = set(kev_df["cveID"])
    epss_cves = set(epss_df["cve"])
    common_cves = kev_cves.intersection(epss_cves)

    print("\nCross-reference Analysis:")
    print(f"CVEs in KEV: {len(kev_cves)}")
    print(f"CVEs in EPSS: {len(epss_cves)}")
    print(f"Common CVEs: {len(common_cves)}")

    # Show high-risk vulnerabilities
    high_risk = epss_df[epss_df["cve"].isin(common_cves)].sort_values("epss", ascending=False)
    print("\nHigh-risk vulnerabilities (in both EPSS and KEV):")
    print(high_risk)
    
    # Detect drift in EPSS scores
    drift_detected = detect_drift(epss_df)

def analyze_templates(log_file=None):
    """Analyze log templates and their distribution."""
    if log_file is None:
        log_file = Path(__file__).parent.parent / "tests/data/hdfs/sample.txt"
    """Analyze log templates and their distribution."""
    # Initialize template miner
    template_miner = TemplateMiner()
    template_counts = Counter()
    total_logs = 0
    templates = {}
    
    # Process logs
    with open(log_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 4:
                msg = parts[3]
                result = template_miner.add_log_message(msg)
                template_id = result['cluster_id']
                template_counts[template_id] += 1
                templates[template_id] = result['template_mined']
                total_logs += 1
    
    # Calculate statistics
    template_freqs = {k: v/total_logs for k,v in template_counts.items()}
    entropy = -sum(p * np.log2(p) for p in template_freqs.values())
    
    # Plot template frequency distribution
    plt.figure(figsize=(12, 6))
    freqs = sorted(template_freqs.values(), reverse=True)
    plt.plot(range(len(freqs)), freqs)
    plt.title("Template Frequency Distribution")
    plt.xlabel("Template Rank")
    plt.ylabel("Frequency")
    plt.yscale("log")
    plt.savefig(str(Path(__file__).parent.parent / "eda_results/template_frequencies.png"))
    plt.close()
    
    # Plot component distribution
    components = [parts[2] for parts in (line.strip().split("\t") for line in open(log_file)) 
                 if len(parts) >= 4]
    plt.figure(figsize=(10, 6))
    comp_counts = Counter(components)
    plt.bar(comp_counts.keys(), comp_counts.values())
    plt.title("Component Distribution")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(str(Path(__file__).parent.parent / "eda_results/component_distribution.png"))
    plt.close()
    
    # Plot log level distribution
    levels = [parts[4] if len(parts) > 4 else "UNKNOWN" 
             for parts in (line.strip().split("\t") for line in open(log_file))]
    plt.figure(figsize=(8, 6))
    level_counts = Counter(levels)
    plt.pie(level_counts.values(), labels=level_counts.keys(), autopct="%1.1f%%")
    plt.title("Log Level Distribution")
    plt.savefig(str(Path(__file__).parent.parent / "eda_results/level_distribution.png"))
    plt.close()
    
    # Analyze volume spikes
    timestamps = pd.to_datetime([parts[0] for parts in (line.strip().split("\t") 
                              for line in open(log_file)) if len(parts) >= 4])
    volumes = pd.Series(1, index=timestamps).resample('1H').count()
    
    # Plot volume with 3σ threshold
    plt.figure(figsize=(15, 5))
    mean = volumes.mean()
    std = volumes.std()
    plt.plot(volumes.index, volumes.values, label="Volume")
    plt.axhline(y=mean + 3*std, color='r', linestyle='--', label="3σ threshold")
    plt.title("Log Volume Over Time with 3σ Threshold")
    plt.xlabel("Time")
    plt.ylabel("Events per Hour")
    plt.legend()
    plt.tight_layout()
    plt.savefig(str(Path(__file__).parent.parent / "eda_results/volume_spikes.png"))
    plt.close()
    
    print("\nTemplate Analysis:")
    print(f"Total logs processed: {total_logs}")
    print(f"Unique templates: {len(template_counts)}")
    print(f"Template entropy: {entropy:.2f} bits")
    print(f"Top 5 templates:")
    for template_id, count in template_counts.most_common(5):
        print(f"Template {template_id}: {count} occurrences ({count/total_logs*100:.1f}%)")
        print(f"  Pattern: {templates[template_id]}")

def main():
    """Run data exploration and drift monitoring."""
    # Create output directory
    Path(__file__).parent.parent.joinpath("eda_results").mkdir(exist_ok=True)
    
    # Analyze HDFS logs
    analyze_templates()
    
    # Analyze threat feeds
    epss_df = explore_epss()
    kev_df = explore_kev()
    cross_reference(epss_df, kev_df)
    
    # Save drift results
    if detect_drift(epss_df):
        logger.warning("Distribution drift detected in EPSS scores")
        # In production, this would trigger model retraining

if __name__ == "__main__":
    main()
