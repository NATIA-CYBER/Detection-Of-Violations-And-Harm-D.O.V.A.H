"""HDFS Log Analysis: Drift & Distribution.

Analyzes HDFS logs for:
- Template frequency/entropy
- Class imbalance
- Spike detection (3σ)
- PSI/KS week-over-week
"""
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from collections import Counter
import json
from datetime import datetime, timedelta
from drain3 import TemplateMiner

# Configure plots
# Use default style
sns.set_theme()

def load_hdfs_logs(log_dir: Path) -> pd.DataFrame:
    """Load HDFS logs into DataFrame."""
    records = []
    for log_file in log_dir.glob('*.log'):
        with open(log_file) as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    ts_str, host, comp, msg, *rest = parts
                    level = rest[0] if rest else None
                    records.append({
                        'ts': pd.to_datetime(ts_str),
                        'host': host,
                        'component': comp,
                        'message': msg,
                        'level': level
                    })
    return pd.DataFrame(records)

def analyze_templates(df: pd.DataFrame, output_dir: Path):
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

    # Plot top 20 templates
    plt.figure(figsize=(15, 6))
    template_counts.head(20).plot(kind='bar')
    plt.title('Top 20 Template Frequencies')
    plt.xlabel('Template ID')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'template_frequencies.png')
    plt.close()

    # Calculate entropy
    probs = template_counts / len(df)
    entropy = stats.entropy(probs)
    print(f'Template entropy: {entropy:.2f} bits')

    # Save template stats
    template_stats = {
        'entropy': float(entropy),
        'unique_templates': len(template_counts),
        'top_templates': template_counts.head(10).to_dict()
    }
    with open(output_dir / 'template_stats.json', 'w') as f:
        json.dump(template_stats, f, indent=2)

def analyze_class_imbalance(df: pd.DataFrame, output_dir: Path):
    """Check distribution of log levels and components."""
    # Log level distribution
    plt.figure(figsize=(10, 5))
    df['level'].value_counts().plot(kind='bar')
    plt.title('Log Level Distribution')
    plt.xlabel('Level')
    plt.ylabel('Count')
    plt.tight_layout()
    plt.savefig(output_dir / 'level_distribution.png')
    plt.close()

    # Component distribution
    plt.figure(figsize=(15, 6))
    df['component'].value_counts().head(20).plot(kind='bar')
    plt.title('Top 20 Components')
    plt.xlabel('Component')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'component_distribution.png')
    plt.close()

    # Save distribution stats
    dist_stats = {
        'level_counts': df['level'].value_counts().to_dict(),
        'component_counts': df['component'].value_counts().head(20).to_dict()
    }
    with open(output_dir / 'distribution_stats.json', 'w') as f:
        json.dump(dist_stats, f, indent=2)

def detect_spikes(df: pd.DataFrame, output_dir: Path):
    """Detect anomalous spikes in log volume."""
    # Resample to 5-minute buckets
    ts_counts = df.set_index('ts').resample('5T').size()

    # Calculate mean and std
    mean = ts_counts.mean()
    std = ts_counts.std()
    threshold = mean + 3*std

    # Find spikes
    spikes = ts_counts[ts_counts > threshold]

    # Plot
    plt.figure(figsize=(15, 6))
    ts_counts.plot()
    plt.axhline(y=threshold, color='r', linestyle='--', label='3σ threshold')
    plt.scatter(spikes.index, spikes.values, color='red', label='Spikes')
    plt.title('Log Volume Over Time')
    plt.xlabel('Time')
    plt.ylabel('Logs per 5min')
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / 'volume_spikes.png')
    plt.close()

    print(f'Found {len(spikes)} spikes above 3σ threshold')

    # Save spike stats
    spike_stats = {
        'mean_volume': float(mean),
        'std_volume': float(std),
        'threshold': float(threshold),
        'spike_count': len(spikes),
        'spike_times': [str(t) for t in spikes.index]
    }
    with open(output_dir / 'spike_stats.json', 'w') as f:
        json.dump(spike_stats, f, indent=2)

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

def analyze_drift(df: pd.DataFrame, output_dir: Path):
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
        
        print(f'Week {week1} vs {week2}:')
        print(f'  PSI: {psi:.3f}')
        print(f'  KS stat: {ks_stat:.3f} (p={p_val:.3e})')

    # Save drift stats
    with open(output_dir / 'drift_stats.json', 'w') as f:
        json.dump(drift_stats, f, indent=2)

def main():
    # Setup paths
    log_dir = Path('../tests/data/hdfs')
    output_dir = Path('eda_results')
    output_dir.mkdir(exist_ok=True)

    # Load logs
    print('Loading logs...')
    df = load_hdfs_logs(log_dir)
    print(f'Loaded {len(df):,} log entries')

    # Run analyses
    print('\nAnalyzing template distribution...')
    analyze_templates(df, output_dir)

    print('\nAnalyzing class imbalance...')
    analyze_class_imbalance(df, output_dir)

    print('\nDetecting volume spikes...')
    detect_spikes(df, output_dir)

    print('\nAnalyzing distribution drift...')
    analyze_drift(df, output_dir)

if __name__ == '__main__':
    main()
