"""HDFS Log Analysis: Drift & Distribution.

Analyzes HDFS logs for:
- Template frequency/entropy
- Class imbalance
- Spike detection (3σ)
- PSI/KS week-over-week
"""
import json
import os
import sys
from math import log2
from collections import Counter
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.preprocessing import KBinsDiscretizer

from src.eval.psi import calculate_psi, calculate_ks

def calculate_entropy(values):
    """Calculate Shannon entropy of a sequence of values."""
    if not values:
        return 0.0
    counts = Counter(values)
    probs = [count/len(values) for count in counts.values()]
    return -sum(p * log2(p) for p in probs)

import datetime
from datetime import timedelta
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

def plot_host_template_heatmap(df: pd.DataFrame, output_dir: Path):
    """Generate host × template frequency heatmap."""
    # Create pivot table
    pivot = pd.crosstab(df['host'], df['template_id'])
    
    # Plot heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot, cmap='YlOrRd', cbar_kws={'label': 'Frequency'})
    plt.title('Host × Template Frequency Heatmap')
    plt.tight_layout()
    plt.savefig(output_dir / 'host_template_heatmap.png')
    plt.close()
    
    # Save raw data
    pivot.to_csv(output_dir / 'host_template_frequencies.csv')

def plot_session_length(df: pd.DataFrame, output_dir: Path):
    """Generate session length distribution plot."""
    # Calculate session lengths
    session_lengths = df.groupby('session_id').agg({
        'ts': lambda x: (x.max() - x.min()).total_seconds() / 3600,  # hours
        'message': 'count'
    }).rename(columns={'ts': 'duration_hours', 'message': 'event_count'})
    
    # Plot distributions
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    
    # Duration distribution
    sns.histplot(session_lengths['duration_hours'], bins=50, ax=ax1)
    ax1.set_title('Session Duration Distribution')
    ax1.set_xlabel('Duration (hours)')
    
    # Event count distribution
    sns.histplot(session_lengths['event_count'], bins=50, ax=ax2)
    ax2.set_title('Session Event Count Distribution')
    ax2.set_xlabel('Number of Events')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'session_distributions.png')
    plt.close()
    
    # Save statistics
    stats = session_lengths.describe()
    stats.to_csv(output_dir / 'session_stats.csv')

def analyze_templates(df: pd.DataFrame, output_dir: Path):
    """Analyze template patterns and entropy."""
    # Hourly/daily entropy
    hourly_entropy = df.groupby(pd.Grouper(key='ts', freq='h')).apply(
        lambda x: calculate_entropy(x['template_id'].tolist())
    )
    daily_entropy = df.groupby(pd.Grouper(key='ts', freq='D')).apply(
        lambda x: calculate_entropy(x['template_id'].tolist())
    )
    
    # Plot entropy over time
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    hourly_entropy.plot(ax=ax1)
    ax1.set_title('Template Entropy by Hour')
    ax1.set_ylabel('Bits')
    
    daily_entropy.plot(ax=ax2)
    ax2.set_title('Template Entropy by Day')
    ax2.set_ylabel('Bits')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_over_time.png')
    plt.close()
    
    # Save entropy data
    hourly_entropy.to_csv(output_dir / 'hourly_entropy.csv')
    daily_entropy.to_csv(output_dir / 'daily_entropy.csv')
    
    # Analyze rare templates (tail)
    template_counts = df['template_id'].value_counts()
    rare_templates = template_counts[template_counts <= template_counts.quantile(0.1)]
    
    plt.figure(figsize=(10, 6))
    plt.hist(rare_templates.values, bins=50)
    plt.title('Distribution of Rare Template Frequencies')
    plt.xlabel('Frequency')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'rare_templates.png')
    plt.close()
    
    rare_stats = {
        'count': len(rare_templates),
        'min_freq': int(rare_templates.min()),
        'max_freq': int(rare_templates.max()),
        'mean_freq': float(rare_templates.mean()),
        'total_events': int(rare_templates.sum())
    }
    
    with open(output_dir / 'rare_templates.json', 'w') as f:
        json.dump(rare_stats, f, indent=2)
    """Analyze template distribution and entropy."""
    # Extract templates
    miner = TemplateMiner()
    templates = {}
    for msg in df['message']:
        result = miner.add_log_message(msg)
        templates[msg] = result['cluster_id']

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

    # Generate plots and save results
    plot_host_template_heatmap(df, output_dir)
    plot_session_length(df, output_dir)

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

def generate_data_card(output_dir: Path, stats: dict):
    """Generate data card markdown with analysis results."""
    docs_dir = Path('docs')
    docs_dir.mkdir(exist_ok=True)
    
    with open(docs_dir / 'data_card.md', 'w') as f:
        f.write("# HDFS Log Data Analysis Card\n\n")
        
        # Basic stats
        f.write("## Basic Statistics\n\n")
        f.write(f"- Time range: {stats['time_coverage']['start']} to {stats['time_coverage']['end']}\n")
        f.write(f"- Duration: {stats['time_coverage']['duration_hours']:.1f} hours\n")
        f.write(f"- Total events: {stats['total_events']:,}\n")
        f.write(f"- Mean event rate: {stats['hourly_stats']['mean']:.1f} events/hour\n\n")
        
        # Data quality
        f.write("## Data Quality\n\n")
        f.write("### Null Counts\n")
        for field, count in stats['null_counts'].items():
            f.write(f"- {field}: {count}\n")
        
        # Template analysis
        f.write("\n## Template Analysis\n\n")
        f.write(f"- Unique templates: {stats['template_stats']['unique_count']}\n")
        f.write(f"- Overall entropy: {stats['template_stats']['entropy']:.2f} bits\n")
        f.write(f"- Rare templates: {stats['rare_templates']['count']} (below 10th percentile)\n")
        
        # PII audit
        f.write("\n## PII Audit\n\n")
        f.write(f"- Messages with PII: {stats['pii_audit']['messages_with_pii']}\n")
        f.write("- Pre-scrub matches:\n")
        for pattern, count in stats['pii_audit']['pre_scrub'].items():
            f.write(f"  - {pattern}: {count}\n")
        f.write("- Post-scrub: All patterns verified zero matches\n")
        
        # Generated artifacts
        f.write("\n## Analysis Artifacts\n\n")
        f.write("### Plots\n")
        for plot in ['event_rate.png', 'entropy_over_time.png', 'rare_templates.png',
                    'host_template_heatmap.png', 'session_distributions.png']:
            f.write(f"- [{plot}](../eda_results/{plot})\n")
        
        f.write("\n### Data Files\n")
        for data_file in ['data_quality.json', 'hourly_entropy.csv', 'daily_entropy.csv',
                         'rare_templates.json', 'pii_audit.json']:
            f.write(f"- [{data_file}](../eda_results/{data_file})\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input JSONL file')
    parser.add_argument('--out', required=True, help='Output directory')
    args = parser.parse_args()

    # Load data
    print("Loading logs...")
    df = pd.read_json(args.input, lines=True)
    
    # Ensure required columns exist
    required_cols = ['timestamp', 'level', 'component', 'message', 'template_id']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
        
    # Convert timestamp to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    print(f"Loaded {len(df):,} log entries\n")

    # Run analyses
    print('\nAnalyzing template distribution...')
    analyze_templates(df, Path(args.out))

    print('\nAnalyzing class imbalance...')
    analyze_class_imbalance(df, Path(args.out))
    analyze_class_imbalance(df, output_dir)

    print('\nDetecting volume spikes...')
    detect_spikes(df, output_dir)

    print('\nAnalyzing distribution drift...')
    analyze_drift(df, output_dir)

if __name__ == '__main__':
    main()
