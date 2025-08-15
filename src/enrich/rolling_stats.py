"""Rolling window statistics for vulnerability analysis."""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta

class RollingStats:
    """Compute rolling window statistics for CVE and component analysis."""
    
    def __init__(self, windows: List[int] = [7, 30, 90]):
        """Initialize with window sizes in days."""
        self.windows = windows
        self.stats_cache = {}
        
    def compute_component_stats(self, 
                              events: List[Dict],
                              current_time: Optional[datetime] = None) -> Dict:
        """Compute component-wise rolling statistics.
        
        Args:
            events: List of enriched events with CVE and component info
            current_time: Reference time for windows (default: now)
            
        Returns:
            Dict with per-component statistics including:
                - vulnerability_rate: CVEs per day
                - risk_score: Weighted risk based on EPSS and component
                - patch_coverage: % of CVEs with patches
                - time_to_patch: Average days to patch
        """
        if not events:
            return {}
            
        current_time = current_time or datetime.now()
        df = pd.DataFrame(events)
        
        stats = {}
        for window in self.windows:
            window_start = current_time - timedelta(days=window)
            window_df = df[df['timestamp'] >= window_start]
            
            if window_df.empty:
                continue
                
            window_stats = {}
            for component in window_df['component'].unique():
                comp_df = window_df[window_df['component'] == component]
                
                window_stats[component] = {
                    'vulnerability_rate': len(comp_df) / window,
                    'risk_score': (
                        comp_df['epss_score'] * 
                        comp_df['component_risk']
                    ).mean(),
                    'patch_coverage': (
                        comp_df['patch_available'].sum() / 
                        len(comp_df)
                    ),
                    'time_to_patch': comp_df[
                        comp_df['patch_available']
                    ]['days_since_publish'].mean()
                }
                
            stats[f'{window}d'] = window_stats
            
        return stats
        
    def detect_anomalies(self, 
                        stats: Dict,
                        threshold: float = 2.0) -> Dict:
        """Detect anomalous patterns in rolling statistics.
        
        Args:
            stats: Output from compute_component_stats
            threshold: Z-score threshold for anomaly detection
            
        Returns:
            Dict of anomalies by component and metric
        """
        anomalies = {}
        
        for window, window_stats in stats.items():
            window_anomalies = {}
            
            for component, metrics in window_stats.items():
                metric_anomalies = {}
                
                for metric, value in metrics.items():
                    # Get historical values for this metric
                    historical = [
                        s[component][metric] 
                        for s in stats.values()
                        if component in s and metric in s[component]
                    ]
                    
                    if len(historical) > 1:
                        mean = np.mean(historical)
                        std = np.std(historical)
                        if std > 0:
                            zscore = (value - mean) / std
                            if abs(zscore) > threshold:
                                metric_anomalies[metric] = {
                                    'value': value,
                                    'zscore': zscore,
                                    'historical_mean': mean,
                                    'historical_std': std
                                }
                
                if metric_anomalies:
                    window_anomalies[component] = metric_anomalies
                    
            if window_anomalies:
                anomalies[window] = window_anomalies
                
        return anomalies
