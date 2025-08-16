"""Adaptive drift detection with dynamic thresholds."""
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from .psi import calculate_psi
from scipy import stats

@dataclass
class DriftResult:
    """Results from drift detection."""
    feature_name: str
    drift_detected: bool
    psi_score: float
    ks_statistic: float
    p_value: float
    threshold: float
    confidence: float

class AdaptiveDriftDetector:
    def __init__(
        self,
        baseline_window: timedelta = timedelta(days=7),
        detection_window: timedelta = timedelta(hours=1),
        min_samples: int = 100,
        confidence_level: float = 0.95,
        seasonality: Optional[str] = "auto"
    ):
        """Initialize adaptive drift detector.
        
        Args:
            baseline_window: Window for baseline distribution
            detection_window: Window for current distribution
            min_samples: Minimum samples required
            confidence_level: Statistical confidence level
            seasonality: None, "daily", "weekly" or "auto"
        """
        self.baseline_window = baseline_window
        self.detection_window = detection_window
        self.min_samples = min_samples
        self.confidence_level = confidence_level
        self.seasonality = seasonality
        
        self.baseline_stats: Dict[str, Dict] = {}
        self.thresholds: Dict[str, float] = {}
        self.history: List[Tuple[datetime, Dict[str, float]]] = []
        
    def _detect_seasonality(
        self,
        ts: pd.Series,
        values: pd.Series
    ) -> Optional[str]:
        """Auto-detect seasonality pattern."""
        if len(values) < 24:  # Need at least a day of data
            return None
            
        # Check daily pattern
        hourly = values.groupby(ts.dt.hour).mean()
        hourly_std = hourly.std()
        
        # Check weekly pattern
        daily = values.groupby(ts.dt.day_of_week).mean()
        daily_std = daily.std()
        
        if hourly_std > daily_std * 1.5:
            return "daily"
        elif daily_std > hourly_std * 1.5:
            return "weekly"
        else:
            return None
            
    def _get_seasonal_baseline(
        self,
        feature_name: str,
        current_time: datetime,
        data: pd.DataFrame
    ) -> pd.Series:
        """Get seasonally adjusted baseline."""
        if self.seasonality == "auto":
            pattern = self._detect_seasonality(
                data["ts"],
                data[feature_name]
            )
        else:
            pattern = self.seasonality
            
        if pattern == "daily":
            current_hour = current_time.hour
            return data[
                data["ts"].dt.hour.between(
                    current_hour - 1,
                    current_hour + 1,
                    inclusive="both"
                )
            ][feature_name]
        elif pattern == "weekly":
            current_day = current_time.weekday()
            return data[
                data["ts"].dt.day_of_week == current_day
            ][feature_name]
        else:
            return data[feature_name]
            
    def update_baseline(
        self,
        current_time: datetime,
        data: pd.DataFrame,
        features: List[str]
    ) -> None:
        """Update baseline statistics.
        
        Args:
            current_time: Current timestamp
            data: DataFrame with features and timestamps
            features: List of feature names to monitor
        """
        baseline_start = current_time - self.baseline_window
        baseline_data = data[
            data["ts"].between(baseline_start, current_time)
        ]
        
        if len(baseline_data) < self.min_samples:
            return
            
        for feature in features:
            # Get seasonal baseline if applicable
            baseline_values = self._get_seasonal_baseline(
                feature,
                current_time,
                baseline_data
            )
            
            # Calculate baseline statistics
            self.baseline_stats[feature] = {
                "mean": baseline_values.mean(),
                "std": baseline_values.std(),
                "q25": baseline_values.quantile(0.25),
                "q75": baseline_values.quantile(0.75),
                "last_update": current_time
            }
            
            # Calculate adaptive threshold
            iqr = self.baseline_stats[feature]["q75"] - self.baseline_stats[feature]["q25"]
            z_score = stats.norm.ppf(self.confidence_level)
            self.thresholds[feature] = min(
                1.0,  # Cap at 1.0 for PSI
                (iqr * z_score) / (self.baseline_stats[feature]["q75"] + 1e-10)
            )
            
    def detect_drift(
        self,
        current_time: datetime,
        data: pd.DataFrame,
        features: List[str]
    ) -> Dict[str, DriftResult]:
        """Detect drift in current window.
        
        Args:
            current_time: Current timestamp
            data: DataFrame with features and timestamps
            features: List of feature names to monitor
            
        Returns:
            Dict mapping features to drift results
        """
        detection_start = current_time - self.detection_window
        current_data = data[
            data["ts"].between(detection_start, current_time)
        ]
        
        if len(current_data) < self.min_samples:
            return {}
            
        results = {}
        for feature in features:
            if feature not in self.baseline_stats:
                continue
                
            # Get seasonal baseline
            baseline_values = self._get_seasonal_baseline(
                feature,
                current_time,
                data[
                    data["ts"].between(
                        current_time - self.baseline_window,
                        current_time - self.detection_window
                    )
                ]
            )
            
            current_values = self._get_seasonal_baseline(
                feature,
                current_time,
                current_data
            )
            
            if len(baseline_values) < self.min_samples or len(current_values) < self.min_samples:
                continue
                
            # Calculate PSI
            psi_score, _, _ = calculate_psi(baseline_values, current_values)
            
            # Perform KS test
            ks_statistic, p_value = stats.ks_2samp(
                baseline_values,
                current_values
            )
            
            # Determine if drift occurred
            threshold = self.thresholds[feature]
            drift_detected = psi_score > threshold
            
            # Calculate confidence
            confidence = 1 - p_value if drift_detected else p_value
            
            results[feature] = DriftResult(
                feature_name=feature,
                drift_detected=drift_detected,
                psi_score=psi_score,
                ks_statistic=ks_statistic,
                p_value=p_value,
                threshold=threshold,
                confidence=confidence
            )
            
            # Update history
            self.history.append((
                current_time,
                {"psi": psi_score, "threshold": threshold}
            ))
            
            # Prune old history
            cutoff = current_time - self.baseline_window
            self.history = [
                (t, v) for t, v in self.history
                if t >= cutoff
            ]
            
        return results
