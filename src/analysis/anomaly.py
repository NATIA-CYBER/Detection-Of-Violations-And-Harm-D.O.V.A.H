"""Anomaly detection using Isolation Forest and feature extraction."""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class EventFeatures:
    """Features extracted from event stream."""
    event_count: int
    unique_hosts: int
    unique_processes: int
    error_ratio: float
    avg_severity: float
    max_severity: float
    unique_templates: int
    template_entropy: float
    
class AnomalyDetector:
    def __init__(
        self, 
        window_size: timedelta = timedelta(minutes=5),
        contamination: float = 0.01,
        n_estimators: int = 100,
        random_state: int = 42
    ):
        """Initialize anomaly detector.
        
        Args:
            window_size: Size of sliding window
            contamination: Expected ratio of anomalies
            n_estimators: Number of trees in isolation forest
            random_state: Random seed
        """
        self.window_size = window_size
        self.scaler = StandardScaler()
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state
        )
        self.is_fitted = False
        self.feature_history: List[EventFeatures] = []
        
    def extract_features(self, events_df: pd.DataFrame) -> EventFeatures:
        """Extract features from event window.
        
        Args:
            events_df: DataFrame with events in window
            
        Returns:
            Extracted features
        """
        if events_df.empty:
            return EventFeatures(
                event_count=0,
                unique_hosts=0,
                unique_processes=0,
                error_ratio=0.0,
                avg_severity=0.0,
                max_severity=0.0,
                unique_templates=0,
                template_entropy=0.0
            )
            
        # Basic counts
        event_count = len(events_df)
        unique_hosts = events_df["host"].nunique()
        unique_processes = events_df["process"].nunique()
        
        # Error ratio
        error_ratio = (
            events_df["severity"].isin(["ERROR", "CRITICAL"]).sum() / event_count
        )
        
        # Severity metrics
        severity_map = {
            "DEBUG": 1,
            "INFO": 2,
            "WARNING": 3,
            "ERROR": 4,
            "CRITICAL": 5
        }
        severities = events_df["severity"].map(severity_map).fillna(0)
        avg_severity = severities.mean()
        max_severity = severities.max()
        
        # Template diversity
        templates = events_df["template_id"]
        unique_templates = templates.nunique()
        
        # Template entropy
        template_counts = templates.value_counts(normalize=True)
        template_entropy = -(template_counts * np.log2(template_counts)).sum()
        
        return EventFeatures(
            event_count=event_count,
            unique_hosts=unique_hosts,
            unique_processes=unique_processes,
            error_ratio=error_ratio,
            avg_severity=avg_severity,
            max_severity=max_severity,
            unique_templates=unique_templates,
            template_entropy=template_entropy
        )
        
    def features_to_array(self, features: EventFeatures) -> np.ndarray:
        """Convert features to numpy array."""
        return np.array([
            features.event_count,
            features.unique_hosts,
            features.unique_processes,
            features.error_ratio,
            features.avg_severity,
            features.max_severity,
            features.unique_templates,
            features.template_entropy
        ]).reshape(1, -1)
        
    def fit(self, events_df: pd.DataFrame) -> None:
        """Fit anomaly detector on historical data.
        
        Args:
            events_df: DataFrame with historical events
        """
        # Extract features from sliding windows
        windows = []
        window_end = events_df["ts"].max()
        while window_end > events_df["ts"].min():
            window_start = window_end - self.window_size
            window_df = events_df[
                (events_df["ts"] >= window_start) &
                (events_df["ts"] < window_end)
            ]
            features = self.extract_features(window_df)
            windows.append(self.features_to_array(features))
            window_end = window_start
            
        # Fit scaler and model
        X = np.vstack(windows)
        self.scaler.fit(X)
        self.model.fit(self.scaler.transform(X))
        self.is_fitted = True
        
    def predict(self, events_df: pd.DataFrame) -> Tuple[bool, float]:
        """Predict if current window is anomalous.
        
        Args:
            events_df: DataFrame with events in current window
            
        Returns:
            (is_anomaly, anomaly_score)
        """
        if not self.is_fitted:
            raise RuntimeError("Model must be fitted before prediction")
            
        features = self.extract_features(events_df)
        X = self.features_to_array(features)
        X_scaled = self.scaler.transform(X)
        
        # Get anomaly score (-1 for anomalies, 1 for normal)
        score = self.model.score_samples(X_scaled)[0]
        
        # Convert to probability-like score between 0 and 1
        # where 1 indicates high likelihood of anomaly
        prob_score = 1 - (score + 1) / 2
        
        return prob_score > 0.5, prob_score
