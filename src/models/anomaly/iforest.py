"""Isolation Forest anomaly detection.

Detects anomalies in HDFS logs using windowed features.
"""
import logging
import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IForestConfig(BaseModel):
    """Isolation Forest configuration."""
    feature_columns: List[str] = Field(
        default=[
            "event_count",
            "unique_components",
            "error_ratio",
            "template_entropy",
            "component_entropy"
        ],
        description="Features to use for anomaly detection"
    )
    n_estimators: int = Field(
        default=100,
        description="Number of isolation trees"
    )
    contamination: float = Field(
        default=0.1,
        description="Expected proportion of anomalies"
    )

class IForestModel:
    """Isolation Forest anomaly detector."""
    
    def __init__(self, config: Optional[IForestConfig] = None):
        self.config = config or IForestConfig()
        # Use DOVAH_ANALYSIS_SEED from env or default to 42
        seed = int(os.getenv("DOVAH_ANALYSIS_SEED", "42"))
        self.model = IsolationForest(
            n_estimators=self.config.n_estimators,
            contamination=self.config.contamination,
            random_state=seed
        )
        # StandardScaler doesn't have random_state, but numpy seed affects its internals
        np.random.seed(seed)
        self.scaler = StandardScaler()
    
    def _extract_features(self, events: List[Dict]) -> pd.DataFrame:
        """Extract features from events with NaN handling."""
        features = []
        valid_events = []
        
        for event in events:
            feature_dict = {}
            valid = True
            
            # Extract features with NaN check
            for col in self.config.feature_columns:
                if col not in event:
                    valid = False
                    break
                val = event[col]
                if pd.isna(val) or np.isinf(val):
                    valid = False
                    break
                feature_dict[col] = val
            
            if valid:
                features.append(feature_dict)
                valid_events.append(event)
            else:
                logger.warning(f"Skipping event with invalid features: {event}")
        
        if not features:
            return pd.DataFrame(), []
        
        return pd.DataFrame(features), valid_events
    
    def fit(self, events: List[Dict]) -> None:
        """Fit model on training data."""
        # Extract features
        features_df, _ = self._extract_features(events)
        if len(features_df) == 0:
            raise ValueError("No valid features to train on")
        
        # Scale features
        X = self.scaler.fit_transform(features_df)
        
        # Fit model
        self.model.fit(X)
        logger.info(f"Trained IsolationForest on {len(X)} samples")
    
    def predict(self, events: List[Dict]) -> Dict[str, Dict[str, float]]:
        """Predict anomaly scores for events, returning per-window results.
        
        Returns:
            Dict mapping session_id to Dict containing:
            - score: anomaly score (0-1)
            - ts: window timestamp
        """
        # Extract features with NaN handling
        features_df, valid_events = self._extract_features(events)
        if len(features_df) == 0:
            logger.warning("No valid features to score")
            return {}
        
        # Scale features
        X = self.scaler.transform(features_df)
        
        # Get raw decision scores
        raw_scores = self.model.score_samples(X)
        
        # Convert to probability-like scores (0-1)
        if len(raw_scores) > 1:
            scores = 1 - (raw_scores - np.min(raw_scores)) / (
                np.max(raw_scores) - np.min(raw_scores)
            )
        else:
            # Single score case - use absolute value scaling
            scores = 1 / (1 + np.exp(-np.abs(raw_scores)))
        
        # Map scores to events with ts
        result = {}
        for event, score in zip(valid_events, scores):
            session_id = event["session_id"]
            ts = event["ts"]
            if session_id not in result or ts > result[session_id]["ts"]:
                result[session_id] = {
                    "score": float(score),
                    "ts": ts
                }
        
        return result
    
    def save(self, path: str) -> None:
        """Save model to disk."""
        import joblib
        joblib.dump({
            "model": self.model,
            "scaler": self.scaler,
            "config": self.config
        }, path)
    
    @classmethod
    def load(cls, path: str) -> "IForestModel":
        """Load model from disk."""
        import joblib
        data = joblib.load(path)
        model = cls(config=data["config"])
        model.model = data["model"]
        model.scaler = data["scaler"]
        return model
