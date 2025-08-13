"""Local windowing stub for feature computation.

Implements sliding window feature computation using pandas.
Production implementation will use Flink/Kinesis.
"""
import datetime
import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WindowConfig:
    """Window configuration."""
    def __init__(
        self,
        window_size: int = 60,  # seconds
        window_slide: int = 10,  # seconds
        min_events: int = 5
    ):
        self.window_size = window_size
        self.window_slide = window_slide
        self.min_events = min_events

class WindowFeatures:
    """Compute features over sliding windows."""
    
    def __init__(self, config: Optional[WindowConfig] = None):
        self.config = config or WindowConfig()
        self.engine = create_engine(
            "postgresql://dovah:dovah@localhost:5432/dovah"
        )
    
    def compute_window_features(
        self,
        events: List[Dict],
        window_end: datetime.datetime
    ) -> Optional[Dict]:
        """Compute features for a single window."""
        if not events or len(events) < self.config.min_events:
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(events)
        
        # Basic counts
        event_count = len(df)
        unique_components = df["component"].nunique()
        
        # Error ratio
        error_count = df[
            df["level"].isin(["ERROR", "CRITICAL"])
        ].shape[0]
        error_ratio = error_count / event_count
        
        # Entropy features
        def entropy(series: pd.Series) -> float:
            counts = series.value_counts(normalize=True)
            return float(-np.sum(counts * np.log2(counts)))
        
        template_entropy = entropy(df["template_id"])
        component_entropy = entropy(df["component"])
        
        # Create feature dict
        features = {
            "ts": window_end,
            "session_id": events[0]["session_id"],
            "host": events[0]["host"],
            "window_size": self.config.window_size,
            "window_slide": self.config.window_slide,
            "event_count": event_count,
            "unique_components": unique_components,
            "error_ratio": error_ratio,
            "template_entropy": template_entropy,
            "component_entropy": component_entropy
        }
        
        return features
    
    def process_events(self, events: List[Dict]) -> List[Dict]:
        """Process events and compute window features."""
        if not events:
            return []
        
        # Sort by timestamp
        events = sorted(events, key=lambda x: x["ts"])
        start_ts = events[0]["ts"]
        end_ts = events[-1]["ts"]
        
        # Generate window endpoints
        window_ends = []
        current = start_ts
        while current <= end_ts:
            window_ends.append(current)
            current += datetime.timedelta(
                seconds=self.config.window_slide
            )
        
        # Compute features for each window
        features = []
        for window_end in window_ends:
            window_start = window_end - datetime.timedelta(
                seconds=self.config.window_size
            )
            
            # Get events in window
            window_events = [
                e for e in events
                if window_start <= e["ts"] <= window_end
            ]
            
            if window_features := self.compute_window_features(
                window_events,
                window_end
            ):
                features.append(window_features)
        
        return features
    
    def store_features(self, features: List[Dict]) -> None:
        """Store window features in database."""
        if not features:
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(features)
        
        # Store in database
        df.to_sql(
            "window_features",
            self.engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        logger.info(f"Stored {len(features)} window features")

def main() -> None:
    """Main entry point."""
    try:
        # Initialize windowing
        windowing = WindowFeatures()
        
        # Get recent events
        query = """
        SELECT *
        FROM hdfs_events
        WHERE ts >= NOW() - INTERVAL '1 hour'
        """
        
        df = pd.read_sql(query, windowing.engine)
        events = df.to_dict("records")
        
        # Process events
        features = windowing.process_events(events)
        
        # Store features
        windowing.store_features(features)
        
    except Exception as e:
        logger.error(f"Window feature computation failed: {e}")
        raise

if __name__ == "__main__":
    main()
