"""Tests for anomaly detection module."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.analysis.anomaly import AnomalyDetector, EventFeatures

def generate_normal_events(n_events: int, start_time: datetime) -> pd.DataFrame:
    """Generate synthetic normal events."""
    return pd.DataFrame({
        "timestamp": [start_time + timedelta(minutes=i) for i in range(n_events)],
        "host": np.random.choice(["host1", "host2", "host3"], n_events),
        "process": np.random.choice(["proc1", "proc2"], n_events),
        "severity": np.random.choice(
            ["INFO", "WARNING", "ERROR"],
            n_events,
            p=[0.7, 0.2, 0.1]
        ),
        "template_id": np.random.choice(["t1", "t2", "t3"], n_events),
    })

def generate_anomalous_events(n_events: int, start_time: datetime) -> pd.DataFrame:
    """Generate synthetic anomalous events."""
    return pd.DataFrame({
        "timestamp": [start_time + timedelta(minutes=i) for i in range(n_events)],
        "host": ["host1"] * n_events,  # Single host
        "process": ["proc1"] * n_events,  # Single process
        "severity": ["ERROR"] * n_events,  # All errors
        "template_id": ["t1"] * n_events,  # Single template
    })

def test_feature_extraction():
    """Test feature extraction from events."""
    detector = AnomalyDetector(window_size=timedelta(minutes=5))
    
    # Test normal events
    events = generate_normal_events(100, datetime.now())
    features = detector.extract_features(events)
    
    assert isinstance(features, EventFeatures)
    assert features.event_count == 100
    assert 1 < features.unique_hosts <= 3
    assert 1 < features.unique_processes <= 2
    assert 0 < features.error_ratio < 0.2
    assert 1.5 < features.avg_severity < 2.5
    assert features.max_severity >= 4.0
    assert 1 < features.unique_templates <= 3
    assert features.template_entropy > 0

def test_anomaly_detection():
    """Test anomaly detection with normal and anomalous events."""
    detector = AnomalyDetector(window_size=timedelta(minutes=5))
    
    # Generate training data (normal)
    train_events = generate_normal_events(1000, datetime.now())
    detector.fit(train_events)
    
    # Test normal events
    normal_events = generate_normal_events(100, datetime.now())
    is_anomaly, score = detector.predict(normal_events)
    assert not is_anomaly
    assert score < 0.5
    
    # Test anomalous events
    anomalous_events = generate_anomalous_events(100, datetime.now())
    is_anomaly, score = detector.predict(anomalous_events)
    assert is_anomaly
    assert score > 0.5

def test_empty_events():
    """Test handling of empty event sets."""
    detector = AnomalyDetector()
    
    # Empty training should raise error
    with pytest.raises(RuntimeError):
        detector.fit(pd.DataFrame())
    
    # Train with normal data first
    train_events = generate_normal_events(1000, datetime.now())
    detector.fit(train_events)
    
    # Empty prediction set should return normal
    is_anomaly, score = detector.predict(pd.DataFrame())
    assert not is_anomaly
    assert score < 0.5

def test_feature_stability():
    """Test stability of feature extraction."""
    detector = AnomalyDetector()
    
    # Generate same events multiple times
    events = generate_normal_events(100, datetime.now())
    features1 = detector.extract_features(events)
    features2 = detector.extract_features(events)
    
    # Features should be identical
    assert features1 == features2
