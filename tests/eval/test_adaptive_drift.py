"""Tests for adaptive drift detection."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.eval.adaptive_drift import AdaptiveDriftDetector, DriftResult

def generate_stable_data(n_samples: int, start_time: datetime) -> pd.DataFrame:
    """Generate synthetic data with stable distribution."""
    return pd.DataFrame({
        "timestamp": [start_time + timedelta(minutes=i) for i in range(n_samples)],
        "value": np.random.normal(loc=10, scale=1, size=n_samples),
        "count": np.random.poisson(lam=5, size=n_samples)
    })

def generate_drift_data(n_samples: int, start_time: datetime) -> pd.DataFrame:
    """Generate synthetic data with distribution drift."""
    return pd.DataFrame({
        "timestamp": [start_time + timedelta(minutes=i) for i in range(n_samples)],
        "value": np.random.normal(loc=15, scale=2, size=n_samples),  # Changed mean and variance
        "count": np.random.poisson(lam=10, size=n_samples)  # Changed lambda
    })

def test_baseline_update():
    """Test baseline statistics update."""
    detector = AdaptiveDriftDetector(
        baseline_window=timedelta(hours=1),
        detection_window=timedelta(minutes=10)
    )
    
    now = datetime.now()
    data = generate_stable_data(1000, now - timedelta(hours=1))
    
    # Update baseline
    detector.update_baseline(now, data, ["value", "count"])
    
    # Check baseline stats
    assert "value" in detector.baseline_stats
    assert "count" in detector.baseline_stats
    assert abs(detector.baseline_stats["value"]["mean"] - 10) < 0.5
    assert abs(detector.baseline_stats["count"]["mean"] - 5) < 0.5

def test_drift_detection():
    """Test drift detection with stable and drifting data."""
    detector = AdaptiveDriftDetector(
        baseline_window=timedelta(hours=1),
        detection_window=timedelta(minutes=10)
    )
    
    now = datetime.now()
    
    # Generate and add baseline data
    baseline_data = generate_stable_data(1000, now - timedelta(hours=1))
    detector.update_baseline(now, baseline_data, ["value", "count"])
    
    # Test stable data
    stable_data = generate_stable_data(100, now - timedelta(minutes=10))
    stable_results = detector.detect_drift(now, stable_data, ["value", "count"])
    
    assert not stable_results["value"].drift_detected
    assert not stable_results["count"].drift_detected
    assert stable_results["value"].psi_score < stable_results["value"].threshold
    
    # Test drift data
    drift_data = generate_drift_data(100, now - timedelta(minutes=10))
    drift_results = detector.detect_drift(now, drift_data, ["value", "count"])
    
    assert drift_results["value"].drift_detected
    assert drift_results["count"].drift_detected
    assert drift_results["value"].psi_score > drift_results["value"].threshold

def test_seasonality():
    """Test seasonal pattern detection and adjustment."""
    detector = AdaptiveDriftDetector(
        baseline_window=timedelta(days=2),
        detection_window=timedelta(hours=1),
        seasonality="daily"
    )
    
    now = datetime.now()
    
    # Generate data with daily pattern
    timestamps = []
    values = []
    for day in range(2):
        for hour in range(24):
            base_time = now - timedelta(days=2-day, hours=24-hour)
            # Higher values during business hours
            if 9 <= hour <= 17:
                mean = 15
            else:
                mean = 5
            for minute in range(60):
                timestamps.append(base_time + timedelta(minutes=minute))
                values.append(np.random.normal(loc=mean, scale=1))
                
    data = pd.DataFrame({
        "timestamp": timestamps,
        "value": values
    })
    
    # Update baseline
    detector.update_baseline(now, data, ["value"])
    
    # Test detection at different times
    business_time = now.replace(hour=14, minute=0)  # 2 PM
    night_time = now.replace(hour=2, minute=0)  # 2 AM
    
    # Business hours data should not trigger drift
    business_data = pd.DataFrame({
        "timestamp": [business_time + timedelta(minutes=i) for i in range(60)],
        "value": np.random.normal(loc=15, scale=1, size=60)
    })
    business_results = detector.detect_drift(business_time, business_data, ["value"])
    assert not business_results["value"].drift_detected
    
    # Night time data should not trigger drift
    night_data = pd.DataFrame({
        "timestamp": [night_time + timedelta(minutes=i) for i in range(60)],
        "value": np.random.normal(loc=5, scale=1, size=60)
    })
    night_results = detector.detect_drift(night_time, night_data, ["value"])
    assert not night_results["value"].drift_detected

def test_adaptive_thresholds():
    """Test threshold adaptation based on data patterns."""
    detector = AdaptiveDriftDetector(
        baseline_window=timedelta(hours=1),
        detection_window=timedelta(minutes=10),
        confidence_level=0.95
    )
    
    now = datetime.now()
    
    # Generate baseline with different variability
    high_var_data = pd.DataFrame({
        "timestamp": [now - timedelta(minutes=i) for i in range(1000)],
        "high_var": np.random.normal(loc=10, scale=5, size=1000),
        "low_var": np.random.normal(loc=10, scale=0.5, size=1000)
    })
    
    detector.update_baseline(now, high_var_data, ["high_var", "low_var"])
    
    # Check that thresholds adapt to variability
    assert detector.thresholds["high_var"] > detector.thresholds["low_var"]
