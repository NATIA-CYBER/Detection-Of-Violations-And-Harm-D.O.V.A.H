"""Tests for component and severity distribution analysis."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def test_component_distribution():
    """Test analysis of component distribution."""
    # Create test data with known component distribution
    events = pd.DataFrame({
        "timestamp": [datetime.now() + timedelta(seconds=i) for i in range(10)],
        "component": ["DataNode", "DataNode", "NameNode", "DataNode", 
                     "SecondaryNameNode", "DataNode", "NameNode", "DataNode",
                     "DataNode", "NameNode"]
    })
    
    # Calculate distribution
    dist = events["component"].value_counts(normalize=True)
    
    # Verify expected proportions
    assert abs(dist["DataNode"] - 0.6) < 0.01  # 6/10
    assert abs(dist["NameNode"] - 0.3) < 0.01  # 3/10
    assert abs(dist["SecondaryNameNode"] - 0.1) < 0.01  # 1/10

def test_severity_distribution():
    """Test analysis of severity level distribution."""
    events = pd.DataFrame({
        "timestamp": [datetime.now() + timedelta(seconds=i) for i in range(10)],
        "level": ["INFO", "INFO", "WARN", "INFO", "ERROR",
                 "INFO", "WARN", "INFO", "INFO", "ERROR"]
    })
    
    # Calculate distribution
    dist = events["level"].value_counts(normalize=True)
    
    # Verify expected proportions
    assert abs(dist["INFO"] - 0.6) < 0.01  # 6/10
    assert abs(dist["WARN"] - 0.2) < 0.01  # 2/10
    assert abs(dist["ERROR"] - 0.2) < 0.01  # 2/10

def test_temporal_distribution():
    """Test analysis of distributions over time windows."""
    # Create events across multiple time windows
    now = datetime.now()
    events = pd.DataFrame({
        "timestamp": [
            # Window 1: More DataNode, mostly INFO
            now + timedelta(minutes=1),
            now + timedelta(minutes=2),
            now + timedelta(minutes=3),
            now + timedelta(minutes=4),
            # Window 2: More NameNode, more errors
            now + timedelta(minutes=11),
            now + timedelta(minutes=12),
            now + timedelta(minutes=13),
            now + timedelta(minutes=14)
        ],
        "component": [
            "DataNode", "DataNode", "NameNode", "DataNode",
            "NameNode", "NameNode", "DataNode", "NameNode"
        ],
        "level": [
            "INFO", "INFO", "WARN", "INFO",
            "ERROR", "ERROR", "WARN", "ERROR"
        ]
    })
    
    # Analyze first window (1-5 minutes)
    window1 = events[events["timestamp"] < now + timedelta(minutes=10)]
    comp_dist1 = window1["component"].value_counts(normalize=True)
    sev_dist1 = window1["level"].value_counts(normalize=True)
    
    # Verify window 1 distributions
    assert abs(comp_dist1["DataNode"] - 0.75) < 0.01  # 3/4
    assert abs(comp_dist1["NameNode"] - 0.25) < 0.01  # 1/4
    assert abs(sev_dist1["INFO"] - 0.75) < 0.01  # 3/4
    assert abs(sev_dist1["WARN"] - 0.25) < 0.01  # 1/4
    
    # Analyze second window (11-15 minutes)
    window2 = events[events["timestamp"] >= now + timedelta(minutes=10)]
    comp_dist2 = window2["component"].value_counts(normalize=True)
    sev_dist2 = window2["level"].value_counts(normalize=True)
    
    # Verify window 2 distributions
    assert abs(comp_dist2["NameNode"] - 0.75) < 0.01  # 3/4
    assert abs(comp_dist2["DataNode"] - 0.25) < 0.01  # 1/4
    assert abs(sev_dist2["ERROR"] - 0.75) < 0.01  # 3/4
    assert abs(sev_dist2["WARN"] - 0.25) < 0.01  # 1/4

def test_distribution_change():
    """Test detection of significant distribution changes."""
    # Create baseline and current distributions
    baseline = pd.DataFrame({
        "timestamp": [datetime.now() for _ in range(100)],
        "component": ["DataNode"] * 70 + ["NameNode"] * 25 + ["SecondaryNameNode"] * 5,
        "level": ["INFO"] * 80 + ["WARN"] * 15 + ["ERROR"] * 5
    })
    
    current = pd.DataFrame({
        "timestamp": [datetime.now() for _ in range(100)],
        "component": ["DataNode"] * 40 + ["NameNode"] * 55 + ["SecondaryNameNode"] * 5,
        "level": ["INFO"] * 60 + ["WARN"] * 20 + ["ERROR"] * 20
    })
    
    # Calculate chi-square test statistic for component distribution change
    comp_obs = pd.crosstab(current["component"], columns="count")
    comp_exp = pd.crosstab(baseline["component"], columns="count")
    chi2 = sum(((comp_obs - comp_exp) ** 2 / comp_exp).values)
    
    # Verify significant change in component distribution
    assert chi2 > 20  # High chi-square indicates significant change
    
    # Calculate chi-square for severity distribution change
    sev_obs = pd.crosstab(current["level"], columns="count")
    sev_exp = pd.crosstab(baseline["level"], columns="count")
    chi2 = sum(((sev_obs - sev_exp) ** 2 / sev_exp).values)
    
    # Verify significant change in severity distribution
    assert chi2 > 20  # High chi-square indicates significant change
