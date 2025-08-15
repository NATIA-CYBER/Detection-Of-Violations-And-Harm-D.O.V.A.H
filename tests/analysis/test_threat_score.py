"""Tests for threat scoring module."""
import pytest
from datetime import datetime, timedelta
from src.analysis.threat_score import ThreatScorer, ThreatScore

def test_cve_scoring():
    """Test CVE-based scoring."""
    scorer = ThreatScorer()
    
    # Test high severity CVE
    score = scorer.calculate_threat_score(
        cvss_score=9.8,
        epss_score=0.5,
        is_actively_exploited=True,
        host="test_host",
        event_time=datetime.now()
    )
    assert score.cve_subscore > 90
    assert score.total_score > 70
    
    # Test medium severity CVE
    score = scorer.calculate_threat_score(
        cvss_score=5.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="test_host",
        event_time=datetime.now()
    )
    assert 40 < score.cve_subscore < 60
    assert score.total_score < 70
    
    # Test no CVE
    score = scorer.calculate_threat_score(
        cvss_score=None,
        epss_score=0.5,
        is_actively_exploited=False,
        host="test_host",
        event_time=datetime.now()
    )
    assert score.cve_subscore == 0

def test_epss_scoring():
    """Test EPSS-based scoring."""
    scorer = ThreatScorer()
    
    # Test high probability
    score = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.9,
        is_actively_exploited=False,
        host="test_host",
        event_time=datetime.now()
    )
    assert score.epss_subscore > 80
    
    # Test low probability
    score = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.01,
        is_actively_exploited=False,
        host="test_host",
        event_time=datetime.now()
    )
    assert score.epss_subscore < 20

def test_frequency_scoring():
    """Test frequency-based scoring."""
    scorer = ThreatScorer(frequency_window=timedelta(hours=1))
    now = datetime.now()
    
    # Single event
    score1 = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="test_host",
        event_time=now
    )
    
    # Multiple events
    score2 = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="test_host",
        event_time=now + timedelta(minutes=1)
    )
    
    assert score2.frequency_subscore > score1.frequency_subscore

def test_asset_scoring():
    """Test asset criticality scoring."""
    asset_criticality = {
        "critical_host": 1.0,
        "medium_host": 0.5,
        "low_host": 0.1
    }
    scorer = ThreatScorer(asset_criticality=asset_criticality)
    
    # Test critical asset
    score = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="critical_host",
        event_time=datetime.now()
    )
    assert score.asset_subscore == 100
    
    # Test low criticality asset
    score = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="low_host",
        event_time=datetime.now()
    )
    assert score.asset_subscore == 10

def test_temporal_decay():
    """Test temporal decay of threat scores."""
    scorer = ThreatScorer(decay_halflife=timedelta(days=1))
    now = datetime.now()
    
    # Fresh event
    score1 = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="test_host",
        event_time=now,
        current_time=now
    )
    
    # Day-old event
    score2 = scorer.calculate_threat_score(
        cvss_score=7.0,
        epss_score=0.5,
        is_actively_exploited=False,
        host="test_host",
        event_time=now - timedelta(days=1),
        current_time=now
    )
    
    assert score2.total_score < score1.total_score
    assert score2.temporal_decay == 0.5  # Half-life
