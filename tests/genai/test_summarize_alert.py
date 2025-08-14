"""Tests for alert summarization."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from src.genai.summarize_alert import AlertSummarizer

@pytest.fixture
def mock_engine():
    with patch('sqlalchemy.create_engine') as mock:
        yield mock

@pytest.fixture
def mock_connection():
    connection = MagicMock()
    return connection

@pytest.fixture
def summarizer(mock_engine):
    return AlertSummarizer('postgresql://test')

@pytest.fixture
def sample_alert():
    return {
        'detection_id': 'test-id',
        'timestamp': datetime.utcnow(),
        'anomaly_score': 0.95,
        'session_id': 'test-session',
        'event_count': 150,
        'unique_components': 12,
        'error_ratio': 0.15,
        'template_entropy': 3.5,
        'component_entropy': 2.8,
        'epss_score': 0.75,
        'kev_name': 'CVE-2023-1234',
        'kev_description': 'Test vulnerability'
    }

def test_fetch_recent_alerts(summarizer, mock_connection, sample_alert):
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [sample_alert]
    mock_connection.execute.return_value = mock_result
    
    with patch.object(summarizer.engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_connection
        alerts = summarizer.fetch_recent_alerts(hours=24, min_score=0.8)
        
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert['anomaly_score'] == 0.95
    assert alert['event_count'] == 150
    
    mock_connection.execute.assert_called_once()

def test_generate_summary(summarizer, sample_alert):
    summary = summarizer.generate_summary(sample_alert)
    
    assert 'Alert detected at' in summary
    assert 'Anomaly score: 0.950' in summary
    assert '150 events in window' in summary
    assert '12 unique components' in summary
    assert 'Error ratio: 15.00%' in summary
    assert 'Template entropy: 3.50' in summary
    assert 'Component entropy: 2.80' in summary
    assert 'EPSS exploit probability: 75.0%' in summary
    assert 'CVE-2023-1234' in summary
    assert 'Test vulnerability' in summary

def test_generate_summary_no_threat_intel(summarizer, sample_alert):
    alert = sample_alert.copy()
    del alert['epss_score']
    del alert['kev_name']
    del alert['kev_description']
    
    summary = summarizer.generate_summary(alert)
    
    assert 'Alert detected at' in summary
    assert 'EPSS exploit probability' not in summary
    assert 'Known Exploited Vulnerability' not in summary

def test_filter_allowed_terms(summarizer):
    summary = """
    Alert detected at 2023-08-14T12:00:00Z
    Anomaly score: 0.950
    
    Key factors:
    - 150 events in window
    - Error ratio: 15.00%
    
    EPSS exploit probability: 75.0%
    """
    
    allow_list = ['Error ratio', 'EPSS']
    filtered = summarizer.filter_allowed_terms(summary, allow_list)
    
    assert 'Error ratio: 15.00%' in filtered
    assert 'EPSS exploit probability: 75.0%' in filtered
    assert 'Alert detected' not in filtered
    assert '150 events' not in filtered

def test_filter_allowed_terms_no_matches(summarizer):
    summary = "Test summary without matches"
    allow_list = ['not-present']
    
    filtered = summarizer.filter_allowed_terms(summary, allow_list)
    assert filtered == summary  # Returns original if no matches

def test_filter_allowed_terms_none(summarizer):
    summary = "Test summary"
    filtered = summarizer.filter_allowed_terms(summary, None)
    assert filtered == summary  # Returns original if allow_list is None
