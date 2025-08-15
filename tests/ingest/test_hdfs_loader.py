"""Tests for HDFS log loader."""
import pytest
from datetime import datetime, timezone
from pathlib import Path

from src.ingest.hdfs_loader import HDFSLoader

@pytest.fixture
def loader():
    """Create HDFSLoader instance for testing."""
    return HDFSLoader()

def test_timestamp_normalization(loader):
    """Test UTC timestamp normalization and RFC3339 parsing."""
    # Test various timestamp formats
    test_cases = [
        "2023-08-15T12:34:56Z",
        "2023-08-15T12:34:56.123Z",
        "2023-08-15T12:34:56+00:00",
        "2023-08-15 12:34:56.123456"
    ]
    
    for ts_str in test_cases:
        ts = loader._normalize_timestamp(ts_str, "test-host")
        assert ts.tzinfo == timezone.utc
        assert isinstance(ts, datetime)

def test_deduplication(loader):
    """Test event deduplication."""
    # Same event, different formats
    log1 = "2023-08-15T12:34:56Z\thost1\thdfs\tBlock xyz replicated\tINFO"
    log2 = "2023-08-15T12:34:56.000Z\thost1\thdfs\tBlock xyz replicated\tINFO"
    
    event1 = loader.parse_log_line(log1)
    assert event1 is not None
    
    # Second identical event should be deduplicated
    event2 = loader.parse_log_line(log2)
    assert event2 is None

def test_template_extraction(loader):
    """Test log template extraction and caching."""
    # Similar logs with different values
    logs = [
        "2023-08-15T12:34:56Z\thost1\thdfs\tBlock 123 replicated\tINFO",
        "2023-08-15T12:35:56Z\thost1\thdfs\tBlock 456 replicated\tINFO",
        "2023-08-15T12:36:56Z\thost2\thdfs\tBlock 789 replicated\tINFO"
    ]
    
    template_ids = set()
    for log in logs:
        event = loader.parse_log_line(log)
        assert event is not None
        template_ids.add(event["template_id"])
    
    # All logs should map to same template
    assert len(template_ids) == 1

def test_clock_skew_correction(loader):
    """Test host clock skew detection and correction."""
    # Simulate host with 2s clock skew
    now = datetime.now(timezone.utc)
    skewed_ts = now.replace(second=now.second - 2)
    
    log = f"{skewed_ts.isoformat()}\tskewed-host\thdfs\tTest message\tINFO"
    event = loader.parse_log_line(log)
    
    assert event is not None
    # After a few events, clock skew should be detected
    assert abs(loader.host_offsets["skewed-host"]) > 1

def test_pii_pseudonymization(loader):
    """Test PII pseudonymization."""
    # Test consistent hashing
    value1 = loader.pseudonymize("test@example.com")
    value2 = loader.pseudonymize("test@example.com")
    assert value1 == value2
    assert len(value1) == 64  # SHA-256 hex digest
