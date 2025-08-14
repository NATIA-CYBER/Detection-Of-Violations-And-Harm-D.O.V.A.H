import os
import json
import pytest
from datetime import datetime, timezone
from pathlib import Path

from src.stream.job import SchemaValidator, StreamingAdapter, WindowFeatures, calculate_entropy

@pytest.fixture
def schema_validator():
    return SchemaValidator()

@pytest.fixture
def valid_event():
    return {
        "timestamp": "2025-08-14T13:00:00Z",
        "host": "node-17",
        "component": "DataNode",
        "template_id": "143",
        "session_id": "S123456",
        "schema_ver": "1.0.0",
        "level": "ERROR"
    }

@pytest.fixture
def invalid_event():
    return {
        "timestamp": "invalid-date",
        "host": "node-17",
        "component": "DataNode"
    }

def test_schema_validator_valid_event(schema_validator, valid_event):
    """Test schema validation with valid event."""
    assert schema_validator.validate_event(valid_event) is True

def test_schema_validator_invalid_event(schema_validator, invalid_event):
    """Test schema validation with invalid event."""
    assert schema_validator.validate_event(invalid_event) is False

def test_streaming_adapter_deserialize_valid(valid_event):
    """Test streaming adapter deserialize with valid event."""
    adapter = StreamingAdapter()
    event_bytes = json.dumps(valid_event).encode('utf-8')
    result = adapter.validate_and_deserialize(event_bytes)
    assert result == valid_event

def test_streaming_adapter_deserialize_invalid(invalid_event):
    """Test streaming adapter deserialize with invalid event."""
    adapter = StreamingAdapter()
    event_bytes = json.dumps(invalid_event).encode('utf-8')
    result = adapter.validate_and_deserialize(event_bytes)
    assert result is None

def test_window_features_computation():
    """Test window features computation."""
    window = WindowFeatures(window_size=60)
    events = [
        {
            "timestamp": "2025-08-14T13:00:00Z",
            "host": "node-17",
            "component": "DataNode",
            "template_id": "143",
            "session_id": "S123456",
            "level": "ERROR"
        },
        {
            "timestamp": "2025-08-14T13:00:30Z",
            "host": "node-17",
            "component": "NameNode",
            "template_id": "144",
            "session_id": "S123456",
            "level": "INFO"
        }
    ]
    
    features = window.compute_features(events)
    assert features['event_count'] == 2
    assert features['unique_components'] == 2
    assert features['error_ratio'] == 0.5
    assert 0 <= features['template_entropy'] <= 1.0
    assert 0 <= features['component_entropy'] <= 1.0

def test_entropy_calculation():
    """Test entropy calculation function."""
    items = ['A', 'A', 'B', 'C', 'C', 'C']
    entropy = calculate_entropy(items)
    assert entropy > 0
    assert entropy <= 1.0  # Normalized entropy for 3 unique items

def test_window_features_empty_window():
    """Test window features with empty window."""
    window = WindowFeatures(window_size=60)
    with pytest.raises(ValueError):
        window.compute_features([])

def test_streaming_adapter_invalid_source():
    """Test streaming adapter with invalid source type."""
    with pytest.raises(ValueError):
        adapter = StreamingAdapter(source_type='invalid')
        adapter.get_source('test-topic')
