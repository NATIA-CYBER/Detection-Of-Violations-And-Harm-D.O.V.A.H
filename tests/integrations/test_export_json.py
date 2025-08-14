"""Tests for JSON evidence export."""
import json
import os
from datetime import datetime, timezone
import pytest
from src.integrations.export_json import ExportManager

@pytest.fixture
def sample_detection():
    """Sample detection data for testing."""
    return {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "ts": datetime.now(timezone.utc),
        "session_id": "test-session-001",
        "score": 0.95,
        "model_version": "iforest-v1",
        "event_count": 150,
        "unique_components": 12,
        "error_ratio": 0.05,
        "template_entropy": 3.2,
        "component_entropy": 2.8,
        "epss_score": 0.89,
        "kev_name": "Critical Vulnerability",
        "kev_description": "A critical security vulnerability"
    }

def test_json_export(tmp_path, sample_detection):
    """Test JSON evidence export with signing."""
    export_dir = tmp_path / "evidence"
    export_dir.mkdir()
    
    # Initialize with test key
    manager = ExportManager(
        signing_key="test-key-123",
        export_dir=str(export_dir)
    )
    
    # Export single detection
    filename = manager.export_json([sample_detection])
    
    # Verify file exists
    assert os.path.exists(filename)
    
    # Load and verify content
    with open(filename) as f:
        data = json.load(f)
    
    # Check structure
    assert "metadata" in data
    assert "detections" in data
    assert "signature" in data
    
    # Verify detection data
    detection = data["detections"][0]
    assert detection["detection"]["session_id"] == sample_detection["session_id"]
    assert detection["detection"]["score"] == sample_detection["score"]
    assert detection["window_features"]["event_count"] == sample_detection["event_count"]
    
    # Verify signature exists and is non-empty
    assert data["signature"]
    assert len(data["signature"]) > 0

def test_csv_export(tmp_path, sample_detection):
    """Test CSV evidence export."""
    export_dir = tmp_path / "evidence"
    export_dir.mkdir()
    
    manager = ExportManager(
        signing_key="test-key-123",
        export_dir=str(export_dir)
    )
    
    # Export as CSV
    filename = manager.export_csv([sample_detection])
    
    # Verify file exists and content
    assert os.path.exists(filename)
    
    with open(filename) as f:
        content = f.read()
        
    # Check headers and data
    assert "session_id,timestamp,score,event_count" in content
    assert sample_detection["session_id"] in content
    assert str(sample_detection["score"]) in content

def test_error_handling(tmp_path):
    """Test error handling in export."""
    export_dir = tmp_path / "evidence"
    export_dir.mkdir()
    
    manager = ExportManager(
        signing_key="test-key-123",
        export_dir=str(export_dir)
    )
    
    # Test with invalid detection data
    invalid_detection = {
        "id": "invalid-no-required-fields"
    }
    
    with pytest.raises(ValueError):
        manager.export_json([invalid_detection])
        
    # Test with missing signing key
    with pytest.raises(ValueError):
        ExportManager(signing_key="", export_dir=str(export_dir))
        
    # Test with non-existent export directory
    with pytest.raises(ValueError):
        ExportManager(
            signing_key="test-key",
            export_dir="/nonexistent/dir"
        )
