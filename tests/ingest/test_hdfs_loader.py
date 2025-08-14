"""Tests for HDFS log loader."""
import pytest
from datetime import datetime
from src.ingest.hdfs_loader import parse_log_line, normalize_log

def test_log_parsing():
    """Test HDFS log line parsing."""
    # Sample log line from LogHub HDFS dataset
    log_line = '081109 203518 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010'
    
    parsed = parse_log_line(log_line)
    
    assert parsed is not None
    assert isinstance(parsed["ts"], datetime)
    assert parsed["level"] == "INFO"
    assert "DataNode" in parsed["component"]
    assert "Receiving block" in parsed["message"]
    
def test_log_normalization():
    """Test log normalization to schema format."""
    # Sample parsed log
    parsed = {
        "ts": datetime(2008, 11, 9, 20, 35, 18),
        "level": "INFO",
        "component": "dfs.DataNode",
        "message": "Receiving block blk_123 src: /10.0.0.1:1234 dest: /10.0.0.2:5678"
    }
    
    normalized = normalize_log(parsed)
    
    # Check schema compliance
    assert "ts" in normalized
    assert "host" in normalized
    assert "user" in normalized
    assert "op" in normalized
    assert "path" in normalized
    assert "status" in normalized
    assert isinstance(normalized.get("latency_ms", 0), (int, float))
    assert isinstance(normalized.get("size", 0), (int, float))
    assert isinstance(normalized["template_id"], str)
