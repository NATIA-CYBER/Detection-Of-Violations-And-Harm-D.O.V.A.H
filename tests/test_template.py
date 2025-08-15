"""Tests for log template extraction."""
import pytest
from src.ingest.template_extract import TemplateMiner

def test_template_extraction():
    miner = TemplateMiner()
    test_cases = [
        # Basic number replacement
        ("Block 1234 received", "Block * received"),
        ("Failed to replicate block 5678", "Failed to replicate block *"),
        
        # IP addresses
        ("Connection from 192.168.1.1", "Connection from *"),
        ("Failed to connect to 10.0.0.1:8080", "Failed to connect to *:*"),
        
        # UUIDs and hex
        ("UUID: 123e4567-e89b-12d3-a456-426614174000", "UUID: *"),
        ("Hash: a1b2c3d4e5f6", "Hash: *"),
        
        # Multiple variables
        ("Block 1234 from 192.168.1.1", "Block * from *"),
        ("ID 5678 hash a1b2c3d4 from 10.0.0.1", "ID * hash * from *")
    ]
    
    for msg, expected in test_cases:
        template_id = miner.extract(msg)
        template = miner.get_template(template_id)
        assert template == expected

def test_template_stability():
    """Test that same messages get same template IDs."""
    miner = TemplateMiner()
    
    # Same pattern, different values
    msg1 = "Block 1234 received"
    msg2 = "Block 5678 received"
    
    id1 = miner.extract(msg1)
    id2 = miner.extract(msg2)
    assert id1 == id2
    
    # Different patterns should get different IDs
    msg3 = "Connection from 192.168.1.1"
    id3 = miner.extract(msg3)
    assert id1 != id3

def test_empty_input():
    """Test handling of empty input."""
    miner = TemplateMiner()
    assert miner.extract("") == ""
    assert miner.extract(None) == ""

def test_hdfs_templates():
    """Test with real HDFS log patterns."""
    miner = TemplateMiner()
    logs = [
        "DataNode: Receiving block blk_1234 src: /10.0.1.1:50010",
        "DataNode: Receiving block blk_5678 src: /10.0.1.2:50010",
        "NameNode: Starting checkpoint for transaction ID 12345",
        "NameNode: Starting checkpoint for transaction ID 67890",
        "DataNode: Block blk_1234 is corrupt",
        "DataNode: Block blk_5678 is corrupt"
    ]
    
    # Get template IDs
    template_ids = [miner.extract(log) for log in logs]
    
    # Should find 3 unique templates
    unique_templates = {miner.get_template(tid) for tid in template_ids}
    assert len(unique_templates) == 3
    
    # Check specific patterns
    templates = unique_templates
    assert "DataNode: Receiving block blk_* src: *:*" in templates
    assert "NameNode: Starting checkpoint for transaction ID *" in templates
    assert "DataNode: Block blk_* is corrupt" in templates
