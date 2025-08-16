"""Test template extraction caching."""
import pytest
import json
from pathlib import Path
from src.ingest.template_cache import TemplateCache

def test_template_extraction(tmp_path):
    cache = TemplateCache(tmp_path)
    
    # First extraction
    msg = "User alice logged in from 10.0.0.1"
    tid1, pattern1 = cache.extract_template(msg)
    
    # Should be cached
    tid2, pattern2 = cache.extract_template(msg)
    assert tid1 == tid2
    assert pattern1 == pattern2
    
    # Different message = different template
    msg2 = "Failed login attempt from 10.0.0.2"
    tid3, pattern3 = cache.extract_template(msg2)
    assert tid3 != tid1
    
    # Cache should persist
    cache2 = TemplateCache(tmp_path)
    tid4, pattern4 = cache2.extract_template(msg)
    assert tid4 == tid1
    assert pattern4 == pattern1
    
def test_template_stats(tmp_path):
    cache = TemplateCache(tmp_path)
    
    # Add some templates
    messages = [
        "User alice logged in from 10.0.0.1",
        "User bob logged in from 10.0.0.2",
        "Failed login from 10.0.0.3",
        "System shutdown initiated",
        "System startup completed"
    ]
    
    for msg in messages:
        cache.extract_template(msg)
        
    stats = cache.get_stats()
    assert stats['cache_size'] == len(messages)
    assert stats['total_templates'] > 0
    assert len(stats['clusters']) == stats['total_templates']
    
    # Check cluster details
    for cluster in stats['clusters']:
        assert 'id' in cluster
        assert 'size' in cluster
        assert 'pattern' in cluster

def test_cache_persistence(tmp_path):
    cache = TemplateCache(tmp_path)
    
    # Add template
    msg = "User alice logged in from 10.0.0.1"
    tid1, pattern1 = cache.extract_template(msg)
    
    # Verify cache file exists
    cache_file = tmp_path / "template_cache.json"
    assert cache_file.exists()
    
    # Check cache contents
    with open(cache_file) as f:
        data = json.load(f)
        assert len(data) == 1
        
    # New cache instance should load existing templates
    cache2 = TemplateCache(tmp_path)
    tid2, pattern2 = cache2.extract_template(msg)
    assert tid2 == tid1
    assert pattern2 == pattern1
