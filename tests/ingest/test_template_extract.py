"""Tests for ML-based template extraction."""
import pytest
from src.ingest.template_extract import TemplateExtractor, Template

def test_basic_template_extraction():
    """Test basic template extraction functionality."""
    extractor = TemplateExtractor(min_cluster_size=2)
    
    messages = [
        "User alice logged in from 192.168.1.100",
        "User bob logged in from 192.168.1.101",
        "User charlie logged in from 192.168.1.102",
        "Failed login attempt from 10.0.0.1",
        "Failed login attempt from 10.0.0.2"
    ]
    
    templates = extractor.extract_templates(messages)
    assert len(templates) == 2
    
    # Check login success template
    success_template = next(
        t for t in templates.values()
        if "logged in" in t.pattern
    )
    assert "User <*> logged in from <*>" in success_template.pattern
    assert len(success_template.variable_indices) == 2
    
    # Check login failure template
    failure_template = next(
        t for t in templates.values()
        if "Failed" in t.pattern
    )
    assert "Failed login attempt from <*>" in failure_template.pattern
    assert len(failure_template.variable_indices) == 1

def test_variable_extraction():
    """Test extraction of variable parts."""
    extractor = TemplateExtractor()
    
    messages = [
        "Process 1234 started on host-01 at 2024-01-01 10:00:00",
        "Process 5678 started on host-02 at 2024-01-01 11:00:00",
        "Process 9012 started on host-03 at 2024-01-01 12:00:00"
    ]
    
    templates = extractor.extract_templates(messages)
    assert len(templates) == 1
    
    template = list(templates.values())[0]
    assert "Process <*> started on <*> at <*>" in template.pattern
    assert len(template.variable_indices) == 3
    
    # Test matching
    template_id, variables = extractor.match_template(
        "Process 3456 started on host-04 at 2024-01-01 13:00:00"
    )
    assert template_id == template.template_id
    assert len(variables) == 3
    assert "3456" in variables.values()
    assert "host-04" in variables.values()

def test_multiline_templates():
    """Test handling of multi-line log messages."""
    extractor = TemplateExtractor()
    
    messages = [
        "Error in module:\nStackTrace:\n  at line 10\n  at line 20",
        "Error in module:\nStackTrace:\n  at line 30\n  at line 40",
        "Warning: disk space low\nDetails: 95% used"
    ]
    
    templates = extractor.extract_templates(messages)
    assert len(templates) == 2
    
    # Find stack trace template
    stack_template = next(
        t for t in templates.values()
        if "Error in module" in t.pattern
    )
    assert "Error in module:\nStackTrace:\n" in stack_template.pattern
    assert "<*>" in stack_template.pattern

def test_template_merging():
    """Test merging of similar templates."""
    extractor = TemplateExtractor()
    
    messages = [
        "Connection to server-1 failed: timeout",
        "Connection to server-2 failed: refused",
        "Connected to server-3 successfully",
        "Connected to server-4 successfully in 100ms",
    ]
    
    templates = extractor.extract_templates(messages)
    
    # Should merge similar templates
    assert len(templates) == 2
    
    patterns = [t.pattern for t in templates.values()]
    assert any("Connection to <*> failed: <*>" in p for p in patterns)
    assert any("Connected to <*> successfully" in p for p in patterns)

def test_regex_matching():
    """Test regex-based template matching."""
    extractor = TemplateExtractor()
    
    messages = [
        "User admin deleted file /path/to/file1.txt",
        "User root deleted file /path/to/file2.txt",
    ]
    
    templates = extractor.extract_templates(messages)
    template = list(templates.values())[0]
    
    # Test exact match
    template_id, vars1 = extractor.match_template(
        "User guest deleted file /path/to/file3.txt"
    )
    assert template_id == template.template_id
    assert "guest" in vars1.values()
    assert "/path/to/file3.txt" in vars1.values()
    
    # Test no match
    template_id, vars2 = extractor.match_template(
        "Completely different message"
    )
    assert template_id is None
    assert not vars2

def test_template_limits():
    """Test template count limiting."""
    extractor = TemplateExtractor(max_templates=2)
    
    messages = [
        "Error type 1: details",
        "Error type 2: details",
        "Warning type 1: details",
        "Warning type 2: details",
        "Info type 1: details",
    ]
    
    templates = extractor.extract_templates(messages)
    assert len(templates) <= 2
