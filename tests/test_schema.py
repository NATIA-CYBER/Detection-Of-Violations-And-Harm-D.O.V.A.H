"""Tests for JSONL log schema validation."""
import json
from pathlib import Path

import jsonschema
import pytest

def load_schema():
    """Load the JSONL schema from file."""
    schema_path = Path(__file__).parent.parent / 'parsed_log.schema.json'
    with open(schema_path) as f:
        schema = json.load(f)
        # Add explicit format validation
        schema['properties']['timestamp']['pattern'] = '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:Z|[+-]\\d{2}:\\d{2})$'
        return schema

def test_valid_log_entry():
    """Test validation of a valid log entry."""
    schema = load_schema()
    validator = jsonschema.validators.validator_for(schema)(schema)
    validator.format_checker = jsonschema.FormatChecker()
    
    valid_entry = {
        "timestamp": "2025-08-01T12:00:00Z",
        "host": "h123abc",
        "level": "INFO",
        "component": "DataNode",
        "message": "Starting up",
        "template_id": "T1",
        "session_id": "h123abc_user1_202508011200",
        "dedup_key": "abc123"  # Optional
    }
    
    validator.validate(valid_entry)

def test_invalid_log_entries():
    """Test validation fails for invalid entries."""
    schema = load_schema()
    validator = jsonschema.validators.validator_for(schema)(schema)
    
    # Test each invalid case separately
    def validate_invalid(entry):
        with pytest.raises(jsonschema.exceptions.ValidationError):
            validator.validate(entry)
    
    # Missing required field
    invalid_entry = {
        "timestamp": "2025-08-01T12:00:00Z",
        "level": "INFO",
        # Missing host
        "component": "DataNode",
        "message": "Starting up",
        "template_id": "T1",
        "session_id": "h123abc_user1_202508011200"
    }
    validate_invalid(invalid_entry)
    
    # Invalid timestamp format
    invalid_entry = {
        "timestamp": "2025-08-01",  # Missing time
        "host": "h123abc",
        "level": "INFO",
        "component": "DataNode",
        "message": "Starting up",
        "template_id": "T1",
        "session_id": "h123abc_user1_202508011200"
    }
    validate_invalid(invalid_entry)
    
    # Invalid log level
    invalid_entry = {
        "timestamp": "2025-08-01T12:00:00Z",
        "host": "h123abc",
        "level": "DEBUG",  # Not in enum
        "component": "DataNode",
        "message": "Starting up",
        "template_id": "T1",
        "session_id": "h123abc_user1_202508011200"
    }
    validate_invalid(invalid_entry)

def test_sample_jsonl():
    """Test validation of sample JSONL file."""
    schema = load_schema()
    validator = jsonschema.validators.validator_for(schema)(schema)
    validator.format_checker = jsonschema.FormatChecker()
    sample_path = Path(__file__).parent / 'data' / 'hdfs' / 'sample.jsonl'
    
    with open(sample_path) as f:
        for line in f:
            entry = json.loads(line)
            validator.validate(entry)
