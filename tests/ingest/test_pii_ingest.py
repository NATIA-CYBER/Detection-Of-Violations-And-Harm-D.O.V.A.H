"""Tests for PII scrubbing and pseudonymization."""
import os
from pathlib import Path

import pytest

from src.ingest.hdfs_loader import HDFSLoader

@pytest.fixture
def test_key():
    """Test HMAC key."""
    return "a" * 64

@pytest.fixture
def mock_schema(tmp_path):
    """Create a mock schema file."""
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    schema_file = schema_dir / "parsed_log.schema.json"
    schema_file.write_text('{"type": "object"}')
    return schema_dir

@pytest.fixture
def loader(test_key, mock_schema, monkeypatch):
    """Create HDFSLoader instance with test key."""
    os.environ["DOVAH_HMAC_KEY"] = test_key
    
    # Mock schema path
    monkeypatch.setattr(HDFSLoader, "SCHEMA_PATH", mock_schema)
    
    # Mock database connection
    class MockEngine:
        def __init__(self):
            pass
    
    loader = HDFSLoader(tenant_id="test_tenant")
    loader.engine = MockEngine()
    return loader

@pytest.fixture
def other_tenant_loader(test_key, mock_schema, monkeypatch):
    """Create HDFSLoader instance for a different tenant."""
    os.environ["DOVAH_HMAC_KEY"] = test_key
    
    # Mock schema path
    monkeypatch.setattr(HDFSLoader, "SCHEMA_PATH", mock_schema)
    
    # Mock database connection
    class MockEngine:
        def __init__(self):
            pass
    
    loader = HDFSLoader(tenant_id="other_tenant")
    loader.engine = MockEngine()
    return loader

def test_pseudonymize_deterministic(loader):
    """Test HMAC-SHA256 pseudonymization is deterministic."""
    # Same input should produce same output
    assert loader.pseudonymize("test") == loader.pseudonymize("test")
    
    # Different inputs should produce different outputs
    assert loader.pseudonymize("test1") != loader.pseudonymize("test2")
    
    # Test expected length (64 chars for SHA256)
    assert len(loader.pseudonymize("test")) == 64

def test_pseudonymize_tenant_isolation(loader, other_tenant_loader):
    """Test that different tenants get different pseudonyms."""
    value = "test_value"
    # Same value should produce different hashes for different tenants
    assert loader.pseudonymize(value) != other_tenant_loader.pseudonymize(value)

def test_pseudonymize_context(loader):
    """Test context-aware pseudonymization."""
    value = "test@example.com"
    # Same value in different contexts should produce different hashes
    email_hash = loader.pseudonymize(value, context="email")
    username_hash = loader.pseudonymize(value, context="username")
    assert email_hash != username_hash

def test_scrub_pii_email(loader):
    """Test email scrubbing."""
    text = "User alice@example.com reported an error"
    scrubbed = loader.scrub_pii(text)
    assert "alice@example.com" not in scrubbed
    assert "<email:" in scrubbed

def test_scrub_pii_token(loader):
    """Test API token scrubbing."""
    text = "api_key=1234567890abcdef1234567890abcdef"
    scrubbed = loader.scrub_pii(text)
    assert "1234567890abcdef1234567890abcdef" not in scrubbed
    assert "<token:" in scrubbed

def test_scrub_pii_aws_secret(loader):
    """Test AWS secret scrubbing."""
    text = "aws_secret_key=AKIAIOSFODNN7EXAMPLE/K7MDENG/bPxRfiCYEXAMPLEKEY"
    scrubbed = loader.scrub_pii(text)
    assert "AKIAIOSFODNN7EXAMPLE" not in scrubbed
    assert "<aws_secret:" in scrubbed

def test_scrub_pii_password(loader):
    """Test password scrubbing."""
    text = "password=super_secret123"
    scrubbed = loader.scrub_pii(text)
    assert "super_secret123" not in scrubbed
    assert "<password:" in scrubbed

def test_scrub_pii_private_key(loader):
    """Test private key scrubbing."""
    text = """-----BEGIN RSA PRIVATE KEY-----
    MIIEpAIBAAKCAQEA1234567890
    -----END RSA PRIVATE KEY-----"""
    scrubbed = loader.scrub_pii(text)
    assert "PRIVATE KEY" not in scrubbed
    assert "<private_key:" in scrubbed

def test_scrub_pii_multiple(loader):
    """Test scrubbing multiple PII in one text."""
    text = "User bob@example.com with key=abcdef1234567890 and pass=secret"
    scrubbed = loader.scrub_pii(text)
    assert "bob@example.com" not in scrubbed
    assert "abcdef1234567890" not in scrubbed
    assert "secret" not in scrubbed
    assert "<email:" in scrubbed
    assert "<token:" in scrubbed
    assert "<password:" in scrubbed
