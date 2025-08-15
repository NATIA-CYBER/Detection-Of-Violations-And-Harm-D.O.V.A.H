"""Tests for PII scrubbing and pseudonymization."""
import pytest
from src.ingest.scrub import scrub, PATTERNS
from src.common.pseudo import hmac_sha256_hex

def test_scrub_email():
    text = "Contact admin@example.com for support"
    result = scrub(text)
    assert "admin@example.com" not in result
    assert "<REDACTED:email>" in result

def test_scrub_ip():
    text = "Failed login from 192.168.1.1 and 2001:db8::1"
    result = scrub(text)
    assert "192.168.1.1" not in result
    assert "2001:db8::1" not in result
    assert result.count("<REDACTED:ipv4>") == 1
    assert result.count("<REDACTED:ipv6>") == 1

def test_scrub_api_keys():
    text = """
    api_key=abcdef1234567890
    Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0
    AKIAIOSFODNN7EXAMPLE
    aws_secret=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    """
    result = scrub(text)
    assert "abcdef1234567890" not in result
    assert "AKIAIOSFODNN7EXAMPLE" not in result
    assert "wJalrXUtnFEMI" not in result
    assert result.count("<REDACTED") >= 3

def test_scrub_multiple_pii():
    text = "user=alice@corp.com pass=secret123 ip=10.0.0.1"
    result = scrub(text)
    assert "alice@corp.com" not in result
    assert "secret123" not in result
    assert "10.0.0.1" not in result
    assert result.count("<REDACTED") >= 3

def test_hmac_stability():
    salt1 = b"tenant1"
    salt2 = b"tenant2"
    value = "test-value"
    
    # Same salt should give same result
    assert hmac_sha256_hex(value, salt1) == hmac_sha256_hex(value, salt1)
    
    # Different salts should give different results
    assert hmac_sha256_hex(value, salt1) != hmac_sha256_hex(value, salt2)
    
    # Different values should give different results
    assert hmac_sha256_hex("value1", salt1) != hmac_sha256_hex("value2", salt1)

def test_pattern_compilation():
    """Verify all regex patterns compile."""
    for name, pattern in PATTERNS.items():
        try:
            import re
            re.compile(pattern)
        except re.error as e:
            pytest.fail(f"Pattern {name} failed to compile: {e}")

def test_empty_input():
    """Test handling of empty/None input."""
    assert scrub("") == ""
    assert scrub(None) == None
