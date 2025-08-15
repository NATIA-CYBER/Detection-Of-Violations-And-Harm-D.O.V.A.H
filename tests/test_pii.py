"""Tests for PII scrubbing and pseudonymization."""
import pytest
from src.ingest.scrub import scrub, PATTERNS
from src.common.pseudo import hmac_sha256_hex

def test_scrub_email():
    emails = [
        "admin@example.com",
        "user.name+tag@example.co.uk",
        "user.name@subdomain.example.com",
        "user-name@example.io"
    ]
    for email in emails:
        text = f"Contact {email} for support"
        result = scrub(text)
        assert email not in result
        assert "<REDACTED:email>" in result
        
    # Test multiple emails in one text
    text = "Contact admin@example.com or support@example.com"
    result = scrub(text)
    assert result.count("<REDACTED:email>") == 2

def test_scrub_ip():
    ipv4_cases = [
        "192.168.1.1",
        "10.0.0.1",
        "172.16.254.1",
        "0.0.0.0",
        "255.255.255.255"
    ]
    
    ipv6_cases = [
        "2001:db8::1",
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "fe80::1ff:fe23:4567:890a",
        "::1"
    ]
    
    # Test individual IPs
    for ip in ipv4_cases:
        result = scrub(f"Failed login from {ip}")
        assert ip not in result
        assert "<REDACTED:ipv4>" in result
        
    for ip in ipv6_cases:
        result = scrub(f"Failed login from {ip}")
        assert ip not in result
        assert "<REDACTED:ipv6>" in result
        
    # Test multiple IPs in one text
    text = "Connections: 192.168.1.1, 10.0.0.1, 2001:db8::1"
    result = scrub(text)
    assert result.count("<REDACTED:ipv4>") == 2
    assert result.count("<REDACTED:ipv6>") == 1

def test_scrub_api_keys():
    test_cases = [
        # API Keys
        "api_key=abcdef1234567890xyz",
        "API_KEY: ghijk12345lmnop",
        "apikey=abcdef1234567890",
        
        # Bearer tokens
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0",
        "bearer_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9",
        
        # AWS credentials
        "aws_access_key_id=AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "AWS_SESSION_TOKEN=AQoDYXdzEPT//////////wEXAMPLE"
    ]
    
    for case in test_cases:
        result = scrub(case)
        # Check key is removed
        assert case.split('=')[-1] not in result
        # Check appropriate redaction
        assert "<REDACTED" in result

def test_scrub_multiple_pii():
    text = "user=alice@corp.com pass=secret123 ip=10.0.0.1"
    result = scrub(text)
    assert "alice@corp.com" not in result
    assert "secret123" not in result
    assert "10.0.0.1" not in result
    assert result.count("<REDACTED") >= 3

def test_hmac_stability():
    # Test basic stability
    salt1 = b"tenant1"
    salt2 = b"tenant2"
    value = "test-value"
    
    # Same salt should give same result
    assert hmac_sha256_hex(value, salt1) == hmac_sha256_hex(value, salt1)
    
    # Different salts should give different results
    assert hmac_sha256_hex(value, salt1) != hmac_sha256_hex(value, salt2)
    
    # Different values should give different results
    assert hmac_sha256_hex("value1", salt1) != hmac_sha256_hex("value2", salt1)
    
    # Test with various input types
    test_values = [
        "simple string",
        "unicode_string_🔑",
        "string with spaces",
        "12345",
        "!@#$%^&*()",
        ""  # Empty string
    ]
    
    for val in test_values:
        # Verify stability
        assert hmac_sha256_hex(val, salt1) == hmac_sha256_hex(val, salt1)
        # Verify length
        assert len(hmac_sha256_hex(val, salt1)) == 64  # SHA-256 hex is 64 chars
        # Verify hex format
        assert all(c in '0123456789abcdef' for c in hmac_sha256_hex(val, salt1))

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
