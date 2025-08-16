"""Tests for PII scrubbing and pseudonymization."""
import re
import pytest
from src.ingest.scrub import scrub, PATTERNS
from src.common.pseudo import hmac_sha256_hex

def test_scrub_email():
    emails = [
        "admin@example.com",
        "user.name+tag@example.co.uk",
        "user.name@subdomain.example.com",
        "user-name@example.io",
    ]
    for email in emails:
        txt = f"Contact {email} for support"
        out = scrub(txt)
        assert email not in out
        assert "<REDACTED:email>" in out

    # multiple emails
    out = scrub("Contact admin@example.com or support@example.com")
    assert out.count("<REDACTED:email>") == 2

def test_scrub_ip():
    ipv4 = ["192.168.1.1", "10.0.0.1", "172.16.254.1", "0.0.0.0", "255.255.255.255"]
    ipv6 = ["2001:db8::1", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "fe80::1ff:fe23:4567:890a", "::1"]

    for ip in ipv4:
        out = scrub(f"Failed login from {ip}")
        assert ip not in out
        assert "<REDACTED:ipv4>" in out

    for ip in ipv6:
        out = scrub(f"Failed login from {ip}")
        assert ip not in out
        assert "<REDACTED:ipv6>" in out

    # counts with mixed IPs
    out = scrub("Connections: 192.168.1.1, 10.0.0.1, 2001:db8::1")
    assert out.count("<REDACTED:ipv4>") == 2
    assert out.count("<REDACTED:ipv6>") == 1

def _extract_secret(s: str):
    """Helper to grab the value after ':' or '=' for assertions."""
    m = re.search(r"[:=]\s*['\"]?([^'\" \t]+)", s)
    return m.group(1) if m else None

def test_scrub_api_keys_and_tokens():
    cases = [
        # API keys (accept ':' or '=', value length >= 12)
        "api_key=abcdef1234567890xyz",
        "API_KEY: ghijk12345lmnopq",
        "apikey=abcdef1234567890",

        # Bearer/JWT-like
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0",
        "bearer_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9",

        # AWS creds
        "aws_access_key_id=AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "AWS_SESSION_TOKEN=AQoDYXdzEPT//////////wEXAMPLE",
    ]
    for s in cases:
        out = scrub(s)
        val = _extract_secret(s)
        if val:  # where we can parse the secret part
            assert val not in out
        assert "<REDACTED" in out  # some redaction tag present

def test_scrub_multiple_pii():
    s = "user=alice@corp.com pass=secret123 ip=10.0.0.1"
    out = scrub(s)
    assert "alice@corp.com" not in out
    assert "secret123" not in out
    assert "10.0.0.1" not in out
    assert out.count("<REDACTED") >= 3

def test_hmac_stability():
    salt1, salt2 = b"tenant1", b"tenant2"
    assert hmac_sha256_hex("test-value", salt1) == hmac_sha256_hex("test-value", salt1)
    assert hmac_sha256_hex("test-value", salt1) != hmac_sha256_hex("test-value", salt2)
    assert hmac_sha256_hex("value1", salt1) != hmac_sha256_hex("value2", salt1)
    for val in ["simple string", "unicode_string_🔑", "string with spaces", "12345", "!@#$%^&*()", ""]:
        h = hmac_sha256_hex(val, salt1)
        assert len(h) == 64 and all(c in "0123456789abcdef" for c in h)

def test_pattern_compilation():
    for name, pattern in PATTERNS.items():
        try:
            re.compile(pattern)
        except re.error as e:
            pytest.fail(f"Pattern {name} failed to compile: {e}")

def test_empty_input_and_idempotent():
    assert scrub("") == ""
    assert scrub(None) is None
    s = "user=a@b.com token=abcdef123456"
    once = scrub(s)
    twice = scrub(once)
    assert once == twice

def test_secret_case_insensitive():
    s = "PASSWORD=foo APIKey=bar ToKeN=baz"
    out = scrub(s)
    assert "foo" not in out and "bar" not in out and "baz" not in out
    assert out.count("<REDACTED") >= 3
