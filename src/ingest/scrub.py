"""PII scrubbing with enhanced pattern matching."""
import re
from typing import Dict, Pattern

# Extended PII patterns with common variations
PATTERNS: Dict[str, str] = {
    "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
    "ipv4": r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\b",
    "ipv6": r"(?:\b(?:[A-Fa-f0-9]{1,4}:){7}[A-Fa-f0-9]{1,4}\b)|(?:\b(?:[A-Fa-f0-9]{1,4}:){1,6}:[A-Fa-f0-9]{1,4}\b)",
    "bearer": r"Bearer\s+[A-Za-z0-9\-_.~+/=]{20,}",
    "aws_key": r"\b(?:AKIA|A3T|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}\b",
    "aws_secret": r"(?i)\b[A-Za-z0-9/+=]{40}\b",
    "secret": r"(?i)(?:pass(?:word)?|secret|token|key)\s*[=:]\s*['\"]?[^\s'\"]{6,}['\"]?",
    "jwt": r"eyJ[A-Za-z0-9-_=]+\.eyJ[A-Za-z0-9-_=]+\.[A-Za-z0-9-_.+/=]+",
    "private_key": r"-----BEGIN (?:RSA |DSA )?PRIVATE KEY-----[^-]*-----END (?:RSA |DSA )?PRIVATE KEY-----",
    "mac": r"\b(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}\b"
}

# Compile patterns once
COMPILED: Dict[str, Pattern] = {k: re.compile(v) for k, v in PATTERNS.items()}

def scrub(text: str, patterns: Dict[str, Pattern] = COMPILED) -> str:
    """Scrub PII from text using compiled patterns.
    
    Args:
        text: Input text to scrub
        patterns: Optional dict of compiled regex patterns
        
    Returns:
        Scrubbed text with PII replaced by <REDACTED>
    """
    if not text:
        return text
        
    result = text
    for pattern_name, pattern in patterns.items():
        result = pattern.sub(f"<REDACTED:{pattern_name}>", result)
    return result
