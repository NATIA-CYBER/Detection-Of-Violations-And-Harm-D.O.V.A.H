"""PII scrubbing with enhanced pattern matching."""
import re
from typing import Optional, Any

# Patterns tuned to your tests
PATTERNS = {
    "email": r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b",
    "ipv4": r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b",

    # IPv6 including compressed forms like ::1 (no \b at start so leading ':' is matched)
    "ipv6": r"(?:(?:[A-Fa-f0-9]{1,4}:){7}[A-Fa-f0-9]{1,4}|(?:[A-Fa-f0-9]{1,4}:){1,7}:|:(?::[A-Fa-f0-9]{1,4}){1,7})",

    # Header-style Bearer tokens (e.g., "Authorization: Bearer <jwt>")
    "bearer": r"(?i)\bBearer\s+[A-Za-z0-9\-\._~\+/=]{20,}",

    # AWS-shaped keys (kept for safety—can be toggled later if you wish)
    "aws_access_key": r"\bAKIA[0-9A-Z]{16}\b",
    "aws_secret": r"(?i)\baws_secret_access_key\b\s*[:=]\s*[A-Za-z0-9/\+=]{40,}",

    # API keys (accept ':' or '=', require 12+ chars)
    "api_key": r"(?i)\b(api[_\-]?key|apikey|api-key)\b\s*[:=]\s*['\"]?[A-Za-z0-9\-_]{12,}['\"]?",

    # Any k/v whose key contains 'token' or api (SESSION_TOKEN, bearer_token, accessToken, apikey etc.)
    "kv_token": r"(?i)\b([A-Za-z0-9_]*token[A-Za-z0-9_]*|api[_\-]?key)\b\s*[:=]\s*['\"]?([^'\"\s]+)['\"]?",

    # Password/secret/pass keys with ANY non-space value length
    "secret_kv": r"(?i)\b(password|pass|secret)\b\s*[:=]\s*['\"]?([^'\"\s]+)['\"]?"
}

_COMPILED = {k: re.compile(v) for k, v in PATTERNS.items()}

# Consider keys in structured logs that should always be scrubbed if string-like
SENSITIVE_KEYS = re.compile(r"(?i)\b(pass(word)?|secret|token|api[_\-]?key|session|bearer|auth)\b")

def scrub(s: Optional[str]) -> Optional[str]:
    """Redact PII/secrets from free text. Idempotent."""
    if s is None:
        return None
    out = s
    # Generic secrets first to catch short values
    for k in ("secret_kv", "kv_token"):
        out = _COMPILED[k].sub(f"<REDACTED:{k}>", out)
    # Then specific patterns
    for k in ("bearer", "aws_access_key", "aws_secret", "api_key"):
        out = _COMPILED[k].sub(f"<REDACTED:{k}>", out)
    # Finally network identifiers
    out = _COMPILED["email"].sub("<REDACTED:email>", out)
    out = _COMPILED["ipv4"].sub("<REDACTED:ipv4>", out)
    out = _COMPILED["ipv6"].sub("<REDACTED:ipv6>", out)
    return out

def scrub_mapping(obj: Any) -> Any:
    """
    Recursively scrub dict/list structures (e.g., JSON log events).
    - For string values: apply scrub()
    - For dict keys that look sensitive: scrub their values even if non-string
    """
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if isinstance(v, str):
                v_clean = scrub(v)
            elif SENSITIVE_KEYS.search(str(k)):
                # Coerce to string and scrub; preserve type by replacing with tag
                v_clean = "<REDACTED:value>"
            else:
                v_clean = scrub_mapping(v)
            cleaned[k] = v_clean
        return cleaned
    elif isinstance(obj, list):
        return [scrub_mapping(v) for v in obj]
    elif isinstance(obj, str):
        return scrub(obj)
    else:
        return obj
