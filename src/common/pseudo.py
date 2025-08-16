"""Pseudonymization utilities with HMAC-SHA256."""
import hmac
import hashlib
import os
from typing import Optional, Dict, Pattern

def get_salt() -> bytes:
    """Get salt from environment or use dev default.
    
    Returns:
        Salt bytes from DOVAH_TENANT_SALT env var or default
    """
    return os.getenv("DOVAH_TENANT_SALT", "dev_salt_change_me").encode()

def hmac_sha256_hex(value: str, tenant_salt: bytes) -> str:
    """Create HMAC-SHA256 hex digest with tenant-specific salt.
    
    Args:
        value: Value to pseudonymize
        tenant_salt: Tenant-specific salt
        
    Returns:
        Hex digest of HMAC-SHA256
    """
    return hmac.new(tenant_salt, value.encode(), hashlib.sha256).hexdigest()

def pseudo_host(host: Optional[str]) -> Optional[str]:
    """Pseudonymize hostname using environment salt.
    
    Args:
        host: Hostname to pseudonymize
        
    Returns:
        Pseudonymized hostname or None if input is None
    """
    if not host:
        return None
    return "h_" + hmac_sha256_hex(host, get_salt())[:16]

def pseudo_user(user: Optional[str]) -> Optional[str]:
    """Pseudonymize username using environment salt.
    
    Args:
        user: Username to pseudonymize
        
    Returns:
        Pseudonymized username or None if input is None
    """
    if not user:
        return None
    return "u_" + hmac_sha256_hex(user, get_salt())[:16]
