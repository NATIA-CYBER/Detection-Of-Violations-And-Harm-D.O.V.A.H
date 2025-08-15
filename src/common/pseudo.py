"""Pseudonymization utilities with HMAC-SHA256."""
import hmac
import hashlib
from typing import Optional

def hmac_sha256_hex(value: str, tenant_salt: bytes) -> str:
    """Create HMAC-SHA256 hex digest with tenant-specific salt.
    
    Args:
        value: Value to pseudonymize
        tenant_salt: Tenant-specific salt
        
    Returns:
        Hex digest of HMAC-SHA256
    """
    return hmac.new(tenant_salt, value.encode(), hashlib.sha256).hexdigest()

def pseudo_user(user: Optional[str], tenant_salt: bytes) -> Optional[str]:
    """Pseudonymize username with tenant-specific salt.
    
    Args:
        user: Username to pseudonymize
        tenant_salt: Tenant-specific salt
        
    Returns:
        Pseudonymized username or None if input is None
    """
    if not user:
        return None
    return f"u_{hmac_sha256_hex(user, tenant_salt)[:16]}"

def derive_tenant_salt(base_key: str, tenant_id: str) -> bytes:
    """Derive tenant-specific salt from base key.
    
    Args:
        base_key: Base HMAC key (from environment)
        tenant_id: Tenant identifier
        
    Returns:
        Tenant-specific salt as bytes
    """
    if not base_key or not tenant_id:
        raise ValueError("Both base_key and tenant_id are required")
        
    # Use HMAC to derive tenant-specific key
    return hmac.new(
        base_key.encode(),
        tenant_id.encode(),
        hashlib.sha256
    ).digest()
