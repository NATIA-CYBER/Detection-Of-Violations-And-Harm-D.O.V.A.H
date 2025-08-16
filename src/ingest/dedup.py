"""Event deduplication with content hashing."""
import hashlib
import json
from typing import List, Dict, Set, Tuple
from datetime import datetime

def generate_event_hash(event: Dict, 
                       content_keys: List[str] = None) -> str:
    """Generate stable hash of event content.
    
    Args:
        event: Event dict to hash
        content_keys: List of keys to include in hash. If None,
                     uses all keys except timestamp
    
    Returns:
        SHA-256 hex digest of stable event representation
    """
    if content_keys is None:
        content_keys = [k for k in event.keys() if k != 'timestamp']
        
    # Create ordered dict of specified fields
    content = {k: event[k] for k in sorted(content_keys) if k in event}
    
    # Generate stable string representation
    content_str = json.dumps(content, sort_keys=True)
    
    return hashlib.sha256(content_str.encode()).hexdigest()

def dedup_events(events: List[Dict],
                window: int = 300,
                content_keys: List[str] = None) -> Tuple[List[Dict], Dict]:
    """Remove duplicate events within time window.
    
    Args:
        events: List of event dicts
        window: Time window in seconds to check for dupes
        content_keys: List of keys to use for content hash
        
    Returns:
        Tuple of (deduplicated events, dedup stats)
    """
    if not events:
        return [], {'total': 0, 'duplicates': 0}
        
    # Track seen hashes with timestamps
    seen_hashes: Dict[str, datetime] = {}
    duplicates = 0
    deduped = []
    
    for event in events:
        event_hash = generate_event_hash(event, content_keys)
        ts = event['timestamp']
        
        # Check if hash seen within window
        if event_hash in seen_hashes:
            last_ts = seen_hashes[event_hash]
            if (ts - last_ts).total_seconds() <= window:
                duplicates += 1
                continue
                
        # Not a duplicate, keep event and update seen
        deduped.append(event)
        seen_hashes[event_hash] = ts
        
    stats = {
        'total': len(events),
        'duplicates': duplicates,
        'unique': len(deduped)
    }
    
    return deduped, stats
