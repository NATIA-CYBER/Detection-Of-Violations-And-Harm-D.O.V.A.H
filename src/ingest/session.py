"""Time-based log sessionization with privacy."""
import datetime
from datetime import timezone
from typing import Optional, Dict, List, Tuple
import pandas as pd
from dateutil.parser import parse as parse_date
from ..common.pseudo import pseudo_host, pseudo_user

def parse_rfc3339(ts_str: str) -> datetime.datetime:
    """Parse RFC3339 timestamp with validation."""
    try:
        dt = parse_date(ts_str)
        if not dt.tzinfo:
            raise ValueError("Missing timezone")
        return dt.astimezone(timezone.utc)
    except Exception as e:
        raise ValueError(f"Invalid RFC3339: {ts_str}") from e

def detect_clock_skew(timestamps: List[datetime.datetime], 
                     window: int = 300) -> Dict[str, float]:
    """Detect clock skew using rolling window.
    
    Args:
        timestamps: List of UTC timestamps
        window: Analysis window in seconds (default: 5min)
        
    Returns:
        Dict with skew statistics
    """
    if not timestamps:
        return {}
        
    ts_series = pd.Series(timestamps).sort_values()
    
    # Calculate time deltas between consecutive events
    deltas = ts_series.diff().dt.total_seconds()
    
    # Find negative deltas (out of order timestamps)
    neg_deltas = deltas[deltas < 0]
    
    return {
        'max_backwards_secs': float(abs(neg_deltas.min())) if len(neg_deltas) else 0,
        'backwards_events': len(neg_deltas),
        'total_events': len(timestamps)
    }

def fix_clock_skew(events: List[Dict], 
                   timestamp_key: str = 'timestamp') -> List[Dict]:
    """Fix clock skew by sorting and adjusting timestamps.
    
    Args:
        events: List of event dicts
        timestamp_key: Key for timestamp field
        
    Returns:
        Events with corrected timestamps
    """
    # Sort by timestamp
    sorted_events = sorted(events, key=lambda e: e[timestamp_key])
    
    # Ensure strictly increasing timestamps
    last_ts = None
    for event in sorted_events:
        ts = event[timestamp_key]
        if last_ts and ts <= last_ts:
            # Add 1 microsecond to make it strictly increasing
            ts = last_ts + datetime.timedelta(microseconds=1)
            event[timestamp_key] = ts
        last_ts = ts
        
    return sorted_events

def generate_session_id(host: str, 
                       user: Optional[str],
                       timestamp: datetime.datetime) -> str:
    """Generate privacy-preserving session ID.
    
    Args:
        host: Hostname
        user: Optional username
        timestamp: UTC timestamp
        
    Returns:
        Session ID string
    """
    # Pseudonymize identifiers
    p_host = pseudo_host(host)
    p_user = pseudo_user(user) if user else "nouser"
    
    # Get 5-minute time bucket
    bucket = timestamp.replace(
        minute=(timestamp.minute // 5) * 5,
        second=0,
        microsecond=0
    )
    
    return f"{p_host}_{p_user}_{bucket.strftime('%Y%m%d%H%M')}"

def sessionize(events: List[Dict],
              timestamp_key: str = 'timestamp',
              host_key: str = 'host',
              user_key: Optional[str] = 'user',
              window: int = 300) -> Tuple[List[Dict], Dict]:
    """Group events into sessions with privacy.
    
    Args:
        events: List of event dicts
        timestamp_key: Key for timestamp field
        host_key: Key for host field
        user_key: Optional key for user field
        window: Session timeout in seconds
        
    Returns:
        Tuple of (sessionized events, session stats)
    """
    if not events:
        return [], {}
        
    # Ensure UTC timestamps
    for event in events:
        if isinstance(event[timestamp_key], str):
            event[timestamp_key] = parse_rfc3339(event[timestamp_key])
            
    # Fix clock skew
    events = fix_clock_skew(events, timestamp_key)
    
    # Track sessions and stats
    sessions = {}
    stats = {'total_events': len(events), 'total_sessions': 0}
    
    for event in events:
        ts = event[timestamp_key]
        host = event[host_key]
        user = event.get(user_key) if user_key else None
        
        # Generate session ID
        sid = generate_session_id(host, user, ts)
        
        # Check if new session needed
        if sid not in sessions:
            sessions[sid] = {
                'first_event': ts,
                'last_event': ts,
                'event_count': 0
            }
            stats['total_sessions'] += 1
            
        session = sessions[sid]
        
        # Update session
        session['last_event'] = ts
        session['event_count'] += 1
        
        # Add session ID to event
        event['session_id'] = sid
        
    # Calculate session stats
    durations = []
    for s in sessions.values():
        duration = (s['last_event'] - s['first_event']).total_seconds()
        durations.append(duration)
        
    if durations:
        stats.update({
            'min_duration': min(durations),
            'max_duration': max(durations),
            'mean_duration': sum(durations) / len(durations),
            'total_duration': sum(durations)
        })
        
    return events, stats
