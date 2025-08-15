# Log Sessionization

## Overview
DOVAH uses a time-based sessionization approach to group related log events into coherent activity sequences. Sessions are defined by the tuple (host, user, time_bucket) to capture related activities while maintaining privacy.

## Session ID Format
```
{pseudonymized_host}_{pseudonymized_user}_{YYYYMMDDHHMM}
```
Example: `h123abc_user1_202508011200`

## Time Bucket Calculation
- Base unit: 5-minute buckets
- Bucket start times aligned to 5-minute boundaries (00:00, 00:05, etc.)
- UTC timestamps used throughout to avoid timezone issues
- Clock skew correction applied before bucket assignment

## Grouping Logic
1. Events are first grouped by host identifier
2. Within each host group, events are grouped by user (if available)
3. Time buckets are calculated for each event's UTC timestamp
4. Events within the same (host, user, bucket) get the same session ID

## Session Boundaries
- New session starts when:
  - Time gap > 5 minutes between events
  - User changes
  - Host changes
- Session ends when:
  - No events for 5 minutes
  - Different user seen
  - Different host seen

## Implementation
```python
def generate_session_id(host: str, user: str, timestamp: datetime) -> str:
    """Generate session ID from components."""
    bucket = timestamp.replace(
        minute=(timestamp.minute // 5) * 5,
        second=0,
        microsecond=0
    )
    return f"{host}_{user}_{bucket.strftime('%Y%m%d%H%M')}"
```

## Privacy Considerations
- Host IDs are pseudonymized using HMAC-SHA256
- User IDs are pseudonymized using HMAC-SHA256
- Time buckets provide temporal grouping while reducing precision
- No PII is included in session IDs
