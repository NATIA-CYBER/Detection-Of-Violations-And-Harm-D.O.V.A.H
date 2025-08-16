"""Test event deduplication."""
import pytest
from datetime import datetime, timezone, timedelta
from src.ingest.dedup import generate_event_hash, dedup_events

def test_generate_event_hash():
    event = {
        'timestamp': datetime(2025, 8, 16, 10, 0, tzinfo=timezone.utc),
        'host': 'host1',
        'message': 'test',
        'level': 'INFO'
    }
    
    # Same content should get same hash
    h1 = generate_event_hash(event)
    h2 = generate_event_hash(event)
    assert h1 == h2
    
    # Different content should get different hash
    event2 = event.copy()
    event2['message'] = 'different'
    assert generate_event_hash(event2) != h1
    
    # Timestamp changes shouldn't affect hash
    event3 = event.copy()
    event3['timestamp'] = datetime(2025, 8, 16, 11, 0, tzinfo=timezone.utc)
    assert generate_event_hash(event3) == h1
    
    # Should respect content_keys
    h_partial = generate_event_hash(event, ['message', 'level'])
    event4 = event.copy()
    event4['host'] = 'different'
    assert generate_event_hash(event4, ['message', 'level']) == h_partial

def test_dedup_events():
    base_ts = datetime(2025, 8, 16, 10, 0, tzinfo=timezone.utc)
    events = [
        {
            'timestamp': base_ts,
            'host': 'host1',
            'message': 'test',
            'level': 'INFO'
        },
        {
            'timestamp': base_ts + timedelta(seconds=1),
            'host': 'host1',
            'message': 'test',  # Duplicate within window
            'level': 'INFO'
        },
        {
            'timestamp': base_ts + timedelta(seconds=301),
            'host': 'host1',
            'message': 'test',  # Not duplicate (outside window)
            'level': 'INFO'
        },
        {
            'timestamp': base_ts + timedelta(seconds=2),
            'host': 'host1',
            'message': 'different',  # Different content
            'level': 'INFO'
        }
    ]
    
    deduped, stats = dedup_events(events, window=300)
    
    assert len(deduped) == 3  # One duplicate removed
    assert stats['total'] == 4
    assert stats['duplicates'] == 1
    assert stats['unique'] == 3
    
    # Check dedup with content keys
    deduped2, stats2 = dedup_events(events, window=300, 
                                  content_keys=['host', 'level'])
    assert len(deduped2) == 1  # All events same host/level
    assert stats2['duplicates'] == 3

def test_dedup_empty():
    deduped, stats = dedup_events([])
    assert deduped == []
    assert stats['total'] == 0
    assert stats['duplicates'] == 0
