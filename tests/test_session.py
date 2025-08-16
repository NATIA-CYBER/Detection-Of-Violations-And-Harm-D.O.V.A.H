"""Test sessionization logic."""
import pytest
from datetime import datetime, timezone, timedelta
from src.ingest.session import (
    parse_rfc3339, detect_clock_skew, fix_clock_skew,
    generate_session_id, sessionize
)

def test_parse_rfc3339():
    # Valid timestamps
    assert parse_rfc3339("2025-08-16T10:20:30Z")
    assert parse_rfc3339("2025-08-16T10:20:30+00:00")
    assert parse_rfc3339("2025-08-16T10:20:30.123Z")
    
    # Invalid timestamps
    with pytest.raises(ValueError):
        parse_rfc3339("2025-08-16")  # Missing time
    with pytest.raises(ValueError):
        parse_rfc3339("2025-08-16T10:20:30")  # Missing timezone

def test_detect_clock_skew():
    ts = [
        datetime(2025, 8, 16, 10, 0, tzinfo=timezone.utc),
        datetime(2025, 8, 16, 9, 59, tzinfo=timezone.utc),  # Back 1 min
        datetime(2025, 8, 16, 10, 1, tzinfo=timezone.utc),
    ]
    
    stats = detect_clock_skew(ts)
    assert stats['backwards_events'] == 1
    assert stats['max_backwards_secs'] == 60
    assert stats['total_events'] == 3

def test_fix_clock_skew():
    events = [
        {'timestamp': datetime(2025, 8, 16, 10, 0, tzinfo=timezone.utc), 'id': 1},
        {'timestamp': datetime(2025, 8, 16, 9, 59, tzinfo=timezone.utc), 'id': 2},
        {'timestamp': datetime(2025, 8, 16, 10, 1, tzinfo=timezone.utc), 'id': 3},
    ]
    
    fixed = fix_clock_skew(events)
    
    # Should be sorted and strictly increasing
    assert [e['id'] for e in fixed] == [2, 1, 3]
    for i in range(len(fixed)-1):
        assert fixed[i]['timestamp'] < fixed[i+1]['timestamp']

def test_generate_session_id():
    ts = datetime(2025, 8, 16, 10, 2, 30, tzinfo=timezone.utc)
    
    # Should use 5-minute buckets
    sid = generate_session_id("host1", "user1", ts)
    assert sid.endswith("202508161000")  # 10:02:30 -> 10:00
    
    # Should handle missing user
    sid_no_user = generate_session_id("host1", None, ts)
    assert "nouser" in sid_no_user

def test_sessionize():
    events = [
        {
            'timestamp': datetime(2025, 8, 16, 10, 0, tzinfo=timezone.utc),
            'host': 'host1',
            'user': 'user1',
            'msg': 'msg1'
        },
        {
            'timestamp': datetime(2025, 8, 16, 10, 1, tzinfo=timezone.utc),
            'host': 'host1',
            'user': 'user1',
            'msg': 'msg2'
        },
        {
            'timestamp': datetime(2025, 8, 16, 10, 6, tzinfo=timezone.utc),
            'host': 'host1',
            'user': 'user1',
            'msg': 'msg3'  # New session (>5min gap)
        },
        {
            'timestamp': datetime(2025, 8, 16, 10, 7, tzinfo=timezone.utc),
            'host': 'host1',
            'user': 'user2',
            'msg': 'msg4'  # New session (different user)
        }
    ]
    
    sessionized, stats = sessionize(events)
    
    # Check session boundaries
    assert sessionized[0]['session_id'] == sessionized[1]['session_id']
    assert sessionized[1]['session_id'] != sessionized[2]['session_id']
    assert sessionized[2]['session_id'] != sessionized[3]['session_id']
    
    # Check stats
    assert stats['total_events'] == 4
    assert stats['total_sessions'] == 3  # 2 session boundaries crossed
    assert 'mean_duration' in stats
