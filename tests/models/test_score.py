"""Tests for anomaly scoring module."""
import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from src.models.score import SessionScorer

@pytest.fixture
def sample_db(tmp_path):
    """Create test database with required tables and sample data."""
    engine = create_engine("sqlite:///:memory:")
    
    # Create tables
    with engine.connect() as conn:
        # Window features table
        conn.execute(text("""
            CREATE TABLE window_features (
                id INTEGER PRIMARY KEY,
                ts TIMESTAMP,
                event_count INTEGER,
                unique_components INTEGER,
                error_ratio FLOAT,
                template_entropy FLOAT,
                component_entropy FLOAT
            )
        """))
        
        # Detections table
        conn.execute(text("""
            CREATE TABLE detections (
                id TEXT PRIMARY KEY,
                ts TIMESTAMP,
                session_id TEXT,
                window_id INTEGER,
                score FLOAT,
                source TEXT,
                model_version TEXT,
                FOREIGN KEY(window_id) REFERENCES window_features(id)
            )
        """))
        
        # Insert sample data
        base_time = datetime.now(timezone.utc)
        
        # Windows
        windows = [
            (1, base_time, 100, 10, 0.05, 2.5, 1.8),
            (2, base_time + timedelta(minutes=1), 150, 12, 0.08, 2.8, 2.0),
            (3, base_time + timedelta(minutes=2), 80, 8, 0.03, 2.2, 1.5)
        ]
        
        conn.execute(text("""
            INSERT INTO window_features (id, ts, event_count, unique_components, 
                                      error_ratio, template_entropy, component_entropy)
            VALUES (:id, :ts, :event_count, :unique_components,
                   :error_ratio, :template_entropy, :component_entropy)
        """), [
            {
                "id": w[0],
                "ts": w[1],
                "event_count": w[2],
                "unique_components": w[3],
                "error_ratio": w[4],
                "template_entropy": w[5],
                "component_entropy": w[6]
            }
            for w in windows
        ])
        
        # Detections
        detections = [
            ("d1", base_time, "session1", 1, 0.8, "iforest", "v1"),
            ("d2", base_time + timedelta(minutes=1), "session1", 2, 0.9, "iforest", "v1"),
            ("d3", base_time + timedelta(minutes=2), "session1", 3, 0.7, "iforest", "v1"),
            ("d4", base_time, "session2", 1, 0.6, "iforest", "v1")
        ]
        
        conn.execute(text("""
            INSERT INTO detections (id, ts, session_id, window_id, score, source, model_version)
            VALUES (:id, :ts, :session_id, :window_id, :score, :source, :model_version)
        """), [
            {
                "id": d[0],
                "ts": d[1],
                "session_id": d[2],
                "window_id": d[3],
                "score": d[4],
                "source": d[5],
                "model_version": d[6]
            }
            for d in detections
        ])
        
    return engine

def test_session_scoring(sample_db):
    """Test session-level score aggregation."""
    scorer = SessionScorer(sample_db)
    
    # Get active sessions
    sessions = scorer.get_active_sessions(
        start_time=datetime.now(timezone.utc) - timedelta(hours=1),
        min_events=50
    )
    
    assert len(sessions) > 0
    session = sessions[0]
    assert "session_id" in session
    assert "avg_score" in session
    assert "total_events" in session
    assert session["total_events"] >= 50

def test_session_details(sample_db):
    """Test detailed session statistics."""
    scorer = SessionScorer(sample_db)
    
    # Get details for session1
    details = scorer.get_session_details("session1")
    
    assert details is not None
    assert details["total_events"] == 330  # Sum of all events
    assert 0.7 <= details["avg_score"] <= 0.9  # Between min and max scores
    assert details["avg_unique_components"] > 0
    assert details["avg_error_ratio"] > 0
    assert details["avg_template_entropy"] > 0

def test_error_handling(sample_db):
    """Test error handling in scoring."""
    scorer = SessionScorer(sample_db)
    
    # Test with non-existent session
    details = scorer.get_session_details("nonexistent-session")
    assert details is None
    
    # Test with invalid time range
    sessions = scorer.get_active_sessions(
        start_time=datetime.now(timezone.utc) + timedelta(days=1),  # Future time
        min_events=50
    )
    assert len(sessions) == 0
