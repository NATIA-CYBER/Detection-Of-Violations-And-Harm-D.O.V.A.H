"""Test full ingestion pipeline."""
import pytest
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.models import Base, EPSS, KEV, ComponentRisk
from src.ingest.pipeline import Pipeline

@pytest.fixture
def db_session():
    """Create test database session."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add test data
    now = datetime.now(timezone.utc)
    
    epss_data = [
        EPSS(cve='CVE-2025-1234', epss_score=0.8, ts=now)
    ]
    session.add_all(epss_data)
    
    kev_data = [
        KEV(cve_id='CVE-2025-1234', date_added=now, ts=now)
    ]
    session.add_all(kev_data)
    
    risk_data = [
        ComponentRisk(
            component='test-app',
            cve_count=5,
            high_risk_ratio=0.4,
            epss_trend=0.15,
            ts=now
        )
    ]
    session.add_all(risk_data)
    
    session.commit()
    return session

def test_pipeline_processing(db_session, tmp_path):
    pipeline = Pipeline(db_session, tmp_path)
    
    # Test events with various features
    events = [
        {
            # Event with PII and CVE
            'timestamp': '2025-08-16T10:00:00Z',
            'host': 'host1',
            'user': 'alice@company.com',
            'message': 'Found CVE-2025-1234 in test-app v1.0',
            'cve_id': 'CVE-2025-1234',
            'component': 'test-app',
            'version': '1.0'
        },
        {
            # Duplicate event
            'timestamp': '2025-08-16T10:00:01Z',
            'host': 'host1',
            'user': 'alice@company.com',
            'message': 'Found CVE-2025-1234 in test-app v1.0',
            'cve_id': 'CVE-2025-1234',
            'component': 'test-app',
            'version': '1.0'
        },
        {
            # Event with clock skew
            'timestamp': '2025-08-16T09:59:00Z',
            'host': 'host1',
            'user': 'bob@company.com',
            'message': 'User logged in from 10.0.0.1'
        },
        {
            # Event in new session
            'timestamp': '2025-08-16T10:06:00Z',
            'host': 'host2',
            'user': 'alice@company.com',
            'message': 'Application startup'
        }
    ]
    
    processed = pipeline.process_events(events)
    stats = pipeline.get_stats()
    
    # Check deduplication
    assert len(processed) == 3  # One duplicate removed
    assert stats['events_deduped'] == 1
    
    # Check PII scrubbing
    assert all('company.com' not in event['message'] 
              for event in processed)
    assert all('10.0.0.1' not in event['message'] 
              for event in processed)
    assert stats['pii_found'] > 0
    
    # Check CVE enrichment
    cve_event = next(e for e in processed 
                    if 'cve_id' in e)
    assert cve_event['epss_score'] == 0.8
    assert cve_event['kev_status'] is True
    assert cve_event['component_risk'] == 0.15
    assert stats['cves_enriched'] == 1
    
    # Check sessionization
    assert len(set(e['session_id'] for e in processed)) == 2
    assert stats['sessions_created'] == 2
    
    # Check template caching
    assert all('template_id' in e for e in processed)
    assert 'template_stats' in stats
    
    # Verify clock skew fix
    timestamps = [e['timestamp'] for e in processed]
    assert all(timestamps[i] < timestamps[i+1] 
              for i in range(len(timestamps)-1))

def test_empty_input(db_session, tmp_path):
    pipeline = Pipeline(db_session, tmp_path)
    
    assert pipeline.process_events([]) == []
    assert pipeline.get_stats()['events_processed'] == 0

def test_invalid_timestamp(db_session, tmp_path):
    pipeline = Pipeline(db_session, tmp_path)
    
    with pytest.raises(ValueError):
        pipeline.process_events([
            {'timestamp': 'invalid'}
        ])
