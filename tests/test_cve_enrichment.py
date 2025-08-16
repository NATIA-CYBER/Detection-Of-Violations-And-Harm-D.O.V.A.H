"""Test CVE enrichment functionality."""
import pytest
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.models import Base, EPSS, KEV, ComponentRisk
from src.enrich.cve_context import CVEEnricher, CVEContext

@pytest.fixture
def db_session():
    """Create test database session."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add test data
    now = datetime.now()
    
    # EPSS scores
    epss_data = [
        EPSS(cve='CVE-2025-1234', epss_score=0.8, ts=now),
        EPSS(cve='CVE-2025-1234', epss_score=0.7, ts=now - timedelta(days=1)),
        EPSS(cve='CVE-2025-5678', epss_score=0.2, ts=now)
    ]
    session.add_all(epss_data)
    
    # KEV data
    kev_data = [
        KEV(
            cve_id='CVE-2025-1234',
            date_added=now - timedelta(days=10),
            ts=now
        )
    ]
    session.add_all(kev_data)
    
    # Component risk data
    risk_data = [
        ComponentRisk(
            component='test-component',
            cve_count=5,
            high_risk_ratio=0.4,
            epss_trend=0.15,
            ts=now
        )
    ]
    session.add_all(risk_data)
    
    session.commit()
    return session

def test_cve_enrichment(db_session):
    enricher = CVEEnricher(db_session)
    
    # Test high-risk CVE
    context = enricher.enrich_cve(
        'CVE-2025-1234',
        'test-component',
        '1.0'
    )
    
    assert context.cve_id == 'CVE-2025-1234'
    assert context.epss_score == 0.8
    assert context.kev_status is True
    assert context.component == 'test-component'
    assert context.version == '1.0'
    assert context.patch_available is True
    assert context.days_since_publish == 10
    assert context.component_risk == 0.15
    assert 'cve_count' in context.rolling_stats
    
    # Test low-risk CVE
    context2 = enricher.enrich_cve(
        'CVE-2025-5678',
        'test-component'
    )
    
    assert context2.epss_score == 0.2
    assert context2.kev_status is False
    assert context2.patch_available is False
    
    # Test unknown CVE
    context3 = enricher.enrich_cve(
        'CVE-2025-9999',
        'unknown-component'
    )
    
    assert context3.epss_score == 0.0
    assert context3.kev_status is False
    assert context3.component_risk == 0.0
    assert context3.rolling_stats == {}

def test_epss_trends(db_session):
    enricher = CVEEnricher(db_session)
    
    # CVE-2025-1234 has two scores (0.8, 0.7)
    trends = enricher.epss_trends['CVE-2025-1234']
    
    assert '7d_mean' in trends
    assert '7d_std' in trends
    assert trends['7d_trend'] > 0  # Increasing trend
    
    # CVE-2025-5678 has one score
    trends2 = enricher.epss_trends['CVE-2025-5678']
    assert trends2['7d_trend'] == 0  # No trend with single point

def test_bulk_enrichment(db_session):
    enricher = CVEEnricher(db_session)
    
    cves = [
        {'cve_id': 'CVE-2025-1234', 'component': 'test-component'},
        {'cve_id': 'CVE-2025-5678', 'component': 'test-component'},
        {'cve_id': 'CVE-2025-9999', 'component': 'unknown-component'}
    ]
    
    contexts = enricher.enrich_multiple(cves)
    
    assert len(contexts) == 3
    assert all(isinstance(c, CVEContext) for c in contexts)
    assert contexts[0].epss_score > contexts[1].epss_score
