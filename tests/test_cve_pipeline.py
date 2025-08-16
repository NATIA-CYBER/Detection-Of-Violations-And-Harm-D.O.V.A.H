"""Tests for CVE extraction and enrichment pipeline."""
import pandas as pd
import numpy as np
from datetime import datetime

from src.db import get_session, models
from src.enrich.cve_pipeline import process_cves

def test_cve_pipeline():
    # Test data
    df = pd.DataFrame({
        "message": [
            "Found CVE-2023-1234 in nginx",
            "Multiple issues: CVE-2023-5678, CVE-2023-9012",
            "No CVEs here"
        ],
        "component": ["nginx", "apache", "other"]
    })
    
    epss_data = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678"],
        "epss_score": [0.8, 0.2],
        "percentile": [95, 45]
    })
    
    kev_data = pd.DataFrame({
        "cve_id": ["CVE-2023-1234"],
        "required_action": ["Update nginx"],
        "due_date": ["2023-12-31"]
    })
    
    # Process CVEs
    results = process_cves(df, epss_data, kev_data)
    
    # Check extracted CVEs
    assert len(results["cves"]) == 3
    assert "CVE-2023-1234" in results["cves"]["cve"].values
    assert "nginx" in results["cves"].iloc[0]["context"]
    
    # Check enriched data
    enriched = results["enriched"]
    assert len(enriched) == 3
    
    # Check EPSS enrichment
    assert enriched[enriched["cve"] == "CVE-2023-1234"]["epss_score"].iloc[0] == 0.8
    assert enriched[enriched["cve"] == "CVE-2023-5678"]["epss_score"].iloc[0] == 0.2
    assert np.isnan(enriched[enriched["cve"] == "CVE-2023-9012"]["epss_score"].iloc[0])
    
    # Check KEV enrichment
    assert enriched[enriched["cve"] == "CVE-2023-1234"]["in_kev"].iloc[0]
    assert not enriched[enriched["cve"] == "CVE-2023-5678"]["in_kev"].iloc[0]
    
    # Test empty input
    empty_results = process_cves(
        pd.DataFrame({"message": []}),
        epss_data,
        kev_data
    )
    assert empty_results["cves"].empty
    assert empty_results["enriched"].empty
    assert empty_results["components"].empty

def test_component_enrichment():
    # Create test session with sample data
    session = get_session()
    
    # Add test EPSS data
    session.add(models.EPSS(
        cve="CVE-2023-1234",
        epss_score=0.8,
        percentile=95,
        ts=datetime.now()
    ))
    
    # Add test KEV data
    session.add(models.KEV(
        cve_id="CVE-2023-1234",
        required_action="Update nginx",
        due_date=datetime(2023, 12, 31),
        ts=datetime.now()
    ))
    
    # Add historical component risk
    session.add(models.ComponentRisk(
        component="nginx",
        cve_count=5,
        high_risk_ratio=0.4,
        epss_trend=0.1,
        ts=datetime.now()
    ))
    session.commit()
    
    df = pd.DataFrame({
        "message": ["Found CVE-2023-1234 in nginx"],
        "component": ["nginx"]
    })
    
    epss_data = pd.DataFrame({
        "cve": ["CVE-2023-1234"],
        "epss_score": [0.8],
        "percentile": [95]
    })
    
    kev_data = pd.DataFrame({
        "cve_id": ["CVE-2023-1234"],
        "required_action": ["Update nginx"],
        "due_date": ["2023-12-31"]
    })
    
    results = process_cves(df, epss_data, kev_data, session=session)
    
    # Check component enrichment
    components = results["components"]
    assert len(components) == 1
    nginx = components.iloc[0]
    assert nginx["component"] == "nginx"
    assert nginx["cve_count"] == 5
    assert nginx["high_risk_ratio"] == 0.4
    assert nginx["epss_trend"] == 0.1
