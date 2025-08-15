"""Tests for CVE extraction and enrichment."""
import pandas as pd
import numpy as np
from src.enrich.cve_join import extract_cves, enrich_with_epss, enrich_with_kev

def test_cve_extraction():
    df = pd.DataFrame({
        "message": [
            "Found CVE-2023-1234 in logs",
            "Multiple CVE-2023-5678 and cve-2023-9012",
            "No CVE here",
            "Invalid CVE-203-123"  # Wrong format
        ]
    })
    
    result = extract_cves(df)
    assert len(result) == 3  # Should find 3 valid CVEs
    assert set(result["cve"]) == {"CVE-2023-1234", "CVE-2023-5678", "CVE-2023-9012"}
    assert "context" in result.columns

def test_epss_enrichment():
    events = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678"],
        "message": ["msg1", "msg2"]
    })
    
    epss = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678"],
        "epss": [0.8, 0.3],
        "percentile": [95, 50],
        "date": ["2023-08-01", "2023-08-01"]
    })
    
    result = enrich_with_epss(events, epss)
    assert "epss_score" in result.columns
    assert "is_high_risk" in result.columns
    assert result[result["cve"] == "CVE-2023-1234"]["is_high_risk"].iloc[0]

def test_kev_enrichment():
    events = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678"],
        "message": ["msg1", "msg2"]
    })
    
    kev = pd.DataFrame({
        "cveID": ["CVE-2023-1234"],
        "vendorProject": ["Apache"],
        "product": ["Hadoop"],
        "dueDate": ["2023-09-01"]
    })
    
    result = enrich_with_kev(events, kev)
    assert "in_kev" in result.columns
    assert result[result["cve"] == "CVE-2023-1234"]["in_kev"].iloc[0]
    assert not result[result["cve"] == "CVE-2023-5678"]["in_kev"].iloc[0]
