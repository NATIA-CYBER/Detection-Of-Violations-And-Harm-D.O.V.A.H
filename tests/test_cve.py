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
            "Invalid CVE-203-123",  # Wrong format
            "CVE-2023-123456789",  # Too long
            "CVE-2023-123",  # Too short
            "Related to CVE-2023-12345 and CVE-2023-67890",  # Multiple per line
            "Discussing cve-2023-45678 vulnerability",  # Case insensitive
            "Found in CVE-2023-1234-extra and CVE2023-5678",  # Invalid formats
            "CVE-2023-1234\nCVE-2023-5678",  # Newlines
            "#CVE-2023-1234, [CVE-2023-5678]",  # Special chars
        ]
    })
    
    result = extract_cves(df)
    expected_cves = {
        "CVE-2023-1234", "CVE-2023-5678", "CVE-2023-9012",
        "CVE-2023-12345", "CVE-2023-67890", "CVE-2023-45678"
    }
    assert len(result) == len(expected_cves)
    assert set(result["cve"]) == expected_cves
    assert "context" in result.columns
    
    # Check context extraction
    for _, row in result.iterrows():
        # Context should contain CVE in any case
        assert row["cve"].upper() in row["context"].upper()
        assert len(row["context"]) <= 100  # Context window

def test_epss_enrichment():
    events = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678", "CVE-2023-9012"],
        "message": ["msg1", "msg2", "msg3"]
    })
    
    epss = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678", "CVE-2023-0000"],
        "epss": [0.8, 0.3, 0.1],
        "percentile": [95, 50, 10],
        "date": ["2023-08-01", "2023-08-01", "2023-08-01"]
    })
    
    result = enrich_with_epss(events, epss)
    
    # Check columns
    assert "epss_score" in result.columns
    assert "epss_percentile" in result.columns
    assert "is_high_risk" in result.columns
    assert "date" in result.columns
    
    # Check high risk flag
    assert result[result["cve"] == "CVE-2023-1234"]["is_high_risk"].iloc[0]
    assert not result[result["cve"] == "CVE-2023-5678"]["is_high_risk"].iloc[0]
    
    # Check missing scores
    missing = result[result["cve"] == "CVE-2023-9012"]
    assert missing["epss_score"].isna().iloc[0]
    assert missing["epss_percentile"].isna().iloc[0]
    assert not missing["is_high_risk"].iloc[0]

def test_kev_enrichment():
    events = pd.DataFrame({
        "cve": ["CVE-2023-1234", "CVE-2023-5678", "CVE-2023-9012"],
        "message": ["msg1", "msg2", "msg3"]
    })
    
    kev = pd.DataFrame({
        "cveID": ["CVE-2023-1234", "CVE-2023-5678"],
        "vendorProject": ["Apache", "Microsoft"],
        "product": ["Hadoop", "Exchange"],
        "shortDescription": ["desc1", "desc2"],
        "requiredAction": ["action1", "action2"],
        "dueDate": ["2023-09-01", "2023-09-02"],
        "knownRansomwareCampaignUse": [True, False]
    })
    
    result = enrich_with_kev(events, kev)
    
    # Check columns
    assert "in_kev" in result.columns
    assert "vendorProject" in result.columns
    assert "product" in result.columns
    assert "shortDescription" in result.columns
    assert "requiredAction" in result.columns
    assert "dueDate" in result.columns
    assert "knownRansomwareCampaignUse" in result.columns
    
    # Check KEV flags
    assert result[result["cve"] == "CVE-2023-1234"]["in_kev"].iloc[0]
    assert result[result["cve"] == "CVE-2023-5678"]["in_kev"].iloc[0]
    assert not result[result["cve"] == "CVE-2023-9012"]["in_kev"].iloc[0]
    
    # Check ransomware flags
    assert result[result["cve"] == "CVE-2023-1234"]["knownRansomwareCampaignUse"].iloc[0]
    assert not result[result["cve"] == "CVE-2023-5678"]["knownRansomwareCampaignUse"].iloc[0]
    
    # Check vendor info
    assert result[result["cve"] == "CVE-2023-1234"]["vendorProject"].iloc[0] == "Apache"
    assert result[result["cve"] == "CVE-2023-5678"]["product"].iloc[0] == "Exchange"
