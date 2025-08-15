"""DOVAH Data Exploration.

This script demonstrates data exploration and feature analysis for the DOVAH security ML pipeline,
focusing on EPSS and KEV data analysis.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
from pathlib import Path

def explore_epss():
    """Explore EPSS dataset."""
    epss_path = Path("../tests/data/epss/sample.csv")
    epss_df = pd.read_csv(epss_path)
    print("\nEPSS Dataset:")
    print(f"Total CVEs: {len(epss_df)}")
    print("\nSample EPSS scores:")
    print(epss_df.sort_values("epss", ascending=False).head())
    return epss_df

def explore_kev():
    """Explore KEV dataset."""
    kev_path = Path("../tests/data/kev/sample.json")
    with open(kev_path) as f:
        kev_data = json.load(f)
    
    print("\nKEV Catalog:")
    print(f"Version: {kev_data['catalogVersion']}")
    print(f"Total vulnerabilities: {kev_data['count']}")
    
    kev_df = pd.DataFrame(kev_data["vulnerabilities"])
    print("\nKEV Vendors:")
    print(kev_df["vendorProject"].value_counts())
    return kev_df

def cross_reference(epss_df, kev_df):
    """Cross-reference EPSS and KEV data."""
    kev_cves = set(kev_df["cveID"])
    epss_cves = set(epss_df["cve"])
    common_cves = kev_cves.intersection(epss_cves)

    print("\nCross-reference Analysis:")
    print(f"CVEs in KEV: {len(kev_cves)}")
    print(f"CVEs in EPSS: {len(epss_cves)}")
    print(f"Common CVEs: {len(common_cves)}")

    # Show high-risk vulnerabilities
    high_risk = epss_df[epss_df["cve"].isin(common_cves)].sort_values("epss", ascending=False)
    print("\nHigh-risk vulnerabilities (in both EPSS and KEV):")
    print(high_risk)

def main():
    """Run data exploration."""
    epss_df = explore_epss()
    kev_df = explore_kev()
    cross_reference(epss_df, kev_df)

if __name__ == "__main__":
    main()
