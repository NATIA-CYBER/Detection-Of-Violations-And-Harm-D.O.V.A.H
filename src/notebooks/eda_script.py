"""DOVAH Data Exploration.

This notebook explores the EPSS and KEV datasets to validate data quality and analyze threat patterns.
"""

import pandas as pd
import json
from pathlib import Path

# Load EPSS data
epss_path = Path("../tests/data/epss/sample.csv")
epss_df = pd.read_csv(epss_path)
print("\nEPSS Dataset:")
print(f"Total CVEs: {len(epss_df)}")
print("\nSample EPSS scores:")
print(epss_df.sort_values("epss", ascending=False).head())

# Load KEV data
kev_path = Path("../tests/data/kev/sample.json")
with open(kev_path) as f:
    kev_data = json.load(f)
    
print("\nKEV Catalog:")
print(f"Version: {kev_data['catalogVersion']}")
print(f"Total vulnerabilities: {kev_data['count']}")

# Create KEV DataFrame
kev_df = pd.DataFrame(kev_data["vulnerabilities"])
print("\nKEV Vendors:")
print(kev_df["vendorProject"].value_counts())

# Cross-reference EPSS and KEV
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
