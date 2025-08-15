"""CVE extraction and enrichment with EPSS/KEV data."""
import re
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

# Enhanced CVE regex to catch edge cases
CVE_PATTERN = re.compile(
    r"\b(?:CVE-\d{4}-(?:\d{4,}|\d{4,7})|cve-\d{4}-\d{4,7})\b",
    re.IGNORECASE
)

def extract_cves(df: pd.DataFrame, text_col: str = "message") -> pd.DataFrame:
    """Extract CVEs from text column with context.
    
    Args:
        df: Input DataFrame
        text_col: Column containing text to extract CVEs from
        
    Returns:
        DataFrame with CVEs and context
    """
    if df.empty:
        return pd.DataFrame(columns=["cve", "context"])
    def _extract_row(row: pd.Series) -> List[Dict]:
        text = str(row.get(text_col, ""))
        cves = CVE_PATTERN.findall(text)
        if not cves:
            return []
            
        # Normalize CVE format
        cves = [cve.upper() for cve in cves]
        
        # Get surrounding context
        contexts = []
        for cve in cves:
            idx = text.upper().find(cve)
            start = max(0, idx - 50)
            end = min(len(text), idx + len(cve) + 50)
            context = text[start:end].strip()
            contexts.append({
                "cve": cve,
                "context": context,
                **{k:v for k,v in row.items() if k != text_col}
            })
        return contexts
    
    # Extract CVEs with context
    cves = df.apply(_extract_row, axis=1)
    cves = [item for sublist in cves for item in sublist]
    
    if not cves:
        return pd.DataFrame(columns=["cve", "context"])
        
    result = pd.DataFrame(cves)
    return result.drop_duplicates(subset=["cve"])

def enrich_with_epss(
    events: pd.DataFrame,
    epss: pd.DataFrame,
    cve_col: str = "cve"
) -> pd.DataFrame:
    """Enrich events with EPSS scores.
    
    Args:
        events: DataFrame with CVE events
        epss: EPSS scores DataFrame
        cve_col: Column containing CVE IDs
        
    Returns:
        Enriched DataFrame with EPSS scores
    """
    # Normalize column names
    epss = epss.rename(columns={
        "cve": cve_col,
        "epss": "epss_score",
        "percentile": "epss_percentile"
    })
    
    # Join and handle missing scores
    result = events.merge(
        epss[[cve_col, "epss_score", "epss_percentile", "date"]],
        on=cve_col,
        how="left",
        suffixes=("", "_epss")
    )
    
    # Flag high-risk CVEs (top 10% EPSS)
    result["is_high_risk"] = result["epss_percentile"] >= 90
    
    return result

def enrich_with_kev(
    events: pd.DataFrame,
    kev: pd.DataFrame,
    cve_col: str = "cve"
) -> pd.DataFrame:
    """Enrich events with KEV data.
    
    Args:
        events: DataFrame with CVE events
        kev: KEV catalog DataFrame
        cve_col: Column containing CVE IDs
        
    Returns:
        Enriched DataFrame with KEV data
    """
    # Normalize column names
    kev = kev.rename(columns={"cveID": cve_col})
    
    # Select relevant KEV columns
    kev_cols = [
        cve_col, "vendorProject", "product",
        "shortDescription", "requiredAction",
        "dueDate", "knownRansomwareCampaignUse"
    ]
    kev_cols = [c for c in kev_cols if c in kev.columns]
    
    # Join and add KEV flag
    result = events.merge(
        kev[kev_cols],
        on=cve_col,
        how="left",
        suffixes=("", "_kev")
    )
    result["in_kev"] = result["vendorProject"].notna()
    
    return result

def calculate_risk_metrics(df: pd.DataFrame) -> Dict[str, float]:
    """Calculate risk metrics from enriched data.
    
    Args:
        df: Enriched DataFrame with EPSS and KEV data
        
    Returns:
        Dict of risk metrics
    """
    metrics = {
        "total_cves": len(df),
        "high_risk_pct": 100 * df["is_high_risk"].mean(),
        "kev_pct": 100 * df["in_kev"].mean(),
        "mean_epss": df["epss_score"].mean(),
        "p95_epss": df["epss_score"].quantile(0.95)
    }
    return {k: round(float(v), 2) for k,v in metrics.items()}
