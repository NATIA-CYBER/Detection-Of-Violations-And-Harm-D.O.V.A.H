"""CVE extraction and enrichment with EPSS/KEV data."""
import re
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

# Enhanced CVE regex to catch edge cases
CVE_PATTERN = re.compile(
    r"\b(?:CVE-\d{4}-\d{4,7})\b",  # Only match CVE-YYYY-NNNNNNN format (4-7 digits)
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
        # First find all matches case-insensitively
        matches = list(CVE_PATTERN.finditer(text.upper()))
        if not matches:
            return []
        
        # Extract CVEs with context
        contexts = []
        for match in matches:
            cve = match.group(0)
            # Find the original case in the text
            start_idx = match.start()
            end_idx = match.end()
            original_cve = text[start_idx:end_idx]
            
            # Extract context window
            start = max(0, start_idx - 50)
            end = min(len(text), end_idx + 50)
            context = text[start:end].strip()
            
            contexts.append({
                "cve": cve,  # Already uppercase from searching uppercase text
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
        epss: DataFrame with EPSS scores
        cve_col: Column containing CVE IDs
        
    Returns:
        Enriched DataFrame with EPSS scores and risk metrics
    """
    if events.empty:
        return pd.DataFrame()
        
    # Merge EPSS scores
    result = events.merge(
        epss[[cve_col, "epss_score", "percentile"]],
        on=cve_col,
        how="left"
    )
    
    # Add risk indicators
    result["is_high_risk"] = (
        result["percentile"].fillna(0) >= 95
    )
    
    # Keep missing scores as NaN
    result["epss_percentile"] = result["percentile"]
    
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
    if events.empty:
        return pd.DataFrame()
        
    # Handle empty KEV data
    if kev.empty:
        result = events.copy()
        result["in_kev"] = False
        return result
        
    # Normalize column names
    kev = kev.rename(columns={"cveID": "cve_id"})
    
    # Join and add KEV flag
    result = events.merge(
        kev,
        left_on=cve_col,
        right_on="cve_id",
        how="left"
    )
    result["in_kev"] = result["cve_id"].notna()
    
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
