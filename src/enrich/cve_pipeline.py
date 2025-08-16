"""CVE extraction and enrichment pipeline."""
import pandas as pd
from typing import Dict, Optional

from .cve_join import extract_cves, enrich_with_epss, enrich_with_kev
from .cve_context import CVEEnricher
from .nvd_fetch import NVDEnricher

def process_cves(
    df: pd.DataFrame,
    epss_data: pd.DataFrame,
    kev_data: pd.DataFrame,
    text_col: str = "message",
    session = None
) -> Dict[str, pd.DataFrame]:
    """Extract and enrich CVEs from text data.
    
    Args:
        df: Input dataframe with text column
        epss_data: EPSS scores dataframe
        kev_data: KEV data dataframe
        text_col: Column containing text to extract CVEs from
        session: Optional SQLAlchemy session for component context
        
    Returns:
        Dict with dataframes:
            - cves: Extracted CVEs with context
            - enriched: CVEs enriched with EPSS/KEV scores
            - components: Component-level risk metrics
    """
    # Extract CVEs with context
    cves = extract_cves(df, text_col=text_col)
    if cves.empty:
        return {
            "cves": pd.DataFrame(),
            "enriched": pd.DataFrame(),
            "components": pd.DataFrame()
        }
    
    # Enrich with EPSS and KEV data
    enriched = enrich_with_epss(cves, epss_data)
    enriched = enrich_with_kev(enriched, kev_data)
    
    # Add component context if session provided
    components = pd.DataFrame()
    if session is not None:
        enricher = CVEEnricher(session)
        contexts = enricher.enrich_multiple([
            {"cve_id": cve, "component": row.get("component", "unknown")}
            for cve, row in enriched.iterrows()
        ])
        
        # Convert contexts to component metrics
        components = pd.DataFrame([{
            "component": c.component,
            "risk_score": c.component_risk,
            "cve_count": c.rolling_stats.get("cve_count", 0),
            "high_risk_ratio": c.rolling_stats.get("high_risk_ratio", 0.0),
            "epss_trend": c.rolling_stats.get("epss_trend", 0.0)
        } for c in contexts])
    
    return {
        "cves": cves,
        "enriched": enriched,
        "components": components
    }
