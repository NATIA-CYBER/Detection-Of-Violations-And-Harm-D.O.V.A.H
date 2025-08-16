"""CVE Context Enrichment.

Enriches CVE data with:
- EPSS scores and trends
- KEV status and details
- Component-specific risk scoring
- Patch availability tracking
"""
import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import pandas as pd
import numpy as np
from sqlalchemy import select, and_

from src.db import models

@dataclass
class CVEContext:
    """Enriched CVE context."""
    cve_id: str
    epss_score: float
    kev_status: bool
    component: str
    version: Optional[str]
    patch_available: bool
    days_since_publish: int
    component_risk: float
    rolling_stats: Dict[str, float]

class CVEEnricher:
    """Enriches CVE data with EPSS, KEV, and component context."""
    
    def __init__(self, session):
        """Initialize with database session."""
        self.session = session
        self._load_reference_data()
        
    def _load_reference_data(self):
        """Load EPSS and KEV data from database."""
        # Load latest EPSS scores
        epss_query = select(models.EPSS).order_by(models.EPSS.ts.desc())
        self.epss_df = pd.read_sql(epss_query, self.session.bind)
        
        # Load KEV data
        kev_query = select(models.KEV).order_by(models.KEV.ts.desc())
        self.kev_df = pd.read_sql(kev_query, self.session.bind)
        
        # Load component risk data
        risk_query = select(models.ComponentRisk).order_by(models.ComponentRisk.ts.desc())
        self.risk_df = pd.read_sql(risk_query, self.session.bind)
        
        # Calculate EPSS trends (7/30/90 day windows)
        self._calculate_epss_trends()
        
    def _calculate_epss_trends(self):
        """Calculate EPSS score trends."""
        self.epss_trends = {}
        windows = [7, 30, 90]
        
        for cve in self.epss_df['cve'].unique():
            cve_scores = self.epss_df[self.epss_df['cve'] == cve]
            trends = {}
            
            for window in windows:
                recent = cve_scores.head(window)
                if not recent.empty:
                    trends[f'{window}d_mean'] = recent['epss_score'].mean()
                    trends[f'{window}d_std'] = recent['epss_score'].std()
                    trends[f'{window}d_trend'] = np.polyfit(
                        range(len(recent)), recent['epss_score'], 1
                    )[0] if len(recent) > 1 else 0
            
            self.epss_trends[cve] = trends
    
    def enrich_cve(self, cve_id: str, component: str, 
                   version: Optional[str] = None) -> CVEContext:
        """Enrich a CVE with full context."""
        # Get EPSS score and trends
        epss_score = float(self.epss_df[
            self.epss_df['cve'] == cve_id
        ]['epss_score'].iloc[0]) if cve_id in self.epss_df['cve'].values else 0.0
        
        epss_trends = self.epss_trends.get(cve_id, {})
        
        # Check KEV status
        kev_status = bool(cve_id in self.kev_df['cve_id'].values)
        
        # Get patch status and age
        kev_row = self.kev_df[self.kev_df['cve_id'] == cve_id]
        if not kev_row.empty:
            patch_available = True
            published_date = pd.to_datetime(kev_row['date_added'].iloc[0])
            days_since_publish = (
                datetime.datetime.now() - published_date
            ).days
        else:
            patch_available = False
            days_since_publish = 0
        
        # Get component risk metrics
        component_risk = 0.0
        rolling_stats = {}
        
        if component:
            risk_rows = self.risk_df[self.risk_df['component'] == component]
            if not risk_rows.empty:
                latest = risk_rows.iloc[0]
                component_risk = latest['epss_trend']
                rolling_stats = {
                    'cve_count': latest['cve_count'],
                    'high_risk_ratio': latest['high_risk_ratio'],
                    'epss_trend': latest['epss_trend']
                }
        
        return CVEContext(
            cve_id=cve_id,
            epss_score=epss_score,
            kev_status=kev_status,
            component=component,
            version=version,
            patch_available=patch_available,
            days_since_publish=days_since_publish,
            component_risk=component_risk,
            rolling_stats=rolling_stats
        )
    
    def enrich_multiple(self, cves: List[Dict]) -> List[CVEContext]:
        """Enrich multiple CVEs with context."""
        return [
            self.enrich_cve(
                cve['cve_id'],
                cve.get('component', 'Default'),
                cve.get('version')
            )
            for cve in cves
        ]
