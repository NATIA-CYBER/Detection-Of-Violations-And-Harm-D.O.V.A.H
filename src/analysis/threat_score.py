"""Threat scoring combining CVE severity, EPSS scores and event frequency."""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class ThreatScore:
    """Threat score components."""
    total_score: float  # 0-100
    cve_subscore: float
    epss_subscore: float
    frequency_subscore: float
    asset_subscore: float
    temporal_decay: float

class ThreatScorer:
    def __init__(
        self,
        decay_halflife: timedelta = timedelta(days=7),
        frequency_window: timedelta = timedelta(hours=24),
        asset_criticality: Optional[Dict[str, float]] = None
    ):
        """Initialize threat scorer.
        
        Args:
            decay_halflife: Time for threat to decay by half
            frequency_window: Window for frequency calculation
            asset_criticality: Dict mapping hosts to criticality scores (0-1)
        """
        self.decay_halflife = decay_halflife
        self.frequency_window = frequency_window
        self.asset_criticality = asset_criticality or {}
        self.event_history: List[datetime] = []
        
    def _calculate_cve_subscore(
        self,
        cvss_score: Optional[float],
        is_actively_exploited: bool
    ) -> float:
        """Calculate CVE severity subscore."""
        if cvss_score is None:
            return 0.0
            
        # Boost score if actively exploited
        exploit_multiplier = 1.5 if is_actively_exploited else 1.0
        return min(100, cvss_score * 10 * exploit_multiplier)
        
    def _calculate_epss_subscore(self, epss_score: Optional[float]) -> float:
        """Calculate EPSS probability subscore."""
        if epss_score is None:
            return 0.0
            
        # Convert probability to 0-100 score with exponential scaling
        # to emphasize high probabilities
        return 100 * (1 - np.exp(-5 * epss_score))
        
    def _calculate_frequency_subscore(
        self,
        current_time: datetime,
        event_times: List[datetime]
    ) -> float:
        """Calculate frequency-based subscore."""
        # Count events in window
        window_start = current_time - self.frequency_window
        window_events = [t for t in event_times if t >= window_start]
        count = len(window_events)
        
        # Convert to 0-100 score with diminishing returns
        return 100 * (1 - np.exp(-0.1 * count))
        
    def _calculate_asset_subscore(self, host: str) -> float:
        """Calculate asset criticality subscore."""
        criticality = self.asset_criticality.get(host, 0.5)  # Default medium
        return criticality * 100
        
    def _calculate_temporal_decay(
        self,
        current_time: datetime,
        event_time: datetime
    ) -> float:
        """Calculate temporal decay factor."""
        age = (current_time - event_time).total_seconds()
        halflife_seconds = self.decay_halflife.total_seconds()
        return np.exp(-np.log(2) * age / halflife_seconds)
        
    def calculate_threat_score(
        self,
        cvss_score: Optional[float],
        epss_score: Optional[float],
        is_actively_exploited: bool,
        host: str,
        event_time: datetime,
        current_time: Optional[datetime] = None
    ) -> ThreatScore:
        """Calculate overall threat score.
        
        Args:
            cvss_score: CVSS base score (0-10) if available
            epss_score: EPSS probability (0-1) if available
            is_actively_exploited: Whether vulnerability is being exploited
            host: Hostname for asset criticality
            event_time: Time of current event
            current_time: Current time (defaults to now)
            
        Returns:
            ThreatScore with components
        """
        current_time = current_time or datetime.now()
        
        # Calculate subscores
        cve_subscore = self._calculate_cve_subscore(
            cvss_score,
            is_actively_exploited
        )
        epss_subscore = self._calculate_epss_subscore(epss_score)
        frequency_subscore = self._calculate_frequency_subscore(
            current_time,
            self.event_history + [event_time]
        )
        asset_subscore = self._calculate_asset_subscore(host)
        
        # Calculate temporal decay
        decay = self._calculate_temporal_decay(current_time, event_time)
        
        # Weighted combination of subscores
        weights = {
            "cve": 0.3,
            "epss": 0.2,
            "frequency": 0.2,
            "asset": 0.3
        }
        
        total_score = (
            weights["cve"] * cve_subscore +
            weights["epss"] * epss_subscore +
            weights["frequency"] * frequency_subscore +
            weights["asset"] * asset_subscore
        ) * decay
        
        # Update history
        self.event_history.append(event_time)
        
        # Prune old events
        cutoff = current_time - self.frequency_window
        self.event_history = [
            t for t in self.event_history if t >= cutoff
        ]
        
        return ThreatScore(
            total_score=total_score,
            cve_subscore=cve_subscore,
            epss_subscore=epss_subscore,
            frequency_subscore=frequency_subscore,
            asset_subscore=asset_subscore,
            temporal_decay=decay
        )
