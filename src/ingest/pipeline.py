"""Main ingestion pipeline combining all components."""
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy.orm import Session

from .scrub import scrub, scrub_mapping
from .session import parse_rfc3339, fix_clock_skew, sessionize
from .dedup import dedup_events
from .template_cache import TemplateCache
from ..enrich.cve_context import CVEEnricher

class Pipeline:
    """Main log ingestion pipeline."""
    
    def __init__(self, 
                 db_session: Session,
                 cache_dir: Path,
                 window: int = 300):
        """Initialize pipeline components.
        
        Args:
            db_session: Database session for enrichment
            cache_dir: Directory for template cache
            window: Time window in seconds for dedup/sessions
        """
        self.db_session = db_session
        self.cache_dir = cache_dir
        self.window = window
        
        # Initialize components
        self.template_cache = TemplateCache(cache_dir)
        self.cve_enricher = CVEEnricher(db_session)
        
        # Track stats
        self.stats = {
            'events_processed': 0,
            'events_deduped': 0,
            'sessions_created': 0,
            'cves_enriched': 0,
            'pii_found': 0
        }
        
    def process_events(self, events: List[Dict]) -> List[Dict]:
        """Process events through full pipeline.
        
        Steps:
        1. Parse and validate timestamps
        2. Fix clock skew
        3. Deduplicate events
        4. Extract templates (with caching)
        5. Scrub PII
        6. Create sessions
        7. Enrich CVEs
        
        Args:
            events: List of event dicts
            
        Returns:
            Processed events with all enrichments
        """
        if not events:
            return []
            
        # 1. Parse timestamps
        for event in events:
            if isinstance(event.get('timestamp'), str):
                event['timestamp'] = parse_rfc3339(event['timestamp'])
                
        # 2. Fix clock skew
        events = fix_clock_skew(events)
        
        # 3. Deduplicate
        events, dedup_stats = dedup_events(events, self.window)
        self.stats['events_deduped'] += dedup_stats['duplicates']
        
        # 4. Extract templates
        for event in events:
            if 'message' in event:
                tid, pattern = self.template_cache.extract_template(
                    event['message']
                )
                event['template_id'] = tid
                event['template_pattern'] = pattern
                
        # 5. Scrub PII
        pii_stats = {'pre_scrub': {}, 'post_scrub': {}}
        for event in events:
            # Track PII matches
            orig_msg = event.get('message', '')
            orig_host = event.get('host', '')
            
            # Scrub message and host
            if 'message' in event:
                event['message'] = scrub(event['message'])
                if event['message'] != orig_msg:
                    pii_stats['pre_scrub']['message'] = \
                        pii_stats['pre_scrub'].get('message', 0) + 1
                    
            if 'host' in event:
                event['host'] = scrub(event['host'])
                if event['host'] != orig_host:
                    pii_stats['pre_scrub']['host'] = \
                        pii_stats['pre_scrub'].get('host', 0) + 1
                    
        self.stats['pii_found'] += sum(pii_stats['pre_scrub'].values())
        
        # 6. Create sessions
        events, session_stats = sessionize(
            events,
            timestamp_key='timestamp',
            host_key='host',
            user_key='user',
            window=self.window
        )
        self.stats['sessions_created'] += session_stats['total_sessions']
        
        # 7. Enrich CVEs
        cves = []
        for event in events:
            if 'cve_id' in event:
                cves.append({
                    'cve_id': event['cve_id'],
                    'component': event.get('component'),
                    'version': event.get('version')
                })
                
        if cves:
            enriched = self.cve_enricher.enrich_multiple(cves)
            cve_map = {c.cve_id: c for c in enriched}
            
            for event in events:
                if 'cve_id' in event:
                    context = cve_map[event['cve_id']]
                    event['epss_score'] = context.epss_score
                    event['kev_status'] = context.kev_status
                    event['component_risk'] = context.component_risk
                    event['patch_available'] = context.patch_available
                    
            self.stats['cves_enriched'] += len(cves)
            
        self.stats['events_processed'] += len(events)
        return events
        
    def get_stats(self) -> Dict:
        """Get pipeline processing stats."""
        return {
            **self.stats,
            'template_stats': self.template_cache.get_stats()
        }
