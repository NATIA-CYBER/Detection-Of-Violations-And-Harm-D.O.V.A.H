"""NVD API Integration for additional vulnerability context."""

import os
import time
import logging
from typing import Dict, List, Optional
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class NVDEnricher:
    """Fetches additional vulnerability data from NVD API."""
    
    BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"
    
    def __init__(self):
        """Initialize with API key if available."""
        self.api_key = os.getenv('NVD_API_KEY')
        self.headers = {
            'apiKey': self.api_key
        } if self.api_key else {}
        self.rate_limit = timedelta(seconds=6 if not self.api_key else 0.6)
        self.last_request = datetime.min
        
    def _rate_limit(self):
        """Enforce rate limiting."""
        now = datetime.now()
        if (now - self.last_request) < self.rate_limit:
            time.sleep(self.rate_limit.total_seconds())
        self.last_request = now
        
    def get_cve_details(self, cve_id: str) -> Optional[Dict]:
        """Get detailed CVE information from NVD.
        
        Args:
            cve_id: CVE ID to lookup
            
        Returns:
            Dict with:
                - description
                - cvss_v3_score
                - attack_vector
                - attack_complexity
                - affected_versions
                - references
                - patch_urls
        """
        self._rate_limit()
        
        try:
            response = requests.get(
                f"{self.BASE_URL}",
                params={'cveId': cve_id},
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            if not data.get('vulnerabilities'):
                return None
                
            vuln = data['vulnerabilities'][0]['cve']
            metrics = vuln.get('metrics', {}).get('cvssMetricV31', [{}])[0]
            
            return {
                'description': vuln.get('descriptions', [{}])[0].get('value', ''),
                'cvss_v3_score': metrics.get('cvssData', {}).get('baseScore'),
                'attack_vector': metrics.get('cvssData', {}).get('attackVector'),
                'attack_complexity': metrics.get('cvssData', {}).get('attackComplexity'),
                'affected_versions': [
                    v.get('version') for v in 
                    vuln.get('configurations', [{}])[0].get('nodes', [{}])[0].get('cpeMatch', [])
                ],
                'references': [
                    ref.get('url') for ref in vuln.get('references', [])
                ],
                'patch_urls': [
                    ref.get('url') for ref in vuln.get('references', [])
                    if any(x in ref.get('url', '').lower() 
                          for x in ['patch', 'fix', 'update', 'advisory'])
                ]
            }
            
        except Exception as e:
            logger.error(f"Error fetching NVD data for {cve_id}: {e}")
            return None
            
    def enrich_multiple(self, cves: List[str]) -> Dict[str, Dict]:
        """Get NVD details for multiple CVEs.
        
        Args:
            cves: List of CVE IDs
            
        Returns:
            Dict mapping CVE IDs to their details
        """
        return {
            cve: self.get_cve_details(cve)
            for cve in cves
        }
