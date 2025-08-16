"""Template extraction with persistent caching."""
import json
import os
from pathlib import Path
from typing import Dict, Optional, Tuple
from drain3 import TemplateMiner
from ..common.pseudo import hmac_sha256_hex, get_salt

class TemplateCache:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.miner = TemplateMiner()
        self._load_cache()
        
    def _get_cache_key(self, message: str) -> str:
        """Generate stable cache key for message."""
        return hmac_sha256_hex(message, get_salt())
        
    def _load_cache(self):
        """Load cached templates."""
        self.cache: Dict[str, Dict] = {}
        cache_file = self.cache_dir / "template_cache.json"
        if cache_file.exists():
            with open(cache_file) as f:
                self.cache = json.load(f)
                
    def _save_cache(self):
        """Save cached templates."""
        cache_file = self.cache_dir / "template_cache.json"
        with open(cache_file, 'w') as f:
            json.dump(self.cache, f)
            
    def extract_template(self, message: str) -> Tuple[int, str]:
        """Extract template ID and pattern, using cache if available.
        
        Args:
            message: Log message to extract template from
            
        Returns:
            Tuple of (template_id, template_pattern)
        """
        cache_key = self._get_cache_key(message)
        
        # Check cache first
        if cache_key in self.cache:
            return (
                self.cache[cache_key]['id'],
                self.cache[cache_key]['pattern']
            )
            
        # Cache miss - extract template
        result = self.miner.add_log_message(message)
        template = {
            'id': result['cluster_id'],
            'pattern': result['template_mined']
        }
        
        # Update cache
        self.cache[cache_key] = template
        self._save_cache()
        
        return template['id'], template['pattern']
        
    def get_stats(self) -> Dict:
        """Get template extraction stats."""
        return {
            'total_templates': len(self.miner.drain.clusters),
            'cache_size': len(self.cache),
            'clusters': [
                {
                    'id': c.cluster_id,
                    'size': c.size,
                    'pattern': c.get_template()
                }
                for c in self.miner.drain.clusters
            ]
        }
