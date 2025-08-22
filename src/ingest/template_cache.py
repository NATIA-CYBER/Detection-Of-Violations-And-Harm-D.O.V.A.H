"""Template extraction with persistent caching and pattern matching."""
import json
import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

# Prefer real Drain3; fall back to a local compat that mimics its API.
try:
    from drain3 import TemplateMiner as TemplateMiner  # type: ignore
    _HAVE_DRAIN3 = True
except Exception:
    _HAVE_DRAIN3 = False
    from .template_extract import TemplateMiner as _LocalMiner  # our lightweight miner

    class _CompatResult:
        def __init__(self, cluster_id: str, template_mined: str) -> None:
            self.cluster_id = cluster_id
            self.template_mined = template_mined

    class TemplateMiner:  # compat wrapper exposing add_log_message(...)
        def __init__(self) -> None:
            self._local = _LocalMiner()
        def add_log_message(self, msg: str):
            tid = self._local.extract(msg)
            return _CompatResult(cluster_id=tid, template_mined=self._local.get_template(tid))

from ..common.pseudo import hmac_sha256_hex, get_salt


class TemplateCache:
    # Common variable patterns
    PATTERNS = {
        'block_id': re.compile(r'\bblk_\d+\b'),
        'uuid': re.compile(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b'),
        'ip_port': re.compile(r'/?(\b(?:\d{1,3}\.){3}\d{1,3}\b)(?::(\d+))?'),
        'hex': re.compile(r'\b[0-9a-fA-F]{6,}\b'),
        'number': re.compile(r'\b\d+\b'),
        'block_ref': re.compile(r'\bblock \d+\b')
    }

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.miner = TemplateMiner()  # real Drain3 if installed; otherwise compat wrapper
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

    def _normalize_pattern(self, message: str) -> str:
        """Normalize message by replacing variable parts with wildcards."""
        if not message:
            return ''

        template = message
        # Replace block IDs
        template = self.PATTERNS['block_id'].sub('blk_*', template)
        # Replace UUIDs
        template = self.PATTERNS['uuid'].sub('*', template)
        # Replace IP addresses with ports
        template = self.PATTERNS['ip_port'].sub(lambda m: '*' + (':*' if m.group(2) else ''), template)
        # Replace hex and numbers
        template = self.PATTERNS['hex'].sub('*', template)
        template = self.PATTERNS['number'].sub('*', template)
        # Clean up block references
        template = self.PATTERNS['block_ref'].sub('block *', template)
        return template

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

        # Cache miss - normalize and extract template
        normalized = self._normalize_pattern(message)
        result = self.miner.add_log_message(normalized)
        template = {
            'id': result.cluster_id,
            'pattern': result.template_mined
        }

        # Update cache
        self.cache[cache_key] = template
        self._save_cache()

        return template['id'], template['pattern']

    def get_stats(self) -> Dict:
        """Get template extraction stats."""
        # With Drain3, .drain.clusters exists; with compat miner, it doesn't.
        clusters = []
        drain = getattr(getattr(self.miner, "drain", None), "clusters", None)
        if drain:
            clusters = [
                {
                    'id': c.cluster_id,
                    'size': c.size,
                    'pattern': c.get_template()
                }
                for c in drain
            ]
        return {
            'total_templates': len(clusters),
            'cache_size': len(self.cache),
            'clusters': clusters
        }
