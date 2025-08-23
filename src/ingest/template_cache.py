"""Template extraction with persistent caching and pattern matching."""
import json, re
from pathlib import Path
from typing import Dict, Tuple

# Prefer real Drain3; fall back to a local compat that mimics its API.
try:
    from drain3 import TemplateMiner as TemplateMiner  # type: ignore
except Exception:
    from .template_extract import TemplateMiner as _LocalMiner
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
    PATTERNS = {
        'block_id':  re.compile(r'\bblk_\d+\b'),
        'uuid':      re.compile(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b'),
        'ip_port':   re.compile(r'/?(\b(?:\d{1,3}\.){3}\d{1,3}\b)(?::(\d+))?'),
        'hex':       re.compile(r'\b[0-9a-fA-F]{6,}\b'),
        'number':    re.compile(r'\b\d+\b'),
        'block_ref': re.compile(r'\bblock \d+\b')
    }
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir; self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.miner = TemplateMiner()  # real Drain3 if installed; otherwise compat wrapper
        self._load_cache()
    def _get_cache_key(self, message: str) -> str:
        return hmac_sha256_hex(message, get_salt())
    def _load_cache(self):
        self.cache: Dict[str, Dict] = {}
        f = self.cache_dir / "template_cache.json"
        if f.exists():
            with open(f) as fh: self.cache = json.load(fh)
    def _save_cache(self):
        f = self.cache_dir / "template_cache.json"
        with open(f, 'w') as fh: json.dump(self.cache, fh)
    def _normalize_pattern(self, message: str) -> str:
        if not message: return ''
        t = message
        t = self.PATTERNS['block_id'].sub('blk_*', t)
        t = self.PATTERNS['uuid'].sub('*', t)
        t = self.PATTERNS['ip_port'].sub(lambda m: '*' + (':*' if m.group(2) else ''), t)
        t = self.PATTERNS['hex'].sub('*', t)
        t = self.PATTERNS['number'].sub('*', t)
        t = self.PATTERNS['block_ref'].sub('block *', t)
        return t
    def extract_template(self, message: str) -> Tuple[int, str]:
        key = self._get_cache_key(message)
        if key in self.cache:
            ent = self.cache[key]; return ent['id'], ent['pattern']
        normalized = self._normalize_pattern(message)
        result = self.miner.add_log_message(normalized)
        ent = {'id': result.cluster_id, 'pattern': result.template_mined}
        self.cache[key] = ent; self._save_cache()
        return ent['id'], ent['pattern']
    def get_stats(self) -> Dict:
        clusters = []
        drain = getattr(getattr(self.miner, "drain", None), "clusters", None)
        if drain:
            clusters = [{'id': c.cluster_id, 'size': c.size, 'pattern': c.get_template()} for c in drain]
        return {'total_templates': len(clusters), 'cache_size': len(self.cache), 'clusters': clusters}
