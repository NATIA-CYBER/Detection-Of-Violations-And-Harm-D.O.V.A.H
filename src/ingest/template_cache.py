# src/ingest/template_cache.py
from __future__ import annotations
import os
from pathlib import Path

# Try real Drain3 first; fall back to local miner if unavailable.
try:
    from drain3 import TemplateMiner as DrainTemplateMiner  # type: ignore
    try:
        # Older/newer Drain3s place this here
        from drain3.template_miner_config import TemplateMinerConfig  # type: ignore
        _HAVE_TM_CONFIG = True
    except Exception:
        _HAVE_TM_CONFIG = False
    _HAVE_DRAIN3 = True
except Exception:
    _HAVE_DRAIN3 = False
    from .template_extract import TemplateMiner as LocalTemplateMiner  # our lightweight miner

class TemplateCache:
    """
    Wrapper that prefers drain3.TemplateMiner if installed; otherwise uses a local miner.
    If Drain3 is available and a config path is provided (or discoverable), it will be loaded.
    """

    def __init__(self, config_path: str | None = None) -> None:
        if _HAVE_DRAIN3:
            cfg = None
            if _HAVE_TM_CONFIG:
                cfg_path = self._discover_config(config_path)
                if cfg_path:
                    try:
                        cfg = TemplateMinerConfig()
                        cfg.load(str(cfg_path))
                    except Exception:
                        cfg = None  # fall back to defaults if INI malformed
            # If cfg is None, Drain3 uses its defaults
            self.miner = DrainTemplateMiner(config=cfg) if cfg is not None else DrainTemplateMiner()
        else:
            # Local lightweight miner ignores Drain3 INI (by design)
            self.miner = LocalTemplateMiner()

    def extract_id(self, msg: str) -> str:
        return self.miner.extract(msg)

    def template_str(self, tid: str) -> str:
        # Drain3 miner does not expose get_template by default; local one does.
        # For Drain3, we can't easily retrieve a template string from id without monkeypatching;
        # tests that need the string should use the local miner or verify via normalization.
        get_tpl = getattr(self.miner, "get_template", None)
        return get_tpl(tid) if callable(get_tpl) else ""

    @staticmethod
    def _discover_config(explicit: str | None) -> Path | None:
        """
        Return a usable INI path if present:
        1) explicit arg
        2) $DRAIN3_CONFIG
        3) repo-local usual suspects
        """
        candidates = []
        if explicit:
            candidates.append(Path(explicit))
        env = os.getenv("DRAIN3_CONFIG")
        if env:
            candidates.append(Path(env))
        # common repo locations (adjust if you keep your INI elsewhere)
        here = Path(__file__).resolve()
        candidates.extend([
            here.with_name("drain3.ini"),
            here.parents[2] / "config" / "drain3.ini",
            here.parents[2] / "configs" / "drain3.ini",
            here.parents[2] / "drain3.ini",
        ])
        for p in candidates:
            if p and p.exists() and p.is_file():
                return p
        return None
