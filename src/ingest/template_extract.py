# /Users/natiautrugashvili/Desktop/Detection-Of-Violations-And-Harm-D.O.V.A.H/src/ingest/template_extract.py
import re
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

# --- Patterns for normalization ---
UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
IP_PORT_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b")
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
BLK_RE = re.compile(r"\bblk_\d+\b")
HEX_LONG_RE = re.compile(r"\b[0-9a-fA-F]{6,}\b")
NUM_RE = re.compile(r"\b\d+\b")

def _normalize_star(text: str) -> str:
    """
    Normalize a message into a template using '*' placeholders.
    Order matters: specific → general to avoid over-replacement.
    """
    if not text:
        return ""
    s = text
    s = BLK_RE.sub("blk_*", s)
    s = IP_PORT_RE.sub("*:*", s)
    s = IP_RE.sub("*", s)
    s = UUID_RE.sub("*", s)
    s = HEX_LONG_RE.sub("*", s)
    s = NUM_RE.sub("*", s)
    return s

def _normalize_angle(text: str) -> str:
    """
    Normalize using Drain-style '<*>' placeholders (helpful for variable extraction).
    """
    if not text:
        return ""
    s = text
    s = BLK_RE.sub("blk_<*>", s)
    s = IP_PORT_RE.sub("<*>:<*>", s)
    s = IP_RE.sub("<*>", s)
    s = UUID_RE.sub("<*>", s)
    s = HEX_LONG_RE.sub("<*>", s)
    s = NUM_RE.sub("<*>", s)
    return s

class TemplateMiner:
    """
    Minimal template miner for unit tests.
    - extract(msg) -> stable template_id (sha1 of template)
    - get_template(template_id) -> template string
    - Empty/None input → ""
    """
    def __init__(self) -> None:
        self._tpl_to_id: Dict[str, str] = {}
        self._id_to_tpl: Dict[str, str] = {}

    def extract(self, msg: Optional[str]) -> str:
        tpl = _normalize_star(msg or "")
        if tpl == "":
            return ""  # test_empty_input requires this
        tid = self._tpl_to_id.get(tpl)
        if not tid:
            tid = hashlib.sha1(tpl.encode("utf-8")).hexdigest()[:8]
            self._tpl_to_id[tpl] = tid
            self._id_to_tpl[tid] = tpl
        return tid

    def get_template(self, template_id: str) -> str:
        return self._id_to_tpl.get(template_id, "")

@dataclass(frozen=True)
class Template:
    id: str
    pattern: str
    support: int

class TemplateExtractor:
    """
    Batch template extractor with simple clustering by normalized pattern.
    Parameters:
      - min_cluster_size: drop clusters smaller than this (default 2)
      - max_templates: keep at most this many templates by support (desc)
    Methods:
      - extract_templates(messages) -> Dict[str, Template]
      - match_template(msg) -> (template_id | None, variables: List[str])
    """
    def __init__(self, min_cluster_size: int = 2, max_templates: Optional[int] = None) -> None:
        self.min_cluster_size = max(1, int(min_cluster_size))
        self.max_templates = max_templates
        self._templates: Dict[str, Template] = {}

    def extract_templates(self, messages: List[str]) -> Dict[str, Template]:
        counts: Dict[str, int] = {}
        for m in messages or []:
            norm = _normalize_angle(m or "")
            if norm:
                counts[norm] = counts.get(norm, 0) + 1

        # Apply min_cluster_size
        items = [(pat, cnt) for pat, cnt in counts.items() if cnt >= self.min_cluster_size]
        # Sort by support desc, then lexicographically for stability
        items.sort(key=lambda x: (-x[1], x[0]))

        # Apply max_templates
        if self.max_templates is not None:
            items = items[: max(0, int(self.max_templates))]

        # Build Template objects with stable ids
        out: Dict[str, Template] = {}
        for pattern, support in items:
            tid = hashlib.sha1(pattern.encode("utf-8")).hexdigest()[:8]
            out[tid] = Template(id=tid, pattern=pattern, support=support)

        self._templates = out
        return out

    def match_template(self, msg: str) -> Tuple[Optional[str], List[str]]:
        """
        Return (template_id, variables) for a single message.
        Variables are extracted by aligning tokens with the '<*>' pattern.
        If no existing template matches, returns (None, []).
        """
        if not self._templates:
            # Lazily initialize from the single message so tests don't explode
            self.extract_templates([msg])

        raw = msg or ""
        pattern = _normalize_angle(raw)
        tid = hashlib.sha1(pattern.encode("utf-8")).hexdigest()[:8]
        tpl = self._templates.get(tid)
        if not tpl:
            return None, []

        vars: List[str] = []
        ptoks = tpl.pattern.split()
        mtoks = raw.split()
        i = 0
        for p in ptoks:
            if i >= len(mtoks):
                break
            if p == "<*>":
                vars.append(mtoks[i])
            elif p.endswith("<*>") and p != "<*>":
                # handle tokens like 'blk_<*>'
                prefix = p[:-3]
                token = mtoks[i]
                if token.startswith(prefix):
                    vars.append(token[len(prefix):])
                else:
                    vars.append(token)
            # else: literal token; nothing to extract
            i += 1
        return tid, vars
