"""HDFS log loader and parser.

Loads HDFS logs from LogHub, parses them according to schema,
and applies privacy-preserving pseudonymization.
"""
import datetime
import hashlib
import hmac
import json
import logging
import os
import re
from collections import defaultdict
from datetime import timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np

import pandas as pd
from drain3 import TemplateMiner
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParsedLogEvent(BaseModel):
    """Parsed HDFS log event matching schema."""
    ts: datetime.datetime = Field(..., description="Event timestamp")
    host: str = Field(..., description="Source host")
    component: str = Field(..., description="HDFS component")
    template_id: int = Field(..., description="Log template ID")
    session_id: str = Field(..., description="Session ID (pseudonymized)")
    level: Optional[str] = Field(None, description="Log level")
    labels: Optional[Dict] = Field(None, description="Additional labels")
    schema_ver: str = Field(..., description="Schema version")

class HDFSLoader:
    """Loads and processes HDFS logs with privacy controls."""
    
    SCHEMA_PATH = Path(__file__).parent.parent.parent / "schemas"
    SCHEMA_VERSION = "1.0.0"
    CACHE_DIR = Path("cache")
    
    # RFC3339 with optional subsecond precision
    TS_PATTERN = re.compile(
        r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})[T ]" +
        r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})" +
        r"(?:\.(?P<subsec>\d{1,6}))?(?:Z|[+-]\d{2}:?\d{2})?"
    )
    
    # PII patterns
    PII_PATTERNS = {
        # RFC5322 email
        "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
        # API keys, tokens (common formats)
        "token": re.compile(r"(?i)(?:key|token|api[_-]?key)[_-]?(?:id)?\s*[=:]\s*[A-Za-z0-9]{16,}"),
        # AWS-style secrets
        "aws_secret": re.compile(r"(?i)aws[_-]?(?:secret|key)[_-]?(?:id|access)?[_-]?(?:key)?\s*[=:]\s*[A-Za-z0-9/+=]{20,}"),
        # Password in config/logs
        "password": re.compile(r"(?i)pass(?:word)?[_-]?(?:key)?\s*[=:]\s*\S+"),
        # Private keys
        "private_key": re.compile(r"-----BEGIN (?:RSA )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA )?PRIVATE KEY-----"),
        # IP addresses (optional)
        "ip": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
    }
    
    # Advanced CVE patterns
    CVE_PATTERNS = {
        # Basic CVE format
        'cve': re.compile(r"CVE-\d{4}-\d{4,7}", re.IGNORECASE),
        
        # Version-specific patterns
        'hadoop_ver': re.compile(r"hadoop-([0-9]+\.[0-9]+\.[0-9]+)", re.IGNORECASE),
        'hdfs_ver': re.compile(r"hdfs-([0-9]+\.[0-9]+\.[0-9]+)", re.IGNORECASE),
        
        # Component patterns
        'namenode': re.compile(r"NameNode|name\s*node", re.IGNORECASE),
        'datanode': re.compile(r"DataNode|data\s*node", re.IGNORECASE),
        'journalnode': re.compile(r"JournalNode|journal\s*node", re.IGNORECASE),
        
        # Common vulnerability keywords
        'vuln_terms': re.compile(r"vulnerability|exploit|overflow|injection|bypass", re.IGNORECASE)
    }
    
    # Component risk weights (used for scoring)
    COMPONENT_RISK_WEIGHTS = {
        'NameNode': 1.0,  # Critical - controls filesystem metadata
        'DataNode': 0.8,  # High - stores actual data
        'JournalNode': 0.7,  # Medium-high - HA metadata
        'ResourceManager': 0.6,  # Medium - YARN scheduling
        'NodeManager': 0.5,  # Medium-low - YARN execution
        'Default': 0.3  # Low - other components
    }
    
    def __init__(self, tenant_id: str = "default"):
        """Initialize loader with tenant-specific HMAC key.
        
        Args:
            tenant_id: Unique tenant identifier for salt
        """
        # Get base HMAC key from environment
        hmac_key_hex = os.getenv("DOVAH_HMAC_KEY")
        if not hmac_key_hex:
            raise ValueError(
                "DOVAH_HMAC_KEY environment variable not set. "
                "This must be a 64-character hex string."
            )
        
        try:
            # Convert hex to bytes
            base_key = bytes.fromhex(hmac_key_hex)
            
            # Create tenant-specific key by combining base key with tenant salt
            tenant_salt = hashlib.sha256(tenant_id.encode()).digest()
            self.hmac_key = hmac.new(base_key, tenant_salt, hashlib.sha256).digest()
            
        except ValueError as e:
            raise ValueError(
                "DOVAH_HMAC_KEY must be a valid 64-character hex string "
                "(32 bytes when decoded)"
            ) from e
        
        # Get database URL from environment
        self.db_url = os.getenv(
            "DOVAH_DB_URL",
            "postgresql+psycopg://dovah:dovah@localhost:5432/dovah"
        )
        self.engine = create_engine(self.db_url)
        
        # Load schema for validation
        schema_file = self.SCHEMA_PATH / "parsed_log.schema.json"
        with open(schema_file) as f:
            self.schema = json.load(f)
            
        # Initialize template miner with drain3.ini config
        config_file = Path(__file__).parent.parent.parent / "drain3.ini"
        self.template_miner = TemplateMiner(config_file)
        self._load_template_cache()
        
        # Initialize template stats
        self.template_counts = defaultdict(int)
        self.template_last_seen = {}
        self.total_events = 0
        
        # Deduplication set for this batch
        self.seen_events: Set[str] = set()
        
        # Host clock skew and latency tracking
        self.host_offsets: Dict[str, float] = defaultdict(float)
        self.host_ts_counts: Dict[str, int] = defaultdict(int)
        
        # Rolling window of latencies for P95 tracking
        self.latency_window_size = 1000  # Keep last 1000 events
        self.latencies: List[float] = []
        self.p95_latency: float = 0.0
        
        # Get latency SLO from environment
        self.latency_slo_ms = float(os.getenv("LATENCY_SLO_MS", "2000"))  # Default 2s
    
    def pseudonymize(self, value: str, context: str = "") -> str:
        """Create HMAC-SHA256 pseudonym for PII with optional context.
        
        Args:
            value: Value to pseudonymize
            context: Optional context string (e.g. field name) to prevent
                    cross-field pseudonym reuse
        """
        if not value:
            return value
            
        # Add context to prevent cross-field pseudonym reuse
        data = f"{context}:{value}" if context else value
        h = hmac.new(self.hmac_key, data.encode(), hashlib.sha256)
        return h.hexdigest()
    
    def scrub_pii(self, text: str) -> str:
        """Remove or pseudonymize PII from text."""
        if not text:
            return text
            
        # Start with the original text
        scrubbed = text
        
        # Apply each PII pattern
        for pattern_name, pattern in self.PII_PATTERNS.items():
            matches = pattern.finditer(scrubbed)
            for match in matches:
                # Get the full match
                pii = match.group(0)
                # Create a pseudonym
                pseudonym = self.pseudonymize(pii)
                # Replace with type indicator and truncated hash
                replacement = f"<{pattern_name}:{pseudonym[:8]}>"
                scrubbed = scrubbed.replace(pii, replacement)
        
        return scrubbed
        
    def extract_cves(self, text: str) -> List[str]:
        """Extract CVE IDs from text.
        
        Returns:
            List of CVE IDs in canonical format (CVE-YYYY-NNNNN[NN]).
        """
        if not text:
            return []
            
        # Find all CVE matches
        matches = self.CVE_PATTERN.finditer(text)
        
        # Extract and normalize CVE IDs
        cves = []
        for match in matches:
            cve = match.group(0).upper()  # Normalize to uppercase
            if cve not in cves:  # Deduplicate
                cves.append(cve)
                
        return cves
    
    def _load_template_cache(self) -> None:
        """Load template cache from disk."""
        cache_file = self.CACHE_DIR / "templates.json"
        if cache_file.exists():
            with open(cache_file) as f:
                for line in f:
                    self.template_miner.load_from_dict(json.loads(line))
    
    def _save_template_cache(self) -> None:
        """Save template cache to disk."""
        self.CACHE_DIR.mkdir(exist_ok=True)
        cache_file = self.CACHE_DIR / "templates.json"
        with open(cache_file, "w") as f:
            for template in self.template_miner.drain.clusters:
                json.dump(template.to_dict(), f)
                f.write("\n")
    
    def _normalize_timestamp(self, ts_str: str, host: str) -> datetime.datetime:
        """Normalize timestamp to UTC RFC3339 with clock skew correction."""
        match = self.TS_PATTERN.match(ts_str)
        if not match:
            raise ValueError(f"Invalid timestamp format: {ts_str}")
            
        # Parse components
        components = match.groupdict()
        
        # Ensure subsecond precision is exactly 6 digits (microseconds)
        subsec = components.get("subsec", "0").ljust(6, "0")[:6]
        
        # Parse timezone if present, default to UTC
        tz_str = match.group().split()[-1] if ' ' in match.group() else match.group()[-6:]
        if tz_str == 'Z':
            tz = timezone.utc
        elif '+' in tz_str or '-' in tz_str:
            # Convert ±HH:MM to timezone
            sign = 1 if '+' in tz_str else -1
            hours, minutes = map(int, tz_str.replace(':', '')[1:].split())
            offset = sign * (hours * 3600 + minutes * 60)
            tz = datetime.timezone(datetime.timedelta(seconds=offset))
        else:
            tz = timezone.utc
            
        # Create datetime in source timezone
        dt = datetime.datetime(
            int(components["year"]), int(components["month"]), 
            int(components["day"]), int(components["hour"]),
            int(components["minute"]), 
            int(components["second"]), 
            int(subsec), 
            tzinfo=tz
        )
        
        # Convert to UTC
        dt = dt.astimezone(timezone.utc)
        
        now = datetime.datetime.now(timezone.utc)
        latency_ms = (now - dt).total_seconds() * 1000
        
        # Update rolling latency window
        self.latencies.append(latency_ms)
        if len(self.latencies) > self.latency_window_size:
            self.latencies.pop(0)
        
        # Calculate P95 latency with minimum sample size
        if len(self.latencies) >= 20:
            self.p95_latency = float(np.percentile(self.latencies, 95))
            if self.p95_latency > self.latency_slo_ms:
                logging.warning(f"P95 latency {self.p95_latency:.1f}ms exceeds SLO {self.latency_slo_ms}ms")
        
        # Exponential moving average for clock skew
        alpha = 0.1  # Smoothing factor
        offset = latency_ms / 1000
        if host not in self.host_offsets:
            self.host_offsets[host] = offset
        else:
            self.host_offsets[host] = alpha * offset + (1 - alpha) * self.host_offsets[host]
        
        # Dynamic skew threshold based on P95
        skew_threshold = max(1.0, self.p95_latency / 2000)
        if abs(self.host_offsets[host]) > skew_threshold:
            dt += datetime.timedelta(seconds=self.host_offsets[host])
            logging.info(f"Corrected {abs(self.host_offsets[host]):.1f}s clock skew for host {host}")
        
        return dt
    
    def extract_cve_context(self, msg: str) -> dict:
        """Extract CVE and context information from message.
        
        Returns:
            dict: CVE context including:
                - cves: List of CVE IDs
                - versions: List of version strings
                - components: List of affected components
                - risk_terms: List of vulnerability terms
                - component_risk: Float risk score based on components
        """
        context = {
            'cves': [],
            'versions': [],
            'components': [],
            'risk_terms': [],
            'component_risk': self.COMPONENT_RISK_WEIGHTS['Default']
        }
        
        # Extract CVEs
        if cves := self.CVE_PATTERNS['cve'].findall(msg):
            context['cves'] = cves
            
        # Extract versions
        for pattern in ['hadoop_ver', 'hdfs_ver']:
            if versions := self.CVE_PATTERNS[pattern].findall(msg):
                context['versions'].extend(versions)
                
        # Extract components and calculate risk
        max_risk = self.COMPONENT_RISK_WEIGHTS['Default']
        for comp, pattern in [
            ('NameNode', 'namenode'),
            ('DataNode', 'datanode'),
            ('JournalNode', 'journalnode')
        ]:
            if self.CVE_PATTERNS[pattern].search(msg):
                context['components'].append(comp)
                max_risk = max(max_risk, self.COMPONENT_RISK_WEIGHTS[comp])
        context['component_risk'] = max_risk
        
        # Extract vulnerability terms
        if terms := self.CVE_PATTERNS['vuln_terms'].findall(msg):
            context['risk_terms'] = terms
            
        return context

    def _compute_dedupe_key(self, ts: datetime.datetime, host: str, msg: str, proc: str = '') -> str:
        """Generate deduplication key from event fields.
        
        Args:
            ts: Event timestamp
            host: Source host
            msg: Raw message
            proc: Optional process name for better uniqueness
            
        Returns:
            SHA-256 hex digest
        """
        h = hashlib.sha256()
        # Use millisecond precision for timestamp
        h.update(str(int(ts.timestamp() * 1000)).encode())
        # Add process name for better uniqueness
        h.update(proc.encode())
        h.update(host.encode())
        # Strip whitespace from message to normalize
        h.update(msg.strip().encode())
        return h.hexdigest()
    
    def parse_log_line(self, line: str) -> Optional[Dict]:
        """Parse raw log line into structured format."""
        try:
            # Split on tabs, handling optional fields
            parts = line.strip().split("\t")
            if len(parts) < 4:
                logger.warning(f"Invalid log format: {line}")
                return None
                
            ts_str, host, comp, msg, *rest = parts
            level = rest[0] if rest else None
            
            # Normalize timestamp to UTC
            try:
                ts = self._normalize_timestamp(ts_str, host)
            except ValueError as e:
                logger.error(f"Timestamp error: {e}")
                return None
                
            # Get or create template
            template_result = self.template_miner.add_log_message(msg)
            template_id = template_result.cluster_id
            
            # Compute deduplication hash
            event_hash = self._compute_dedupe_key(ts, host, msg)
            if event_hash in self.seen_events:
                return None
            self.seen_events.add(event_hash)
            
            # Update template statistics
            self.template_counts[template_id] += 1
            self.template_last_seen[template_id] = ts
            self.total_events += 1
            
            # Scrub PII from message
            scrubbed_msg = self.scrub_pii(msg)
            
            # Extract CVEs for enrichment
            cves = self.extract_cves(msg)
            
            return {
                "ts": ts,
                "host": self.pseudonymize(host),  # Always pseudonymize hosts
                "component": comp,
                "template_id": template_id,
                "message": scrubbed_msg,  # Store scrubbed message
                "session_id": self.pseudonymize(f"{host}:{ts.date()}"),
                "level": level,
                "labels": {
                    "cves": cves  # Add CVEs for EPSS/KEV enrichment
                },
                "schema_ver": self.SCHEMA_VERSION
            }
        except Exception as e:
            logger.error(f"Failed to parse log line: {e}")
            return None
    
    def validate_event(self, event: Dict) -> Optional[ParsedLogEvent]:
        """Validate parsed event against schema."""
        try:
            return ParsedLogEvent(**event)
        except Exception as e:
            logger.error(f"Event validation failed: {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    def process_log_file(self, input_path: Path) -> List[ParsedLogEvent]:
        """Process log file with retry logic."""
        logger.info(f"Processing log file: {input_path}")
        validated_events = []
        
        with open(input_path) as f:
            for line in f:
                if event := self.parse_log_line(line):
                    if validated := self.validate_event(event):
                        validated_events.append(validated)
        
        return validated_events
    
    def store_events(self, events: List[ParsedLogEvent]) -> None:
        """Store parsed events in both JSON and PostgreSQL."""
        if not events:
            logger.warning("No valid events to store")
            return
            
        # Save to JSON
        date_str = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        json_path = output_dir / f"parsed_logs_{date_str}.json"
        with open(json_path, "w") as f:
            json.dump(
                [event.model_dump() for event in events],
                f,
                indent=2,
                default=str
            )
        logger.info(f"Saved {len(events)} events to {json_path}")
        
        # Save to PostgreSQL
        df = pd.DataFrame([event.model_dump() for event in events])
        df.to_sql(
            "hdfs_events",
            self.engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        logger.info(f"Saved {len(events)} events to PostgreSQL")
        
        # Save template cache
        self._save_template_cache()

def main() -> None:
    """Main entry point."""
    try:
        # Initialize loader
        loader = HDFSLoader()
        
        # Process all .txt files in raw directory
        raw_dir = Path("data/raw")
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        for input_path in raw_dir.glob("*.txt"):
            try:
                events = loader.process_log_file(input_path)
                if events:
                    loader.store_events(events)
                    logger.info(
                        f"Processed {len(events)} events from {input_path}"
                    )
                    # Move to processed
                    processed_dir = raw_dir / "processed"
                    processed_dir.mkdir(exist_ok=True)
                    input_path.rename(processed_dir / input_path.name)
                else:
                    logger.warning(f"No valid events in {input_path}")
            except Exception as e:
                logger.error(f"Failed to process {input_path}: {e}")
                # Move to failed
                failed_dir = raw_dir / "failed"
                failed_dir.mkdir(exist_ok=True)
                input_path.rename(failed_dir / input_path.name)
                continue
        
        logger.info("HDFS log processing completed")
            
    except Exception as e:
        logger.error(f"Failed to process HDFS logs: {e}")
        raise

if __name__ == "__main__":
    main()
