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
    
    def __init__(self):
        # Get HMAC key from environment
        hmac_key_hex = os.getenv("DOVAH_HMAC_KEY")
        if not hmac_key_hex:
            raise ValueError(
                "DOVAH_HMAC_KEY environment variable not set. "
                "This must be a 64-character hex string."
            )
        
        try:
            # Convert hex to bytes
            self.hmac_key = bytes.fromhex(hmac_key_hex)
        except ValueError as e:
            raise ValueError(
                "DOVAH_HMAC_KEY must be a valid 64-character hex string "
                "(32 bytes when decoded)"
            ) from e
        
        # Get database URL from environment
        self.db_url = os.getenv(
            "DOVAH_DB_URL",
            "postgresql://dovah:dovah@localhost:5432/dovah"
        )
        self.engine = create_engine(self.db_url)
        
        # Load schema for validation
        schema_file = self.SCHEMA_PATH / "parsed_log.schema.json"
        with open(schema_file) as f:
            self.schema = json.load(f)
            
        # Initialize template miner
        self.template_miner = TemplateMiner()
        self._load_template_cache()
        
        # Deduplication set for this batch
        self.seen_events: Set[str] = set()
        
        # Host clock skew tracking
        self.host_offsets: Dict[str, float] = defaultdict(float)
        self.host_ts_counts: Dict[str, int] = defaultdict(int)
    
    def pseudonymize(self, value: str) -> str:
        """Create HMAC-SHA256 pseudonym for PII."""
        h = hmac.new(self.hmac_key, value.encode(), hashlib.sha256)
        return h.hexdigest()
    
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
        """Normalize timestamp to UTC with clock skew correction."""
        match = self.TS_PATTERN.match(ts_str)
        if not match:
            raise ValueError(f"Invalid timestamp format: {ts_str}")
            
        # Parse components
        components = match.groupdict()
        subsec = components.get("subsec", "0").ljust(6, "0")[:6]
        
        # Create UTC datetime
        ts = datetime.datetime(
            int(components["year"]), int(components["month"]), 
            int(components["day"]), int(components["hour"]),
            int(components["minute"]), int(components["second"]),
            int(subsec), tzinfo=timezone.utc
        )
        
        # Update host clock skew
        now = datetime.datetime.now(timezone.utc)
        offset = (now - ts).total_seconds()
        
        n = self.host_ts_counts[host]
        self.host_offsets[host] = (
            (n * self.host_offsets[host] + offset) / (n + 1)
        )
        self.host_ts_counts[host] += 1
        
        # Apply correction if significant skew
        if abs(self.host_offsets[host]) > 1:
            ts += datetime.timedelta(seconds=self.host_offsets[host])
            
        return ts
    
    def _compute_event_hash(self, ts: datetime.datetime, host: str, msg: str) -> str:
        """Compute deduplication hash for event."""
        return hashlib.sha256(
            f"{ts.isoformat()}|{host}|{msg}".encode()
        ).hexdigest()
    
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
            event_hash = self._compute_event_hash(ts, host, msg)
            if event_hash in self.seen_events:
                return None
            self.seen_events.add(event_hash)
            
            return {
                "ts": ts,
                "host": host,
                "component": comp,
                "template_id": template_id,
                "session_id": self.pseudonymize(f"{host}:{ts.date()}"),
                "level": level,
                "labels": {},
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
