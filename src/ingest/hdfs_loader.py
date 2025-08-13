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
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import create_engine
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
    
    def pseudonymize(self, value: str) -> str:
        """Create HMAC-SHA256 pseudonym for PII."""
        h = hmac.new(self.hmac_key, value.encode(), hashlib.sha256)
        return h.hexdigest()
    
    def parse_log_line(self, line: str) -> Dict:
        """Parse raw log line into structured format."""
        # TODO: Implement proper log parsing logic
        # This is a placeholder that assumes tab-separated format
        try:
            ts, host, comp, msg, level = line.strip().split("\t")
            return {
                "ts": datetime.datetime.fromisoformat(ts),
                "host": host,
                "component": comp,
                "template_id": hash(msg) % 1000,  # Placeholder
                "session_id": self.pseudonymize(f"{host}:{ts}"),
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
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
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
