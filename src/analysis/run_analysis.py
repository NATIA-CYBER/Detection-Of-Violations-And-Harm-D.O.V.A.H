"""Run HDFS log analysis.

Usage:
    python -m src.analysis.run_analysis /path/to/hdfs/logs
"""
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from jsonschema import ValidationError

from src.analysis.drift import analyze_logs
from src.analysis.validate import load_schema, validate_log_entry
from src.common.pseudo import pseudonymize
from src.ingest.template_extract import TemplateMiner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Regular expression for parsing log lines
LOG_PATTERN = re.compile(r'(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\t'
                         r'(?P<host>\S+)\t'
                         r'(?P<component>\S+)\t'
                         r'(?P<level>\S+)\t'
                         r'(?P<message>.*)')

def parse_ts(ts_str: str) -> datetime:
    """Parse timestamp string into datetime object."""
    return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S,%f')

def pseudonymize(host: str) -> str:
    """Pseudonymize host name."""
    # TO DO: implement pseudonymization
    return host

def generate_session_id(ts: datetime, host: str) -> str:
    """Generate session ID."""
    # TO DO: implement session ID generation
    return f"{ts.isoformat()}_{host}"

def generate_dedup_key(ts: datetime, host: str, message: str) -> str:
    """Generate deduplication key."""
    # TO DO: implement deduplication key generation
    return f"{ts.isoformat()}_{host}_{message}"

def template_miner_add_log_message(message: str) -> str:
    """Add log message to template miner."""
    # TO DO: implement template miner
    return message

def load_hdfs_logs(log_dir: Path) -> pd.DataFrame:
    """Load HDFS logs into DataFrame and validate against schema."""
    records = []
    for log_file in log_dir.glob('*.log'):
        with open(log_file) as f:
            for line in f:
                try:
                    # Parse log line
                    match = LOG_PATTERN.match(line)
                    if not match:
                        continue
                        
                    # Extract fields
                    ts = parse_timestamp(match.group('timestamp'))
                    record = {
                        'timestamp': ts.isoformat(),
                        'level': match.group('level'),
                        'component': match.group('component'),
                        'message': match.group('message'),
                        'template_id': template_miner_add_log_message(match.group('message')),
                        'host': pseudonymize(match.group('host')),
                        'session_id': generate_session_id(ts, match.group('host')),
                        'dedup_key': generate_dedup_key(ts, match.group('host'), match.group('message'))
                    }
                    
                    # Validate against schema
                    validate_log_entry(record, SCHEMA)
                    records.append(record)
                except ValidationError as e:
                    logger.warning(f"Invalid log entry: {e}")
                    continue
                    
    return pd.DataFrame(records)

def main(argv: List[str]):
    if len(argv) != 2:
        print(__doc__)
        sys.exit(1)
        
    # Load schema
    global SCHEMA
    SCHEMA = load_schema('parsed_log.schema.json')
        
    log_dir = Path(argv[1])
    if not log_dir.is_dir():
        logger.error(f"Directory not found: {log_dir}")
        sys.exit(1)
        
    # Load logs
    logger.info(f"Loading logs from {log_dir}")
    df = load_hdfs_logs(log_dir)
    logger.info(f"Loaded {len(df):,} log entries")
    
    # Run analysis
    logger.info("Running analysis...")
    results = analyze_logs(df)
    
    # Save results
    output_file = log_dir / "analysis_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Results saved to {output_file}")

if __name__ == '__main__':
    main(sys.argv)
