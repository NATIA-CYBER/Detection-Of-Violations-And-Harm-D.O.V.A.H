"""Run HDFS log analysis.

Usage:
    python -m src.analysis.run_analysis /path/to/hdfs/logs
"""
import sys
import json
import logging
from pathlib import Path
import pandas as pd
from typing import List

from src.analysis.drift import analyze_logs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_hdfs_logs(log_dir: Path) -> pd.DataFrame:
    """Load HDFS logs into DataFrame."""
    records = []
    for log_file in log_dir.glob('*.log'):
        with open(log_file) as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    ts_str, host, comp, msg, *rest = parts
                    level = rest[0] if rest else None
                    records.append({
                        'ts': pd.to_datetime(ts_str),
                        'host': host,
                        'component': comp,
                        'message': msg,
                        'level': level
                    })
    return pd.DataFrame(records)

def main(argv: List[str]):
    if len(argv) != 2:
        print(__doc__)
        sys.exit(1)
        
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
