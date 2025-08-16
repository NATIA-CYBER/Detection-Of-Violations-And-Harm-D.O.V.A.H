"""Processes a real-time stream of parsed logs to generate windowed features.

Usage:
    python -m src.stream.replay --input-file sample_data/hdfs/sample.jsonl | \
    python -m src.stream.features --window-size-sec 60
"""

import argparse
import json
import logging
import sys
import time
from collections import Counter, deque
from itertools import takewhile
from datetime import datetime, timedelta, timezone

import numpy as np
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,  # Log to stderr to keep stdout clean for piped data
)

def process_window(
    window_events: list, latency_log_writer=None
) -> dict:
    """Calculates and emits features for a single window of events."""
    if not window_events:
        return {}

    # --- Basic Features ---
    event_count = len(window_events)
    timestamps = [e['_internal']['original_ts'] for e in window_events]
    window_start = min(timestamps)
    window_end = max(timestamps)

    # --- Template Features ---
    template_counts = Counter(e.get('template_id') for e in window_events)
    unique_templates = len(template_counts)
    rare_templates = sum(1 for count in template_counts.values() if count == 1)
    rare_rate = rare_templates / event_count if event_count > 0 else 0

    # --- Component Churn (temporarily disabled for performance testing) ---
    component_churn = 0

    # --- CEP Features ---
    is_burst = False

    # --- Latency Instrumentation ---
    processing_end_ts = datetime.now(timezone.utc)
    ingest_latencies = [(e['_internal']['received_ts'] - e['_internal']['replay_ts']).total_seconds() for e in window_events]
    feature_latencies = [(processing_end_ts - e['_internal']['received_ts']).total_seconds() for e in window_events]

    p50_ingest_ms = np.percentile(ingest_latencies, 50) * 1000 if ingest_latencies else 0
    p95_ingest_ms = np.percentile(ingest_latencies, 95) * 1000 if ingest_latencies else 0
    p50_feature_ms = np.percentile(feature_latencies, 50) * 1000 if feature_latencies else 0
    p95_feature_ms = np.percentile(feature_latencies, 95) * 1000 if feature_latencies else 0

    feature_record = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "event_count": event_count,
        "unique_templates": unique_templates,
        "rare_template_rate": round(rare_rate, 4),
        "component_churn": component_churn,
        "is_burst": is_burst,
        "p50_ingest_latency_ms": round(p50_ingest_ms, 2),
        "p95_ingest_latency_ms": round(p95_ingest_ms, 2),
        "p50_feature_latency_ms": round(p50_feature_ms, 2),
        "p95_feature_latency_ms": round(p95_feature_ms, 2),
        # Internal stats for next window's calculation
        "_internal": {}
    }

    if latency_log_writer:
        latency_log_writer.writerow({
            'window_end_ts': window_end.isoformat(),
            'p50_ingest_ms': round(p50_ingest_ms, 2),
            'p95_ingest_ms': round(p95_ingest_ms, 2),
            'p50_feature_ms': round(p50_feature_ms, 2),
            'p95_feature_ms': round(p95_feature_ms, 2),
        })

    # Emit feature record to stdout (without internal fields)
    emit_record = {k: v for k, v in feature_record.items() if k != '_internal'}
    try:
        sys.stdout.write(json.dumps(emit_record) + '\n')
        sys.stdout.flush()
    except BrokenPipeError:
        logging.warning("Broken pipe. Exiting feature generation.")
        sys.exit(0)
    
    return feature_record

def stream_processor(window_size_sec: int, window_stride_sec: float, latency_log_file: str = None):
    """Reads events from stdin, windows them, and computes features in a streaming fashion."""
    logging.info(f"Starting stream processing with {window_size_sec}s windows and {window_stride_sec}s stride.")

    # State variables
    events_buffer = deque()
    first_event_time = None
    next_window_start = None

    # Setup latency logging
    latency_file = None
    latency_log_writer = None
    if latency_log_file:
        try:
            latency_file = open(latency_log_file, 'w', newline='')
            fieldnames = ['window_end_ts', 'p50_ingest_ms', 'p95_ingest_ms', 'p50_feature_ms', 'p95_feature_ms']
            latency_log_writer = csv.DictWriter(latency_file, fieldnames=fieldnames)
            latency_log_writer.writeheader()
            logging.info(f"Logging latency stats to {latency_log_file}")
        except IOError as e:
            logging.error(f"Could not open latency log file {latency_log_file}: {e}")

    for line in sys.stdin:
        try:
            event = json.loads(line)
            received_ts = datetime.now(timezone.utc)
            event_ts = datetime.fromisoformat(event['timestamp'])
            replay_ts = datetime.fromisoformat(event['replay_ts'])

            event['_internal'] = {
                'received_ts': received_ts,
                'replay_ts': replay_ts,
                'original_ts': event_ts
            }
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.warning(f"Skipping invalid event line: {line} ({e})")
            continue

        if first_event_time is None:
            first_event_time = event_ts
            next_window_start = first_event_time

        events_buffer.append(event)

        # Process all windows that have completed based on the current event's timestamp
        while event_ts >= next_window_start + timedelta(seconds=window_size_sec):
            window_end = next_window_start + timedelta(seconds=window_size_sec)
            
            # Efficiently create the window view without a full scan, as the buffer is sorted.
            # We only need to check the upper bound because the lower bound is handled by pruning.
            events_in_window = list(takewhile(lambda e: e['_internal']['original_ts'] < window_end, events_buffer))

            if events_in_window:
                logging.info(f"Processing window ending {window_end.isoformat()} with {len(events_in_window)} events.")
                process_window(events_in_window, latency_log_writer)

            # Slide the window forward
            next_window_start += timedelta(seconds=window_stride_sec)

            # Prune the buffer of events that are older than the new window start
            while events_buffer and events_buffer[0]['_internal']['original_ts'] < next_window_start:
                events_buffer.popleft()

    # Process any remaining events in the buffer
    if events_buffer:
        logging.info(f"Processing final window with {len(events_buffer)} events.")
        process_window(list(events_buffer), latency_log_writer)

    if latency_file:
        latency_file.close()

    logging.info("Finished feature generation.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate windowed features from a log stream.")
    parser.add_argument(
        "--window-size-sec",
        type=int,
        default=60,  # 1 minute
        help="The size of the time window in seconds for feature aggregation."
    )
    parser.add_argument(
        "--latency-log-file",
        type=str,
        default=None,
        help="Path to a CSV file to log latency metrics."
    )
    parser.add_argument(
        "--window-stride-sec",
        type=float,
        default=1,
        help="The stride of the sliding window in seconds."
    )
    args = parser.parse_args()


    stream_processor(
        window_size_sec=args.window_size_sec,
        window_stride_sec=args.window_stride_sec,
        latency_log_file=args.latency_log_file
    )
