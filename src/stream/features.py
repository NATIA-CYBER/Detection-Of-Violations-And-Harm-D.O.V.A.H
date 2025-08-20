# src/stream/features.py
"""Processes a real-time stream of parsed logs to generate windowed features.

Usage:
    python -m src.stream.replay --input-file sample_data/hdfs/sample.jsonl | \
    python -m src.stream.features --window-size-sec 60
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from itertools import takewhile
from pathlib import Path
from typing import Deque, Dict, List, Optional, Set

import numpy as np
from jsonschema import ValidationError, validate

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)

# ---------- helpers ----------
def _parse_ts(s: str) -> datetime:
    """Robust ISO-8601 parser that accepts 'Z' (UTC) and offsets."""
    if s is None:
        raise ValueError("timestamp is None")
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

# ---------- window processor ----------
def process_window(
    window_events: List[Dict],
    prev_window_components: Set[str],
    seen_templates: Set[str],
    recent_event_counts: Deque[int],
    latency_log_writer: Optional[csv.DictWriter] = None,
) -> Dict:
    """Calculates and emits features for a single window of events."""
    if not window_events:
        return {}

    # --- Basic Features ---
    timestamps = [e["_internal"]["original_ts"] for e in window_events]
    window_start = min(timestamps)
    window_end = max(timestamps)
    event_count = len(window_events)

    # --- Template Features ---
    template_counts = Counter(e.get("template_id") for e in window_events)
    current_window_templates = set(template_counts.keys())
    unique_templates = len(template_counts)
    rare_templates = sum(1 for c in template_counts.values() if c == 1)
    rare_rate = rare_templates / event_count if event_count > 0 else 0.0

    # --- Component Churn ---
    current_window_components = {e.get("component") for e in window_events if e.get("component")}
    new_components = current_window_components - prev_window_components
    disappeared_components = prev_window_components - current_window_components
    component_churn = len(new_components) + len(disappeared_components)

    # --- CEP Features ---
    new_templates = current_window_templates - seen_templates
    is_unseen_template = len(new_templates) > 0

    is_burst = False
    if recent_event_counts:
        avg_count = float(np.mean(recent_event_counts))
        std_count = float(np.std(recent_event_counts))
        if event_count > avg_count + 2 * std_count and event_count > 20:
            is_burst = True

    # --- Latency (ingest -> features) ---
    emit_ts = datetime.now(timezone.utc)
    last_replay_ts = max(e["_internal"]["replay_ts"] for e in window_events)
    lat_ms = (emit_ts - last_replay_ts).total_seconds() * 1000.0

    feature_record = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "event_count": event_count,
        "unique_templates": unique_templates,
        "rare_template_rate": round(rare_rate, 4),
        "component_churn": component_churn,
        "is_burst": is_burst,
        "is_unseen_template": is_unseen_template,
        "emit_ts": emit_ts.isoformat(),
        "last_replay_ts": last_replay_ts.isoformat(),
        "lat_ms": round(lat_ms, 1),
        "_internal": {
            "components": current_window_components,
            "templates": current_window_templates,
        },
    }

    # optional CSV logging (match headers!)
    if latency_log_writer:
        latency_log_writer.writerow(
            {
                "window_end_ts": window_end.isoformat(),
                "lat_ms": round(lat_ms, 1),
            }
        )

    # Emit to stdout (without internals)
    out = {k: v for k, v in feature_record.items() if k != "_internal"}
    try:
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()
    except BrokenPipeError:
        logging.warning("Broken pipe. Exiting feature generation.")
        sys.exit(0)

    return feature_record

# ---------- stream processor ----------
def stream_processor(
    window_size_sec: int,
    window_stride_sec: float,
    latency_log_file: Optional[str] = None,
    schema_path: Optional[str] = None,
):
    """Reads events from stdin, windows them, and computes features in a streaming fashion."""
    logging.info(f"Starting stream processing with {window_size_sec}s windows and {window_stride_sec}s stride.")

    # Resolve schema path (default: file next to this module)
    if schema_path is None:
        schema_path = str(Path(__file__).with_name("parsed_log.schema.json"))

    try:
        schema = json.loads(Path(schema_path).read_text())
    except Exception as e:
        logging.error(f"Could not load or parse schema '{schema_path}': {e}")
        sys.exit(1)

    # State
    events_buffer: Deque[Dict] = deque()
    first_event_time: Optional[datetime] = None
    next_window_start: Optional[datetime] = None
    prev_window_components: Set[str] = set()
    seen_templates: Set[str] = set()
    recent_event_counts: Deque[int] = deque(maxlen=10)

    # CSV latency logging
    latency_file = None
    latency_log_writer: Optional[csv.DictWriter] = None
    if latency_log_file:
        try:
            latency_file = open(latency_log_file, "w", newline="")
            fieldnames = ["window_end_ts", "lat_ms"]
            latency_log_writer = csv.DictWriter(latency_file, fieldnames=fieldnames)
            latency_log_writer.writeheader()
            logging.info(f"Logging latency stats to {latency_log_file}")
        except IOError as e:
            logging.error(f"Could not open latency log file {latency_log_file}: {e}")

    # Consume stream
    for line in sys.stdin:
        try:
            event = json.loads(line)

            # 'replay_ts' is not in the formal schema; pop it before validate
            replay_ts_str = event.pop("replay_ts", None)
            validate(instance=event, schema=schema)

            received_ts = datetime.now(timezone.utc)
            event_ts = _parse_ts(event["timestamp"])
            replay_ts_dt = _parse_ts(replay_ts_str) if replay_ts_str else received_ts

            event["_internal"] = {
                "received_ts": received_ts,
                "replay_ts": replay_ts_dt,
                "original_ts": event_ts,
            }
        except (json.JSONDecodeError, KeyError, ValueError, ValidationError) as e:
            logging.warning(f"Skipping invalid event: {line.strip()} ({e})")
            continue

        if first_event_time is None:
            first_event_time = event_ts
            next_window_start = first_event_time

        events_buffer.append(event)

        # Process all complete windows
        assert next_window_start is not None
        while event_ts >= next_window_start + timedelta(seconds=window_size_sec):
            window_end = next_window_start + timedelta(seconds=window_size_sec)

            # take events < window_end (buffer is time-ordered)
            events_in_window = list(
                takewhile(lambda e: e["_internal"]["original_ts"] < window_end, events_buffer)
            )

            if events_in_window:
                logging.info(
                    f"Processing window ending {window_end.isoformat()} with {len(events_in_window)} events."
                )
                feature_record = process_window(
                    events_in_window,
                    prev_window_components,
                    seen_templates,
                    recent_event_counts,
                    latency_log_writer,
                )
                if feature_record and "_internal" in feature_record:
                    prev_window_components = feature_record["_internal"].get("components", set())
                    seen_templates.update(feature_record["_internal"].get("templates", set()))
                    recent_event_counts.append(feature_record["event_count"])

            # slide window
            next_window_start += timedelta(seconds=window_stride_sec)

            # prune buffer (events older than new window start)
            while events_buffer and events_buffer[0]["_internal"]["original_ts"] < next_window_start:
                events_buffer.popleft()

    # Flush remaining events as a final window
    if events_buffer:
        logging.info(f"Processing final window with {len(events_buffer)} events.")
        process_window(
            list(events_buffer),
            prev_window_components,
            seen_templates,
            recent_event_counts,
            latency_log_writer,
        )

    if latency_file:
        latency_file.close()

    logging.info("Finished feature generation.")

# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate windowed features from a log stream.")
    parser.add_argument(
        "--window-size-sec",
        type=int,
        default=60,
        help="Time window size in seconds for feature aggregation.",
    )
    parser.add_argument(
        "--window-stride-sec",
        type=float,
        default=1.0,
        help="Stride of the sliding window in seconds.",
    )
    parser.add_argument(
        "--latency-log-file",
        type=str,
        default=None,
        help="Path to a CSV file to log latency metrics (window_end_ts,lat_ms).",
    )
    parser.add_argument(
        "--schema-path",
        type=str,
        default=None,
        help="Path to the JSON schema for parsed events. Defaults to parsed_log.schema.json next to this file.",
    )
    args = parser.parse_args()

    stream_processor(
        window_size_sec=args.window_size_sec,
        window_stride_sec=args.window_stride_sec,
        latency_log_file=args.latency_log_file,
        schema_path=args.schema_path,
    )
