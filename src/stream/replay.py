"""Replays parsed logs to simulate a real-time event stream.

Usage:
    python -m src.stream.replay \
        --input-file sample_data/hdfs/sample.jsonl \
        --eps 100 \
        --run-duration-sec 900
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,  # Log to stderr to keep stdout clean for piped data
)

def replay_events(
    input_file: str, eps: int, warmup_sec: int, run_duration_sec: int, num_events: int = None
) -> None:
    """Replays events from a file at a target EPS.

    Args:
        input_file: Path to the JSONL file with parsed logs.
        eps: Target events per second.
        warmup_sec: Duration of the warm-up period in seconds.
        run_duration_sec: Total duration of the steady-state run in seconds.
    """
    logging.info(f"Starting replay from '{input_file}' at {eps} EPS.")
    
    # Read all events into memory
    try:
        with open(input_file, 'r') as f:
            events = [json.loads(line) for line in f]
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Failed to read or parse {input_file}: {e}")
        return

    if not events:
        logging.warning("Input file is empty. Nothing to replay.")
        return

    logging.info(f"Loaded {len(events)} events.")

    # --- Warm-up Period ---
    logging.info(f"Starting warm-up for {warmup_sec} seconds...")
    start_time = time.time()
    while time.time() - start_time < warmup_sec:
        # During warm-up, you might pre-load caches or run other prep tasks.
        # For now, we just wait.
        time.sleep(1)
    logging.info("Warm-up complete.")

    # --- Steady-State Run ---
    logging.info(f"Starting steady-state run for {run_duration_sec} seconds...")
    start_run_time = time.time()
    total_run_duration = time.time() - start_run_time
    event_index = 0
    total_events_sent = 0
    sleep_interval = 1.0 / eps

    while True:
        if num_events is not None and total_events_sent >= num_events:
            logging.info(f"Sent specified {num_events} events. Stopping.")
            break

        if num_events is None and total_run_duration >= run_duration_sec:
            logging.info(f"Run duration of {run_duration_sec}s reached. Stopping.")
            break
        loop_start_time = time.time()

        # Get the next event, looping back to the start if we reach the end
        event = events[event_index % len(events)].copy()  # Use a copy to avoid modifying the original
        event_index += 1

        # Add a replay timestamp and update the event timestamp to be monotonic
        now = datetime.now(timezone.utc)
        event['replay_ts'] = now.isoformat()
        event['timestamp'] = now.isoformat()

        # Write event to stdout
        try:
            sys.stdout.write(json.dumps(event) + '\n')
            sys.stdout.flush()
            total_events_sent += 1
        except BrokenPipeError:
            logging.warning("Broken pipe. Consumer has likely exited. Shutting down.")
            break

        # Sleep to maintain the target EPS rate
        loop_end_time = time.time()
        elapsed = loop_end_time - loop_start_time
        time.sleep(max(0, sleep_interval - elapsed))
        total_run_duration = time.time() - start_run_time

    logging.info(f"Replay finished. Sent {total_events_sent} events in {total_run_duration:.2f} seconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay parsed logs to simulate a stream.")
    parser.add_argument(
        "--input-file",
        type=str,
        required=True,
        help="Path to the JSONL file containing parsed logs."
    )
    parser.add_argument(
        "--eps",
        type=int,
        default=100,
        help="Target events per second."
    )
    parser.add_argument(
        "--warmup-sec",
        type=int,
        default=10,
        help="Warm-up period in seconds before the main run."
    )
    parser.add_argument(
        "--run-duration-sec",
        type=int,
        default=900,  # 15 minutes
        help="Total duration of the steady-state replay in seconds."
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=None,
        help="Total number of events to replay. Overrides run-duration-sec if set."
    )
    args = parser.parse_args()

    replay_events(
        input_file=args.input_file,
        eps=args.eps,
        warmup_sec=args.warmup_sec,
        run_duration_sec=args.run_duration_sec,
        num_events=args.num_events,
    )
