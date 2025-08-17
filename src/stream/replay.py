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
import random
from datetime import datetime, timezone
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,  # Log to stderr to keep stdout clean for piped data
)

def _stream_events(input_file: str, do_loop: bool, do_shuffle: bool):
    """Yields events from a file, with options for looping and shuffling."""
    events = []
    if do_shuffle:
        logging.info("Shuffling requires loading the entire file into memory.")
        try:
            with open(input_file, 'r') as f:
                events = [json.loads(line) for line in f if line.strip()]
            random.shuffle(events)
            logging.info(f"Loaded and shuffled {len(events)} events.")
        except (IOError, FileNotFoundError) as e:
            logging.error(f"Cannot read {input_file} for shuffling: {e}")
            return  # Stop if the file is unreadable

    while True:
        if do_shuffle:
            for event in events:
                yield event
        else:
            try:
                with open(input_file, 'r') as f:
                    for line in f:
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            logging.warning(f"Skipping malformed JSON line: {line.strip()}")
                            continue
            except (IOError, FileNotFoundError) as e:
                logging.error(f"Cannot read {input_file}: {e}")
                break  # Stop if the file is unreadable
        
        if not do_loop:
            break


def replay_events(
    input_file: str,
    eps: int,
    warmup_sec: int,
    run_duration_sec: int,
    num_events: Optional[int] = None,
    do_loop: bool = False,
    do_shuffle: bool = False,
    jitter: float = 0.1,
    warmup_eps_fraction: float = 0.2,
) -> None:
    """Replays events from a file at a target EPS.

    Args:
        input_file: Path to the JSONL file with parsed logs.
        eps: Target events per second.
        warmup_sec: Duration of the warm-up period in seconds.
        run_duration_sec: Total duration of the steady-state run in seconds.
    """
    if eps <= 0:
        logging.error("EPS must be a positive number.")
        return

    logging.info(f"Starting replay from '{input_file}' at {eps} EPS.")
    event_stream = _stream_events(input_file, do_loop, do_shuffle)

    # --- Warm-up Period ---
    warmup_eps = int(eps * warmup_eps_fraction)
    logging.info(f"Starting warm-up for {warmup_sec} seconds at {warmup_eps} EPS...")
    warmup_deadline = time.time() + warmup_sec
    warmup_events_sent = 0
    if warmup_eps > 0:
        warmup_sleep_interval = 1.0 / warmup_eps
        for event in event_stream:
            if time.time() >= warmup_deadline:
                break
            loop_start_time = time.time()

            # Write event to stdout
            try:
                sys.stdout.write(json.dumps(event) + '\n')
                sys.stdout.flush()
                warmup_events_sent += 1
            except BrokenPipeError:
                logging.warning("Broken pipe during warm-up. Shutting down.")
                return

            # Sleep to maintain the target EPS rate
            elapsed = time.time() - loop_start_time
            time.sleep(max(0, warmup_sleep_interval - elapsed))
    else:
        time.sleep(warmup_sec) # If warmup EPS is 0, just wait

    logging.info(f"Warm-up complete. Sent {warmup_events_sent} events.")

    # --- Steady-State Run ---
    # If not looping, the event_stream generator was consumed by the warm-up.
    # We create a new one for the main run. If looping, we continue with the same generator.
    if not do_loop:
        event_stream = _stream_events(input_file, do_loop, do_shuffle)

    logging.info(f"Starting steady-state run for {run_duration_sec} seconds...")
    start_run_time = time.time()
    deadline = start_run_time + run_duration_sec
    total_events_sent = 0
    base_sleep_interval = 1.0 / eps
    # Jitter is a fraction of the base sleep interval
    jitter_amount = base_sleep_interval * jitter

    for event in event_stream:
        if num_events is not None and total_events_sent >= num_events:
            logging.info(f"Sent specified {num_events} events. Stopping.")
            break

        if num_events is None and time.time() >= deadline:
            logging.info(f"Run duration of {run_duration_sec}s reached. Stopping.")
            break

        loop_start_time = time.time()

        # Preserve original timestamp and add a replay timestamp
        event.setdefault('orig_ts', event.get('timestamp'))
        now = datetime.now(timezone.utc)
        event['replay_ts'] = now.isoformat()
        event['timestamp'] = now.isoformat()  # Update timestamp for monotonic stream

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
        sleep_interval = random.uniform(
            base_sleep_interval - jitter_amount,
            base_sleep_interval + jitter_amount
        )
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
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Loop the dataset from the beginning upon reaching the end."
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle the dataset before replaying. Requires loading the entire file into memory."
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=0.1,
        help="Fraction of sleep interval to apply as random jitter (0 to 1)."
    )
    parser.add_argument(
        "--warmup-eps-fraction",
        type=float,
        default=0.2,
        help="Fraction of target EPS to emit during warm-up."
    )
    args = parser.parse_args()

    replay_events(
        input_file=args.input_file,
        eps=args.eps,
        warmup_sec=args.warmup_sec,
        run_duration_sec=args.run_duration_sec,
        num_events=args.num_events,
        do_loop=args.loop,
        do_shuffle=args.shuffle,
        jitter=args.jitter,
        warmup_eps_fraction=args.warmup_eps_fraction,
    )
