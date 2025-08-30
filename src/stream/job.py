# src/stream/job.py
from __future__ import annotations
"""
DOVAH streaming launcher with THREE modes:

- python (default): pure-Python replay → features (Day-3/4 golden path). No Flink, no jars.
- smoke: quick PyFlink connector factory check (Kafka/JDBC/Postgres). Prints clear “MISSING …” if jars are absent.
- flink: guarded Flink stub that first runs the smoke; if OK, you extend it with real logic later.

Why: Day-4 must never be blocked by PyFlink. This file guarantees it.

Run examples (from repo root):
  python -m src.stream.job --mode python \
    --input sample_data/hdfs/sample.jsonl \
    --window-size-sec 10 --window-stride-sec 1 \
    --latency-log-file reports/phase4/metrics/latency.csv \
    --out reports/phase4/metrics/features.jsonl \
    --eps 100 --warmup-sec 10 --run-duration-sec 120

  # Optional: diagnose Flink jars
  python -m src.stream.job --mode smoke

  # Optional: try Flink (only after smoke is green)
  python -m src.stream.job --mode flink

Environment:
  DOVAH_USE_FLINK=1   -> selects flink unless --mode given.
  Jars expected in ./jars/ when using smoke/flink:
    - flink-sql-connector-kafka-1.18.1.jar
    - flink-connector-jdbc-3.1.2-1.18.jar
    - postgresql-42.7.3.jar
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List


# -----------------------------
# Pure-Python pipeline (safe)
# -----------------------------
def _run_python_pipeline(
    input_path: str,
    window_size_sec: int,
    window_stride_sec: int,
    latency_log_file: str,
    out_path: str,
    eps: int,
    warmup_sec: int,
    run_duration_sec: int,
) -> int:
    """
    Same composition as your Makefile:
      replay | features > features.jsonl
    Uses the current interpreter and -m so src.* imports always resolve.
    """
    py = sys.executable
    out_fp = Path(out_path)
    out_fp.parent.mkdir(parents=True, exist_ok=True)

    replay_cmd = [
        py, "-m", "src.stream.replay",
        "--input-file", input_path,
        "--eps", str(eps),
        "--warmup-sec", str(warmup_sec),
        "--run-duration-sec", str(run_duration_sec),
    ]
    features_cmd = [
        py, "-m", "src.stream.features",
        "--window-size-sec", str(window_size_sec),
        "--window-stride-sec", str(window_stride_sec),
        "--latency-log-file", latency_log_file,
    ]

    print("[job] python pipeline:")
    print("  ", " ".join(replay_cmd))
    print("   |", " ".join(features_cmd))
    print("   >", out_fp)
    with subprocess.Popen(replay_cmd, stdout=subprocess.PIPE) as p1:
        with out_fp.open("wb") as fh:
            p2 = subprocess.Popen(features_cmd, stdin=p1.stdout, stdout=fh)
            p1.stdout.close()
            rc2 = p2.wait()
        rc1 = p1.wait()

    if rc1 != 0:
        print(f"[job] replay exited with {rc1}", file=sys.stderr)
        return rc1
    if rc2 != 0:
        print(f"[job] features exited with {rc2}", file=sys.stderr)
        return rc2
    print(f"[job] OK -> {out_fp}")
    return 0


# -----------------------------
# Flink helpers (optional)
# -----------------------------
def _discover_jars(jars_dir: Path) -> List[Path]:
    patterns = [
        "flink-sql-connector-kafka-*.jar",
        "flink-connector-jdbc-*-1.18.jar",
        "postgresql-*.jar",
    ]
    out: List[Path] = []
    for pat in patterns:
        out.extend(sorted(jars_dir.glob(pat)))
    return out


def _flink_smoke(jars_dir: str) -> int:
    """
    Verifies that PyFlink imports and the connector factories are discoverable.
    DOES NOT connect to Kafka/Postgres; it only checks that the factories exist.
    """
    try:
        import pyflink  # noqa: F401
        from pyflink.table import EnvironmentSettings, TableEnvironment
        from pyflink.common import Configuration
    except Exception as e:
        print("❌ PyFlink import failed:", e, file=sys.stderr)
        print("   Fix: pip install 'pyflink==1.18.1' 'jsonpickle<3'", file=sys.stderr)
        return 1

    jars = _discover_jars(Path(jars_dir))
    if not jars:
        print("❌ No connector jars found in ./jars", file=sys.stderr)
        print("   Need: flink-sql-connector-kafka-1.18.1.jar, flink-connector-jdbc-3.1.2-1.18.jar, postgresql-42.7.3.jar", file=sys.stderr)
        return 2

    uris = ";".join(f"file://{p.resolve()}" for p in jars)
    conf = Configuration()
    conf.set_string("pipeline.jars", uris)
    print("[job] pipeline.jars:", uris)

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode(), conf)

    ddl_kafka = """
    CREATE TEMPORARY TABLE _k (
      k STRING, v STRING
    ) WITH (
      'connector'='kafka',
      'topic'='demo',
      'properties.bootstrap.servers'='localhost:9092',
      'format'='json',
      'scan.startup.mode'='earliest-offset'
    )
    """
    ddl_jdbc = """
    CREATE TEMPORARY TABLE _j (
      id INT, score DOUBLE
    ) WITH (
      'connector'='jdbc',
      'url'='jdbc:postgresql://localhost:5432/dovah',
      'table-name'='detections',
      'username'='dovah',
      'password'='dovah',
      'driver'='org.postgresql.Driver'
    )
    """

    ok = True
    try:
        t_env.execute_sql(ddl_kafka)
        print("✅ Kafka connector factory present")
    except Exception as e:
        ok = False
        print("❌ Kafka factory not available:", e, file=sys.stderr)

    try:
        t_env.execute_sql(ddl_jdbc)
        print("✅ JDBC connector factory present (driver OK)")
    except Exception as e:
        ok = False
        print("❌ JDBC factory not available:", e, file=sys.stderr)

    return 0 if ok else 3


def _flink_job(jars_dir: str) -> int:
    """
    Guarded Flink entry. First ensures factories exist (smoke).
    Extend THIS function later with your actual Table API job once smoke is green.
    """
    rc = _flink_smoke(jars_dir)
    if rc != 0:
        print("[job] Flink pre-check failed; not starting streaming.", file=sys.stderr)
        return rc

    # ---- Stub you can extend: define tables and simple pass-through SQL ----
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.common import Configuration
    jars = _discover_jars(Path(jars_dir))
    conf = Configuration()
    conf.set_string("pipeline.jars", ";".join(f"file://{p.resolve()}" for p in jars))
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode(), conf)

    # Example: create a VALUES source and print sink (to prove the pipeline runs)
    t_env.execute_sql("""
    CREATE TEMPORARY TABLE src_demo (id INT, score DOUBLE) WITH ('connector'='values')
    """)
    t_env.execute_sql("""
    CREATE TEMPORARY TABLE sink_print (id INT, score DOUBLE) WITH ('connector'='print')
    """)
    t_env.execute_sql("INSERT INTO sink_print SELECT id, score FROM src_demo").wait()
    print("[job] Flink pipeline stub executed (VALUES → PRINT). Replace with real sources/sinks later.")
    return 0


# -----------------------------
# CLI
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="DOVAH stream launcher (safe by default)")
    ap.add_argument("--mode", choices=("auto", "python", "smoke", "flink"), default="auto")

    # Python pipeline args
    ap.add_argument("--input", default="sample_data/hdfs/sample.jsonl")
    ap.add_argument("--window-size-sec", type=int, default=10)
    ap.add_argument("--window-stride-sec", type=int, default=1)
    ap.add_argument("--latency-log-file", default="reports/phase4/metrics/latency.csv")
    ap.add_argument("--out", default="reports/phase4/metrics/features.jsonl")
    ap.add_argument("--eps", type=int, default=100)
    ap.add_argument("--warmup-sec", type=int, default=10)
    ap.add_argument("--run-duration-sec", type=int, default=120)

    # Flink jars dir
    ap.add_argument("--jars-dir", default="jars")
    args = ap.parse_args()

    mode = args.mode
    if mode == "auto":
        mode = "flink" if os.getenv("DOVAH_USE_FLINK") == "1" else "python"

    if mode == "python":
        return _run_python_pipeline(
            input_path=args.input,
            window_size_sec=args.window_size_sec,
            window_stride_sec=args.window_stride_sec,
            latency_log_file=args.latency_log_file,
            out_path=args.out,
            eps=args.eps,
            warmup_sec=args.warmup_sec,
            run_duration_sec=args.run_duration_sec,
        )
    if mode == "smoke":
        return _flink_smoke(args.jars_dir)
    # flink
    return _flink_job(args.jars_dir)


if __name__ == "__main__":
    raise SystemExit(main())
