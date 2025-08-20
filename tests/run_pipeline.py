import subprocess
import sys
from pathlib import Path

def main():
    repo_root = Path(__file__).resolve().parents[1]
    input_file = repo_root / "sample_data" / "hdfs" / "sample.jsonl"
    output_file = Path("/tmp/features.jsonl")

   
    replay_cmd = [
        sys.executable, "-m", "src.stream.replay",
        "--input-file", str(input_file),
        "--eps", "80",
        "--warmup-sec", "3",
        "--run-duration-sec", "20",
        "--loop"
    ]
    features_cmd = [
        sys.executable, "-m", "src.stream.features",
        "--window-size-sec", "5"
    ]

    print("Starting producer:", " ".join(replay_cmd))
    producer = subprocess.Popen(replay_cmd, stdout=subprocess.PIPE, text=True)

    print(f"Starting consumer → {output_file}:", " ".join(features_cmd))
    with output_file.open("w") as f_out:
        consumer = subprocess.Popen(features_cmd, stdin=producer.stdout, stdout=f_out, text=True)


    if producer.stdout:
        producer.stdout.close()

    
    prod_rc = producer.wait()
    cons_rc = consumer.wait()

    print(f"Pipeline finished. rc: replay={prod_rc} features={cons_rc}")
    sys.exit(0 if prod_rc == 0 and cons_rc == 0 else 1)

if __name__ == "__main__":
    main()
