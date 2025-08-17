import argparse
import pandas as pd
import sys

def summarize_latency(file_path: str, column_name: str, sla_ms: int):
    """Calculates and prints summary statistics for latency data."""
    try:
        df = pd.read_csv(file_path)
        
        # Calculate total latency if column is 'total'
        if column_name == 'total':
            if 'p95_ingest_ms' not in df.columns or 'p95_feature_ms' not in df.columns:
                print("Error: 'p95_ingest_ms' or 'p95_feature_ms' columns not found for total calculation.")
                sys.exit(1)
            df['total_latency_ms'] = df['p95_ingest_ms'] + df['p95_feature_ms']
            target_column = 'total_latency_ms'
        else:
            if column_name not in df.columns:
                print(f"Error: '{column_name}' column not found in the CSV file.")
                sys.exit(1)
            target_column = column_name

        # Calculate summary statistics
        summary = df[target_column].describe(percentiles=[.5, .9, .95, .99])
        print(f"Latency Summary Statistics for '{target_column}':")
        print(summary)

        p95 = summary.get('95%')
        if p95 is not None:
            print(f"\nP95: {p95:.1f} ms ({p95/1000:.3f} s)")
            if sla_ms and p95 > sla_ms:
                print(f"FAIL: P95 latency exceeds {sla_ms}ms SLA.")
                sys.exit(1)

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Calculate summary statistics for latency data.")
    parser.add_argument(
        "--input-file", type=str, required=True, help="Path to the CSV file containing latency stats."
    )
    parser.add_argument(
        "--column",
        type=str,
        default='total',
        help="The name of the latency column to analyze, or 'total' for p95_ingest_ms + p95_feature_ms."
    )
    parser.add_argument(
        "--sla-ms", type=int, default=2000, help="P95 Service Level Agreement in milliseconds."
    )

    args = parser.parse_args()
    summarize_latency(args.input_file, args.column, args.sla_ms)
