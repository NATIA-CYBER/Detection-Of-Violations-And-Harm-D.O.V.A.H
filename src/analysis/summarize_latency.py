import argparse
import pandas as pd

def summarize_latency(file_path):
    """Calculates and prints summary statistics for latency data."""
    try:
        df = pd.read_csv(file_path)

        # Ensure the column exists
        if 'p95_feature_ms' not in df.columns:
            print("Error: 'p95_feature_ms' column not found in the CSV file.")
            return

        # Calculate summary statistics
        summary = df['p95_feature_ms'].describe(percentiles=[.5, .9, .95, .99])

        print("Latency Summary Statistics for 'p95_feature_ms':")
        print(summary)

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Calculate summary statistics for latency data.")
    parser.add_argument(
        "--input-file",
        type=str,
        required=True,
        help="Path to the CSV file containing latency stats."
    )
    args = parser.parse_args()
    summarize_latency(args.input_file)
