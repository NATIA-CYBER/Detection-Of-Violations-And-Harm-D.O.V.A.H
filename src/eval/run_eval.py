"""Evaluation harness for DOVAH anomaly detection."""
import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import precision_score, recall_score

def load_jsonl(path: str) -> List[Dict]:
    """Load JSONL file."""
    with open(path) as f:
        return [json.loads(line) for line in f]

def calculate_metrics(
    predictions: List[Dict],
    labels: List[Dict],
    k: Optional[int] = None,
    events_per_window: int = 1000
) -> Dict[str, float]:
    """Calculate evaluation metrics.
    
    Args:
        predictions: List of prediction dicts with session_id, ts, score
        labels: List of label dicts with session_id, ts
        k: Optional k for precision@k
        events_per_window: Number of events per window for FP/1k calculation
        
    Returns:
        Dict of metrics
    """
    # Convert to dataframes
    pred_df = pd.DataFrame(predictions)
    label_df = pd.DataFrame(labels)
    
    # Align predictions with labels
    merged = pd.merge(
        pred_df,
        label_df,
        on=["session_id", "ts"],
        how="left",
        indicator=True
    )
    merged["label"] = (merged["_merge"] == "both").astype(int)
    
    # Basic metrics
    metrics = {
        "precision": precision_score(merged["label"], merged["score"] > 0.5),
        "recall": recall_score(merged["label"], merged["score"] > 0.5),
    }
    
    # Precision@k
    if k is not None:
        top_k = merged.nlargest(k, "score")
        metrics[f"precision@{k}"] = top_k["label"].mean()
    
    # FP/1k events
    total_windows = len(merged)
    total_events = total_windows * events_per_window
    false_positives = ((merged["score"] > 0.5) & (merged["label"] == 0)).sum()
    metrics["fp_per_1k"] = (false_positives / total_events) * 1000
    
    return metrics

def main():
    parser = argparse.ArgumentParser(description="Evaluate anomaly detection")
    parser.add_argument(
        "--predictions",
        type=str,
        required=True,
        help="Path to predictions JSONL"
    )
    parser.add_argument(
        "--labels",
        type=str,
        required=True,
        help="Path to labels JSONL"
    )
    parser.add_argument(
        "--k",
        type=int,
        default=100,
        help="k for precision@k"
    )
    args = parser.parse_args()
    
    # Load data
    start_time = time.time()
    predictions = load_jsonl(args.predictions)
    labels = load_jsonl(args.labels)
    
    # Calculate metrics
    metrics = calculate_metrics(predictions, labels, k=args.k)
    end_time = time.time()
    
    # Print results
    print("\nResults:")
    print(f"Precision: {metrics['precision']:.3f}")
    print(f"Recall: {metrics['recall']:.3f}")
    print(f"Precision@{args.k}: {metrics[f'precision@{args.k}']:.3f}")
    print(f"FP/1k events: {metrics['fp_per_1k']:.1f}")
    print(f"\np95_ms={int((end_time - start_time) * 1000)}")

if __name__ == "__main__":
    main()
