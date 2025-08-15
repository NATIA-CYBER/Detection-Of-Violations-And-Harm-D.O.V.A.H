# DOVAH Sample Data Bundle

This bundle contains minimal sample data to test and validate the DOVAH pipeline.

## Contents

### HDFS Logs (`hdfs/`)
- `sample.jsonl`: Pre-processed JSONL format logs
- `sample.log`: Raw HDFS logs for testing ingestion
- Expected: 100 events, 3 templates, all severity levels

### Security Data (`security/`)
- `epss_scores.csv`: EPSS score sample (5 CVEs)
- `kev_entries.json`: KEV catalog sample (3 CVEs)
- Coverage: 3 common CVEs between EPSS and KEV

### Time Series (`timeseries/`)
- `week1.jsonl`: Baseline week
- `week2.jsonl`: Current week for drift analysis
- Contains known distribution changes for testing

## Usage
```bash
# Run pipeline on sample data
python -m src.eval.run_eval \
  --input-path sample_data/hdfs/sample.jsonl \
  --output-dir results/
```

## Expected Results
- 3 template clusters
- 2 volume spikes
- 1 significant drift in ERROR ratio
- 3 high-risk CVEs detected
