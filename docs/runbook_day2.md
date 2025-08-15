# Day 2 EDA & Analysis Runbook

## Quick Start
```bash
# Run full EDA pipeline
make eda

# Run individual components
python -m src.analysis.run_analysis --input data/sample.jsonl --out reports/eda
python -m src.eval.run_eval --baseline data/week1.jsonl --current data/week2.jsonl
```

## Outputs
All outputs are generated in `reports/eda/`:

### Visualizations
- `volume_spikes.png`: 5-min volume with 3σ threshold
- `level_distribution.png`: INFO/WARN/ERROR distribution
- `component_distribution.png`: Top-K components
- `template_frequencies.png`: Template patterns

### Analysis Files
- `spike_stats.json`: Volume anomaly thresholds
- `distribution_stats.json`: Level and component stats
- `template_stats.json`: Template entropy metrics
- `psi_table.csv`: Population stability metrics
- `ks_summary.json`: KS test results
- `cve_enriched.csv`: CVEs with EPSS/KEV data
- `cve_summary.json`: CVE analysis summary

## Schema Validation
All outputs are validated against `parsed_log.schema.json`

## Sample Data
Test files in `tests/data/`:
- `hdfs/sample.jsonl`: Example HDFS logs
- `epss/epss_latest.csv`: EPSS scores
- `kev/cisa_kev.csv`: KEV data
