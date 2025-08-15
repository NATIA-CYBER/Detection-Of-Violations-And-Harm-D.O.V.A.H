# DOVAH Command Reference

## Single Entrypoint Command
Run the complete DOVAH pipeline with a single command:

```bash
python -m src.eval.run_eval \
  --input-path /path/to/hdfs/logs \
  --output-dir ./results \
  --config config.yml
```

## Required Environment Variables
Set these in `.env` file or environment:
```bash
# API Keys
NVD_API_KEY=xxx        # For CVE data
EPSS_API_KEY=xxx       # For EPSS scores

# Pseudonymization
HMAC_KEY=xxx          # For consistent hashing

# Processing
MAX_WORKERS=4         # Parallel processing
CACHE_DIR=.cache     # Template/CVE cache
```

## Input Requirements
- HDFS logs in JSONL format
- One event per line
- UTC timestamps (RFC3339)
- Required fields per schema

## Output Structure
```
results/
├── drift/
│   ├── psi_table.csv        # Feature drift scores
│   └── drift_report.json    # Detailed drift analysis
├── stats/
│   ├── component_dist.png   # Component distribution
│   ├── severity_dist.png    # Severity distribution
│   └── volume_spikes.png    # Volume anomalies
├── security/
│   ├── cve_matches.json     # Found CVEs
│   ├── epss_scores.csv      # EPSS risk scores
│   └── kev_matches.json     # KEV entries
└── templates/
    ├── template_cache.json  # Drain3 templates
    └── clusters.csv         # Template clusters
```

## Configuration Options
In `config.yml`:
```yaml
drift:
  window_size: 7d           # Week-over-week comparison
  min_samples: 1000         # Min samples for drift
  psi_threshold: 0.2        # Drift alert threshold

template:
  sim_threshold: 0.5        # Template clustering
  max_clusters: 100         # Max template groups

security:
  min_epss: 0.5            # Min EPSS score to report
  kev_only: false          # Only match KEV CVEs

privacy:
  scrub_ips: true          # Scrub IP addresses
  host_salt: ""            # Extra salt for hosts
```

## Examples

1. Basic run with defaults:
```bash
python -m src.eval.run_eval --input-path logs/
```

2. Custom config and output:
```bash
python -m src.eval.run_eval \
  --input-path logs/ \
  --output-dir results/ \
  --config custom_config.yml
```

3. Debug mode with verbose output:
```bash
python -m src.eval.run_eval \
  --input-path logs/ \
  --debug
```

## Error Handling
- Invalid timestamps -> Skipped with warning
- Missing fields -> Error if required
- API failures -> Cached data used
- Invalid config -> Validation error
