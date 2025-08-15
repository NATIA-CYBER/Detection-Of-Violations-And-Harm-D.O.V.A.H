# HDFS Log Data Card

## Schema
- `ts` (datetime): Event timestamp in RFC3339 UTC format
- `host` (str): Pseudonymized source host identifier
- `component` (str): HDFS component (NameNode, DataNode, etc.)
- `template_id` (int): Log template ID from Drain clustering
- `session_id` (str): Pseudonymized session identifier
- `level` (str, optional): Log level (INFO, WARN, ERROR)
- `labels` (dict): Additional metadata including CVEs
- `schema_ver` (str): Schema version

## Privacy Controls
- Host identifiers are pseudonymized using HMAC-SHA256
- Session IDs are derived from host+date and pseudonymized
- PII is scrubbed using regex patterns:
  - Emails
  - API tokens/keys
  - AWS secrets
  - Passwords
  - Private keys
  - IP addresses (optional)

## Known Issues
- Clock skew between hosts is corrected but may have up to 1s residual error
- Template extraction may group similar but distinct messages (entropy: 1.10 bits)
- CVE detection relies on standard format (CVE-YYYY-NNNNN)
- Log level distribution shows predominant INFO (73.3%) with WARN (13.3%) and ERROR (13.3%)
- Template clustering identifies 3 main message patterns
- Volume spikes detected using 3σ threshold for hourly bins

## Retention
- Raw logs: 30 days
- Processed events: 1 year
- Template cache: Persistent (3 templates in current dataset)
- PII scrubbed data: No retention limit
- EPSS scores: Daily refresh
- KEV data: Weekly refresh

## Cross-Reference Analysis
- EPSS coverage: 5 CVEs total
- KEV coverage: 3 CVEs total
- Common vulnerabilities: 3 CVEs
- High-risk focus: CVE-2022-41082, CVE-2021-44228, CVE-2021-26855
