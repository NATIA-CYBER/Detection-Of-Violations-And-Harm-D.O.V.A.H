"""Schema validation utilities."""
import json
from pathlib import Path
from typing import Dict, Any

import jsonschema
import pandas as pd

def load_schema(schema_path: str) -> Dict[str, Any]:
    """Load JSON schema from file."""
    with open(schema_path) as f:
        return json.load(f)

def validate_log_entry(entry: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """Validate a single log entry against schema."""
    jsonschema.validate(instance=entry, schema=schema)

def validate_logs_df(df: pd.DataFrame, schema_path: str) -> None:
    """Validate all log entries in DataFrame against schema."""
    schema = load_schema(schema_path)
    
    # Convert DataFrame to list of dicts for validation
    records = df.to_dict(orient='records')
    
    for record in records:
        try:
            validate_log_entry(record, schema)
        except jsonschema.exceptions.ValidationError as e:
            raise ValueError(f"Log entry validation failed: {e}")
