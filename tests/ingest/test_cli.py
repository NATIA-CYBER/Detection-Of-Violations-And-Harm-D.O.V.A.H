"""Tests for CLI functionality of data fetchers."""
import json
from pathlib import Path
import pytest
from datetime import datetime

from src.ingest.epss_fetch import main as epss_main
from src.ingest.kev_fetch import main as kev_main
from src.stream.job import main as job_main

def test_epss_fetch_cli(tmp_path: Path, sample_epss_data, monkeypatch):
    """Test EPSS fetcher CLI."""
    data_dir = tmp_path / "data" / "epss"
    data_dir.mkdir(parents=True)
    
    # Mock requests.get
    class MockResponse:
        def __init__(self, text):
            self.text = text
            
    def mock_get(*args, **kwargs):
        return MockResponse(sample_epss_data)
    
    monkeypatch.setattr("requests.get", mock_get)
    
    # Run CLI
    today = datetime.now().strftime("%Y-%m-%d")
    epss_main(["--output-dir", str(data_dir)])
    
    # Verify output
    output_file = data_dir / f"{today}.csv"
    assert output_file.exists()
    content = output_file.read_text()
    assert "CVE-2023-1234,0.95123,0.99876" in content

def test_kev_fetch_cli(tmp_path: Path, sample_kev_data, monkeypatch):
    """Test KEV fetcher CLI."""
    data_dir = tmp_path / "data" / "kev"
    data_dir.mkdir(parents=True)
    
    # Mock requests.get
    class MockResponse:
        def __init__(self, json_data):
            self._json = json_data
            
        def json(self):
            return self._json
            
    def mock_get(*args, **kwargs):
        return MockResponse(sample_kev_data)
    
    monkeypatch.setattr("requests.get", mock_get)
    
    # Run CLI
    today = datetime.now().strftime("%Y-%m-%d")
    kev_main(["--output-dir", str(data_dir)])
    
    # Verify output
    json_file = data_dir / f"{today}.json"
    csv_file = data_dir / f"{today}.csv"
    assert json_file.exists()
    assert csv_file.exists()
    
    # Check JSON content
    content = json.loads(json_file.read_text())
    assert content["vulnerabilities"][0]["cveID"] == "CVE-2023-1234"
    
    # Check CSV content
    csv_content = csv_file.read_text()
    assert "CVE-2023-1234,Critical RCE,2023-08-14" in csv_content

def test_stream_job_cli(tmp_path: Path, sample_hdfs_logs):
    """Test streaming job CLI."""
    # Create input file
    input_file = tmp_path / "logs_small.jsonl"
    with open(input_file, "w") as f:
        for log in sample_hdfs_logs:
            f.write(json.dumps({"message": log}) + "\n")
    
    # Run CLI with stdout sink
    try:
        job_main([
            "--input", str(input_file),
            "--sink", "stdout",
            "--duration", "1s"
        ])
    except SystemExit as e:
        assert e.code == 0  # Clean exit
        
    # TODO: Add assertions for stdout capture when needed
