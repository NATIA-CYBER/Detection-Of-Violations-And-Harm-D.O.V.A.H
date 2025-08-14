"""Test fixtures for DOVAH."""
import json
import pytest
from pathlib import Path
from typing import Dict, List

@pytest.fixture
def sample_epss_data() -> str:
    """Sample EPSS data for testing."""
    return '''cve,epss,percentile
CVE-2023-1234,0.95123,0.99876
CVE-2023-5678,0.75432,0.89765
CVE-2023-9012,0.12345,0.45678'''

@pytest.fixture
def sample_kev_data() -> Dict:
    """Sample KEV data for testing."""
    return {
        "title": "Known Exploited Vulnerabilities Catalog",
        "catalogVersion": "2023.08.14",
        "vulnerabilities": [
            {
                "cveID": "CVE-2023-1234",
                "vulnerabilityName": "Critical RCE",
                "dateAdded": "2023-08-14",
                "shortDescription": "Remote code execution vulnerability"
            },
            {
                "cveID": "CVE-2023-5678",
                "vulnerabilityName": "Auth Bypass",
                "dateAdded": "2023-08-13",
                "shortDescription": "Authentication bypass vulnerability"
            }
        ]
    }

@pytest.fixture
def sample_hdfs_logs() -> List[str]:
    """Sample HDFS log lines for testing."""
    return [
        '081109 203518 INFO dfs.DataNode$DataXceiver: Receiving block blk_123 src: /10.0.0.1:1234 dest: /10.0.0.2:5678',
        '081109 203519 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/test/data.txt',
        '081109 203520 INFO dfs.DataNode$DataXceiver: Served block blk_123 to /10.0.0.1'
    ]

@pytest.fixture
def test_data_dir(tmp_path: Path) -> Path:
    """Create temporary test data directory."""
    data_dir = tmp_path / "data"
    (data_dir / "epss").mkdir(parents=True)
    (data_dir / "kev").mkdir()
    (data_dir / "logs").mkdir()
    return data_dir
