import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExportConfig(BaseModel):
    db_url: str = os.getenv('POSTGRES_URL', 'postgresql://dovah:dovah@localhost:5432/dovah')
    export_dir: Path = Path('data/exports')
    signing_key: bytes = os.getenv('DOVAH_SIGNING_KEY', '').encode()

class EvidenceExporter:
    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig()
        self.engine = create_engine(self.config.db_url)
        self.config.export_dir.mkdir(parents=True, exist_ok=True)
    
    def fetch_detection(self, detection_id: str) -> Optional[Dict]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                    SELECT d.*, w.*, e.epss_score, k.vulnerability_name as kev_name
                    FROM detections d
                    LEFT JOIN window_features w ON d.window_id = w.id
                    LEFT JOIN epss e ON d.cve_id = e.cve_id
                    LEFT JOIN kev k ON d.cve_id = k.cve_id
                    WHERE d.id = :detection_id
                    """),
                    {"detection_id": detection_id}
                )
                row = result.first()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching detection {detection_id}: {e}")
            return None
    
    def fetch_evidence(self, detection_id: str) -> Dict:
        detection = self.fetch_detection(detection_id)
        if not detection:
            raise ValueError(f"Detection {detection_id} not found")
            
        evidence = {
            "detection": {
                "id": detection_id,
                "timestamp": detection["timestamp"].isoformat(),
                "score": detection["score"],
                "model_version": detection["model_version"]
            },
            "window_features": {
                "event_count": detection["event_count"],
                "unique_components": detection["unique_components"],
                "error_ratio": detection["error_ratio"],
                "template_entropy": detection["template_entropy"],
                "component_entropy": detection["component_entropy"]
            },
            "threat_intel": {
                "epss_score": detection["epss_score"],
                "kev_name": detection["kev_name"]
            },
            "metadata": {
                "export_time": datetime.datetime.utcnow().isoformat(),
                "schema_version": "1.0.0"
            }
        }
        return evidence
    
    def sign_evidence(self, evidence: Dict) -> str:
        if not self.config.signing_key:
            raise ValueError("DOVAH_SIGNING_KEY not set")
            
        evidence_bytes = json.dumps(evidence, sort_keys=True).encode()
        signature = hmac.new(
            self.config.signing_key,
            evidence_bytes,
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode()
    
    def export_detection(self, detection_id: str, format: str = 'json') -> Path:
        evidence = self.fetch_evidence(detection_id)
        signature = self.sign_evidence(evidence)
        
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if format == 'json':
            output = {
                "evidence": evidence,
                "signature": signature
            }
            output_path = self.config.export_dir / f"evidence_{detection_id}_{timestamp}.json"
            with open(output_path, 'w') as f:
                json.dump(output, f, indent=2)
                
        elif format == 'csv':
            df = pd.json_normalize(evidence)
            output_path = self.config.export_dir / f"evidence_{detection_id}_{timestamp}.csv"
            df.to_csv(output_path, index=False)
            
            # Save signature separately
            sig_path = output_path.with_suffix('.sig')
            with open(sig_path, 'w') as f:
                f.write(signature)
                
        else:
            raise ValueError(f"Unsupported format: {format}")
            
        logger.info(f"Exported evidence for detection {detection_id} to {output_path}")
        return output_path

def main():
    try:
        exporter = EvidenceExporter()
        
        # Example: export latest detection
        with exporter.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id FROM detections ORDER BY timestamp DESC LIMIT 1")
            )
            if detection_id := result.scalar():
                exporter.export_detection(detection_id, format='json')
                exporter.export_detection(detection_id, format='csv')
            else:
                logger.warning("No detections found to export")
                
    except Exception as e:
        logger.error(f"Evidence export failed: {e}")
        raise

if __name__ == "__main__":
    main()
