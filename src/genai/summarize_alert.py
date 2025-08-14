"""Alert summarization with allow-list filtering and evidence."""
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

class AlertSummarizer:
    def __init__(self, db_url: str):
        try:
            self.engine: Engine = create_engine(db_url)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
        
    def fetch_recent_alerts(
        self,
        hours: int = 24,
        min_score: float = 0.8,
        limit: int = 100
    ) -> List[Dict]:
        """Fetch recent high-scoring alerts with window features and threat intel."""
        query = text("""
            SELECT 
                d.id as detection_id,
                d.timestamp,
                d.score as anomaly_score,
                d.session_id,
                w.event_count,
                w.unique_components,
                w.error_ratio,
                w.template_entropy,
                w.component_entropy,
                e.epss_score,
                k.vulnerability_name as kev_name,
                k.description as kev_description,
                d.model_version
            FROM detections d
            JOIN window_features w ON d.window_id = w.id
            LEFT JOIN epss e ON d.cve_id = e.cve_id
            LEFT JOIN kev k ON d.cve_id = k.cve_id
            WHERE d.timestamp >= :start_time
            AND d.score >= :min_score
            ORDER BY d.score DESC, d.timestamp DESC
            LIMIT :limit
        """)
        
        with self.engine.connect() as conn:
            try:
                result = conn.execute(
                    query,
                    {
                        "start_time": datetime.utcnow() - timedelta(hours=hours),
                        "min_score": min_score,
                        "limit": limit
                    }
                )
                return [dict(row) for row in result]
            except Exception as e:
                logger.error(f"Failed to fetch alerts: {e}")
                return []

    def generate_summary(self, alert: Dict) -> str:
        """Generate a human-readable summary of the alert with key factors."""
        summary_parts = [
            f"Alert detected at {alert['timestamp'].isoformat()}Z",
            f"Anomaly score: {alert['anomaly_score']:.3f}",
            "\nKey factors:",
            f"- {alert['event_count']} events in window",
            f"- {alert['unique_components']} unique components",
            f"- Error ratio: {alert['error_ratio']:.2%}",
            f"- Template entropy: {alert['template_entropy']:.2f}",
            f"- Component entropy: {alert['component_entropy']:.2f}"
        ]
        
        if alert.get('epss_score'):
            summary_parts.append(
                f"\nEPSS exploit probability: {alert['epss_score']:.1%}"
            )
            
        if alert.get('kev_name'):
            summary_parts.extend([
                f"\nKnown Exploited Vulnerability:",
                f"- {alert['kev_name']}",
                f"- {alert.get('kev_description', 'No description available')}"
            ])
            
        return "\n".join(summary_parts)

    def filter_allowed_terms(
        self,
        summary: str,
        allow_list: Optional[List[str]] = None
    ) -> str:
        """Filter summary to only include allowed terms."""
        if not allow_list:
            return summary
            
        filtered_lines = []
        for line in summary.split("\n"):
            if any(term in line for term in allow_list):
                filtered_lines.append(line)
                
        return "\n".join(filtered_lines) if filtered_lines else summary
