import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KEVConfig(BaseModel):
    kev_url: str = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
    db_url: str = os.getenv('POSTGRES_URL', 'postgresql://dovah:dovah@localhost:5432/dovah')

class KEVFetcher:
    def __init__(self, config: Optional[KEVConfig] = None):
        self.config = config or KEVConfig()
        self.engine = create_engine(self.config.db_url)
    
    def fetch_latest(self) -> pd.DataFrame:
        try:
            response = requests.get(self.config.kev_url)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data.get('vulnerabilities', []))
            
            if 'dateAdded' in df.columns:
                df['date_added'] = pd.to_datetime(df['dateAdded'])
                df.drop('dateAdded', axis=1, inplace=True)
                
            if 'vendorProject' in df.columns:
                df.rename(columns={'vendorProject': 'vendor'}, inplace=True)
                
            logger.info(f"Fetched {len(df)} KEV entries")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching KEV data: {e}")
            return pd.DataFrame()
    
    def save_to_db(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
            
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS kev (
                        cve_id VARCHAR(20) PRIMARY KEY,
                        vendor VARCHAR(255),
                        product VARCHAR(255),
                        vulnerability_name TEXT,
                        date_added TIMESTAMP,
                        description TEXT,
                        action TEXT,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                df['updated_at'] = datetime.utcnow()
                df.to_sql(
                    'kev',
                    con=conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                logger.info(f"Saved {len(df)} KEV entries to database")
                return True
                
        except Exception as e:
            logger.error(f"Error saving KEV data: {e}")
            return False
    
    def get_cves(self) -> List[str]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT cve_id FROM kev"))
                return [row.cve_id for row in result]
                
        except Exception as e:
            logger.error(f"Error getting KEV CVEs: {e}")
            return []
    
    def get_details(self, cve_id: str) -> Optional[Dict]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT * FROM kev WHERE cve_id = :cve"),
                    {"cve": cve_id}
                )
                row = result.first()
                return dict(row) if row else None
                
        except Exception as e:
            logger.error(f"Error getting KEV details: {e}")
            return None

def main():
    fetcher = KEVFetcher()
    df = fetcher.fetch_latest()
    if not df.empty:
        fetcher.save_to_db(df)

if __name__ == "__main__":
    main()
