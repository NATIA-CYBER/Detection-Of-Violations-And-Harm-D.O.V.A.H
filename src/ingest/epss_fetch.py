import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EPSSConfig(BaseModel):
    epss_url: str = "https://epss.cyentia.com/epss_scores-current.csv.gz"
    update_frequency: int = 24  # hours
    db_url: str = os.getenv('POSTGRES_URL', 'postgresql://dovah:dovah@localhost:5432/dovah')

class EPSSFetcher:
    def __init__(self, config: Optional[EPSSConfig] = None):
        self.config = config or EPSSConfig()
        self.engine = create_engine(self.config.db_url)
    
    def fetch_latest(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                self.config.epss_url,
                compression='gzip',
                names=['cve_id', 'epss_score', 'percentile'],
                skiprows=1
            )
            logger.info(f"Fetched {len(df)} EPSS scores")
            return df
        except Exception as e:
            logger.error(f"Error fetching EPSS data: {e}")
            return pd.DataFrame()
    
    def save_to_db(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
            
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS epss (
                        cve_id VARCHAR(20) PRIMARY KEY,
                        epss_score FLOAT NOT NULL,
                        percentile FLOAT NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                df['updated_at'] = datetime.utcnow()
                df.to_sql(
                    'epss',
                    con=conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                logger.info(f"Saved {len(df)} EPSS scores to database")
                return True
                
        except Exception as e:
            logger.error(f"Error saving EPSS data: {e}")
            return False
    
    def get_score(self, cve_id: str) -> Optional[float]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT epss_score FROM epss WHERE cve_id = :cve"),
                    {"cve": cve_id}
                )
                row = result.first()
                return float(row.epss_score) if row else None
                
        except Exception as e:
            logger.error(f"Error getting EPSS score: {e}")
            return None

def main():
    fetcher = EPSSFetcher()
    df = fetcher.fetch_latest()
    if not df.empty:
        fetcher.save_to_db(df)

if __name__ == "__main__":
    main()
