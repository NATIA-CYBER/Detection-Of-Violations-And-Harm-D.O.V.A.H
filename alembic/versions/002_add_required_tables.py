"""Add required tables for core functionality

Revision ID: 002
Revises: 001
Create Date: 2025-08-14 17:54:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create detections table
    op.execute("""
    CREATE TABLE IF NOT EXISTS detections (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        ts TIMESTAMP WITH TIME ZONE NOT NULL,
        session_id TEXT NOT NULL,
        window_id INTEGER REFERENCES window_features(id),
        score FLOAT NOT NULL,
        source TEXT NOT NULL,
        model_version TEXT NOT NULL,
        cve_id TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ts, session_id)
    )
    """)
    
    # Create EPSS scores table
    op.execute("""
    CREATE TABLE IF NOT EXISTS epss (
        cve_id TEXT PRIMARY KEY,
        epss_score FLOAT NOT NULL,
        percentile FLOAT NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Create KEV table
    op.execute("""
    CREATE TABLE IF NOT EXISTS kev (
        cve_id TEXT PRIMARY KEY,
        vulnerability_name TEXT,
        description TEXT,
        date_added TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Create SHAP explanations table
    op.execute("""
    CREATE TABLE IF NOT EXISTS shap_explanations (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        detection_id UUID REFERENCES detections(id),
        feature_importance JSONB NOT NULL,
        feature_effects JSONB NOT NULL,
        base_score FLOAT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(detection_id)
    )
    """)
    
    # Create indexes
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_detections_window 
    ON detections(window_id)
    """)
    
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_detections_session_ts 
    ON detections(session_id, ts DESC)
    """)
    
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_epss_score 
    ON epss(epss_score DESC)
    """)
    
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_kev_date 
    ON kev(date_added DESC)
    """)

def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS shap_explanations")
    op.execute("DROP TABLE IF EXISTS detections")
    op.execute("DROP TABLE IF EXISTS kev")
    op.execute("DROP TABLE IF EXISTS epss")
