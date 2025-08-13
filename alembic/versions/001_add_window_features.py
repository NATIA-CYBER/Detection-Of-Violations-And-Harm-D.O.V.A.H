"""Add window_features table

Revision ID: 001
Revises: 
Create Date: 2025-08-13 03:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create window_features table with IF NOT EXISTS for idempotency
    op.execute("""
    CREATE TABLE IF NOT EXISTS window_features (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMP WITH TIME ZONE NOT NULL,
        session_id TEXT NOT NULL,
        host TEXT NOT NULL,
        window_size INTEGER NOT NULL,
        window_slide INTEGER NOT NULL,
        event_count INTEGER NOT NULL,
        unique_components INTEGER NOT NULL,
        error_ratio FLOAT NOT NULL,
        template_entropy FLOAT NOT NULL,
        component_entropy FLOAT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ts, session_id)
    )
    """)
    
    # Create index on (ts, session_id) for efficient lookups
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_window_features_ts_session 
    ON window_features(ts, session_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS window_features")
