"""Add window_features table (idempotent)."""
from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = "000"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS window_features (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,
        session_id TEXT NOT NULL,
        host TEXT NOT NULL,
        window_size INTEGER NOT NULL,
        window_slide INTEGER NOT NULL,
        event_count INTEGER NOT NULL,
        unique_components INTEGER NOT NULL,
        error_ratio DOUBLE PRECISION NOT NULL,
        template_entropy DOUBLE PRECISION NOT NULL,
        component_entropy DOUBLE PRECISION NOT NULL,
        label TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ts, session_id)
    )
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_window_features_ts_session
    ON window_features(ts, session_id)
    """)

def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS window_features")
    
