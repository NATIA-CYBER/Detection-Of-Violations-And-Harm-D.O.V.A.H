"""Enable pgcrypto/uuid extensions (idempotent)."""
from alembic import op

# revision identifiers
revision = "000"
down_revision = None
branch_labels = None
depends_on = None

def upgrade() -> None:
    # gen_random_uuid() needs pgcrypto; use IF NOT EXISTS to be idempotent
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    # you may also enable uuid-ossp if you ever switch defaults:
    # op.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

def downgrade() -> None:
    # leave extensions in place (safe & often required)
    pass
