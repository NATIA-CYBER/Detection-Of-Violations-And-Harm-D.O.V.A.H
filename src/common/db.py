import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def get_engine() -> Engine:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return create_engine(url, pool_pre_ping=True)



