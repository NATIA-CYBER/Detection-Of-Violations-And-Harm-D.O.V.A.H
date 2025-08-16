"""Database module initialization."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from .models import Base

def get_engine(url="sqlite:///:memory:", echo=False):
    """Create SQLAlchemy engine."""
    return create_engine(
        url,
        echo=echo,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool
    )

def get_session(engine=None):
    """Create SQLAlchemy session."""
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
