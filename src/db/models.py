"""SQLAlchemy models for CVE enrichment."""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class EPSS(Base):
    """EPSS score model."""
    __tablename__ = "epss"
    
    id = Column(Integer, primary_key=True)
    cve = Column(String, index=True)
    epss_score = Column(Float)
    percentile = Column(Float)
    ts = Column(DateTime, index=True)

class KEV(Base):
    """Known Exploited Vulnerabilities model."""
    __tablename__ = "kev"
    
    id = Column(Integer, primary_key=True)
    cve_id = Column(String, index=True)
    required_action = Column(String)
    due_date = Column(DateTime)
    ts = Column(DateTime, index=True)

class ComponentRisk(Base):
    """Component risk metrics model."""
    __tablename__ = "component_risk"
    
    id = Column(Integer, primary_key=True)
    component = Column(String, index=True)
    cve_count = Column(Integer)
    high_risk_ratio = Column(Float)
    epss_trend = Column(Float)
    ts = Column(DateTime, index=True)
