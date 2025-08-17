from typing import Optional
import pandas as pd
import streamlit as st

def _engine():
    if "db" not in st.secrets:
        return None
    try:
        from sqlalchemy import create_engine
        cfg = st.secrets["db"]
        url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        return create_engine(url, pool_pre_ping=True)
    except Exception as e:
        st.toast(f"DB init failed: {e}", icon="⚠️")
        return None

@st.cache_data(show_spinner=False)
def try_read_recent_alerts(limit: int = 200) -> pd.DataFrame:
    eng = _engine()
    if eng is None: return pd.DataFrame()
    q = """
      select id, ts, host, rule, severity, score, epss, kev, mitre, summary
      from v_alerts
      order by ts desc
      limit %(limit)s
    """
    return pd.read_sql(q, eng, params={"limit": limit})

@st.cache_data(show_spinner=False)
def try_read_alert_counts_by_minute(minutes: int = 120) -> pd.DataFrame:
    eng = _engine()
    if eng is None: return pd.DataFrame()
    q = """
      select date_trunc('minute', ts) as bucket, count(*) as count
      from v_alerts
      where ts >= now() - interval %(mins)s
      group by 1
      order by 1
    """
    return pd.read_sql(q, eng, params={"mins": f"{int(minutes)} minutes"})

@st.cache_data(show_spinner=False)
def try_read_drift(limit: int = 200) -> pd.DataFrame:
    eng = _engine()
    if eng is None: return pd.DataFrame()
    q = """
      select feature, window_start, window_end, psi, ks, status
      from v_drift
      order by window_end desc
      limit %(limit)s
    """
    return pd.read_sql(q, eng, params={"limit": limit})
