import os, pandas as pd, streamlit as st
from sqlalchemy import create_engine

st.title("Drift")
MODE = os.getenv("DOVAH_UI_MODE", "mock").lower()

def mock_drift():
    return pd.DataFrame([
        {"feature":"template_id","window_start":"2025-08-15T00:00:00Z","window_end":"2025-08-15T12:00:00Z","psi":0.28,"ks":0.19,"status":"watch"},
        {"feature":"bytes_written","window_start":"2025-08-15T00:00:00Z","window_end":"2025-08-15T12:00:00Z","psi":0.42,"ks":0.25,"status":"drift"}
    ])

@st.cache_resource
def engine():
    if "db" not in st.secrets:
        return None
    cfg = st.secrets["db"]
    url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(show_spinner=False)
def read_drift_live(limit=200):
    eng = engine()
    if eng is None:
        return pd.DataFrame()
    sql = """
      select feature, window_start, window_end, psi, ks, status
      from v_drift order by window_end desc limit %(limit)s
    """
    return pd.read_sql(sql, eng, params={"limit": limit})

df = mock_drift() if MODE == "mock" else read_drift_live()
st.dataframe(df, use_container_width=True)
