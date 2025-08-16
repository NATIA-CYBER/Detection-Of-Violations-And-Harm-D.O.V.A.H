import os, pandas as pd, streamlit as st
from sqlalchemy import create_engine

st.set_page_config(layout="wide")
st.title("Alerts")

MODE = os.getenv("DOVAH_UI_MODE", "mock").lower()

def mock_alerts():
    return pd.DataFrame([
        {"id":"a1","ts":"2025-08-15T12:03:11Z","host":"namenode-1","rule":"LogLM_anomaly","severity":"high","score":0.98,"epss":0.62,"kev":True,"mitre":"T1047","summary":"Unusual template sequence"},
        {"id":"a2","ts":"2025-08-15T12:05:22Z","host":"datanode-3","rule":"IsoForest","severity":"medium","score":0.71,"epss":0.14,"kev":False,"mitre":"T1562","summary":"Window stats anomaly"}
    ])

@st.cache_resource
def engine():
    if "db" not in st.secrets:  # no secrets set yet
        return None
    cfg = st.secrets["db"]
    url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(show_spinner=False)
def read_alerts_live(limit=500):
    eng = engine()
    if eng is None:
        return pd.DataFrame()
    sql = """
      select id, ts, host, rule, severity, score, epss, kev, mitre, summary
      from v_alerts order by ts desc limit %(limit)s
    """
    return pd.read_sql(sql, eng, params={"limit": limit})

# data source
alerts = mock_alerts() if MODE == "mock" else read_alerts_live()

# filters
right = st.sidebar
sev = right.multiselect("Severity", ["low","medium","high"], ["high","medium"])
host = right.text_input("Host contains")
rule = right.text_input("Rule contains")

df = alerts.copy()
if not df.empty:
    if sev:  df = df[df["severity"].isin(sev)]
    if host: df = df[df["host"].str.contains(host, case=False, na=False)]
    if rule: df = df[df["rule"].str.contains(rule, case=False, na=False)]

st.dataframe(df, use_container_width=True)
