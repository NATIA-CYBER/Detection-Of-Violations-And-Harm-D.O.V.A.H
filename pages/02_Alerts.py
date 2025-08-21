# pages/02_Alerts.py

import os
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Alerts • DOVAH", layout="wide")
st.title("Alerts")

MODE = os.getenv("DOVAH_UI_MODE", "mock").lower() 


def _mock_alerts():
    return pd.DataFrame([
        {"id":"a1","ts":"2025-08-15T12:03:11Z","host":"namenode-1","rule":"LogLM_anomaly","severity":"high","score":0.98,"epss":0.62,"kev":True,"mitre":"T1047","summary":"Unusual template sequence"},
        {"id":"a2","ts":"2025-08-15T12:05:22Z","host":"datanode-3","rule":"IsoForest","severity":"medium","score":0.71,"epss":0.14,"kev":False,"mitre":"T1562","summary":"Window stats anomaly"},
        {"id":"a3","ts":"2025-08-15T12:07:10Z","host":"namenode-2","rule":"RegexBurst","severity":"low","score":0.33,"epss":0.05,"kev":False,"mitre":"T1027","summary":"Sporadic pattern"},
    ])


@st.cache_resource
def _engine():
    """Lazy DB engine. Returns None if secrets missing or SQLAlchemy/DB not available."""
    if "db" not in st.secrets:
        return None

    try:
        t
        from sqlalchemy import create_engine
    except Exception as e:
        st.warning(f"SQLAlchemy not available yet; using mock data ({e}).", icon="⚠️")
        return None

    cfg = st.secrets["db"]
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    try:
        return create_engine(url, pool_pre_ping=True)
    except Exception as e:
        st.warning(f"DB init failed; using mock data ({e}).", icon="⚠️")
        return None


@st.cache_data(show_spinner=False)
def _read_alerts_live(limit: int = 500) -> pd.DataFrame:
    eng = _engine()
    if eng is None:
        return pd.DataFrame()
    q = """
      select id, ts, host, rule, severity, score, epss, kev, mitre, summary
      from v_alerts
      order by ts desc
      limit %(limit)s
    """
    try:
        return pd.read_sql(q, eng, params={"limit": limit})
    except Exception as e:
        st.warning(f"Query failed; using mock data ({e}).", icon="⚠️")
        return pd.DataFrame()



use_live = MODE == "live" and "db" in st.secrets
alerts = _read_alerts_live() if use_live else _mock_alerts()


with st.sidebar:
    st.subheader("Filters")
    sev = st.multiselect("Severity", ["low", "medium", "high"], ["high", "medium"])
    host_q = st.text_input("Host contains")
    rule_q = st.text_input("Rule contains")
    limit = st.selectbox("Rows", [50, 100, 200], index=1)

df = alerts.copy().head(int(limit))
if not df.empty:
    if sev:
        df = df[df["severity"].isin(sev)]
    if host_q:
        df = df[df["host"].str.contains(host_q, case=False, na=False)]
    if rule_q:
        df = df[df["rule"].str.contains(rule_q, case=False, na=False)]


s1, s2, s3 = st.columns(3)
s1.metric("Rows", len(df))
s2.metric("High", int((df["severity"] == "high").sum()) if "severity" in df else 0)
s3.metric("KEV-tagged", int(df.get("kev", pd.Series(dtype=bool)).sum()) if "kev" in df else 0)

st.dataframe(df, use_container_width=True, height=560)
