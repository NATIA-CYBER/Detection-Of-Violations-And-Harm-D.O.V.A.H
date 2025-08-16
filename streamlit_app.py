import streamlit as st
import pandas as pd

st.set_page_config(page_title="DOVAH Console", layout="wide")
st.title("DOVAH — Overview (mock)")

alerts = pd.DataFrame([
    {"id":"a1","ts":"2025-08-15T12:03:11Z","host":"namenode-1","rule":"LogLM_anomaly","severity":"high","score":0.98,"epss":0.62,"kev":True,"mitre":"T1047","summary":"Unusual template sequence"},
    {"id":"a2","ts":"2025-08-15T12:05:22Z","host":"datanode-3","rule":"IsoForest","severity":"medium","score":0.71,"epss":0.14,"kev":False,"mitre":"T1562","summary":"Window stats anomaly"}
])

c1, c2, c3, c4 = st.columns(4)
c1.metric("Alerts (24h)", len(alerts))
c2.metric("P95 Latency", "—")
c3.metric("Drifted Features", 0)
c4.metric("Explained Alerts", "—")

st.subheader("Recent alerts")
st.dataframe(alerts, use_container_width=True)
