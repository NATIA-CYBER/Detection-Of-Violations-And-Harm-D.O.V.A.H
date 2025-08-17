
import os
import pandas as pd
import streamlit as st
from utils.ui import load_css, top_nav, stat_grid, section_header, footer
from utils.db import get_engine, read_alerts, read_alert_counts_by_time

# 1) Page config
st.set_page_config(page_title="DOVAH — Console", page_icon="🛡️", layout="wide")

# 2) Global UI chrome
load_css()
top_nav(active="overview")

# 3) Mode toggle
MODE = os.getenv("DOVAH_UI_MODE", "mock").lower()  # mock | live

# 4) Overview KPIs/trend
section_header("Overview", "High-level signal and recent activity")

if MODE == "live" and "db" in st.secrets:
    eng = get_engine()
    df_counts = read_alert_counts_by_time(eng, minutes=120, bucket_minutes=10)
    total_24h = int(df_counts["count"].sum()) if not df_counts.empty else 0
else:
    # Minimal mock data
    total_24h = 128
    df_counts = pd.DataFrame({"bucket": [], "count": []})

c1, c2, c3, c4 = stat_grid(4)
c1.metric("Alerts (24h)", f"{total_24h:,}")
c2.metric("P95 Ingest→Features", "—" if MODE == "mock" else "≤ 800 ms")
c3.metric("Drifted Features", 0 if MODE == "mock" else "—")
c4.metric("Explained Alerts", "—")

with st.container(border=True):
    st.subheader("Alert volume (last 2h)")
    if not df_counts.empty:
        try:
            import plotly.express as px
            fig = px.area(df_counts, x="bucket", y="count")
            fig.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=220)
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.line_chart(df_counts.set_index("bucket"))
    else:
        st.caption("No data yet. Flip DOVAH_UI_MODE=live once DB is wired.")

# 5) Recent alerts table
section_header("Recent alerts", "Latest detections with enrichment & tags")
if MODE == "live" and "db" in st.secrets:
    eng = get_engine()
    alerts = read_alerts(eng, limit=50)
else:
    alerts = pd.DataFrame([
        {"id":"a1","ts":"2025-08-15T12:03:11Z","host":"namenode-1","rule":"LogLM_anomaly","severity":"high","score":0.98,"epss":0.62,"kev":True,"mitre":"T1047","summary":"Unusual template sequence"},
        {"id":"a2","ts":"2025-08-15T12:05:22Z","host":"datanode-3","rule":"IsoForest","severity":"medium","score":0.71,"epss":0.14,"kev":False,"mitre":"T1562","summary":"Window stats anomaly"}
    ])
st.dataframe(alerts, use_container_width=True, height=360)

# 6) Footer
footer()
