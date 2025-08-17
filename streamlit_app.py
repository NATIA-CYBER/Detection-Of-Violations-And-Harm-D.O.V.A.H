import os
import pandas as pd
import streamlit as st
# ensure local packages (utils/, etc.) are importable
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))


from utils.ui import load_css, render_nav, section_header, kpi_cols
from utils.db import try_read_alert_counts_by_minute, try_read_recent_alerts

APP_TITLE = "Detection Of Violations And Harm"
APP_SUB   = "Console"
MODE = os.getenv("DOVAH_UI_MODE", "mock").lower()  # mock | live

st.set_page_config(
    page_title=f"{APP_TITLE} — {APP_SUB}",
    page_icon="icons/dovah_light.png",  # Streamlit will load this from repo root
    layout="wide",
    initial_sidebar_state="collapsed",
)

load_css()
render_nav(title=APP_TITLE, subtitle=APP_SUB, active="overview")

# ---- Data (mock or live) ----
if MODE == "live" and "db" in st.secrets:
    trend = try_read_alert_counts_by_minute(120)  # last 2h
    total_24h = int(trend["count"].sum()) if not trend.empty else 0
    alerts_preview = try_read_recent_alerts(50)
else:
    # Mock
    alerts_preview = pd.DataFrame([
        {"id":"a1","ts":"2025-08-15T12:03:11Z","host":"namenode-1","rule":"LogLM_anomaly","severity":"high","score":0.98,"epss":0.62,"kev":True,"mitre":"T1047","summary":"Unusual template sequence"},
        {"id":"a2","ts":"2025-08-15T12:05:22Z","host":"datanode-3","rule":"IsoForest","severity":"medium","score":0.71,"epss":0.14,"kev":False,"mitre":"T1562","summary":"Window stats anomaly"},
        {"id":"a3","ts":"2025-08-15T12:07:10Z","host":"namenode-2","rule":"RegexBurst","severity":"low","score":0.33,"epss":0.05,"kev":False,"mitre":"T1027","summary":"Sporadic pattern"},
    ])
    trend = alerts_preview.copy()
    trend["bucket"] = pd.to_datetime(trend["ts"]).dt.floor("min")
    trend = trend.groupby("bucket").size().reset_index(name="count")
    total_24h = len(alerts_preview)

# ---- KPIs ----
section_header("Overview", "High-level signal & recent activity")
c1, c2, c3, c4 = kpi_cols(4)
c1.metric("Alerts (24h)", f"{total_24h:,}")
c2.metric("P95 Ingest→Features", "—" if MODE == "mock" else "≤ 800 ms")
c3.metric("Drifted Features", 0 if MODE == "mock" else "—")
c4.metric("Explained Alerts", "—")

# ---- Trend ----
with st.container(border=True):
    st.markdown("**Alert volume (last 2h)**")
    if not trend.empty:
        try:
            import plotly.express as px
            fig = px.area(trend, x="bucket", y="count")
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=220)
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.line_chart(trend.set_index("bucket"))
    else:
        st.caption("No data yet. Flip `DOVAH_UI_MODE=live` and set DB secrets to pull real data.")

# ---- Recent alerts ----
section_header("Recent alerts", "Latest detections with enrichment & tags")
st.dataframe(alerts_preview, use_container_width=True, height=360)
