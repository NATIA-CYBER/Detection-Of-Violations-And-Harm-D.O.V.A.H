import os
import pandas as pd
import streamlit as st

# ---------- Page config ----------
st.set_page_config(page_title="DOVAH — Console", page_icon="🛡️", layout="wide")

# ---------- Minimal CSS (inline, no extra files) ----------
st.markdown("""
<style>
:root{
  --bg:#0b0f19; --card:#121826; --muted:#9aa4af; --text:#e5e7eb;
  --accent:#7c3aed; --border:#1f2937;
}
html, body, [data-testid="stAppViewContainer"]{ background:var(--bg); color:var(--text); }
.block-container{ padding-top:1rem; }
#MainMenu, header, footer{ visibility:hidden; }
.navbar{ display:flex; justify-content:space-between; align-items:center; gap:.5rem;
  background:var(--card); border:1px solid var(--border); border-radius:12px; padding:.6rem .8rem; }
.brand{ font-weight:700; letter-spacing:.3px; }
.kpi-row [data-testid="stMetricValue"]{ color:var(--text); }
.card{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:.6rem; }
.toolbar{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:.4rem .6rem; margin:.4rem 0 .6rem 0; }
.caption-muted{ color:var(--muted); }
</style>
""", unsafe_allow_html=True)

# ---------- Navbar ----------
with st.container():
    st.markdown(
        f"""
        <div class="navbar">
          <div class="brand">🛡️ DOVAH</div>
          <div class="caption-muted">Streamlit {st.__version__}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------- Mode toggle ----------
MODE = os.getenv("DOVAH_UI_MODE", "mock").lower()  # "mock" or "live"

# ---------- Mock data (used until DB is wired) ----------
def mock_alerts():
    return pd.DataFrame([
        {"id":"a1","ts":"2025-08-15T12:03:11Z","host":"namenode-1","rule":"LogLM_anomaly","severity":"high","score":0.98,"epss":0.62,"kev":True,"mitre":"T1047","summary":"Unusual template sequence"},
        {"id":"a2","ts":"2025-08-15T12:05:22Z","host":"datanode-3","rule":"IsoForest","severity":"medium","score":0.71,"epss":0.14,"kev":False,"mitre":"T1562","summary":"Window stats anomaly"},
        {"id":"a3","ts":"2025-08-15T12:07:10Z","host":"namenode-2","rule":"RegexBurst","severity":"low","score":0.33,"epss":0.05,"kev":False,"mitre":"T1027","summary":"Sporadic pattern"}
    ])

def mock_drift():
    return pd.DataFrame([
        {"feature":"template_id","window_start":"2025-08-15T00:00:00Z","window_end":"2025-08-15T12:00:00Z","psi":0.28,"ks":0.19,"status":"watch"},
        {"feature":"bytes_written","window_start":"2025-08-15T00:00:00Z","window_end":"2025-08-15T12:00:00Z","psi":0.42,"ks":0.25,"status":"drift"},
        {"feature":"ops_per_sec","window_start":"2025-08-15T00:00:00Z","window_end":"2025-08-15T12:00:00Z","psi":0.18,"ks":0.11,"status":"ok"}
    ])

# ---------- Optional live helpers (only used if secrets set) ----------
def _engine():
    if "db" not in st.secrets: return None
    try:
        from sqlalchemy import create_engine
        cfg = st.secrets["db"]
        url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        return create_engine(url, pool_pre_ping=True)
    except Exception as e:
        st.toast(f"DB init failed: {e}", icon="⚠️")
        return None

@st.cache_data(show_spinner=False)
def read_alerts_live(limit=200):
    eng = _engine()
    if eng is None: return pd.DataFrame()
    import pandas as pd
    q = """
      select id, ts, host, rule, severity, score, epss, kev, mitre, summary
      from v_alerts order by ts desc limit %(limit)s
    """
    return pd.read_sql(q, eng, params={"limit": limit})

@st.cache_data(show_spinner=False)
def read_alert_counts_by_minute(minutes=120):
    eng = _engine()
    if eng is None: return pd.DataFrame()
    import pandas as pd
    q = """
      with buckets as (
        select date_trunc('minute', ts) as m
        from v_alerts
        where ts >= now() - interval %(mins)s
      )
      select m as bucket, count(*) as count
      from buckets group by 1 order by 1
    """
    return pd.read_sql(q, eng, params={"mins": f"{int(minutes)} minutes"})

@st.cache_data(show_spinner=False)
def read_drift_live(limit=200):
    eng = _engine()
    if eng is None: return pd.DataFrame()
    import pandas as pd
    q = """
      select feature, window_start, window_end, psi, ks, status
      from v_drift order by window_end desc limit %(limit)s
    """
    return pd.read_sql(q, eng, params={"limit": limit})

# ---------- Tabs (like a real site, within one file) ----------
tab_overview, tab_alerts, tab_drift, tab_about = st.tabs(["Overview", "Alerts", "Drift", "About"])

# ======= OVERVIEW =======
with tab_overview:
    st.subheader("Overview")
    # KPIs
    if MODE == "live" and "db" in st.secrets:
        trend = read_alert_counts_by_minute(minutes=120)
        total_24h = int(trend["count"].sum()) if not trend.empty else 0
    else:
        df_mock = mock_alerts()
        total_24h = len(df_mock)
        # simple mock trend from timestamps
        trend = df_mock.copy()
        trend["bucket"] = pd.to_datetime(trend["ts"]).dt.floor("min")
        trend = trend.groupby("bucket").size().reset_index(name="count")

    k1, k2, k3, k4 = st.columns(4, gap="small")
    with k1: st.metric("Alerts (24h)", f"{total_24h:,}")
    with k2: st.metric("P95 Ingest→Features", "—" if MODE=="mock" else "≤ 800 ms")
    with k3: st.metric("Drifted Features", 0 if MODE=="mock" else "—")
    with k4: st.metric("Explained Alerts", "—")

    # Trend
    with st.container(border=True):
        st.markdown("**Alert volume (last 2h)**")
        if not trend.empty:
            try:
                import plotly.express as px
                fig = px.area(trend, x="bucket", y="count")
                fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=220)
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                st.line_chart(trend.set_index("bucket"))
        else:
            st.caption("No data yet.")

    # Recent alerts (preview)
    st.markdown("**Recent alerts**")
    alerts_preview = read_alerts_live(50) if (MODE=="live" and "db" in st.secrets) else mock_alerts()
    st.dataframe(alerts_preview, use_container_width=True, height=320)

# ======= ALERTS =======
with tab_alerts:
    st.subheader("Alerts")
    # Filter toolbar
    with st.container():
        st.markdown('<div class="toolbar">Filters</div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns([1,1,1,1])
        sev = c1.multiselect("Severity", ["low","medium","high"], ["high","medium"])
        host_q = c2.text_input("Host contains", "")
        rule_q = c3.text_input("Rule contains", "")
        limit = c4.selectbox("Rows", [50,100,200], index=0)

    df = read_alerts_live(int(limit)) if (MODE=="live" and "db" in st.secrets) else mock_alerts()
    if not df.empty:
        if sev: df = df[df["severity"].isin(sev)]
        if host_q: df = df[df["host"].str.contains(host_q, case=False, na=False)]
        if rule_q: df = df[df["rule"].str.contains(rule_q, case=False, na=False)]

    s1, s2, s3 = st.columns(3)
    s1.metric("Rows", len(df))
    s2.metric("High", int((df["severity"]=="high").sum()) if "severity" in df else 0)
    s3.metric("KEV-tagged", int(df["kev"].sum()) if "kev" in df else 0)

    st.dataframe(df, use_container_width=True, height=540)

# ======= DRIFT =======
with tab_drift:
    st.subheader("Data & model drift")
    drift = read_drift_live(200) if (MODE=="live" and "db" in st.secrets) else mock_drift()

    c1, c2, c3 = st.columns(3)
    c1.metric("Drifted (PSI>0.3)", int((drift["psi"]>0.3).sum()) if not drift.empty else 0)
    c2.metric("PSI median", f'{drift["psi"].median():.2f}' if "psi" in drift else "—")
    c3.metric("KS median", f'{drift["ks"].median():.2f}' if "ks" in drift else "—")

    if not drift.empty:
        try:
            import plotly.express as px
            fig = px.bar(drift.sort_values("psi", ascending=False).head(15), x="feature", y="psi")
            fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=260)
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.bar_chart(drift.set_index("feature")["psi"].head(15))

    st.dataframe(drift, use_container_width=True, height=540)

# ======= ABOUT =======
with tab_about:
    st.subheader("About")
    st.markdown("""
**DOVAH** is a detection console.  
- **Sources:** FIRST *EPSS* (exploitation likelihood) and CISA *KEV* catalog.  
- **Latency SLO:** P95 ingest→features **< 800 ms** locally.  
- **Privacy:** Pseudonymization at ingest; evidence packs with ATT&CK, EPSS/KEV.
""")
    st.caption("Flip `DOVAH_UI_MODE=live` and set DB secrets to pull real data from `v_alerts`/`v_drift`.")
