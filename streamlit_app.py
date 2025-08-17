import os
import base64
from pathlib import Path
import pandas as pd
import streamlit as st

# ---------- Paths ----------
ICON_PATH = "icons/2.png"   

# ---------- Helpers ----------
def data_uri(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    b64 = base64.b64encode(p.read_bytes()).decode()
    ext = p.suffix.lower().lstrip(".")
    mime = "image/svg+xml" if ext == "svg" else f"image/{ext or 'png'}"
    return f"data:{mime};base64,{b64}"

ICON_URI = data_uri(ICON_PATH)

# ---------- Page config (your icon as favicon) ----------
# Streamlit accepts a local file path for page_icon.
st.set_page_config(page_title="DOVAH — Console", page_icon=ICON_PATH, layout="wide")

# ---------- CSS (richer, still light) ----------
st.markdown(f"""
<style>
:root {{
  --bg:#0b0f19; --card:#121826; --muted:#9aa4af; --text:#e5e7eb; --accent:#7c3aed;
  --border:#1f2937; --ok:#10b981; --warn:#f59e0b; --bad:#ef4444;
}}
html, body, [data-testid="stAppViewContainer"] {{ background:var(--bg); color:var(--text); }}
.block-container {{ padding-top:1rem; }}

# chrome
#MainMenu, header, footer {{ visibility:hidden; }}

.navbar {{
  display:flex; justify-content:space-between; align-items:center; gap:.75rem;
  background:linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.00));
  border:1px solid var(--border); border-radius:16px; padding:.7rem .9rem; backdrop-filter:blur(6px);
  box-shadow:0 6px 18px rgba(0,0,0,.25);
}}
.brand {{ display:flex; align-items:center; gap:.5rem; font-weight:700; letter-spacing:.3px; }}
.brand .logo {{ width:18px; height:18px; border-radius:4px; }}
.tabs {{ display:flex; gap:.6rem; }}
.tab {{ padding:.35rem .6rem; border:1px solid var(--border); border-radius:999px; color:var(--muted); text-decoration:none; }}
.tab.active {{ border-color:var(--accent); color:#fff; background:rgba(124,58,237,.15); }}

.card {{
  background:var(--card); border:1px solid var(--border); border-radius:16px; padding:.8rem .9rem;
  box-shadow:0 8px 24px rgba(0,0,0,.22);
}}
.kpi [data-testid="stMetricValue"] {{ color:var(--text); }}

.badge {{ display:inline-block; padding:.15rem .5rem; border-radius:999px; font-size:.75rem; }}
.badge.ok {{ background:rgba(16,185,129,.18); border:1px solid rgba(16,185,129,.35); }}
.badge.warn {{ background:rgba(245,158,11,.18); border:1px solid rgba(245,158,11,.35); }}
.badge.bad {{ background:rgba(239,68,68,.18); border:1px solid rgba(239,68,68,.35); }}

.caption-muted {{ color:var(--muted); }}
</style>
""", unsafe_allow_html=True)

# ---------- Navbar with your icon ----------
with st.container():
    right = f"Streamlit {st.__version__}"
    brand_img = f'<img class="logo" src="{ICON_URI}">' if ICON_URI else ''
    st.markdown(
        f"""
        <div class="navbar">
          <div class="brand">{brand_img}<span>DOVAH</span></div>
          <div class="tabs">
            <a class="tab active">Overview</a>
            <a class="tab">Alerts</a>
            <a class="tab">Drift</a>
            <a class="tab">About</a>
          </div>
          <div class="caption-muted">{right}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------- Mode toggle ----------
MODE = os.getenv("DOVAH_UI_MODE", "mock").lower()

# ---------- Mock data ----------
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

# ---------- Tabs (same content structure as before) ----------
tab_overview, tab_alerts, tab_drift, tab_about = st.tabs(["Overview", "Alerts", "Drift", "About"])

# ======= OVERVIEW =======
with tab_overview:
    st.subheader("Overview")
    df_mock = mock_alerts()
    total_24h = len(df_mock)

    k1, k2, k3, k4 = st.columns(4, gap="small")
    with k1: st.metric("Alerts (24h)", f"{total_24h:,}")
    with k2: st.metric("P95 Ingest→Features", "—" if MODE=="mock" else "≤ 800 ms")
    with k3: st.metric("Drifted Features", 0 if MODE=="mock" else "—")
    with k4: st.metric("Explained Alerts", "—")

    with st.container(border=True):
        st.markdown("**Alert volume (last 2h)**")
        trend = df_mock.copy()
        trend["bucket"] = pd.to_datetime(trend["ts"]).dt.floor("min")
        trend = trend.groupby("bucket").size().reset_index(name="count")
        if not trend.empty:
            try:
                import plotly.express as px
                fig = px.area(trend, x="bucket", y="count")
                fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=220)
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                st.line_chart(trend.set_index("bucket"))

    st.markdown("**Recent alerts**")
    st.dataframe(df_mock, use_container_width=True, height=330)

# ======= ALERTS =======
with tab_alerts:
    st.subheader("Alerts")
    with st.container():
        st.markdown('<div class="toolbar">Filters</div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns([1,1,1,1])
        sev = c1.multiselect("Severity", ["low","medium","high"], ["high","medium"])
        host_q = c2.text_input("Host contains", "")
        rule_q = c3.text_input("Rule contains", "")
        limit = c4.selectbox("Rows", [50,100,200], index=0)

    df = mock_alerts().head(int(limit))
    if sev: df = df[df["severity"].isin(sev)]
    if host_q: df = df[df["host"].str.contains(host_q, case=False, na=False)]
    if rule_q: df = df[df["rule"].str.contains(rule_q, case=False, na=False)]

    s1, s2, s3 = st.columns(3)
    s1.metric("Rows", len(df))
    s2.metric("High", int((df["severity"]=="high").sum()))
    s3.metric("KEV-tagged", int(df.get("kev", pd.Series(dtype=bool)).sum()) if "kev" in df else 0)

    st.dataframe(df, use_container_width=True, height=540)

# ======= DRIFT =======
with tab_drift:
    st.subheader("Data & model drift")
    drift = mock_drift()
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
    st.dataframe(drift, use_container_width=True, height=520)

# ======= ABOUT =======
with tab_about:
    st.subheader("About")
    st.markdown("""
**DOVAH** — Detection console.

- **Sources:** FIRST *EPSS* (exploitation likelihood), CISA *KEV* catalog.  
- **Latency SLO:** P95 ingest→features **< 800 ms** locally.  
- **Privacy:** Pseudonymization at ingest; evidence packs with ATT&CK, EPSS/KEV.
""")
