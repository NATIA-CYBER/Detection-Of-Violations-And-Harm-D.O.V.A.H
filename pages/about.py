import streamlit as st
from utils.ui import load_css, render_nav, section_header

APP_TITLE = "Detection Of Violations And Harm"
APP_SUB   = "Console"

st.set_page_config(page_title=f"{APP_TITLE} — About", layout="wide", initial_sidebar_state="collapsed")
load_css()
render_nav(title=APP_TITLE, subtitle=APP_SUB, active="about")

section_header("About", "What this console does")
st.markdown("""
**DOVAH** is a detection console.

- **Sources:** FIRST *EPSS* (exploitation likelihood), CISA *KEV* catalog.  
- **Latency SLO:** P95 ingest→features **< 800 ms** locally.  
- **Privacy:** Pseudonymization at ingest; evidence packs with ATT&CK, EPSS/KEV.
""")
