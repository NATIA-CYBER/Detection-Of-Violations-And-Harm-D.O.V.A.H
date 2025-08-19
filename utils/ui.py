# utils/ui.py
import base64, textwrap
from pathlib import Path
import streamlit as st

def load_css():
    p = Path("assets/styles.css")
    if p.exists():
        st.markdown(f"<style>{p.read_text()}</style>", unsafe_allow_html=True)

def _data_uri(path: Path) -> str:
    if not path.exists(): return ""
    b64 = base64.b64encode(path.read_bytes()).decode()
    ext = path.suffix.lower().lstrip(".") or "png"
    mime = "image/svg+xml" if ext == "svg" else f"image/{ext}"
    return f"data:{mime};base64,{b64}"

def _pick_icon() -> Path:
    theme = (st.get_option("theme.base") or "dark").lower()
    return Path("icons/dovah_light.png") if theme == "dark" else Path("icons/dovah_dark.png")

def render_nav(title: str, subtitle: str, active: str = "overview"):
    icon_uri = _data_uri(_pick_icon())
    img = f'<img class="logo" src="{icon_uri}" alt="logo">' if icon_uri else ""

    html = textwrap.dedent(f"""\
    <div class="navbar">
      <div class="brand">
        {img}
        <div class="brand-text">
          <div class="brand-title">{title}</div>
          <div class="brand-sub">{subtitle}</div>
        </div>
      </div>
      <div class="caption-muted">Streamlit {st.__version__}</div>
    </div>
    """)
    st.markdown(html, unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns([1,1,1,1], gap="small")
    with c1: st.page_link("streamlit_app.py",   label="Overview", icon="🏠")
    with c2: st.page_link("pages/02_Alerts.py", label="Alerts",   icon="🚨")
    with c3: st.page_link("pages/03_Drift.py",  label="Drift",    icon="📉")
    with c4: st.page_link("pages/04_About.py",  label="About",    icon="ℹ️")
