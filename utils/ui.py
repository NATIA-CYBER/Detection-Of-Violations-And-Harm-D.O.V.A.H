# utils/ui.py
from __future__ import annotations
import base64, textwrap
from pathlib import Path
import streamlit as st

# ---------- CSS loader ----------
def load_css() -> None:
    p = Path("assets/styles.css")
    if p.exists():
        st.markdown(f"<style>{p.read_text()}</style>", unsafe_allow_html=True)

# ---------- icon helpers ----------
def _data_uri(path: Path) -> str:
    if not path.exists():
        return ""
    ext = path.suffix.lower().lstrip(".") or "png"
    mime = "image/svg+xml" if ext == "svg" else f"image/{ext}"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode()}"

def _pick_icon() -> Path:
    # prefer light icon for dark theme; fall back to whatever exists
    theme = (st.get_option("theme.base") or "dark").lower()
    prefer = ["icons/dovah_light.png", "icons/dovah_dark.png"] if theme == "dark" else ["icons/dovah_dark.png", "icons/dovah_light.png"]
    fallback = ["icons/2.png", "icons/dovah.png"]
    for p in [*prefer, *fallback]:
        if Path(p).exists():
            return Path(p)
    return Path("icons/dovah_light.png")

# ---------- navbar + real links ----------
def render_nav(title: str, subtitle: str, active: str = "overview") -> None:
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

    # make the four tabs clickable; tolerate different filenames
    def _first_existing(paths):  # type: ignore[override]
        for p in paths:
            if Path(p).exists():
                return p
        return paths[0]

    p_overview = "streamlit_app.py"
    p_alerts   = _first_existing(["pages/02_Alerts.py", "pages/alerts.py", "pages/Alerts.py"])
    p_drift    = _first_existing(["pages/03_Drift.py",  "pages/drift.py",  "pages/Drift.py"])
    p_about    = _first_existing(["pages/04_About.py",  "pages/about.py",  "pages/About.py"])

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1], gap="small")
    with c1: st.page_link(p_overview, label="Overview", icon="🏠")
    with c2: st.page_link(p_alerts,   label="Alerts",   icon="🚨")
    with c3: st.page_link(p_drift,    label="Drift",    icon="📉")
    with c4: st.page_link(p_about,    label="About",    icon="ℹ️")

# ---------- section header + KPI cols ----------
def section_header(title: str, subtitle: str = "") -> None:
    st.markdown(f"<h3 class='section-title'>{title}</h3>", unsafe_allow_html=True)
    if subtitle:
        st.caption(subtitle)

def kpi_cols(n: int = 4):
    return st.columns(n, gap="small")
