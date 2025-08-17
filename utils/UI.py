import base64
from pathlib import Path
import streamlit as st

# ---- CSS loader ----
def load_css():
    css_path = Path("assets/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)

# ---- icon helpers ----
def _data_uri(path: Path) -> str:
    if not path.exists(): return ""
    b64 = base64.b64encode(path.read_bytes()).decode()
    ext = path.suffix.lower().lstrip(".") or "png"
    mime = "image/svg+xml" if ext == "svg" else f"image/{ext}"
    return f"data:{mime};base64,{b64}"

def _pick_icon() -> Path:
    override = (st.session_state.get("DOVAH_ICON") or "").lower()
    if override in {"light", "dark"}:
        return Path(f"icons/dovah_{override}.png")
    theme = (st.get_option("theme.base") or "dark").lower()
    return Path("icons/dovah_light.png") if theme == "dark" else Path("icons/dovah_dark.png")

# ---- nav / chrome ----
def render_nav(title: str, subtitle: str, active: str = "overview"):
    icon_path = _pick_icon()
    icon_uri  = _data_uri(icon_path)
    right = f"Streamlit {st.__version__}"
    brand_img = f'<img class="logo" src="{icon_uri}" alt="logo">' if icon_uri else ''
    st.markdown(
        f"""
        <div class="navbar">
          <div class="brand">
            {brand_img}
            <div class="brand-text">
              <div class="brand-title">{title}</div>
              <div class="brand-sub">{subtitle}</div>
            </div>
          </div>
          <div class="caption-muted">{right}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def section_header(title: str, subtitle: str = ""):
    st.markdown(f"<h3 class='section-title'>{title}</h3>", unsafe_allow_html=True)
    if subtitle: st.caption(subtitle)

def kpi_cols(n=4):
    return st.columns(n, gap="small")
