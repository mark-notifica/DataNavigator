from pathlib import Path
import streamlit as st

def list_pages(pages_dir: Path = None) -> list[dict]:
    """Returns a sorted list of page metadata (with label and route) from the pages directory."""
    if pages_dir is None:
        pages_dir = Path(__file__).parent / "pages"

    page_files = sorted(pages_dir.glob("*.py"))
    pages = []
    for p in page_files:
        stem = p.stem
        # Split numeric prefix if present
        if "_" in stem and stem.split("_")[0].isdigit():
            _, route = stem.split("_", 1)
        else:
            route = stem
        label = route.replace("_", " ").capitalize()
        pages.append({"label": label, "route": route})
    return pages

def render_page_navigation(title="📂 Pagina's", pages_dir: Path = None):
    """Renders a list of navigation buttons for each registered page."""
    st.subheader(title)
    pages = list_pages(pages_dir)
    for page in pages:
        if st.button(f"➡️ {page['label']}"):
            st.markdown(
                f'<meta http-equiv="refresh" content="0; url=./{page["route"]}" />',
                unsafe_allow_html=True
            )

def go_to_connection_manager(
    *,
    mode: str,
    connection_id: int,
    config_id: int | None = None,
    route: str = "Connection_manager",
    extra_state: dict | None = None
):
    """
    Navigates to the config manager page, setting session state for either 'new' or 'edit' mode.
    
    Parameters:
        mode: 'new' or 'edit'
        connection_id: main connection ID
        config_id: optional, ID of the config to edit
        route: target page route (without prefix)
        extra_state: optional dictionary of additional session state keys/values
    """

    # Standard session state
    if mode == "new":
        st.session_state["new_config_connection_id"] = connection_id
        st.session_state["edit_config_id"] = None
    elif mode == "edit":
        st.session_state["edit_config_id"] = config_id
        st.session_state["new_config_connection_id"] = connection_id
    else:
        st.error(f"Unknown config mode: {mode}")
        return

    # Optional overrides or additions
    if extra_state:
        for k, v in extra_state.items():
            st.session_state[k] = v

    # Navigate using HTML meta refresh
    st.markdown(
        f'<meta http-equiv="refresh" content="0; url=./{route}" />',
        unsafe_allow_html=True
    )