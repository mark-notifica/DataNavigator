import streamlit as st
import sqlalchemy as sa
import app_boot


# Add page config as the FIRST Streamlit command
st.set_page_config(
    page_title="Connection Manager",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
from pathlib import Path
import os
import sys
import pyodbc
import logging

from datetime import datetime
from pytz import UTC
from shared_utils import (
    test_main_connection,
    test_catalog_config,
    test_ai_config,
    get_main_connection_test_status,
    get_catalog_config_test_status,
    get_ai_config_test_status,
)

# Apply styling
sys.path.append(str(Path(__file__).parent.parent))
from shared_utils import apply_compact_styling
apply_compact_styling()

# Print Python executable path
# print("Python executable:", sys.executable)
# st.write(st.__version__)

# Load environment variables for the DataNavigator database
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# Connect to the DataNavigator database
db_url = sa.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("NAV_DB_USER"),
    password=os.getenv("NAV_DB_PASSWORD"),
    host=os.getenv("NAV_DB_HOST"),
    port=os.getenv("NAV_DB_PORT"),
    database=os.getenv("NAV_DB_NAME")
)
engine = sa.create_engine(db_url)

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

# --- Helper functies voor resetten sessievelden ---

def reset_main_connection_session_keys():
    keys = [
        "edit_name",
        "edit_type",
        "edit_host",
        "edit_port",
        "edit_user",
        "edit_folder",
        "edit_active",
        "edit_password",
    ]
    for key in keys:
        if key in st.session_state:
            st.session_state.pop(key)

def reset_catalog_config_session_keys(config_id="new"):
    keys = [
        f"catalog_name_{config_id}",
        f"catalog_dbf_{config_id}",
        f"catalog_scf_{config_id}",
        f"catalog_tbf_{config_id}",
        f"catalog_views_{config_id}",
        f"catalog_sys_{config_id}",
        f"catalog_active_{config_id}",
        f"catalog_notes_{config_id}",
    ]
    for key in keys:
        if key in st.session_state:
            st.session_state.pop(key)

def reset_ai_config_session_keys(config_id="new"):
    keys = [
        f"ai_name_{config_id}",
        f"ai_database_filter_{config_id}",
        f"ai_schema_filter_{config_id}",
        f"ai_table_filter_{config_id}",
        f"ai_model_version_{config_id}",
        f"ai_active_{config_id}",
        f"ai_notes_{config_id}",
    ]
    for key in keys:
        if key in st.session_state:
            st.session_state.pop(key)

# --- Functie om catalog config formulier te resetten vanuit DB ---
def reset_catalog_config_session_keys(uid):
    keys = [
        f"catalog_name_{uid}",
        f"catalog_database_filter_{uid}",
        f"catalog_schema_filter_{uid}",
        f"catalog_table_filter_{uid}",
        f"catalog_active_{uid}",
        f"catalog_notes_{uid}"
    ]
    for key in keys:
        if key in st.session_state:
            del st.session_state[key]

def trigger_reset_catalog_config(config_id):
    st.session_state["reset_catalog_form"] = True
    st.session_state["reset_catalog_form_id"] = config_id
    st.rerun() 


def reset_catalog_config_form(engine, config_id):
    with engine.connect() as conn:
        result = conn.execute(sa.text("""
            SELECT config_name, catalog_database_filter, catalog_schema_filter, catalog_table_filter,
                   is_active, notes
            FROM config.catalog_connection_config
            WHERE id = :id
        """), {"id": config_id}).fetchone()

    if result:
        row = dict(result._mapping)  # <-- hier omzetten naar dict
        st.session_state[f"catalog_name_{config_id}"] = row["config_name"]
        st.session_state[f"catalog_database_filter_{config_id}"] = row["catalog_database_filter"] or ""
        st.session_state[f"catalog_schema_filter_{config_id}"] = row["catalog_schema_filter"] or ""
        st.session_state[f"catalog_table_filter_{config_id}"] = row["catalog_table_filter"] or ""
        st.session_state[f"catalog_active_{config_id}"] = row["is_active"]
        st.session_state[f"catalog_notes_{config_id}"] = row["notes"] or ""

# --- Functie om ai config formulier te resetten vanuit DB ---

def reset_ai_config_form(engine, config_id):
    with engine.connect() as conn:
        result = conn.execute(
            sa.text("""
                SELECT config_name, ai_database_filter, ai_schema_filter, ai_table_filter,
                       ai_model_version, is_active, notes
                FROM config.ai_analyzer_connection_config
                WHERE id = :id
            """),
            {"id": config_id}
        ).fetchone()

    if result:
        row = dict(result._mapping)  # Zet om naar dict
        st.session_state[f"ai_name_{config_id}"] = row["config_name"]
        st.session_state[f"ai_database_filter_{config_id}"] = row["ai_database_filter"] or ""
        st.session_state[f"ai_schema_filter_{config_id}"] = row["ai_schema_filter"] or ""
        st.session_state[f"ai_table_filter_{config_id}"] = row["ai_table_filter"] or ""
        st.session_state[f"ai_model_version_{config_id}"] = row["ai_model_version"] or ""
        st.session_state[f"ai_active_{config_id}"] = row["is_active"]
        st.session_state[f"ai_notes_{config_id}"] = row["notes"] or ""
    st.rerun()

# --- Render functies ---

def render_catalog_config_section(selected_conn, engine):
    if not selected_conn:
        st.info("No connection selected, cannot show catalog configurations.")
        return

    selected_conn_id = selected_conn["id"]

    # Reset check: laad resetwaarden in st.session_state vóór widgets renderen
    if st.session_state.get("reset_catalog_form", False):
        config_id = st.session_state.get("reset_catalog_form_id")
        if config_id is not None:
            with engine.connect() as conn:
                result = conn.execute(sa.text("""
                    SELECT config_name, catalog_database_filter, catalog_schema_filter, catalog_table_filter,
                           is_active, notes
                    FROM config.catalog_connection_config
                    WHERE id = :id
                """), {"id": config_id}).fetchone()

            if result:
                row = dict(result._mapping)
                st.session_state[f"catalog_name_{config_id}"] = row["config_name"]
                st.session_state[f"catalog_database_filter_{config_id}"] = row["catalog_database_filter"] or ""
                st.session_state[f"catalog_schema_filter_{config_id}"] = row["catalog_schema_filter"] or ""
                st.session_state[f"catalog_table_filter_{config_id}"] = row["catalog_table_filter"] or ""
                st.session_state[f"catalog_active_{config_id}"] = row["is_active"]
                st.session_state[f"catalog_notes_{config_id}"] = row["notes"] or ""

        st.session_state["reset_catalog_form"] = False
        st.session_state["reset_catalog_form_id"] = None

    try:
        with engine.connect() as db_conn:
            catalog_configs = db_conn.execute(sa.text(f"""
                SELECT * FROM config.catalog_connection_config
                WHERE connection_id = :id AND is_active = TRUE
                ORDER BY updated_at DESC
            """), {"id": selected_conn_id}).fetchall()
    except Exception as e:
        st.error(f"Failed to fetch catalog configurations: {e}")
        catalog_configs = []

    # st.write(f"Catalog configs for connection {selected_conn_id}:", catalog_configs)

    st.divider()
    st.markdown("## 📚 Catalog Configurations")
    show_inactive = st.checkbox(
        "Show inactive catalog configurations",
        value=False,
        key=f"show_inactive_catalog_{selected_conn_id}"
    )

    with engine.connect() as db_conn:
        catalog_configs = db_conn.execute(sa.text(f"""
            SELECT * FROM config.catalog_connection_config
            WHERE connection_id = :id
            {"AND is_active = TRUE" if not show_inactive else ""}
        """), {"id": selected_conn_id}).fetchall()

    edit_config_id = st.session_state.get("edit_catalog_config_id")
    new_config_id = st.session_state.get("new_config_connection_id")

    if catalog_configs:
        catalog_options = {
            f"📚 Catalog ID {c.id} | {'🟢 Active' if c.is_active else '🔴 Inactive'} | DB: {c.catalog_database_filter or '-'} | SC: {c.catalog_schema_filter or '-'} | TB: {c.catalog_table_filter or '-'}":
            dict(c._mapping)
            for c in catalog_configs
        }
        selected_label = st.selectbox(
            "Select catalog configuration",
            list(catalog_options.keys()),
            key=f"catalog_select_{selected_conn_id}"
        )
        selected_catalog_config = catalog_options[selected_label]

        if edit_config_id == selected_catalog_config["id"]:
            with st.expander(f"✍️ Edit Catalog Configuration - {selected_catalog_config['config_name']}", expanded=True):
                render_catalog_config_editor(engine, selected_conn_id, edit_config_id)

                if st.button("❌ Cancel", key=f"cancel_edit_catalog_{edit_config_id}"):
                    reset_catalog_config_session_keys(edit_config_id)
                    st.session_state.pop("edit_catalog_config_id", None)
                    st.rerun()


        else:
            with st.expander(f"⚙️ Catalog Configuration - {selected_catalog_config['config_name']}", expanded=False):
                render_catalog_config_readonly(selected_catalog_config)

                col_test, col_edit, col_deactivate = st.columns(3)

                with col_test:
                    if st.button("🔍 Test Config", key=f"btn_test_catalog_config_{selected_catalog_config['id']}"):
                        connection_info = {
                            "id": selected_conn["id"],
                            "connection_type": selected_conn["connection_type"],
                            "host": selected_conn.get("host"),
                            "port": selected_conn.get("port"),
                            "username": selected_conn.get("username"),
                            "password": selected_conn.get("password"),
                            "folder_path": selected_conn.get("folder_path")
                        }
                        results = test_catalog_config(connection_info, selected_catalog_config, engine)
                        for msg in results:
                            if msg.startswith("✅"):
                                st.success(msg)
                            elif msg.startswith("❌"):
                                st.error(msg)
                            else:
                                st.info(msg)

                with col_edit:
                    if st.button("✏️ Edit", key=f"btn_edit_catalog_config_{selected_catalog_config['id']}"):
                        st.session_state["edit_catalog_config_id"] = selected_catalog_config["id"]
                        st.session_state["new_config_connection_id"] = None
                        st.rerun()

                with col_deactivate:
                    if st.button("🗑️ Deactivate", key=f"btn_deactivate_catalog_config_{selected_catalog_config['id']}"):
                        try:
                            with engine.begin() as conn:
                                conn.execute(sa.text("""
                                    UPDATE config.catalog_connection_config
                                    SET is_active = FALSE,
                                        updated_at = CURRENT_TIMESTAMP
                                    WHERE id = :id
                                """), {"id": selected_catalog_config["id"]})
                            st.success("Catalog configuration deactivated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Deactivation failed: {e}")

    else:
        st.warning("🔎 No catalog configurations found for this connection.")

    if st.button("➕ Create New Catalog Configuration", key=f"btn_new_config_{selected_conn_id}"):
        st.session_state["new_config_connection_id"] = selected_conn_id
        st.session_state["edit_catalog_config_id"] = None
        st.rerun()

    if new_config_id == selected_conn_id and not edit_config_id:
        with st.expander("➕ New Catalog Configuration", expanded=True):
            render_catalog_config_editor(engine, selected_conn_id, None)


            if st.button("❌ Cancel", key=f"cancel_new_catalog_config_{selected_conn_id}"):
                reset_catalog_config_session_keys("new")
                st.session_state.pop("new_config_connection_id", None)
                st.rerun()

def render_catalog_config_editor(engine, connection_id, config_id):
    with engine.connect() as conn:
        selected_conn = conn.execute(
            sa.text("SELECT * FROM config.connections WHERE id = :id"),
            {"id": connection_id}
        ).fetchone()

    config_to_edit = None
    if config_id:
        with engine.connect() as conn:
            config_to_edit = conn.execute(
                sa.text("SELECT * FROM config.catalog_connection_config WHERE id = :id"),
                {"id": config_id}
            ).fetchone()

    st.divider()

    uid = config_id or "new"

    catalog_name = st.text_input(
        "Configuration name",
        value=config_to_edit.config_name if config_to_edit else "",
        key=f"catalog_name_{uid}"
    )
    catalog_dbf = st.text_input(
        "Database filter (optional, comma-separated)",
        value=config_to_edit.catalog_database_filter if config_to_edit else "",
        help="Leave empty to test only server connectivity. Enter multiple database names separated by commas.",
        key=f"catalog_dbf_{uid}"
    )
    catalog_scf = st.text_input(
        "Schema filter (optional - comma separated)",
        value=config_to_edit.catalog_schema_filter if config_to_edit else "",
        help="Non-existing or inaccessible schemas will be skipped and are not validated in connection tests.",
        key=f"catalog_scf_{uid}"
    )
    catalog_tbf = st.text_input(
        "Table filter (optional - comma separated)",
        value=config_to_edit.catalog_table_filter if config_to_edit else "",
        help="Use comma-separated table names or patterns with wildcards (e.g., sales_*). Non-existing or inaccessible tables are skipped and not tested.",
        key=f"catalog_tbf_{uid}"
    )
    include_views = st.checkbox(
        "📄 Include views",
        value=config_to_edit.include_views if config_to_edit else True,
        key=f"catalog_views_{uid}"
    )
    include_sys = st.checkbox(
        "⚙️ Include system objects",
        value=config_to_edit.include_system_objects if config_to_edit else False,
        key=f"catalog_sys_{uid}"
    )
    is_active = st.checkbox(
        "🟢 Active",
        value=config_to_edit.is_active if config_to_edit else True,
        key=f"catalog_active_{uid}"
    )
    notes = st.text_area(
        "Notes",
        value=config_to_edit.notes if config_to_edit else "",
        key=f"catalog_notes_{uid}"
    )

    if not catalog_name.strip():
        st.warning("⚠️ Please provide a name for this configuration.")
        return False  # Niet opslaan

    if st.button("💾 Save Catalog Configuration", key=f"save_catalog_editor_{uid}"):
        with engine.begin() as conn:
            if config_to_edit:
                conn.execute(sa.text("""
                    UPDATE config.catalog_connection_config
                    SET config_name = :name,
                        catalog_database_filter = :dbf,
                        catalog_schema_filter = :scf,
                        catalog_table_filter = :tbf,
                        include_views = :views,
                        include_system_objects = :sys,
                        is_active = :active,
                        notes = :notes,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """), {
                    "id": config_to_edit.id,
                    "name": catalog_name.strip(),
                    "dbf": catalog_dbf.strip(),
                    "scf": catalog_scf.strip(),
                    "tbf": catalog_tbf.strip(),
                    "views": include_views,
                    "sys": include_sys,
                    "active": is_active,
                    "notes": notes.strip()
                })
                st.success("✅ Configuratie bijgewerkt.")
            else:
                conn.execute(sa.text("""
                    INSERT INTO config.catalog_connection_config
                    (connection_id, config_name, catalog_database_filter, catalog_schema_filter, catalog_table_filter,
                    include_views, include_system_objects, is_active, notes)
                    VALUES (:cid, :name, :dbf, :scf, :tbf, :views, :sys, :active, :notes)
                """), {
                    "cid": selected_conn.id,
                    "name": catalog_name.strip(),
                    "dbf": catalog_dbf.strip(),
                    "scf": catalog_scf.strip(),
                    "tbf": catalog_tbf.strip(),
                    "views": include_views,
                    "sys": include_sys,
                    "active": is_active,
                    "notes": notes.strip()
                })
                st.success("✅ Nieuwe configuratie opgeslagen.")

        # Na opslaan sessie keys verwijderen om editor te sluiten
        st.session_state.pop("edit_catalog_config_id", None)
        st.session_state.pop("new_config_connection_id", None)
        reset_catalog_config_session_keys(uid)
        st.rerun()
        return True

    return False

def get_current_catalog_config_input(config_id=None):
    uid = config_id or "new"

    return {
        "config_name": st.session_state.get(f"catalog_name_{uid}", "").strip(),
        "catalog_database_filter": st.session_state.get(f"catalog_dbf_{uid}", "").strip(),
        "catalog_schema_filter": st.session_state.get(f"catalog_scf_{uid}", "").strip(),
        "catalog_table_filter": st.session_state.get(f"catalog_tbf_{uid}", "").strip(),
        "include_views": st.session_state.get(f"catalog_views_{uid}", True),
        "include_system_objects": st.session_state.get(f"catalog_sys_{uid}", False),
        "is_active": st.session_state.get(f"catalog_active_{uid}", True),
        "notes": st.session_state.get(f"catalog_notes_{uid}", "").strip()
    }

def render_catalog_config_readonly(config: dict):
    st.markdown(f"**Status:** {'🟢 Active' if config['is_active'] else '🔴 Inactive'}")
    st.markdown(f"**Database filter:** `{config['catalog_database_filter'] or '-'}`")
    st.markdown(f"**Schema filter:** `{config['catalog_schema_filter'] or '-'}`")
    st.markdown(f"**Table filter:** `{config['catalog_table_filter'] or '-'}`")
    st.markdown(f"**Include views:** {'✅' if config['include_views'] else '❌'}")
    st.markdown(f"**Include system objects:** {'✅' if config['include_system_objects'] else '❌'}")
    st.markdown(f"**Notes:** {config['notes'] or '-'}")
    st.markdown(f"*Last updated: {config['updated_at'].strftime('%Y-%m-%d %H:%M:%S')}*")

# ---------------- AI Config sectie ----------------

def reset_ai_config_session_keys(uid):
    # uid is config_id of "new"
    keys = [
        f"ai_name_{uid}",
        f"ai_database_filter_{uid}",
        f"ai_schema_filter_{uid}",
        f"ai_table_filter_{uid}",
        f"ai_model_version_{uid}",
        f"ai_active_{uid}",
        f"ai_notes_{uid}"
    ]
    for key in keys:
        if key in st.session_state:
            del st.session_state[key]

def trigger_reset_ai_config(config_id):
    st.session_state["reset_ai_form"] = True
    st.session_state["reset_ai_form_id"] = config_id
    st.rerun()

def render_ai_config_section(selected_conn, engine):
    # Reset form values als resetflag staat
    if st.session_state.get("reset_ai_form", False):
        config_id = st.session_state.get("reset_ai_form_id")
        if config_id is not None:
            with engine.connect() as conn:
                result = conn.execute(sa.text("""
                    SELECT config_name, ai_database_filter, ai_schema_filter, ai_table_filter,
                           ai_model_version, is_active, notes
                    FROM config.ai_analyzer_connection_config
                    WHERE id = :id
                """), {"id": config_id}).fetchone()

            if result:
                row = dict(result._mapping)
                st.session_state[f"ai_name_{config_id}"] = row["config_name"]
                st.session_state[f"ai_database_filter_{config_id}"] = row["ai_database_filter"] or ""
                st.session_state[f"ai_schema_filter_{config_id}"] = row["ai_schema_filter"] or ""
                st.session_state[f"ai_table_filter_{config_id}"] = row["ai_table_filter"] or ""
                st.session_state[f"ai_model_version_{config_id}"] = row["ai_model_version"] or ""
                st.session_state[f"ai_active_{config_id}"] = row["is_active"]
                st.session_state[f"ai_notes_{config_id}"] = row["notes"] or ""

        st.session_state["reset_ai_form"] = False
        st.session_state["reset_ai_form_id"] = None

    if not selected_conn:
        st.info("No connection selected, cannot show AI configurations.")
        return

    selected_conn_id = selected_conn["id"]

    st.divider()
    st.markdown("## 🧠 AI Analysis Configurations")

    show_inactive = st.checkbox(
        "Show inactive AI configurations",
        value=False,
        key=f"show_inactive_ai_{selected_conn_id}"
    )

    try:
        with engine.connect() as db_conn:
            ai_configs = db_conn.execute(sa.text(f"""
                SELECT * FROM config.ai_analyzer_connection_config
                WHERE connection_id = :id
                {"AND is_active = TRUE" if not show_inactive else ""}
                ORDER BY updated_at DESC
            """), {"id": selected_conn_id}).fetchall()
    except Exception as e:
        st.error(f"Failed to fetch AI configurations: {e}")
        ai_configs = []

    edit_ai_id = st.session_state.get("edit_ai_config_id")
    new_ai_conn_id = st.session_state.get("new_ai_config_connection_id")

    if ai_configs:
        ai_options = {
            f"🧠 AI ID {a.id} | {'🟢 Active' if a.is_active else '🔴 Inactive'} | DB: {a.ai_database_filter or '-'} | SC: {a.ai_schema_filter or '-'} | TB: {a.ai_table_filter or '-'}":
            dict(a._mapping)
            for a in ai_configs
        }

        selected_label = st.selectbox(
            "Select AI configuration",
            list(ai_options.keys()),
            key=f"ai_select_{selected_conn_id}"
        )
        selected_ai_config = ai_options[selected_label]

        if edit_ai_id == selected_ai_config["id"]:
            with st.expander(f"✍️ Edit AI Configuration - {selected_ai_config['config_name']}", expanded=True):
                render_ai_config_editor(engine, selected_conn_id, edit_ai_id)


                if st.button("❌ Cancel", key=f"cancel_edit_ai_{selected_ai_config['id']}"):
                    reset_ai_config_session_keys(edit_ai_id)
                    st.session_state.pop("edit_ai_config_id", None)
                    st.rerun()


        else:
            with st.expander(f"⚙️ AI Configuration - {selected_ai_config['config_name']}", expanded=False):
                render_ai_config_readonly(selected_ai_config)

                connection_info = {
                    "id": selected_conn["id"],
                    "connection_type": selected_conn["connection_type"],
                    "host": selected_conn.get("host"),
                    "port": selected_conn.get("port"),
                    "username": selected_conn.get("username"),
                    "password": selected_conn.get("password"),
                    "folder_path": selected_conn.get("folder_path")
                }

                col_test, col_edit, col_deactivate = st.columns(3)

                with col_test:
                    if st.button("🔍 Test AI Config", key=f"btn_test_ai_config_{selected_ai_config['id']}"):
                        results = test_ai_config(connection_info, selected_ai_config, engine)
                        for msg in results:
                            if msg.startswith("✅"):
                                st.success(msg)
                            elif msg.startswith("❌"):
                                st.error(msg)
                            else:
                                st.info(msg)

                        # Status opnieuw ophalen om up-to-date te zijn
                        status_info = get_ai_config_test_status(engine, selected_ai_config["id"])
                        if status_info["status"] == "success":
                            st.markdown("Status: 🟢 Test geslaagd")
                        elif status_info["status"] == "failed":
                            st.markdown("Status: 🔴 Test mislukt")
                        else:
                            st.markdown("Status: ⚪️ Niet getest")

                with col_edit:
                    if st.button("✏️ Edit", key=f"btn_edit_ai_config_{selected_ai_config['id']}"):
                        st.session_state["edit_ai_config_id"] = selected_ai_config["id"]
                        st.session_state["new_ai_config_connection_id"] = None
                        st.rerun()

                with col_deactivate:
                    if st.button("🗑️ Deactivate", key=f"btn_deactivate_ai_config_{selected_ai_config['id']}"):
                        try:
                            with engine.begin() as conn:
                                conn.execute(sa.text("""
                                    UPDATE config.ai_analyzer_connection_config
                                    SET is_active = FALSE,
                                        updated_at = CURRENT_TIMESTAMP
                                    WHERE id = :id
                                """), {"id": selected_ai_config["id"]})
                            st.success("AI configuration deactivated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Deactivation failed: {e}")

    else:
        st.warning("🔎 No AI configurations found for this connection.")

    if st.button("➕ Create New AI Configuration", key=f"btn_new_ai_config_{selected_conn_id}"):
        st.session_state["new_ai_config_connection_id"] = selected_conn_id
        st.session_state["edit_ai_config_id"] = None
        st.rerun()

    if new_ai_conn_id == selected_conn_id and not edit_ai_id:
        with st.expander("➕ New AI Configuration", expanded=True):
            render_ai_config_editor(engine, selected_conn_id, None)


            if st.button("❌ Cancel", key=f"cancel_new_ai_config_{selected_conn_id}"):
                reset_ai_config_session_keys("new")
                st.session_state.pop("new_ai_config_connection_id", None)
                st.rerun()


def render_ai_config_editor(engine, connection_id, config_id):
    with engine.connect() as conn:
        selected_conn = conn.execute(
            sa.text("SELECT * FROM config.connections WHERE id = :id"),
            {"id": connection_id}
        ).fetchone()

    config_to_edit = None
    if config_id:
        with engine.connect() as conn:
            config_to_edit = conn.execute(
                sa.text("SELECT * FROM config.ai_analyzer_connection_config WHERE id = :id"),
                {"id": config_id}
            ).fetchone()

    # Zet initiele values in st.session_state (indien nog niet gezet)
    uid = config_id or "new"

    def init_session_state(key, default):
        if key not in st.session_state:
            st.session_state[key] = default

    init_session_state(f"ai_name_{uid}", config_to_edit.config_name if config_to_edit else "")
    init_session_state(f"ai_database_filter_{uid}", config_to_edit.ai_database_filter if config_to_edit else "")
    init_session_state(f"ai_schema_filter_{uid}", config_to_edit.ai_schema_filter if config_to_edit else "")
    init_session_state(f"ai_table_filter_{uid}", config_to_edit.ai_table_filter if config_to_edit else "")
    init_session_state(f"ai_model_version_{uid}", config_to_edit.ai_model_version if config_to_edit else "default")
    init_session_state(f"ai_active_{uid}", config_to_edit.is_active if config_to_edit else True)
    init_session_state(f"ai_notes_{uid}", config_to_edit.notes if config_to_edit else "")

    st.divider()

    valid_name = True
    valid_db_filter = True

    ai_name = st.text_input(
        "Configuration name *",
        value=st.session_state.get(f"ai_name_{uid}", ""),
        key=f"ai_name_{uid}"
    )

    ai_db_filter = st.text_input(
        "Database filter (required, single database)",
        value=st.session_state.get(f"ai_database_filter_{uid}", ""),
        help="Enter exactly one database name. This database will be tested for connectivity.",
        key=f"ai_database_filter_{uid}"
    )


    ai_schema_filter = st.text_input(
        "Schema filter (optional) - comma separated",
        help="Optional. Non-existing or inaccessible schemas will be skipped and are not validated in connection tests.",
        key=f"ai_schema_filter_{uid}"
    )

    ai_table_filter = st.text_input(
        "Table filter (optional) - comma separated",
        help="Optional. Use comma-separated table names or patterns with wildcards (e.g., sales_*). Non-existing or inaccessible tables are skipped and not tested.",
        key=f"ai_table_filter_{uid}"
    )

    ai_model_version = st.text_input("Model version", key=f"ai_model_version_{uid}")

    is_active = st.checkbox("🟢 Active", key=f"ai_active_{uid}")

    notes = st.text_area("Notes", key=f"ai_notes_{uid}")

   # Validatie Configuration name

    # Valideer alleen voor foutmeldingen (geen return)
    if not ai_name.strip():
        st.error("⚠️ Configuration name is required.")
        valid_name = False

    db_filter_val = ai_db_filter.strip()
    if "," in db_filter_val or db_filter_val == "":
        st.error("⚠️ Please enter exactly one database name (no commas, cannot be empty).")
        valid_db_filter = False


    if not st.session_state[f"ai_name_{uid}"].strip() or not valid_db_filter:
        return False

    if st.button("💾 Save AI Configuration", key=f"save_ai_config_{uid}"):
            if not (valid_name and valid_db_filter):
                st.error("⚠️ Fix errors before saving.")
            else:
                with engine.begin() as conn:
                    if config_to_edit:
                        conn.execute(sa.text("""
                            UPDATE config.ai_analyzer_connection_config
                            SET config_name = :name,
                                ai_database_filter = :dbf,
                                ai_schema_filter = :scf,
                                ai_table_filter = :tbf,
                                ai_model_version = :model,
                                is_active = :active,
                                notes = :notes,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                        """), {
                            "id": config_to_edit.id,
                            "name": ai_name.strip(),
                            "dbf": ai_db_filter.strip(),
                            "scf": ai_schema_filter.strip(),
                            "tbf": ai_table_filter.strip(),
                            "model": ai_model_version.strip(),
                            "active": is_active,
                            "notes": notes.strip()
                        })
                        st.success("✅ AI configuratie bijgewerkt.")
                    else:
                        conn.execute(sa.text("""
                            INSERT INTO config.ai_analyzer_connection_config
                            (connection_id, config_name, ai_database_filter, ai_schema_filter, ai_table_filter,
                            ai_model_version, is_active, notes)
                            VALUES (:cid, :name, :dbf, :scf, :tbf, :model, :active, :notes)
                        """), {
                            "cid": selected_conn.id,
                            "name": ai_name.strip(),
                            "dbf": ai_db_filter.strip(),
                            "scf": ai_schema_filter.strip(),
                            "tbf": ai_table_filter.strip(),
                            "model": ai_model_version.strip(),
                            "active": is_active,
                            "notes": notes.strip()
                        })
                        st.success("✅ Nieuwe AI configuratie opgeslagen.")

            st.session_state.pop("edit_ai_config_id", None)
            st.session_state.pop("new_ai_config_connection_id", None)
            reset_ai_config_session_keys(uid)
            st.rerun()
            return True

    return False

def get_current_ai_config_input(config_id=None):
    uid = config_id or "new"
    return {
        "config_name": st.session_state.get(f"ai_name_{uid}", "").strip(),
        "ai_database_filter": st.session_state.get(f"ai_database_filter_{uid}", "").strip(),
        "ai_schema_filter": st.session_state.get(f"ai_schema_filter_{uid}", "").strip(),
        "ai_table_filter": st.session_state.get(f"ai_table_filter_{uid}", "").strip(),
        "ai_model_version": st.session_state.get(f"ai_model_version_{uid}", "").strip(),
        "is_active": st.session_state.get(f"ai_active_{uid}", True),
        "notes": st.session_state.get(f"ai_notes_{uid}", "").strip(),
        "id": config_id
    }

def render_ai_config_readonly(config: dict):
    header = config.get("config_name") or f"ID {config.get('id')}"
    st.markdown(f"### 🧠 AI Configuration – {header}")
    st.markdown(f"**Status:** {'🟢 Active' if config.get('is_active') else '🔴 Inactive'}")
    st.markdown(f"**Database filter:** `{config.get('ai_database_filter') or '-'}`")
    st.markdown(f"**Schema filter:** `{config.get('ai_schema_filter') or '-'}`")
    st.markdown(f"**Table filter:** `{config.get('ai_table_filter') or '-'}`")
    st.markdown(f"**Model version:** `{config.get('ai_model_version') or '-'}`")
    st.markdown(f"**Notes:** {config.get('notes') or '-'}")
    updated_at = config.get('updated_at')
    last_test_status = config.get('last_test_status')
    last_tested_at = config.get('last_tested_at')
    last_test_notes = config.get('last_test_notes')

    if updated_at:
        if isinstance(updated_at, str):
            try:
                updated_at = datetime.fromisoformat(updated_at)
            except Exception:
                # fallback: toon als tekst zonder formatting
                st.markdown(f"*Last updated: {updated_at}*")
                return
        # Nu is het een datetime object, format netjes
        st.markdown(f"*Last updated: {updated_at.strftime('%Y-%m-%d %H:%M:%S')}*")

    # Format last test info as one line
    if last_test_status or last_tested_at or last_test_notes:
        # Format last_tested_at
        if last_tested_at:
            if isinstance(last_tested_at, str):
                try:
                    last_tested_dt = datetime.fromisoformat(last_tested_at)
                except Exception:
                    last_tested_dt = None
            else:
                last_tested_dt = last_tested_at
            last_tested_str = last_tested_dt.strftime('%Y-%m-%d %H:%M:%S') if last_tested_dt else last_tested_at
        else:
            last_tested_str = "Unknown"
        
        # Construct a concise test info line
        test_info_line = f"**Last test:** Status: `{last_test_status or '-'} | Tested at: {last_tested_str} | Notes:** {last_test_notes or '-'}"
        st.markdown(test_info_line)


# --- Main Connection sectie ---

def get_current_main_connection_input():
    return {
        "name": st.session_state.get("edit_name", "").strip(),
        "host": st.session_state.get("edit_host", "").strip(),
        "port": st.session_state.get("edit_port", "").strip(),
        "username": st.session_state.get("edit_user", "").strip(),
        "password": st.session_state.get("edit_password", "").strip(),  
        "folder_path": st.session_state.get("edit_folder", "").strip(),
        "is_active": st.session_state.get("edit_active", True),
        "connection_type": st.session_state.get("edit_type", ""),
    }

def render_main_connection_section(engine):
    connections = get_source_connections()
    connections = [{**c, "id": int(c["id"])} for c in connections]

    selected_conn_id = st.session_state.get("selected_main_connection_id")
    edit_conn_id = st.session_state.get("edit_main_connection_id")
    new_conn_flag = st.session_state.get("new_main_connection", False)

    conn_labels = [f"{c['name']} (ID: {c['id']})" for c in connections]
    conn_ids = [c["id"] for c in connections]

    def get_selected_index():
        if selected_conn_id and selected_conn_id in conn_ids:
            return conn_ids.index(selected_conn_id)
        return 0

    selected_label = st.selectbox(
        "Select an existing main connection",
        conn_labels,
        index=get_selected_index(),
        key="main_connection_select"
    )
    selected_id = int(selected_label.split("ID: ")[-1].rstrip(")"))

    if selected_id != selected_conn_id:
        st.session_state["selected_main_connection_id"] = selected_id
        st.session_state.pop("edit_main_connection_id", None)
        st.session_state["new_main_connection"] = False
        st.rerun()

    selected_conn = next((c for c in connections if c["id"] == selected_id), None)

    st.divider()

    if selected_conn:
        with st.expander(f"🔌 Main Connection Details - {selected_conn['name']}", expanded=False):

            if edit_conn_id == selected_conn["id"]:
                render_main_connection_editor(engine, selected_conn)
                if st.button("❌ Cancel", key="cancel_edit_main_conn"):
                    st.session_state.pop("edit_main_connection_id", None)
                    st.rerun()

            else:
                # Expliciete interpretatie van is_active
                is_active = selected_conn.get('is_active')
                is_active_bool = False
                if isinstance(is_active, bool):
                    is_active_bool = is_active
                elif isinstance(is_active, int):
                    is_active_bool = (is_active != 0)
                elif isinstance(is_active, str):
                    is_active_bool = is_active.lower() in ('true', '1', 'yes')

                status_text = "🟢 Active" if is_active_bool else "🔴 Inactive"
                st.markdown(f"**Status:** {status_text}")
                st.markdown(f"**Type:** {selected_conn['connection_type']}")
                st.markdown(f"**Host:** {selected_conn.get('host') or '-'}")
                st.markdown(f"**Port:** {selected_conn.get('port') or '-'}")
                st.markdown(f"**Username:** {selected_conn.get('username') or '-'}")
                st.markdown(f"**Folder Path:** {selected_conn.get('folder_path') or '-'}")



                col_test, col_edit, col_deactivate = st.columns(3)
                with col_test:
                    if st.button("🔍 Test Connection", key=f"btn_test_main_conn_{selected_conn['id']}"):
                        results = test_main_connection(selected_conn, engine)
                        for msg in results:
                            if msg.startswith("✅"):
                                st.success(msg)
                            elif msg.startswith("❌"):
                                st.error(msg)
                            else:
                                st.info(msg)
                status_info = get_main_connection_test_status(engine, selected_conn["id"])

                if status_info["status"] == "success":
                    st.markdown("Status: 🟢 Connection OK")
                elif status_info["status"] == "failed":
                    st.markdown("Status: 🔴 Connection Failed")
                else:
                    st.markdown("Status: ⚪️ Niet getest")

                with col_edit:
                    if st.button("✏️ Edit", key=f"btn_edit_main_conn_{selected_conn['id']}"):
                        st.session_state["edit_main_connection_id"] = selected_conn["id"]
                        st.rerun()
                with col_deactivate:
                    if st.button("🗑️ Deactivate", key=f"btn_deactivate_main_conn_{selected_conn['id']}"):
                        with engine.begin() as conn:
                            conn.execute(sa.text("""
                                UPDATE config.connections
                                SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """), {"id": selected_conn["id"]})
                        st.success("Connection deactivated.")
                        st.rerun()

    st.divider()
    if st.button("➕ Create New Main Connection", key="btn_new_main_connection"):
        st.session_state["new_main_connection"] = True
        st.session_state.pop("edit_main_connection_id", None)
        st.rerun()

    if new_conn_flag:
        with st.expander("➕ New Main Connection", expanded=True):
            render_main_connection_editor(engine, None)

            col_test, col_cancel = st.columns([1,1])

            with col_test:
                if st.button("🔍 Test new connection", key="btn_test_new_main_conn"):
                    conn_info = {
                        "name": st.session_state.get("edit_name", ""),
                        "host": st.session_state.get("edit_host", ""),
                        "port": st.session_state.get("edit_port", ""),
                        "username": st.session_state.get("edit_user", ""),
                        "password": st.session_state.get("edit_password", ""), 
                        "folder_path": st.session_state.get("edit_folder", ""),
                        "connection_type": st.session_state.get("edit_type", ""),
                        "is_active": st.session_state.get("edit_active", True)
                    }
                    results = test_main_connection(conn_info, engine)
                    for msg in results:
                        if msg.startswith("✅"):
                            st.success(msg)
                        elif msg.startswith("❌"):
                            st.error(msg)
                        else:
                            st.info(msg)

            with col_cancel:
                if st.button("❌ Cancel", key="cancel_new_main_conn"):
                    st.session_state.pop("new_main_connection", None)
                    st.rerun()

    return selected_conn



def render_main_connection_editor(engine, conn=None):
    allowed_connection_types = [
        "Power BI Semantic Model",
        "Azure SQL Server",
        "PostgreSQL"
    ]

    conn_type = conn["connection_type"] if conn else None
    is_sql = conn_type != "Power BI Semantic Model"

    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input(
            "Name *",
            value=conn["name"] if conn else "",
            key="edit_name"
        )

        if conn:
            # Bestaande connectie: disabled dropdown met huidige waarde
            st.selectbox(
                "Connection Type *",
                options=allowed_connection_types,
                index=allowed_connection_types.index(conn_type) if conn_type in allowed_connection_types else 0,
                disabled=True,
                key="edit_type"
            )
        else:
            # Nieuwe connectie: dropdown select
            conn_type = st.selectbox(
                "Connection Type *",
                options=allowed_connection_types,
                index=0,
                key="edit_type"
            )

        active = st.checkbox(
            "Active",
            value=conn.get("is_active", True) if conn else True,
            key="edit_active"
        )

    with col2:
        if is_sql:
            host = st.text_input(
                "Host *",
                value=conn["host"] if conn else "",
                key="edit_host"
            )
            port = st.text_input(
                "Port",
                value=conn["port"] if conn else "",
                key="edit_port"
            )
            username = st.text_input(
                "Username",
                value=conn["username"] if conn else "",
                key="edit_user"
            )
            password = st.text_input(
                "Password",
                value=conn.get("password", "") if conn else "",
                type="password",
                key="edit_password"
            )
        else:
            folder = st.text_input(
                "Folder Path",
                value=conn.get("folder_path", "") if conn else "",
                key="edit_folder"
            )

    # Validatie verplichte velden
    errors = []
    if not name.strip():
        errors.append("⚠️ Name is required.")
    if not st.session_state.get("edit_type"):
        errors.append("⚠️ Connection Type is required.")
    if is_sql and not st.session_state.get("edit_host", "").strip():
        errors.append("⚠️ Host is required.")

    if errors:
        for err in errors:
            st.error(err)
        return False

    if st.button("💾 Save", key="save_main_connection"):
        with engine.begin() as db_conn:
            if conn:
                db_conn.execute(sa.text("""
                    UPDATE config.connections
                    SET name = :name,
                        host = :host,
                        port = :port,
                        username = :username,
                        password = :password,
                        folder_path = :folder_path,
                        is_active = :active,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """), {
                    "id": conn["id"],
                    "name": name.strip(),
                    "host": host.strip() if is_sql else None,
                    "port": port.strip() if is_sql else None,
                    "username": username.strip() if is_sql else None,
                    "password": st.session_state.get("edit_password", "") if is_sql else None,
                    "folder_path": folder.strip() if not is_sql else None,
                    "active": active
                })
                st.success("Main connection updated.")
                st.session_state.pop("edit_main_connection_id", None)
            else:
                db_conn.execute(sa.text("""
                    INSERT INTO config.connections
                    (name, connection_type, host, port, username, password, folder_path, is_active)
                    VALUES (:name, :ctype, :host, :port, :username, :password, :folder, :active)
                """), {
                    "name": name.strip(),
                    "ctype": st.session_state.get("edit_type"),
                    "host": host.strip() if is_sql else None,
                    "port": port.strip() if is_sql else None,
                    "username": username.strip() if is_sql else None,
                    "password": st.session_state.get("edit_password", "") if is_sql else None,
                    "folder": folder.strip() if not is_sql else None,
                    "active": active
                })
                st.success("New main connection created.")
                st.session_state.pop("new_main_connection", None)
        st.rerun()
        return True

    return False

def update_main_connection_test_status(engine, connection_id, status, notes):
    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE config.connections
            SET last_test_status = :status,
                last_tested_at = CURRENT_TIMESTAMP,
                last_test_notes = :notes
            WHERE id = :id
        """), {
            "id": connection_id,
            "status": status,
            "notes": notes
        })

def get_source_connections():
    """Fetch all source database connections from the config.connections table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text("""
                SELECT id
                     , name
                     , connection_type
                     , host
                     , port
                     , username
                     , password
                     , folder_path
                     , execution_mode
                     , is_active
                FROM config.connections
                WHERE is_active = TRUE
                ORDER BY id
            """))
            connections = [dict(row._mapping) for row in result]
            return connections
    except Exception as e:
        st.error(f"Failed to fetch connections: {e}")
        return []

def get_connection_config(table: str, connection_id: int):
    with engine.connect() as conn:
        result = conn.execute(sa.text(f"""
            SELECT * FROM config.{table}
            WHERE connection_id = :id AND is_active = TRUE
        """), {"id": connection_id}).fetchone()
        return dict(result._mapping) if result else None

def upsert_connection_config(table: str, connection_id: int, data: dict):
    fields = ", ".join(data.keys())
    placeholders = ", ".join(f":{k}" for k in data.keys())
    updates = ", ".join(f"{k} = EXCLUDED.{k}" for k in data.keys())
    query = f"""
        INSERT INTO config.{table} (connection_id, {fields})
        VALUES (:connection_id, {placeholders})
        ON CONFLICT (connection_id) DO UPDATE
        SET {updates}, updated_at = CURRENT_TIMESTAMP
    """
    with engine.begin() as conn:
        conn.execute(sa.text(query), {"connection_id": connection_id, **data})

st.title("Connection Manager")

# Create tabs for active and deleted connections
tab1, tab2 = st.tabs(["Active Connections", "Deleted Connections"])

# === ACTIVE CONNECTIONS SECTION ===
with tab1:
    st.subheader("🔌 Main Connections")
    selected_conn = render_main_connection_section(engine)

    if selected_conn:
        render_catalog_config_section(selected_conn, engine)
        render_ai_config_section(selected_conn, engine)
    else:
        st.info("No connection selected or no connections available.")

# === DELETED CONNECTIONS SECTION ===
with tab2:
    st.subheader("Deleted Connections")

    try:
        # Fetch deleted connections
        with engine.connect() as conn:
            result = conn.execute(sa.text("""
            SELECT id, name, connection_type, host, port, username, password, folder_path
            FROM config.connections
            WHERE is_active = FALSE
            ORDER BY id
            """))
            deleted_connections = [dict(row._mapping) for row in result]

        if deleted_connections:
            # Iterate through each deleted connection and create an expandable box
            for conn in deleted_connections:
                with st.expander(f"{conn['name']} (ID: {conn['id']})"):
                    # Display connection details
                    # Create two columns for layout
                    col1, col2 = st.columns(2)

                    # Display connection details in the first column
                    with col1:
                        st.write(f"**ID:** {conn['id']}")
                        st.write(f"**Type:** {conn['connection_type']}")
                        st.write(f"**Host:** {conn['host']}")
                        st.write(f"**Port:** {conn['port']}")
                        st.write(f"**Username:** {conn['username']}")

                    # Display filters (databases, schemas, and tables) in the second column
                    with col2:
                        if conn["connection_type"] == "Power BI Semantic Model":
                            st.write(f"**Folder Path:** {conn['folder_path'] or 'None'}")
                        else:
                            st.write("**Database:** Not stored in main connection")
                            st.write("**Schemas:** Defined in catalog or AI configs")
                            st.write("**Tables:** Defined in catalog or AI configs")
                    
                    # Add Restore Connection button
                    if st.button(f"Restore {conn['name']}", key=f"restore_{conn['id']}"):
                        try:
                            with engine.begin() as db_conn:
                                db_conn.execute(
                                    sa.text("""
                                        UPDATE config.connections
                                        SET is_active = TRUE
                                        WHERE id = :id
                                    """),
                                    {"id": conn["id"]}
                                )
                            st.success(f"Connection '{conn['name']}' restored.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Failed to restore connection: {e}")
        else:
            st.info("No deleted connections found.")
    except Exception as e:
        st.error(f"Failed to fetch deleted connections: {e}")