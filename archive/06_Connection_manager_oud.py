import streamlit as st
import sqlalchemy as sa

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
from shared_utils import test_main_connection,test_catalog_config,test_ai_config,get_main_connection_test_status,get_catalog_config_test_status,get_ai_config_test_status

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


# === FUNCTIONS ===


def render_catalog_config_section(selected_conn, engine):
    selected_conn_id = selected_conn["id"]

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

                col_cancel, col_reset = st.columns(2)
                with col_cancel:
                    if st.button("✖️ Cancel edit", key=f"cancel_edit_catalog_{selected_catalog_config['id']}"):
                        reset_catalog_config_session_keys(edit_config_id)
                        st.session_state.pop("edit_catalog_config_id", None)
                        st.rerun()
                with col_reset:
                    if st.button("🔁 Reset", key=f"reset_edit_catalog_{selected_catalog_config['id']}"):
                        reset_catalog_config_form(engine, edit_config_id)
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

            col_cancel, col_reset = st.columns(2)
            with col_cancel:
                if st.button("❌ Cancel", key=f"cancel_new_catalog_config_{selected_conn_id}"):
                    reset_catalog_config_session_keys("new")
                    st.session_state.pop("new_config_connection_id", None)
                    st.rerun()
            with col_reset:
                if st.button("🔁 Reset", key=f"reset_new_catalog_config_{selected_conn_id}"):
                    reset_catalog_config_session_keys("new")
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
    """Leest tijdelijke waarden uit Streamlit session_state voor de editor."""
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

def reset_catalog_config_form(engine, config_id):
    with engine.connect() as conn:
        result = conn.execute(
            sa.text("""
                SELECT config_name, catalog_database_filter, catalog_schema_filter, catalog_table_filter,
                       include_views, include_system_objects, is_active, notes
                FROM config.catalog_connection_config
                WHERE id = :id
            """),
            {"id": config_id}
        ).fetchone()

    if result:
        st.session_state[f"catalog_name_{config_id}"] = result["config_name"]
        st.session_state[f"catalog_dbf_{config_id}"] = result["catalog_database_filter"] or ""
        st.session_state[f"catalog_scf_{config_id}"] = result["catalog_schema_filter"] or ""
        st.session_state[f"catalog_tbf_{config_id}"] = result["catalog_table_filter"] or ""
        st.session_state[f"catalog_views_{config_id}"] = result["include_views"]
        st.session_state[f"catalog_sys_{config_id}"] = result["include_system_objects"]
        st.session_state[f"catalog_active_{config_id}"] = result["is_active"]
        st.session_state[f"catalog_notes_{config_id}"] = result["notes"] or ""

    st.rerun()

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



def render_ai_config_section(selected_conn, engine):
    selected_conn_id = selected_conn["id"]

    st.divider()
    st.markdown("## 🧠 AI Analysis Configurations")
    show_inactive = st.checkbox(
        "Show inactive AI configurations", 
        value=False, 
        key=f"show_inactive_ai_{selected_conn_id}"
    )

    with engine.connect() as db_conn:
        ai_configs = db_conn.execute(sa.text(f"""
            SELECT * FROM config.ai_analyzer_connection_config
            WHERE connection_id = :id
            {"AND is_active = TRUE" if not show_inactive else ""}
            ORDER BY updated_at DESC
        """), {"id": selected_conn_id}).fetchall()

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

                col_cancel, col_reset = st.columns(2)
                with col_cancel:
                    if st.button("✖️ Cancel edit", key=f"cancel_edit_ai_{selected_ai_config['id']}"):
                        reset_ai_config_session_keys(edit_ai_id)
                        st.session_state.pop("edit_ai_config_id", None)
                        st.rerun()
                with col_reset:
                    if st.button("🔁 Reset", key=f"reset_edit_ai_{selected_ai_config['id']}"):
                        reset_ai_config_form(engine, edit_ai_id)
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

            col_cancel, col_reset = st.columns(2)
            with col_cancel:
                if st.button("❌ Cancel", key=f"cancel_new_ai_config_{selected_conn_id}"):
                    reset_ai_config_session_keys("new")
                    st.session_state.pop("new_ai_config_connection_id", None)
                    st.rerun()
            with col_reset:
                if st.button("🔁 Reset", key=f"reset_new_ai_config_{selected_conn_id}"):
                    reset_ai_config_session_keys("new")
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

    st.divider()

    uid = config_id or "new"

    ai_name = st.text_input(
        "Configuration name",
        value=config_to_edit.config_name if config_to_edit else "",
        key=f"ai_name_{uid}"
    )
    ai_db_filter = st.text_input(
        "Database filter (required, single database)",
        value=config_to_edit.ai_database_filter if config_to_edit else "",
        help="Enter exactly one database name. This database will be tested for connectivity.",
        key=f"ai_database_filter_{uid}"
    )

    # Validatie databasefilter
    db_filter_val = st.session_state.get(f"ai_database_filter_{uid}", "").strip()
    valid_db_filter = True
    if "," in db_filter_val or db_filter_val == "":
        st.error("⚠️ Please enter exactly one database name (no commas, cannot be empty).")
        valid_db_filter = False

    ai_schema_filter = st.text_input(
        "Schema filter (optional) - comma separated",
        value=config_to_edit.ai_schema_filter if config_to_edit else "",
        help="Optional. Non-existing or inaccessible schemas will be skipped and are not validated in connection tests.",
        key=f"ai_schema_filter_{uid}"
    )

    ai_table_filter = st.text_input(
        "Table filter (optional) - comma separated",
        value=config_to_edit.ai_table_filter if config_to_edit else "",
        help="Optional. Use comma-separated table names or patterns with wildcards (e.g., sales_*). Non-existing or inaccessible tables are skipped and not tested.",
        key=f"ai_table_filter_{uid}"
    )

    ai_model_version = st.text_input(
        "Model version",
        value=config_to_edit.ai_model_version if config_to_edit else "default",
        key=f"ai_model_version_{uid}"
    )

    is_active = st.checkbox(
        "🟢 Active",
        value=config_to_edit.is_active if config_to_edit else True,
        key=f"ai_active_{uid}"
    )
    notes = st.text_area(
        "Notes",
        value=config_to_edit.notes if config_to_edit else "",
        key=f"ai_notes_{uid}"
    )

    if not ai_name.strip():
        st.warning("⚠️ Please provide a name for this AI configuration.")
        return False

    if not valid_db_filter:
        return False

    if st.button("💾 Save AI Configuration", key=f"save_ai_config_{uid}"):
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
    """Toon AI configuratie zonder expander (voor gebruik binnen een expander)."""
    header = config.get("config_name") or f"ID {config.get('id')}"
    st.markdown(f"### 🧠 AI Configuration – {header}")
    st.markdown(f"**Status:** {'🟢 Active' if config.get('is_active') else '🔴 Inactive'}")
    st.markdown(f"**Database filter:** `{config.get('ai_database_filter') or '-'}`")
    st.markdown(f"**Schema filter:** `{config.get('ai_schema_filter') or '-'}`")
    st.markdown(f"**Table filter:** `{config.get('ai_table_filter') or '-'}`")
    st.markdown(f"**Model version:** `{config.get('ai_model_version') or '-'}`")
    st.markdown(f"**Notes:** {config.get('notes') or '-'}")
    updated_at = config.get('updated_at')
    if updated_at:
        st.markdown(f"*Last updated: {updated_at.strftime('%Y-%m-%d %H:%M:%S')}*")

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
        st.session_state[f"ai_name_{config_id}"] = result["config_name"]
        st.session_state[f"ai_database_filter_{config_id}"] = result["ai_database_filter"] or ""
        st.session_state[f"ai_schema_filter_{config_id}"] = result["ai_schema_filter"] or ""
        st.session_state[f"ai_table_filter_{config_id}"] = result["ai_table_filter"] or ""
        st.session_state[f"ai_model_version_{config_id}"] = result["ai_model_version"] or ""
        st.session_state[f"ai_active_{config_id}"] = result["is_active"]
        st.session_state[f"ai_notes_{config_id}"] = result["notes"] or ""

    st.rerun()

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



def render_main_connection_section(engine):
    connections = get_source_connections()

    new_connection = st.session_state.get("new_main_connection", False)
    edit_connection_id = st.session_state.get("edit_main_connection_id")

    selected_conn = None
    if connections and not new_connection:
        conn_labels = [f"{c['name']} (ID: {c['id']})" for c in connections]
        selected_label = st.selectbox("Select an existing main connection", conn_labels, key="main_connection_select")
        selected_id = int(selected_label.split("ID: ")[-1].rstrip(")"))
        selected_conn = next((c for c in connections if c["id"] == selected_id), None)

    if st.button("➕ Create New Main Connection", key="btn_new_main_connection"):
        st.session_state["new_main_connection"] = True
        st.session_state["edit_main_connection_id"] = None
        reset_main_connection_session_keys()
        st.rerun()

    if new_connection:
        with st.expander("➕ New Main Connection", expanded=True):
            # … jouw nieuw formulier inputs met keys “edit_...”
            # Knoppen Save en Cancel:
            col_save, col_cancel = st.columns(2)
            with col_cancel:
                if st.button("❌ Cancel", key="cancel_new_main_connection"):
                    reset_main_connection_session_keys()
                    st.session_state["new_main_connection"] = False
                    st.rerun()
            with col_save:
                if st.button("💾 Save New Main Connection", key="save_new_main_connection"):
                    data = get_current_main_connection_input()
                    with engine.begin() as conn:
                        conn.execute(sa.text("""
                            INSERT INTO config.connections (name, connection_type, host, port, username, folder_path, is_active)
                            VALUES (:name, :type, :host, :port, :user, :folder, :active)
                        """), {
                            "name": data["name"],
                            "type": data["connection_type"],
                            "host": data["host"],
                            "port": data["port"],
                            "user": data["username"],
                            "folder": data["folder_path"],
                            "active": data["is_active"]
                        })
                    st.success("✅ New main connection saved.")
                    reset_main_connection_session_keys()
                    st.session_state["new_main_connection"] = False
                    st.rerun()

    elif selected_conn:
        with st.expander(f"⚙️ Edit Main Connection - {selected_conn['name']}", expanded=True):
            # … inputs met waardes uit st.session_state of selected_conn
            col_save, col_cancel = st.columns(2)
            with col_cancel:
                if st.button("❌ Cancel Edit", key="cancel_edit_main_connection"):
                    reset_main_connection_form(engine, selected_conn["id"])
                    st.session_state.pop("edit_main_connection_id", None)
                    st.rerun()
            with col_save:
                if st.button("💾 Save Edit", key="save_edit_main_connection"):
                    data = get_current_main_connection_input()
                    with engine.begin() as conn:
                        conn.execute(sa.text("""
                            UPDATE config.connections
                            SET name = :name, host = :host, port = :port, username = :username,
                                folder_path = :folder, is_active = :active, updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                        """), {
                            "id": selected_conn["id"],
                            "name": data["name"],
                            "host": data["host"],
                            "port": data["port"],
                            "username": data["username"],
                            "folder": data["folder_path"],
                            "active": data["is_active"],
                        })
                    st.success("✅ Main connection updated.")
                    st.session_state.pop("edit_main_connection_id", None)
                    st.rerun()
                
def render_main_connection_editor(engine, conn):
    # conn is dict (bestaand) of None (nieuw)
    conn_type = conn["connection_type"] if conn else ""
    is_sql = conn_type != "Power BI Semantic Model"

    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Name", value=conn["name"] if conn else "", key="edit_name")
        conn_type_field = st.text_input(
            "Connection Type",
            value=conn_type,
            disabled=True if conn else False,
            key="edit_type"
        )
        active = st.checkbox(
            "Active",
            value=conn.get("is_active", True) if conn else True,
            key="edit_active"
        )
    with col2:
        if is_sql:
            host = st.text_input("Host", value=conn["host"] if conn else "", key="edit_host")
            port = st.text_input("Port", value=conn["port"] if conn else "", key="edit_port")
            username = st.text_input("Username", value=conn["username"] if conn else "", key="edit_user")
        else:
            folder = st.text_input("Folder Path", value=conn.get("folder_path", "") if conn else "", key="edit_folder")

    if st.button("💾 Save Main Connection", key="save_main_connection"):
        data = {
            "name": st.session_state.get("edit_name", "").strip(),
            "connection_type": st.session_state.get("edit_type", ""),
            "host": st.session_state.get("edit_host", "").strip() if is_sql else None,
            "port": st.session_state.get("edit_port", "").strip() if is_sql else None,
            "username": st.session_state.get("edit_user", "").strip() if is_sql else None,
            "folder_path": st.session_state.get("edit_folder", "").strip() if not is_sql else None,
            "is_active": st.session_state.get("edit_active", True)
        }

        with engine.begin() as db_conn:
            if conn:
                db_conn.execute(sa.text("""
                    UPDATE config.connections
                    SET name = :name,
                        host = :host,
                        port = :port,
                        username = :username,
                        folder_path = :folder_path,
                        is_active = :is_active,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """), {
                    "id": conn["id"],
                    **data
                })
                st.success("✅ Main connection updated.")
                # Stop edit modus
                st.session_state.pop("edit_main_connection_id", None)
            else:
                db_conn.execute(sa.text("""
                    INSERT INTO config.connections
                    (name, connection_type, host, port, username, folder_path, is_active)
                    VALUES (:name, :connection_type, :host, :port, :username, :folder_path, :is_active)
                """), data)
                st.success("✅ New main connection created.")
                # Stop nieuw modus
                st.session_state.pop("new_main_connection", None)

        reset_main_connection_session_keys()
        st.rerun()


def get_current_main_connection_input():
    return {
        "name": st.session_state.get("edit_name", "").strip(),
        "host": st.session_state.get("edit_host", "").strip(),
        "port": st.session_state.get("edit_port", "").strip(),
        "username": st.session_state.get("edit_user", "").strip(),
        "folder_path": st.session_state.get("edit_folder", "").strip(),
        "is_active": st.session_state.get("edit_active", True),
        "connection_type": st.session_state.get("edit_type", ""),
    }


def render_main_connection_readonly(conn):
    st.markdown(f"### 🔌 Main Connection – {conn['name']}")
    st.markdown(f"**Type:** {conn['connection_type']}")
    st.markdown(f"**Host:** {conn.get('host') or '-'}")
    st.markdown(f"**Port:** {conn.get('port') or '-'}")
    st.markdown(f"**Username:** {conn.get('username') or '-'}")
    st.markdown(f"**Folder Path:** {conn.get('folder_path') or '-'}")
    st.markdown(f"**Status:** {'🟢 Active' if conn.get('is_active') else '🔴 Inactive'}")



def reset_main_connection_form(engine, connection_id):
    with engine.connect() as conn:
        result = conn.execute(
            sa.text("""
                SELECT name, connection_type, host, port, username, password, folder_path, is_active
                FROM config.connections
                WHERE id = :id
            """),
            {"id": connection_id}
        ).fetchone()

    if result:
        st.session_state["edit_name"] = result["name"]
        st.session_state["edit_type"] = result["connection_type"]
        st.session_state["edit_host"] = result["host"]
        st.session_state["edit_port"] = result["port"]
        st.session_state["edit_user"] = result["username"]
        st.session_state["edit_password"] = result["password"]  # als je dat veld toont/gebruik
        st.session_state["edit_folder"] = result["folder_path"]
        st.session_state["edit_active"] = result["is_active"]

    st.rerun()


def reset_main_connection_session_keys():
    keys = [
        "edit_name",
        "edit_type",
        "edit_host",
        "edit_port",
        "edit_user",
        "edit_folder",
        "edit_active",
        # eventueel ook password als je die gebruikt
        "edit_password",
    ]
    for key in keys:
        if key in st.session_state:
            st.session_state.pop(key)




def fetch_configs_for_connection(conn_id, engine):
    with engine.connect() as db_conn:
        catalog_configs = db_conn.execute(
            sa.text("SELECT * FROM config.catalog_connection_config WHERE connection_id = :id"),
            {"id": conn_id}
        ).fetchall()

        ai_configs = db_conn.execute(
            sa.text("SELECT * FROM config.ai_analyzer_connection_config WHERE connection_id = :id"),
            {"id": conn_id}
        ).fetchall()

    return catalog_configs, ai_configs


def deactivate_catalog_config(config_id: int, engine):
    """Markeer catalog configuratie als inactief (soft delete)."""
    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE config.catalog_connection_config
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """), {"id": config_id})

def deactivate_ai_config(config_id: int, engine):
    """Markeer AI-configuratie als inactief (soft delete)."""
    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE config.ai_analyzer_connection_config
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """), {"id": config_id})

def get_source_connections():
    """Fetch all source database connections from the config.connections table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text("""
                SELECT id, name, connection_type, host, port, username, password, folder_path, execution_mode
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