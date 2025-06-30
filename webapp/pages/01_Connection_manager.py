import streamlit as st

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
from shared_utils import test_connection
import logging
from shared_utils import test_connection, get_connection_info_by_id

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

def reset_form():
    """Reset the form fields."""
    if st.session_state.get("edit_mode", False):
        connection_id = st.session_state["edit_connection_id"]
        logger.debug(f"Resetting form for connection ID: {connection_id}")
        with engine.connect() as db_conn:
            result = db_conn.execute(
                sa.text("""
                    SELECT name, connection_type, host, port, username, password, folder_path, execution_mode
                    FROM config.connections
                    WHERE id = :id
                """),
                {"id": connection_id}
            ).fetchone()

        logger.debug(f"Fetched connection details: {result}")

        st.session_state["temp_connection_name"] = result[0]
        st.session_state["temp_connection_type"] = result[1]
        st.session_state["temp_host"] = result[2]
        st.session_state["temp_port"] = result[3]
        st.session_state["temp_username"] = result[4]
        st.session_state["temp_password"] = result[5]
        st.session_state["temp_folder_path"] = result[6]
        st.session_state["temp_execution_mode"] = result[7]
    else:
        st.session_state["clear_form"] = True
        logger.debug("Clearing form for new connection")

    st.rerun()

def render_catalog_config(config, engine, inside_expander=False):
    if not config["is_active"]:
        st.warning("⚠️ This configuration is **inactive** and will not be used in scheduled catalog or AI analysis runs.")

    conn_id = config["connection_id"]

    def render_content():
        dbf = st.text_input("Database filter", value=config["catalog_database_filter"] or "", key=f"dbf_catalog_{config['id']}")
        sf = st.text_input("Schema filter", value=config["catalog_schema_filter"] or "", key=f"sf_catalog_{config['id']}")
        tf = st.text_input("Table filter", value=config["catalog_table_filter"] or "", key=f"tf_catalog_{config['id']}")
        views = st.checkbox("Include views", value=config["include_views"], key=f"views_catalog_{config['id']}")
        sys = st.checkbox("Include system objects", value=config["include_system_objects"], key=f"sys_catalog_{config['id']}")
        active = st.checkbox("Active", value=config["is_active"], key=f"active_catalog_{config['id']}")

        disabled = not config["is_active"]
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 Save", key=f"save_catalog_{config['id']}"):
                with engine.begin() as db_conn:
                    db_conn.execute(sa.text("""
                        UPDATE config.catalog_connection_config
                        SET catalog_database_filter = :dbf,
                            catalog_schema_filter = :sf,
                            catalog_table_filter = :tf,
                            include_views = :views,
                            include_system_objects = :sys,
                            is_active = :active,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :config_id
                    """), {
                        "config_id": config["id"],
                        "dbf": dbf,
                        "sf": sf,
                        "tf": tf,
                        "views": views,
                        "sys": sys,
                        "active": active
                    })
                st.success("✅ Catalog configuration saved")

        with col2:
            if st.button("🗑️ Delete", key=f"delete_catalog_{config['id']}", disabled=disabled):
                with engine.begin() as db_conn:
                    db_conn.execute(
                        sa.text("DELETE FROM config.catalog_connection_config WHERE id = :id"),
                        {"id": config["id"]}
                    )
                st.success("Catalog configuration deleted")
                st.rerun()

        with col3:
            if st.button("🔍 Test connection", key=f"test_catalog_{config['id']}", disabled=disabled):
                connection_info = get_connection_info_by_id(engine, conn_id)
                db_list = [d.strip() for d in dbf.split(",") if d.strip()] if dbf else None
                test_results = test_connection(connection_info, db_list)
                if test_results:
                    for result in test_results:
                        if result.startswith("✅"):
                            st.success(result)
                        elif result.startswith("❌"):
                            st.error(result)
                        else:
                            st.info(result)
                else:
                    st.info("ℹ️ Geen resultaten ontvangen van test_connection.")

    if inside_expander:
        render_content()
    else:
        with st.expander("📚 Catalog Configuration"):
            render_content()


def render_ai_config(config, engine, inside_expander=False):
    if not config["is_active"]:
        st.warning("⚠️ This configuration is **inactive** and will not be used in scheduled catalog or AI analysis runs.")


    conn_id = config["connection_id"]

    def render_content():
        dbf = st.text_input("Database filter", value=config["ai_database_filter"] or "", key=f"dbf_ai_{config['id']}")
        sf = st.text_input("Schema filter", value=config["ai_schema_filter"] or "", key=f"sf_ai_{config['id']}")
        tf = st.text_input("Table filter", value=config["ai_table_filter"] or "", key=f"tf_ai_{config['id']}")
        model = st.text_input("Model version", value=config["ai_model_version"] or "default", key=f"model_ai_{config['id']}")
        active = st.checkbox("Active", value=config["is_active"], key=f"active_ai_{config['id']}")
        disabled = not config["is_active"]
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 Save", key=f"save_ai_{config['id']}"):
                with engine.begin() as db_conn:
                    db_conn.execute(sa.text("""
                        UPDATE config.ai_analyzer_connection_config
                        SET ai_database_filter = :dbf,
                            ai_schema_filter = :sf,
                            ai_table_filter = :tf,
                            ai_model_version = :model,
                            is_active = :active,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :config_id
                    """), {
                        "config_id": config["id"],
                        "dbf": dbf,
                        "sf": sf,
                        "tf": tf,
                        "model": model,
                        "active": active
                    })
                st.success("✅ AI configuration saved")

        with col2:
            if st.button("🗑️ Delete", key=f"delete_ai_{config['id']}", disabled=disabled):
                with engine.begin() as db_conn:
                    db_conn.execute(
                        sa.text("DELETE FROM config.ai_analyzer_connection_config WHERE id = :id"),
                        {"id": config["id"]}
                    )
                st.success("AI configuration deleted")
                st.rerun()

        with col3:
            if st.button("🔍 Test connection", key=f"test_ai_{config['id']}", disabled=disabled):
                connection_info = get_connection_info_by_id(engine, conn_id)
                test_results = test_connection(connection_info, [dbf] if dbf else None)
                if test_results:
                    for result in test_results:
                        if result.startswith("✅"):
                            st.success(result)
                        elif result.startswith("❌"):
                            st.error(result)
                        else:
                            st.info(result)
                else:
                    st.info("ℹ️ Geen resultaten ontvangen van test_connection.")

    if inside_expander:
        render_content()
    else:
        with st.expander("🤖 AI Analysis Configuration"):
            render_content()



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

    # Fetch existing connections
    connections = get_source_connections()

    if connections:

        selected_conn_label = st.selectbox("🔌 Select a main connection", [f"{c['name']} (ID: {c['id']})" for c in connections], key="select_conn")
        selected_conn_id = int(selected_conn_label.split("ID: ")[-1].rstrip(")"))

        selected_conn = next((c for c in connections if c["id"] == selected_conn_id), None)
        if selected_conn:
            with st.expander("⚙️ Main Connection Details", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    name = st.text_input("Name", value=selected_conn["name"], key="edit_name")
                    conn_type = st.text_input("Connection Type", value=selected_conn["connection_type"], disabled=True, key="edit_type")
                    active = st.checkbox("Active", value=selected_conn.get("is_active", True))
                with col2:
                    if selected_conn["connection_type"] == "Power BI Semantic Model":
                        folder = st.text_input("Folder Path", value=selected_conn.get("folder_path", ""), key="edit_folder")
                    else:
                        host = st.text_input("Host", value=selected_conn["host"], key="edit_host")
                        port = st.text_input("Port", value=selected_conn["port"], key="edit_port")
                        user = st.text_input("Username", value=selected_conn["username"], key="edit_user")
                        # mode = st.selectbox("Execution Mode", options=["manual", "scheduled"], index=0 if selected_conn.get("execution_mode") == "manual" else 1)

                test_col, save_col, delete_col = st.columns(3)
                with test_col:
                    if selected_conn["connection_type"] != "Power BI Semantic Model" and st.button("🔍 Test Connection", key="test_selected"):
                        from shared_utils import test_connection
                        connection_info = {
                            "connection_type": selected_conn["connection_type"],
                            "host": host,
                            "port": port,
                            "username": user,
                            "password": selected_conn["password"]
                        }
                        results = test_connection(connection_info, databases=None)
                        if results:
                            for r in results:
                                if r.startswith("✅"):
                                    st.success(r)
                                elif r.startswith("❌"):
                                    st.error(r)
                                else:
                                    st.info(r)
                        else:
                            st.info("ℹ️ Geen resultaten ontvangen van test_connection.")

                with save_col:
                    if st.button("💾 Save", key="save_selected"):
                        with engine.begin() as db_conn:
                            db_conn.execute(sa.text("""
                                UPDATE config.connections
                                SET name = :name,
                                    host = :host,
                                    port = :port,
                                    username = :username,
                                    folder_path = :folder_path,
                                    is_active = :active,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """), {
                                "id": selected_conn_id,
                                "name": name,
                                "host": host if selected_conn["connection_type"] != "Power BI Semantic Model" else None,
                                "port": port if selected_conn["connection_type"] != "Power BI Semantic Model" else None,
                                "username": user if selected_conn["connection_type"] != "Power BI Semantic Model" else None,
                                "folder_path": folder if selected_conn["connection_type"] == "Power BI Semantic Model" else None,
                                "active": active if selected_conn["connection_type"] != "Power BI Semantic Model" else True
                            })
                        st.success("Main connection updated.")

                with delete_col:
                    if st.button("🗑️ Delete", key="delete_selected"):
                        with engine.begin() as db_conn:
                            db_conn.execute(sa.text("""
                                UPDATE config.connections
                                SET is_active = FALSE
                                WHERE id = :id
                            """), {"id": selected_conn_id})
                        st.success("Main connection deactivated.")
                        st.rerun()



        if selected_conn["connection_type"] != "Power BI Semantic Model":
            st.divider()
            st.markdown("## 📚 Catalog Configurations")
            show_inactive_catalog = st.checkbox("Show inactive catalog configurations", value=False)
            with st.expander("ℹ️ About Catalog Configurations", expanded=False):
                st.markdown("""
                Catalog configurations allow you to define **filters** for scheduled metadata extraction runs (e.g. database, schema, or table-level scope).

                - You can add **multiple configurations** per main connection.
                - Each configuration acts as a specific filter set for targeted extraction jobs.
                - **Active configurations** can be used in automated catalog updates.

                If no configuration is defined, the connection can still be used for **one-time (ad hoc) runs**, where filters can set manually.

                > 💡 Use multiple configurations if you want to split extraction scopes across teams, business domains, or scheduling frequencies.
                """)
            with engine.connect() as db_conn:
                catalog_configs = db_conn.execute(sa.text(f"""
                    SELECT * FROM config.catalog_connection_config
                    WHERE connection_id = :id
                    {"AND is_active = TRUE" if not show_inactive_catalog else ""}
                """), {"id": selected_conn_id}).fetchall()

            if catalog_configs:
                catalog_options = {
                    f"📚 Catalog ID {c.id} | {'🟢 Active' if c.is_active else '🔴 Inactive'} | DB: {c.catalog_database_filter or '-'} | SC: {c.catalog_schema_filter or '-'} | TB: {c.catalog_table_filter or '-'}":
                    dict(c._mapping)
                    for c in catalog_configs
                }
                selected_catalog_label = st.selectbox("Select catalog configuration", list(catalog_options.keys()))
                selected_catalog_config = catalog_options[selected_catalog_label]
                render_catalog_config(selected_catalog_config, engine, inside_expander=False)

            else:
                st.warning("🔎 No catalog configurations found for this connection.")

                st.markdown("""
                You can still use this connection for **one-time (ad hoc) analysis** by providing filters at runtime.

                To use this connection in **scheduled catalog runs**, configure a filter below and activate it.
                """)

            st.markdown("### ➕ Add new catalog configuration")
            with st.expander("Add catalog configuration", expanded=False):
                catalog_dbf = st.text_input("Catalog - Database filter", key="new_catalog_dbf")
                catalog_scf = st.text_input("Catalog - Schema filter", key="new_catalog_scf")
                catalog_tbf = st.text_input("Catalog - Table filter", key="new_catalog_tbf")
                include_views = st.checkbox("Include views", value=True, key="new_catalog_views")
                include_sys = st.checkbox("Include system objects", value=False, key="new_catalog_sys")
                is_active = st.checkbox("Active", value=True, key="new_catalog_active")

                if st.button("💾 Save Catalog Config", key="save_catalog_config"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(sa.text("""
                                INSERT INTO config.catalog_connection_config
                                (connection_id, catalog_database_filter, catalog_schema_filter, catalog_table_filter, include_views, include_system_objects, is_active)
                                VALUES (:cid, :dbf, :scf, :tbf, :views, :sys, :active)
                            """), {
                                "cid": selected_conn_id,
                                "dbf": catalog_dbf.strip(),
                                "scf": catalog_scf.strip(),
                                "tbf": catalog_tbf.strip(),
                                "views": include_views,
                                "sys": include_sys,
                                "active": is_active
                            })
                        st.success("✅ Catalog configuration added.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to save: {e}")

            st.divider()
            st.markdown("## 🤖 AI Analysis Configurations")
            show_inactive_ai = st.checkbox("Show inactive AI configurations", value=False)
            with st.expander("ℹ️ About AI Analysis Configurations", expanded=False):
                st.markdown("""
                AI Analysis configurations allow you to define **filter scopes and model versions** for automatic or scheduled AI-based metadata interpretation.

                - Each configuration specifies which **database, schema, or table patterns** to include in AI analysis runs.
                - You can create **multiple configurations** per connection to support different analysis goals.
                - **Active configurations** will be picked up during scheduled AI runs.

                If no configuration is present, you can still trigger **manual AI analysis** by specifying filters on-the-fly.

                > 💡 Use separate configs to isolate development/test environments, experiments, or tenant-specific logic.
                """)
            with engine.connect() as db_conn:
                ai_configs = db_conn.execute(sa.text(f"""
                    SELECT * FROM config.ai_analyzer_connection_config
                    WHERE connection_id = :id
                    {"AND is_active = TRUE" if not show_inactive_ai else ""}
                """), {"id": selected_conn_id}).fetchall()

            if ai_configs:
                ai_options = {
                    f"🤖 AI ID {a.id} | {'🟢 Active' if a.is_active else '🔴 Inactive'} | DB: {a.ai_database_filter or '-'} | SC: {a.ai_schema_filter or '-'} | TB: {a.ai_table_filter or '-'}":
                    dict(a._mapping)
                    for a in ai_configs
                }
                selected_ai_label = st.selectbox("Select AI configuration", list(ai_options.keys()))
                selected_ai_config = ai_options[selected_ai_label]
                render_ai_config(selected_ai_config, engine, inside_expander=False)
            else:
                st.info("No AI configurations for this connection.")
            
            st.markdown("### ➕ Add another AI analysis configuration")
            with st.expander("Add new AI configuration", expanded=False):
                ai_dbf = st.text_input("AI - Database filter", key="new_ai_dbf")
                ai_scf = st.text_input("AI - Schema filter", key="new_ai_scf")
                ai_tbf = st.text_input("AI - Table filter", key="new_ai_tbf")
                model = st.text_input("AI - Model version", value="default", key="new_ai_model")
                is_active_ai = st.checkbox("Active", value=True, key="new_ai_active")

                if st.button("💾 Save AI Config", key="save_ai_config"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(sa.text("""
                                INSERT INTO config.ai_analyzer_connection_config
                                (connection_id, ai_database_filter, ai_schema_filter, ai_table_filter, ai_model_version, is_active)
                                VALUES (:cid, :dbf, :scf, :tbf, :model, :active)
                            """), {
                                "cid": selected_conn_id,
                                "dbf": ai_dbf.strip(),
                                "scf": ai_scf.strip(),
                                "tbf": ai_tbf.strip(),
                                "model": model.strip(),
                                "active": is_active_ai
                            })
                        st.success("✅ AI configuration added.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to save: {e}")

    else:
        st.warning("No connections available. Please create a connection first.")



# === CREATE NEW CONNECTION SECTION ===
    st.divider()
    if st.session_state.get("edit_mode", False):
        st.subheader("Edit Connection")

        # Retrieve existing connection details
        connection_id = st.session_state["edit_connection_id"]
        with engine.connect() as db_conn:
            result = db_conn.execute(
                sa.text("""
                    SELECT name, connection_type, host, port, username, password
                    FROM config.connections
                    WHERE id = :id
                """),
                {"id": connection_id}
            ).fetchone()

        logger.debug(f"Fetched connection details: {result}")

        st.session_state["connection_name"] = result[0]
        st.session_state["connection_type"] = result[1]
        st.session_state["host"] = result[2]
        st.session_state["port"] = result[3]
        st.session_state["username"] = result[4]
        st.session_state["password"] = result[5]

        connection_name = st.text_input("Connection Name", value=st.session_state.get("temp_connection_name", ""), key="connection_name")
        connection_type = st.selectbox("Connection Type", ["PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"], index=["PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"].index(st.session_state.get("temp_connection_type", "PostgreSQL")), key="connection_type")
        host = st.text_input("Host", value=st.session_state.get("temp_host", ""), key="host")
        port = st.text_input("Port", value=st.session_state.get("temp_port", ""), key="port")
        username = st.text_input("Username", value=st.session_state.get("temp_username", ""), key="username")
        password = st.text_input("Password", value=st.session_state.get("temp_password", ""), type="password", key="password")

        is_sql_type = connection_type in ["PostgreSQL", "Azure SQL Server"]
        required_fields_filled = all([(connection_name or "").strip(), (host or "").strip(), (port or "").strip(), (username or "").strip(), (password or "").strip()])
        any_field_filled = any([(connection_name or "").strip(), (host or "").strip(), (port or "").strip(), (username or "").strip(), (password or "").strip()])

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔁 Reset", disabled=not any_field_filled):
                reset_form()
        with col2:
            if st.button("🔍 Test Connection", disabled=not required_fields_filled or not is_sql_type):
                test_connection(connection_type, host, port, username, password, None)
        with col3:
            if st.button("💾 Save Changes", disabled=not required_fields_filled):
                try:
                    with engine.begin() as db_conn:
                        db_conn.execute(
                            sa.text("""
                                UPDATE config.connections
                                SET name = :name, connection_type = :connection_type, host = :host, port = :port,
                                    username = :username, password = :password
                                WHERE id = :id
                            """),
                            {
                                "id": connection_id,
                                "name": connection_name,
                                "connection_type": connection_type,
                                "host": host,
                                "port": port,
                                "username": username,
                                "password": password
                            }
                        )
                    st.success("Connection details updated successfully!")
                    st.session_state["edit_mode"] = False
                except Exception as e:
                    st.error(f"❌ Failed to update connection details: {e}")

        if is_sql_type:
            st.markdown("## 📚 Configure Additional Settings")
            catalog_db_filter = st.text_input("Catalog - Database filter (optional)", key="edit_catalog_dbf")
            ai_db_filter = st.text_input("AI - Database filter (optional)", key="edit_ai_dbf")

            if st.button("💾 Save Catalog/AI Configs", key="save_config_addons"):
                try:
                    with engine.begin() as conn:
                        if catalog_db_filter.strip():
                            conn.execute(sa.text("""
                                INSERT INTO config.catalog_connection_config (connection_id, catalog_database_filter, is_active)
                                VALUES (:cid, :dbf, TRUE)
                            """), {"cid": connection_id, "dbf": catalog_db_filter.strip()})

                        if ai_db_filter.strip():
                            conn.execute(sa.text("""
                                INSERT INTO config.ai_analyzer_connection_config (connection_id, ai_database_filter, is_active)
                                VALUES (:cid, :dbf, TRUE)
                            """), {"cid": connection_id, "dbf": ai_db_filter.strip()})

                    st.success("Configurations added successfully.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"❌ Failed to save configuration: {e}")

    else:
        st.subheader("Create new main connection")
        if "clear_form" not in st.session_state:
            st.session_state["clear_form"] = False

        connection_type = st.selectbox("Select Connection Type", ["PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"])
        is_sql_type = connection_type in ["PostgreSQL", "Azure SQL Server"]

        connection_name = st.text_input("Connection Name", value="" if st.session_state.get("clear_form", False) else None, key="connection_name")
        folder_path = ""

        if connection_type == "Power BI Semantic Model":
            folder_path = st.text_input("Folder Path", value="" if st.session_state.get("clear_form", False) else None, placeholder="Enter the folder path for Power BI models", key="folder_path")
            host = port = username = password = ""
        else:
            host = st.text_input("Host", value="" if st.session_state.get("clear_form", False) else None, key="host")
            port = st.text_input("Port", value="" if st.session_state.get("clear_form", False) else ("5432" if connection_type == "PostgreSQL" else "1433"), key="port")
            username = st.text_input("Username", value="" if st.session_state.get("clear_form", False) else None, key="username")
            password = st.text_input("Password", value="" if st.session_state.get("clear_form", False) else None, type="password", key="password")

        if st.session_state.get("clear_form", False):
            st.session_state["clear_form"] = False

        required_fields_filled = all([(connection_name or "").strip(), (host or "").strip(), (port or "").strip(), (username or "").strip(), (password or "").strip()]) if is_sql_type else all([(connection_name or "").strip(), (folder_path or "").strip()])
        any_field_filled = any([(connection_name or "").strip(), (host or "").strip(), (port or "").strip(), (username or "").strip(), (password or "").strip(), (folder_path or "").strip()])

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔁 Reset", disabled=not any_field_filled):
                reset_form()
        with col2:
            if st.button("🔍 Test Connection", disabled=not required_fields_filled or not is_sql_type):
                test_connection(connection_type, host, port, username, password, None)
        with col3:
            button_label = "💾 Save Connection" if is_sql_type else "📁 Save Folder Path"
            if st.button(button_label, disabled=not required_fields_filled):
                try:
                    with engine.begin() as db_conn:
                        if is_sql_type:
                            db_conn.execute(sa.text("""
                                INSERT INTO config.connections (name, connection_type, host, port, username, password)
                                VALUES (:name, :connection_type, :host, :port, :username, :password)
                            """), {
                                "name": connection_name,
                                "connection_type": connection_type,
                                "host": host,
                                "port": port,
                                "username": username,
                                "password": password
                            })
                            st.success("Connection details saved successfully!")
                        else:
                            db_conn.execute(sa.text("""
                                INSERT INTO config.connections (name, connection_type, folder_path)
                                VALUES (:name, :connection_type, :folder_path)
                            """), {
                                "name": connection_name,
                                "connection_type": connection_type,
                                "folder_path": folder_path
                            })
                            st.success("Folder path saved successfully!")

                    st.info("ℹ️ Catalog configuration and AI analysis configuration can be added after saving. Go to the 'Active Connections' tab and select your connection to configure these.")

                    st.session_state["clear_form"] = True
                    st.rerun()

                except Exception as e:
                    st.error(f"Failed to save connection details: {e}")


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