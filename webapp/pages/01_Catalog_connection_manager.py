import streamlit as st
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
import os
import sys
import pyodbc

# Print Python executable path
print("Python executable:", sys.executable)
st.write(st.__version__)

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

st.title("Create connection to a data source")

# Initialize clear flag
if "clear_form" not in st.session_state:
    st.session_state["clear_form"] = False

# Dropdown for connection type
connection_type = st.selectbox(
    "Select Connection Type",
    ["PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"]
)

is_sql_type = connection_type in ["PostgreSQL", "Azure SQL Server"]

# Input fields (no form) - use empty values when clear flag is set
connection_name = st.text_input(
    "Connection Name", 
    value="" if st.session_state.get("clear_form", False) else None,
    key="connection_name"
)

if is_sql_type:
    host = st.text_input(
        "Host", 
        value="" if st.session_state.get("clear_form", False) else None,
        key="host"
    )
    port = st.text_input(
        "Port", 
        value="" if st.session_state.get("clear_form", False) else ("5432" if connection_type == "PostgreSQL" else "1433"),
        key="port"
    )
    username = st.text_input(
        "Username", 
        value="" if st.session_state.get("clear_form", False) else None,
        key="username"
    )
    password = st.text_input(
        "Password", 
        value="" if st.session_state.get("clear_form", False) else None,
        type="password", 
        key="password"
    )
    database = st.text_input(
        "Database Name", 
        value="" if st.session_state.get("clear_form", False) else None,
        key="database"
    )
    folder_path = ""
else:
    folder_path = st.text_input(
        "Folder Path", 
        value="" if st.session_state.get("clear_form", False) else None,
        placeholder="Enter the folder path for Power BI models",
        key="folder_path"
    )
    host = port = username = password = database = ""

# Reset clear flag after widgets are created
if st.session_state.get("clear_form", False):
    st.session_state["clear_form"] = False

# Real-time validation
if is_sql_type:
    required_fields_filled = all([
        (connection_name or "").strip(),
        (host or "").strip(),
        (port or "").strip(),
        (username or "").strip(),
        (password or "").strip()
    ])
else:
    required_fields_filled = all([
        (connection_name or "").strip(),
        (folder_path or "").strip()
    ])

any_field_filled = any([
    (connection_name or "").strip(),
    (host or "").strip(),
    (port or "").strip(),
    (username or "").strip(),
    (password or "").strip(),
    (database or "").strip(),
    (folder_path or "").strip()
])

# Buttons with real-time enable/disable
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🔁 Reset", disabled=not any_field_filled):
        # Set clear flag and rerun
        st.session_state["clear_form"] = True
        st.rerun()

with col2:
    if st.button("🧪 Test Connection", disabled=not required_fields_filled or not is_sql_type):
        try:
            if connection_type == "PostgreSQL":
                driver = "postgresql+psycopg2"
                url = sa.engine.URL.create(
                    drivername=driver,
                    username=username,
                    password=password,
                    host=host,
                    port=port,
                    database=database
                )
            else:
                driver = "mssql+pyodbc"
                connection_string = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={host};"
                    f"PORT={port};"
                    f"UID={username};"
                    f"PWD={password};"
                    f"DATABASE={database}"
                )
                url = sa.engine.URL.create(drivername=driver, query={"odbc_connect": connection_string})

            test_engine = sa.create_engine(url)
            with test_engine.connect():
                st.success("Connection successful!")
        except pyodbc.Error:
            st.error("ODBC Driver is missing or misconfigured.")
        except Exception as e:
            st.error(f"Connection failed: {e}")

with col3:
    button_label = "💾 Save Connection" if is_sql_type else "📁 Save Folder Path"
    if st.button(button_label, disabled=not required_fields_filled):
        try:
            if is_sql_type:
                # Save SQL database connection
                with engine.begin() as db_conn:
                    db_conn.execute(
                        sa.text("""
                            INSERT INTO config.connections (name, connection_type, host, port, username, password, database_name)
                            VALUES (:name, :connection_type, :host, :port, :username, :password, :database_name)
                        """),
                        {
                            "name": connection_name,
                            "connection_type": connection_type,
                            "host": host,
                            "port": port,
                            "username": username,
                            "password": password,
                            "database_name": database if database.strip() else None
                        }
                    )
                    st.success("Connection details saved successfully!")
            else:
                # Save Power BI folder path
                with engine.begin() as db_conn:
                    db_conn.execute(
                        sa.text("""
                            INSERT INTO config.connections (name, connection_type, folder_path)
                            VALUES (:name, :connection_type, :folder_path)
                        """),
                        {
                            "name": connection_name,
                            "connection_type": connection_type,
                            "folder_path": folder_path
                        }
                    )
                    st.success("Folder path saved successfully!")
            
            # Set clear flag and rerun to clear fields
            st.session_state["clear_form"] = True
            st.rerun()
            
        except Exception as e:
            st.error(f"Failed to save connection details: {e}")

# === EXISTING CONNECTIONS SECTION ===
st.divider()
st.subheader("Existing Connections")

# Fetch existing connections
try:
    with engine.connect() as db_conn:
        result = db_conn.execute(
            sa.text("SELECT id, name, connection_type, host, port, username, database_name, folder_path, created_at FROM config.connections ORDER BY created_at DESC")
        )
        connections = result.fetchall()
        
    if connections:
        # Convert to DataFrame for better display
        df = pd.DataFrame(connections, columns=['ID', 'Name', 'Type', 'Host', 'Port', 'Username', 'Database', 'Folder Path', 'Created'])
        
        # Display connections in an expandable format
        for idx, connection in enumerate(connections):
            connection_id, name, conn_type, host, port, username, database_name, folder_path, created_at = connection
            
            with st.expander(f"📊 {name} ({conn_type}) - ID: {connection_id}"):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**Type:** {conn_type}")
                    if conn_type in ["PostgreSQL", "Azure SQL Server"]:
                        st.write(f"**Host:** {host}")
                        st.write(f"**Port:** {port}")
                        st.write(f"**Username:** {username}")
                        st.write(f"**Database:** {database_name or 'Not specified'}")
                    else:  # Power BI
                        st.write(f"**Folder Path:** {folder_path}")
                    st.write(f"**Created:** {created_at}")
                
                with col2:
                    # Edit button
                    if st.button(f"✏️ Edit", key=f"edit_{connection_id}"):
                        st.session_state[f"editing_{connection_id}"] = True
                        st.rerun()
                    
                    # Delete button
                    if st.button(f"🗑️ Delete", key=f"delete_{connection_id}"):
                        try:
                            with engine.begin() as db_conn:
                                db_conn.execute(
                                    sa.text("DELETE FROM config.connections WHERE id = :id"),
                                    {"id": connection_id}
                                )
                            st.success(f"Connection '{name}' deleted successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete connection: {e}")
                
                # Edit form (shown when edit button is clicked)
                if st.session_state.get(f"editing_{connection_id}", False):
                    st.write("---")
                    st.write("**Edit Connection:**")
                    
                    with st.form(f"edit_form_{connection_id}"):
                        new_name = st.text_input("Connection Name", value=name)
                        
                        if conn_type in ["PostgreSQL", "Azure SQL Server"]:
                            new_host = st.text_input("Host", value=host or "")
                            new_port = st.text_input("Port", value=str(port) if port else "")
                            new_username = st.text_input("Username", value=username or "")
                            new_password = st.text_input("Password", type="password", help="Leave empty to keep current password")
                            new_database = st.text_input("Database Name", value=database_name or "")
                            new_folder_path = None
                        else:  # Power BI
                            new_folder_path = st.text_input("Folder Path", value=folder_path or "")
                            new_host = new_port = new_username = new_password = new_database = None
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            update_pressed = st.form_submit_button("💾 Update")
                        with col2:
                            test_edit_pressed = st.form_submit_button("🧪 Test") if conn_type in ["PostgreSQL", "Azure SQL Server"] else None
                        with col3:
                            cancel_pressed = st.form_submit_button("❌ Cancel")
                    
                    # Handle form actions
                    if update_pressed:
                        try:
                            if conn_type in ["PostgreSQL", "Azure SQL Server"]:
                                # Update SQL connection
                                if new_password:  # Only update password if provided
                                    update_query = """
                                        UPDATE config.connections 
                                        SET name = :name, host = :host, port = :port, username = :username, 
                                            password = :password, database_name = :database_name
                                        WHERE id = :id
                                    """
                                    params = {
                                        "id": connection_id,
                                        "name": new_name,
                                        "host": new_host,
                                        "port": new_port,
                                        "username": new_username,
                                        "password": new_password,
                                        "database_name": new_database if new_database else None
                                    }
                                else:  # Keep existing password
                                    update_query = """
                                        UPDATE config.connections 
                                        SET name = :name, host = :host, port = :port, username = :username, 
                                            database_name = :database_name
                                        WHERE id = :id
                                    """
                                    params = {
                                        "id": connection_id,
                                        "name": new_name,
                                        "host": new_host,
                                        "port": new_port,
                                        "username": new_username,
                                        "database_name": new_database if new_database else None
                                    }
                            else:  # Power BI
                                update_query = """
                                    UPDATE config.connections 
                                    SET name = :name, folder_path = :folder_path
                                    WHERE id = :id
                                """
                                params = {
                                    "id": connection_id,
                                    "name": new_name,
                                    "folder_path": new_folder_path
                                }
                            
                            with engine.begin() as db_conn:
                                db_conn.execute(sa.text(update_query), params)
                            
                            st.success(f"Connection '{new_name}' updated successfully!")
                            st.session_state[f"editing_{connection_id}"] = False
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Failed to update connection: {e}")
                    
                    elif test_edit_pressed and conn_type in ["PostgreSQL", "Azure SQL Server"]:
                        # Test the edited connection
                        try:
                            if conn_type == "PostgreSQL":
                                driver = "postgresql+psycopg2"
                                url = sa.engine.URL.create(
                                    drivername=driver,
                                    username=new_username,
                                    password=new_password if new_password else "test",  # Use dummy password for test
                                    host=new_host,
                                    port=new_port,
                                    database=new_database
                                )
                            else:
                                driver = "mssql+pyodbc"
                                connection_string = (
                                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                                    f"SERVER={new_host};"
                                    f"PORT={new_port};"
                                    f"UID={new_username};"
                                    f"PWD={new_password if new_password else 'test'};"
                                    f"DATABASE={new_database}"
                                )
                                url = sa.engine.URL.create(drivername=driver, query={"odbc_connect": connection_string})
                            
                            if new_password:  # Only test if password is provided
                                test_engine = sa.create_engine(url)
                                with test_engine.connect():
                                    st.success("Test connection successful!")
                            else:
                                st.warning("Please provide a password to test the connection.")
                                
                        except Exception as e:
                            st.error(f"Test connection failed: {e}")
                    
                    elif cancel_pressed:
                        st.session_state[f"editing_{connection_id}"] = False
                        st.rerun()
    else:
        st.info("No connections found. Create your first connection above!")
        
except Exception as e:
    st.error(f"Failed to load existing connections: {e}")

# Debug info
st.divider()
st.write("Debug Info:")
st.write("Required fields filled:", required_fields_filled)
st.write("Connection Name:", f"'{connection_name}'")
if not is_sql_type:
    st.write("Folder Path:", f"'{folder_path}'")