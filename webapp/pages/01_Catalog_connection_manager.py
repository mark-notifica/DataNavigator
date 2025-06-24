import streamlit as st

# Add page config as the FIRST Streamlit command
st.set_page_config(
    page_title="Catalog Connection Manager",
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

# === FUNCTIONS ===
def get_source_connections():
    """Fetch all source database connections from the config.connections table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text("""
                SELECT id, name, connection_type, host, port, username, password, database_name
                FROM config.connections
                ORDER BY id
            """))
            connections = [dict(row._mapping) for row in result]  # Use row._mapping to convert rows to dictionaries
            return connections
    except Exception as e:
        st.error(f"Failed to fetch connections: {e}")
        return []

st.title("Connection Manager")

# === EXISTING CONNECTIONS SECTION ===
st.subheader("Existing Connections")

# Fetch existing connections
connections = get_source_connections()

if connections:
    st.subheader("Existing Connections")
    
    # Iterate through each connection and create an expandable box
    for conn in connections:
        with st.expander(f"{conn['name']} (ID: {conn['id']})"):
            # Display connection details
            st.write(f"**ID:** {conn['id']}")
            st.write(f"**Type:** {conn['connection_type']}")
            st.write(f"**Host:** {conn['host']}")
            st.write(f"**Port:** {conn['port']}")
            st.write(f"**Username:** {conn['username']}")
            st.write(f"**Database(s):** {conn['database_name']}")
            
            # Add Test Connection button
            if st.button(f"🔍 Test Connection for {conn['name']}", key=f"test_{conn['id']}"):
                try:
                    # Prepare connection info
                    connection_info = {
                        "connection_type": conn['connection_type'],
                        "host": conn['host'],
                        "port": conn['port'],
                        "username": conn['username'],
                        "password": conn['password']
                    }
                    databases_to_test = [db.strip() for db in conn['database_name'].split(',')] if conn['database_name'] else None
                    
                    # Call reusable test_connection function
                    test_results = test_connection(connection_info, databases_to_test)
                    
                    # Display results
                    st.write("**Connection Test Results:**")
                    for result in test_results:
                        if "✅" in result:
                            st.success(result)
                        else:
                            st.error(result)
                except Exception as e:
                    st.error(f"❌ Connection test failed: {e}")
            
            # Add Edit and Delete buttons
            col1, col2 = st.columns(2)
            with col1:
                if st.button(f"✏️ Edit {conn['name']}", key=f"edit_{conn['id']}"):
                    st.info("Edit functionality not implemented yet.")
            with col2:
                if st.button(f"🗑️ Delete {conn['name']}", key=f"delete_{conn['id']}"):
                    st.warning("Delete functionality not implemented yet.")
else:
    st.warning("No connections available. Please create a connection first.")

# === CREATE NEW CONNECTION SECTION ===
st.divider()
st.subheader("Create New Connection")

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

# Initialize folder_path for all connection types
folder_path = ""

if connection_type == "Power BI Semantic Model":
    folder_path = st.text_input(
        "Folder Path", 
        value="" if st.session_state.get("clear_form", False) else None,
        placeholder="Enter the folder path for Power BI models",
        key="folder_path"
    )
    host = port = username = password = database = ""
else:
    # Host field
    host = st.text_input(
        "Host", 
        value="" if st.session_state.get("clear_form", False) else None,
        key="host"
    )


    # Port field
    port = st.text_input(
        "Port", 
        value="" if st.session_state.get("clear_form", False) else ("5432" if connection_type == "PostgreSQL" else "1433"),
        key="port"
    )

    # Username field
    username = st.text_input(
        "Username", 
        value="" if st.session_state.get("clear_form", False) else None,
        key="username"
    )

    # Password field
    password = st.text_input(
        "Password", 
        value="" if st.session_state.get("clear_form", False) else None,
        type="password", 
        key="password"
    )

    # Database names field (moved to the last position)
    database = st.text_input(
        "Database Name(s)*", 
        value="" if st.session_state.get("clear_form", False) else None,
        placeholder="Enter database name(s), e.g., 'db1,db2,db3'",
        help="💡 For PostgreSQL or Azure SQL Server, you can specify multiple databases separated by commas (e.g., `db1,db2,db3`)."
    )

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
    if st.button("🔍 Test Connection", disabled=not required_fields_filled or not is_sql_type):
        try:
            if connection_type == "PostgreSQL":
                driver = "postgresql+psycopg2"
                if database:
                    if ',' in database:
                        # Multiple databases provided
                        databases_to_test = [db.strip() for db in database.split(',')]
                        test_results = []
                        for db_name in databases_to_test:
                            try:
                                url = sa.engine.URL.create(
                                    drivername=driver,
                                    username=username,
                                    password=password,
                                    host=host,
                                    port=port,
                                    database=db_name
                                )
                                test_engine = sa.create_engine(url)
                                with test_engine.connect() as test_conn:
                                    test_conn.execute(sa.text("SELECT 1"))
                                test_results.append(f"✅ {db_name}: Success")
                                test_engine.dispose()
                            except Exception as db_error:
                                test_results.append(f"❌ {db_name}: {str(db_error)}")
                        
                        # Display results
                        st.write("**Connection Test Results:**")
                        for result in test_results:
                            if "✅" in result:
                                st.success(result)
                            else:
                                st.error(result)
                    else:
                        # Single database provided
                        url = sa.engine.URL.create(
                            drivername=driver,
                            username=username,
                            password=password,
                            host=host,
                            port=port,
                            database=database
                        )
                        test_engine = sa.create_engine(url)
                        with test_engine.connect() as test_conn:
                            test_conn.execute(sa.text("SELECT 1"))
                        st.success(f"✅ Connection to database '{database}' successful!")
                        test_engine.dispose()
                else:
                    # No database provided, test connection to server
                    url = sa.engine.URL.create(
                        drivername=driver,
                        username=username,
                        password=password,
                        host=host,
                        port=port,
                        database="postgres"  # Default system database
                    )
                    test_engine = sa.create_engine(url)
                    with test_engine.connect() as test_conn:
                        test_conn.execute(sa.text("SELECT 1"))
                    st.success("✅ Connection to PostgreSQL server successful!")
                    test_engine.dispose()
            
            elif connection_type == "Azure SQL Server":
                driver = "mssql+pyodbc"
                if database:
                    if ',' in database:
                        # Multiple databases provided
                        databases_to_test = [db.strip() for db in database.split(',')]
                        test_results = []
                        for db_name in databases_to_test:
                            try:
                                connection_string = (
                                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                                    f"SERVER={host},{port};"
                                    f"DATABASE={db_name};"
                                    f"UID={username};"
                                    f"PWD={password};"
                                    f"Encrypt=yes;"
                                    f"TrustServerCertificate=no;"
                                    f"Connection Timeout=30;"
                                )
                                test_conn = pyodbc.connect(connection_string)
                                test_conn.close()
                                test_results.append(f"✅ {db_name}: Success")
                            except Exception as db_error:
                                test_results.append(f"❌ {db_name}: {str(db_error)}")
                        
                        # Display results
                        st.write("**Connection Test Results:**")
                        for result in test_results:
                            if "✅" in result:
                                st.success(result)
                            else:
                                st.error(result)
                    else:
                        # Single database provided
                        connection_string = (
                            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                            f"SERVER={host},{port};"
                            f"DATABASE={database};"
                            f"UID={username};"
                            f"PWD={password};"
                            f"Encrypt=yes;"
                            f"TrustServerCertificate=no;"
                            f"Connection Timeout=30;"
                        )
                        test_conn = pyodbc.connect(connection_string)
                        test_conn.close()
                        st.success(f"✅ Connection to database '{database}' successful!")
                else:
                    # No database provided, test connection to server
                    connection_string = (
                        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                        f"SERVER={host},{port};"
                        f"UID={username};"
                        f"PWD={password};"
                        f"Encrypt=yes;"
                        f"TrustServerCertificate=no;"
                        f"Connection Timeout=30;"
                    )
                    test_conn = pyodbc.connect(connection_string)
                    test_conn.close()
                    st.success("✅ Connection to Azure SQL Server successful!")
        except Exception as e:
            st.error(f"❌ Connection test failed: {e}")


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