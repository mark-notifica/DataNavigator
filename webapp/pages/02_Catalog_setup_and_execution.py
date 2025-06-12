import streamlit as st
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
import os
from datetime import datetime
import psycopg2

st.title("Catalog Setup and Execution")

# Load environment variables for the DataNavigator database
dotenv_path = Path(__file__).resolve().parents[2] / ".env"
from dotenv import load_dotenv
load_dotenv(dotenv_path)

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

# === PROGRESS MONITORING FUNCTIONS ===
def get_catalog_progress(connection_id):
    """Get cataloging progress from database"""
    try:
        with engine.connect() as conn:
            # Get database entry
            db_result = conn.execute(sa.text("""
                SELECT id, database_name, date_updated 
                FROM catalog.catalog_databases 
                WHERE curr_id = :conn_id AND date_deleted IS NULL
            """), {"conn_id": str(connection_id)})
            
            database = db_result.fetchone()
            if not database:
                return {"status": "not_started", "progress": 0}
            
            db_id = database[0]
            
            # Get schema count
            schema_result = conn.execute(sa.text("""
                SELECT COUNT(*) FROM catalog.catalog_schemas 
                WHERE database_id = :db_id AND date_deleted IS NULL
            """), {"db_id": db_id})
            schema_count = schema_result.scalar() or 0
            
            # Get table count
            table_result = conn.execute(sa.text("""
                SELECT COUNT(*) FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                WHERE s.database_id = :db_id AND t.date_deleted IS NULL
            """), {"db_id": db_id})
            table_count = table_result.scalar() or 0
            
            # Get column count
            column_result = conn.execute(sa.text("""
                SELECT COUNT(*) FROM catalog.catalog_columns c
                JOIN catalog.catalog_tables t ON c.table_id = t.id
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                WHERE s.database_id = :db_id AND c.date_deleted IS NULL
            """), {"db_id": db_id})
            column_count = column_result.scalar() or 0
            
            # Calculate progress (rough estimate)
            progress = min(95, (schema_count * 5 + table_count * 1 + column_count * 0.1))
            
            return {
                "status": "in_progress" if progress > 0 else "cataloged",
                "database_name": database[1],
                "last_updated": database[2],
                "schemas": schema_count,
                "tables": table_count,
                "columns": column_count,
                "progress": progress
            }
            
    except Exception as e:
        return {"status": "error", "error": str(e)}

def get_latest_catalog_log():
    """Get the latest cataloging log content"""
    try:
        log_dir = Path("logfiles")
        if log_dir.exists():
            log_files = sorted(log_dir.glob("catalog_extraction_*.log"), 
                             key=lambda x: x.stat().st_mtime, reverse=True)
            if log_files:
                with open(log_files[0], 'r') as f:
                    lines = f.readlines()
                    # Return last 15 lines
                    return ''.join(lines[-15:])
        return "No recent log files found"
    except Exception as e:
        return f"Error reading logs: {e}"

def get_databases_preview(connection_info):
    """Preview available databases on a server without cataloging"""
    try:
        # Connect to master/system database to enumerate databases
        if connection_info[2] == 'PostgreSQL':  # connection_type
            master_db = 'postgres'
            conn = psycopg2.connect(
                host=connection_info[3],  # host
                port=connection_info[4],  # port
                database=master_db,
                user=connection_info[5],  # username
                password=connection_info[6]  # password (assuming you store this)
            )
            
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT datname 
                    FROM pg_database 
                    WHERE datistemplate = false 
                    AND datname NOT IN ('postgres', 'template0', 'template1')
                    ORDER BY datname
                """)
                databases = [row[0] for row in cursor.fetchall()]
                
        elif connection_info[2] == 'Azure SQL Server':  # connection_type
            import pyodbc
            master_db = 'master'
            
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={connection_info[3]};"  # host
                f"PORT={connection_info[4]};"    # port
                f"DATABASE={master_db};"
                f"UID={connection_info[5]};"     # username
                f"PWD={connection_info[6]};"     # password
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            
            conn = pyodbc.connect(connection_string)
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT name 
                    FROM sys.databases 
                    WHERE database_id > 4 
                    AND state = 0 
                    AND is_read_only = 0
                    ORDER BY name
                """)
                databases = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return databases
        
    except Exception as e:
        st.error(f"❌ Failed to preview databases: {e}")
        return []

# === SINGLE CONNECTION EXECUTION ===
st.subheader("Single Connection Cataloging")

# Sorting controls for single connection dropdown
col1, col2 = st.columns([1, 3])
with col1:
    single_sort_by = st.selectbox(
        "Sort dropdown by:",
        ["ID", "Name", "Type", "Host"],
        index=1,  # Default to Name
        key="single_sort_by"
    )
with col2:
    single_sort_order = st.radio(
        "Order:",
        ["Ascending", "Descending"],
        index=0,  # Default to Ascending
        horizontal=True,
        key="single_sort_order"
    )

# Fetch available connections for dropdown with sorting
try:
    # Determine sort column and order for single connection
    sort_column_map = {
        "ID": "id",
        "Name": "name",
        "Type": "connection_type", 
        "Host": "host"
    }
    
    sort_column = sort_column_map[single_sort_by]
    sort_direction = "ASC" if single_sort_order == "Ascending" else "DESC"
    
    with engine.connect() as db_conn:
        query = f"""
            SELECT id, name, connection_type, host, port, database_name
            FROM config.connections 
            WHERE connection_type IN ('PostgreSQL', 'Azure SQL Server')
            ORDER BY {sort_column} {sort_direction}
        """
        result = db_conn.execute(sa.text(query))
        connections = result.fetchall()
        
    if connections:
        # Show connection count and sorting info
        st.write(f"**{len(connections)} connection(s) available** - Sorted by {single_sort_by} ({single_sort_order})")
        
        # Create dropdown options with ID, Name, Type, and Host
        if single_sort_by == "ID":
            connection_options = [
                f"ID: {conn[0]} - {conn[1]} ({conn[2]}) - {conn[3]}:{conn[4]}"
                for conn in connections
            ]
        elif single_sort_by == "Name":
            connection_options = [
                f"{conn[1]} - ID: {conn[0]} ({conn[2]}) - {conn[3]}:{conn[4]}"
                for conn in connections
            ]
        elif single_sort_by == "Type":
            connection_options = [
                f"{conn[2]}: {conn[1]} - ID: {conn[0]} - {conn[3]}:{conn[4]}"
                for conn in connections
            ]
        else:  # Host
            connection_options = [
                f"{conn[3]}:{conn[4]} - {conn[1]} - ID: {conn[0]} ({conn[2]})"
                for conn in connections
            ]
        
        # Dropdown for single connection selection
        selected_connection = st.selectbox(
            "Select a connection to catalog:",
            options=connection_options,
            key="single_connection_selection"
        )
        
        if selected_connection:
            # Extract connection ID and get connection info
            if "ID: " in selected_connection:
                connection_id = int(selected_connection.split("ID: ")[1].split(" ")[0].split(" - ")[0])
            else:
                connection_id = int([part for part in selected_connection.split() if part.startswith("ID:")][0].split(":")[1])
            
            selected_conn_info = next(conn for conn in connections if conn[0] == connection_id)
            
            # Display selected connection details
            with st.expander("📋 Selected Connection Details", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**ID:** {selected_conn_info[0]}")
                    st.write(f"**Name:** {selected_conn_info[1]}")
                    st.write(f"**Type:** {selected_conn_info[2]}")
                with col2:
                    st.write(f"**Host:** {selected_conn_info[3]}")
                    st.write(f"**Port:** {selected_conn_info[4]}")
                    st.write(f"**Database:** {selected_conn_info[5] or 'Not specified'}")
            
            # === DATABASE SELECTION SECTION ===
            st.divider()
            st.subheader("🗃️ Database Selection")
            
            # Show current database_name from connection
            current_db = selected_conn_info[5] or "All databases (not specified)"
            st.write(f"**Connection setting:** {current_db}")
            
            # Option to preview and select databases
            col1, col2 = st.columns([1, 2])
            
            with col1:
                if st.button("🔍 Preview Databases", key="preview_dbs"):
                    with st.spinner("Discovering databases on server..."):
                        st.session_state["available_databases"] = get_databases_preview(selected_conn_info)
                        st.session_state["preview_connection_id"] = connection_id
            
            with col2:
                catalog_mode = st.radio(
                    "Cataloging Mode:",
                    ["Use connection setting", "Select specific databases"],
                    index=0,
                    key="catalog_mode"
                )
            
            # Show available databases if previewed
            if (st.session_state.get("preview_connection_id") == connection_id and 
                st.session_state.get("available_databases")):
                
                available_dbs = st.session_state["available_databases"]
                
                if available_dbs:
                    st.success(f"✅ Found {len(available_dbs)} databases on server")
                    
                    # Show databases in expandable section
                    with st.expander(f"📋 Available Databases ({len(available_dbs)})", expanded=True):
                        # Display in columns for better layout
                        cols = st.columns(3)
                        for i, db_name in enumerate(available_dbs):
                            with cols[i % 3]:
                                st.write(f"• **{db_name}**")
                    
                    # Database selection based on mode
                    if catalog_mode == "Select specific databases":
                        st.write("**Select databases to catalog:**")
                        
                        # Multi-select for database selection
                        selected_databases = st.multiselect(
                            "Choose databases:",
                            options=available_dbs,
                            default=available_dbs if len(available_dbs) <= 3 else [],  # Select all if 3 or fewer
                            key="selected_databases"
                        )
                        
                        if selected_databases:
                            st.info(f"📊 Will catalog {len(selected_databases)} database(s): {', '.join(selected_databases)}")
                            # Store selected databases for execution
                            st.session_state["execution_databases"] = selected_databases
                        else:
                            st.warning("⚠️ No databases selected. Please select at least one database.")
                    else:
                        # Use connection setting (all databases or specific from connection)
                        if selected_conn_info[5]:
                            # Connection has specific database
                            st.info(f"📊 Will catalog database specified in connection: **{selected_conn_info[5]}**")
                            st.session_state["execution_databases"] = [selected_conn_info[5]]
                        else:
                            # Connection will catalog all databases
                            st.info(f"📊 Will catalog **all {len(available_dbs)} databases** (connection setting)")
                            st.session_state["execution_databases"] = None  # None means all databases
                else:
                    st.warning("⚠️ No databases found on server or connection failed.")
            
            # Action buttons
            st.divider()
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🧪 Test Connection", key="test_single", type="secondary"):
                    try:
                        with st.spinner(f"Testing connection to {selected_conn_info[1]}..."):
                            # Test connection logic
                            test_databases = get_databases_preview(selected_conn_info)
                            if test_databases:
                                st.success(f"✅ Connection successful! Found {len(test_databases)} databases.")
                            else:
                                st.warning("⚠️ Connection succeeded but no databases found.")
                    except Exception as e:
                        st.error(f"❌ Connection test failed: {e}")
            
            with col2:
                # Enhanced execute button with database info
                execution_ready = True
                button_text = "📊 Execute Cataloging"
                
                if catalog_mode == "Select specific databases":
                    if not st.session_state.get("selected_databases"):
                        execution_ready = False
                        button_text = "⚠️ Select Databases First"
                
                if st.button(button_text, key="execute_single", type="primary", disabled=not execution_ready):
                    # Store execution parameters
                    st.session_state["monitoring_connection_id"] = connection_id
                    st.session_state["monitoring_connection_name"] = selected_conn_info[1]
                    
                    # Show execution details
                    if st.session_state.get("execution_databases"):
                        databases_to_catalog = st.session_state["execution_databases"]
                        st.success(f"🚀 Starting cataloging for {selected_conn_info[1]}!")
                        st.info(f"📊 **Databases to catalog:** {', '.join(databases_to_catalog)}")
                        
                        # Create command with database filter
                        db_list = ','.join(databases_to_catalog)
                        st.code(f"python data_catalog/cataloger_per_server_en_database.py --connection-id {connection_id} --databases '{db_list}'")
                    else:
                        st.success(f"🚀 Starting cataloging for {selected_conn_info[1]}!")
                        st.info("📊 **Mode:** Catalog all databases on server")
                        st.code(f"python data_catalog/cataloger_per_server_en_database.py --connection-id {connection_id}")
                    
                    st.info("💡 **Note:** Run the above command manually, then refresh this page to see progress.")
                    st.rerun()
            
            # SHOW CURRENT CATALOG STATUS
            if st.session_state.get("monitoring_connection_id") == connection_id:
                st.divider()
                st.subheader("📊 Current Catalog Status")
                
                # Get progress info
                progress_info = get_catalog_progress(connection_id)
                
                if progress_info["status"] == "in_progress":
                    # Progress bar
                    progress_value = min(100, progress_info["progress"]) / 100
                    st.progress(progress_value, text=f"Progress: {progress_info['progress']:.1f}%")
                    
                    # Metrics in columns
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📂 Schemas", progress_info["schemas"])
                    with col2:
                        st.metric("📋 Tables", progress_info["tables"])
                    with col3:
                        st.metric("📄 Columns", progress_info["columns"])
                    
                    if progress_info["last_updated"]:
                        st.success(f"✅ **Last activity:** {progress_info['last_updated']}")
                        
                elif progress_info["status"] == "cataloged":
                    st.success(f"✅ **Cataloging completed!**")
                    
                    # Show final metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📂 Schemas", progress_info["schemas"])
                    with col2:
                        st.metric("📋 Tables", progress_info["tables"])
                    with col3:
                        st.metric("📄 Columns", progress_info["columns"])
                        
                elif progress_info["status"] == "not_started":
                    st.info("⏳ **Status:** No catalog data found yet. Run the cataloger script to begin.")
                    
                elif progress_info["status"] == "error":
                    st.error(f"❌ **Error:** {progress_info['error']}")
                
                # Control buttons
                button_col1, button_col2 = st.columns(2)
                with button_col1:
                    if st.button("🔄 Refresh Status", key="refresh_status"):
                        st.rerun()
                
                with button_col2:
                    if st.button("🧹 Clear Monitoring", key="clear_monitoring"):
                        if "monitoring_connection_id" in st.session_state:
                            del st.session_state["monitoring_connection_id"]
                        if "monitoring_connection_name" in st.session_state:
                            del st.session_state["monitoring_connection_name"]
                        st.success("🧹 Monitoring cleared!")
                        st.rerun()
                
                # Optional log display
                with st.expander("📋 Recent Catalog Logs", expanded=False):
                    log_content = get_latest_catalog_log()
                    st.text_area("Log Output", log_content, height=200, key="log_display")
    else:
        st.warning("⚠️ No database connections found. Please add connections in the Connection Manager first.")
        if st.button("➕ Go to Connection Manager"):
            st.switch_page("pages/01_Connection_Manager.py")
        
except Exception as e:
    st.error(f"❌ Failed to load connections: {e}")

# === CLEAR ANY THREADING-RELATED SESSION STATE ===
# Remove any leftover threading session state keys
thread_keys_to_remove = [
    "single_catalog_id", "catalog_start_time", "catalog_status", 
    "catalog_active", "catalog_error", "batch_catalog_ids"
]
for key in thread_keys_to_remove:
    if key in st.session_state:
        del st.session_state[key]

# Your existing batch cataloging and other sections here...
