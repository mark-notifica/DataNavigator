import streamlit as st

# Add page config as the FIRST Streamlit command
st.set_page_config(
    page_title="DataNavigator - Catalog Execution",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
import json
import pandas as pd
import sqlalchemy as sa
import os
import sys
import subprocess
import threading
import time
from datetime import datetime
import psycopg2
from pathlib import Path

# # Add the parent directory and data_catalog folder to sys.path
# data_catalog_path = Path(__file__).resolve().parent.parent.parent / "data_catalog"
# sys.path.append(str(data_catalog_path))
# from database_server_cataloger import catalog_multiple_databases

# Add the parent directory (webapp) to sys.path
webapp_path = Path(__file__).resolve().parent.parent
sys.path.append(str(webapp_path))
from shared_utils import test_connection

# Apply styling
sys.path.append(str(Path(__file__).parent.parent))
from shared_utils import apply_compact_styling
apply_compact_styling()


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

# === FUNCTION DEFINITIONS ===

def get_order_column(sort_by):
    """Get the database column name for sorting"""
    mapping = {
        "ID": "id",
        "Name": "name", 
        "Type": "connection_type",
        "Host": "host"
    }
    return mapping.get(sort_by, "id")

def get_sort_direction():
    """Get sort direction (could be made configurable)"""
    return "ASC"

def execute_powerbi_cataloging(connection_info, folder_path):
    """Execute PowerBI semantic model cataloging"""
    try:
        # Close any existing database connections
        try:
            engine.dispose()
        except:
            pass
        
        # Set monitoring info
        st.session_state["monitoring_connection_id"] = connection_info[0]
        st.session_state["monitoring_connection_name"] = connection_info[1]
        st.session_state["cataloging_type"] = "powerbi"
        
        # Get project paths
        paths = get_project_paths()
        working_dir = str(paths['project_root'])
        
        # Build PowerBI cataloger command
        cmd = f'venv\\Scripts\\python.exe data_catalog\\powerbi_semanticmodel_cataloger.py --connection-id {connection_info[0]} --project-folder "{folder_path}"'
        
        # Start subprocess with better isolation
        process = subprocess.Popen(
            cmd,
            cwd=working_dir,
            shell=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # Set session state and show success
        st.session_state["cataloging_active"] = True
        st.success(f"🚀 PowerBI cataloger started successfully for {connection_info[1]}!")
        st.info("👆 Switch to Live Cataloging view to monitor progress")
        
        time.sleep(1)
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Failed to start PowerBI cataloger: {e}")

def get_absolute_log_path(relative_log_filename):
    """Convert relative log filename to absolute path"""
    if relative_log_filename:
        if os.path.isabs(relative_log_filename):
            return relative_log_filename  # Legacy absolute path
        else:
            paths = get_project_paths()
            return os.path.join(paths['project_root'], relative_log_filename)
    return None
def kill_specific_cataloger_run(run_id):
    """Kill a specific cataloger run by searching for run ID in log filename"""
    killed_count = 0
    
    try:
        st.write(f"🔍 **Searching for cataloger process for run {run_id}...**")
        
        # PowerShell command to find processes with specific run ID
        ps_command = f"""
        Get-WmiObject Win32_Process | Where-Object {{
            $_.CommandLine -like "*database_server_cataloger.py*" -and 
            $_.CommandLine -like "*run_{run_id}*"
        }} | ForEach-Object {{
            Write-Output "$($_.ProcessId):$($_.CommandLine)"
            Stop-Process -Id $_.ProcessId -Force
        }}
        """
        
        result = subprocess.run([
            'powershell', '-Command', ps_command
        ], capture_output=True, text=True, timeout=30)
        
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                if ':' in line and line.strip():
                    pid = line.split(':')[0]
                    st.success(f"   ✅ **Killed specific cataloger process PID:** {pid}")
                    killed_count += 1
        
        # If no specific process found, fall back to killing all cataloger processes
        if killed_count == 0:
            st.info(f"   ℹ️ No specific process found for run {run_id}, trying general cleanup...")
            killed_count = kill_cataloger_processes_powershell()
            
    except Exception as e:
        st.error(f"❌ Failed to kill specific run: {e}")
    
    return killed_count

def kill_cataloger_processes_powershell():
    """Kill cataloger processes using PowerShell"""
    killed_count = 0
    
    try:
        import subprocess
        
        st.write("🔍 **Searching for cataloger processes (PowerShell)...**")
        
        # PowerShell command to find and kill cataloger processes
        ps_command = """
        Get-WmiObject Win32_Process | Where-Object {
            $_.CommandLine -like "*database_server_cataloger.py*"
        } | ForEach-Object {
            Write-Output "$($_.ProcessId):$($_.CommandLine)"
            Stop-Process -Id $_.ProcessId -Force
        }
        """
        
        result = subprocess.run([
            'powershell', '-Command', ps_command
        ], capture_output=True, text=True, timeout=30)
        
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                if ':' in line:
                    pid = line.split(':')[0]
                    st.success(f"   ✅ **Killed cataloger process PID:** {pid}")
                    killed_count += 1
        
        if killed_count == 0:
            st.info("   ℹ️ No cataloger processes found running")
            
    except Exception as e:
        st.error(f"❌ PowerShell process killing failed: {e}")
    
    return killed_count

def kill_cataloger_processes():
    """Kill all cataloger processes on Windows"""
    killed_count = 0
    
    try:
        import subprocess
        
        st.write("🔍 **Searching for cataloger processes...**")
        
        # Find Python processes with command line info
        result = subprocess.run([
            'wmic', 'process', 'where', 'name="python.exe"', 
            'get', 'processid,commandline', '/format:csv'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            st.warning("⚠️ Could not query processes with wmic")
            return 0
        
        # Parse the CSV output
        lines = result.stdout.strip().split('\n')
        
        for line in lines[1:]:  # Skip header
            if not line.strip():
                continue
                
            # Check if this line contains our cataloger script
            if 'database_server_cataloger.py' in line:
                # Extract PID from the CSV line
                # Format: Node,CommandLine,ProcessId
                parts = line.strip().split(',')
                
                if len(parts) >= 3:
                    try:
                        # PID is usually the last column
                        pid = parts[-1].strip()
                        
                        if pid and pid.isdigit():
                            st.write(f"   🎯 **Found cataloger process PID:** {pid}")
                            
                            # Kill the process
                            kill_result = subprocess.run([
                                'taskkill', '/F', '/PID', pid
                            ], capture_output=True, text=True, timeout=10)
                            
                            if kill_result.returncode == 0:
                                st.success(f"   ✅ **Killed process PID {pid}**")
                                killed_count += 1
                            else:
                                st.warning(f"   ⚠️ Failed to kill PID {pid}: {kill_result.stderr}")
                        
                    except Exception as e:
                        st.warning(f"   ⚠️ Error processing PID from line: {e}")
        
        if killed_count == 0:
            st.info("   ℹ️ No cataloger processes found running")
            
    except subprocess.TimeoutExpired:
        st.error("❌ Process search timed out")
    except FileNotFoundError:
        st.error("❌ 'wmic' or 'taskkill' command not found")
    except Exception as e:
        st.error(f"❌ Process killing failed: {e}")
    
    return killed_count

def get_connections_for_dropdown():
    """Get connections for dropdown selection"""
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text("""
                SELECT id, name, connection_type, host, port, username, password, database_name, folder_path
                FROM config.connections
                ORDER BY name ASC
            """))
            return result.fetchall()
    except Exception as e:
        st.error(f"Failed to load connections: {e}")
        return []

# === PROGRESS MONITORING FUNCTIONS ===
def get_catalog_progress(connection_id):
    """Get cataloging progress from database"""
    try:
        with engine.connect() as conn:
            # Get database entry using the new schema
            # Note: Now we need to look for databases cataloged by this connection
            # since is_current doesn't store the connection_id anymore
            db_result = conn.execute(sa.text("""
                SELECT d.id, d.database_name, d.date_updated 
                FROM catalog.catalog_databases d
                JOIN catalog.catalog_runs cr ON d.catalog_run_id = cr.id
                WHERE cr.connection_id = :conn_id 
                AND d.date_deleted IS NULL 
                AND d.is_current = true
                ORDER BY d.date_created DESC
                LIMIT 1
            """), {"conn_id": connection_id})
            
            database = db_result.fetchone()
            if not database:
                return {"status": "not_started", "progress": 0}
            
            db_id = database[0]
            
            # Get schema count
            schema_result = conn.execute(sa.text("""
                SELECT COUNT(*) FROM catalog.catalog_schemas 
                WHERE database_id = :db_id AND date_deleted IS NULL AND is_current = true
            """), {"db_id": db_id})
            schema_count = schema_result.scalar() or 0
            
            # Get table count
            table_result = conn.execute(sa.text("""
                SELECT COUNT(*) FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                WHERE s.database_id = :db_id 
                AND t.date_deleted IS NULL AND t.is_current = true
                AND s.date_deleted IS NULL AND s.is_current = true
            """), {"db_id": db_id})
            table_count = table_result.scalar() or 0
            
            # Get column count
            column_result = conn.execute(sa.text("""
                SELECT COUNT(*) FROM catalog.catalog_columns c
                JOIN catalog.catalog_tables t ON c.table_id = t.id
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                WHERE s.database_id = :db_id 
                AND c.date_deleted IS NULL AND c.is_current = true
                AND t.date_deleted IS NULL AND t.is_current = true
                AND s.date_deleted IS NULL AND s.is_current = true
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

def get_catalog_progress_all_databases(connection_id):
    """Get cataloging progress for all databases from a connection"""
    try:
        with engine.connect() as conn:
            # Get all databases cataloged by this connection
            result = conn.execute(sa.text("""
                SELECT 
                    d.database_name,
                    COUNT(DISTINCT s.id) as schema_count,
                    COUNT(DISTINCT t.id) as table_count,
                    COUNT(DISTINCT c.id) as column_count,
                    MAX(cr.run_completed_at) as last_run
                FROM catalog.catalog_databases d
                JOIN catalog.catalog_runs cr ON d.catalog_run_id = cr.id
                LEFT JOIN catalog.catalog_schemas s ON d.id = s.database_id 
                    AND s.date_deleted IS NULL AND s.is_current = true
                LEFT JOIN catalog.catalog_tables t ON s.id = t.schema_id 
                    AND t.date_deleted IS NULL AND t.is_current = true
                LEFT JOIN catalog.catalog_columns c ON t.id = c.table_id 
                    AND c.date_deleted IS NULL AND c.is_current = true
                WHERE cr.connection_id = :conn_id 
                AND d.date_deleted IS NULL 
                AND d.is_current = true
                GROUP BY d.database_name
                ORDER BY last_run DESC
            """), {"conn_id": connection_id})
            
            databases = result.fetchall()
            
            if not databases:
                return {"status": "not_started", "databases": []}
            
            total_schemas = sum(db[1] for db in databases)
            total_tables = sum(db[2] for db in databases)
            total_columns = sum(db[3] for db in databases)
            
            return {
                "status": "completed" if databases else "not_started",
                "databases": [
                    {
                        "name": db[0],
                        "schemas": db[1],
                        "tables": db[2], 
                        "columns": db[3],
                        "last_run": db[4]
                    }
                    for db in databases
                ],
                "totals": {
                    "databases": len(databases),
                    "schemas": total_schemas,
                    "tables": total_tables,
                    "columns": total_columns
                }
            }
            
    except Exception as e:
        return {"status": "error", "error": str(e)}

def get_latest_catalog_log():
    """Get the latest database server cataloging log content"""
    try:
        paths = get_project_paths()
        log_dir = paths['database_logfiles'] 
        
        if log_dir.exists():
            log_files = sorted(log_dir.glob("catalog_extraction_*.log"), 
                             key=lambda x: x.stat().st_mtime, reverse=True)
            if log_files:
                with open(log_files[0], 'r') as f:
                    lines = f.readlines()
                    return ''.join(lines[-15:])
        return "No recent database server logs found"
    except Exception as e:
        return f"Error reading logs: {e}"

def get_databases_preview(connection_info):
    """Preview available databases on a server without cataloging"""
    DEBUG_MODE = False  # Set to True when you need debugging

    if DEBUG_MODE:
        st.write("Debug - connection_info length:", len(connection_info))
        st.write("Debug - connection_info contents:", connection_info)
    try:
        # Get connection details from the tuple
        conn_id = connection_info[0]
        conn_name = connection_info[1] 
        conn_type = connection_info[2]
        host = connection_info[3]
        port = connection_info[4]
        database_name = connection_info[5] if len(connection_info) > 5 else None
        
        # For preview, we need to get the password from the database
        # Let's get the full connection info including password
        with engine.connect() as db_conn:
            result = db_conn.execute(sa.text("""
                SELECT host, port, username, password, connection_type
                FROM config.connections 
                WHERE id = :conn_id
            """), {"conn_id": conn_id})
            
            full_conn_info = result.fetchone()
            if not full_conn_info:
                st.error("Connection not found")
                return []
            
            host, port, username, password, connection_type = full_conn_info
        
        # Connect to master/system database to enumerate databases
        if connection_type == 'PostgreSQL':
            master_db = 'postgres'
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=master_db,
                user=username,
                password=password
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
                
        elif connection_type == 'Azure SQL Server':
            import pyodbc
            master_db = 'master'
            
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={host};"
                f"PORT={port};"
                f"DATABASE={master_db};"
                f"UID={username};"
                f"PWD={password};"
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
        else:
            st.error(f"Unsupported connection type: {connection_type}")
            return []
        
        conn.close()
        return databases
        
    except Exception as e:
        st.error(f"❌ Failed to preview databases: {e}")
        return []

def find_project_root():
    """Find project root by looking for marker files"""
    current = Path(__file__).parent
    
    # Look for files that indicate project root
    marker_files = [".env", "requirements.txt", "data_catalog"]
    
    while current != current.parent:  # Stop at filesystem root
        # Check if any marker files/directories exist
        if any((current / marker).exists() for marker in marker_files):
            return current
        current = current.parent
    
    # Fallback: couldn't find project root
    return None

def get_project_paths():
    """Get all important project paths"""
    if os.getenv('PROJECT_ROOT'):
        project_root = Path(os.getenv('PROJECT_ROOT'))
    else:
        project_root = find_project_root()
    
    if not project_root:
        raise FileNotFoundError("Could not locate project root directory")
    
    return {
        'project_root': project_root,
        'data_catalog': project_root / "data_catalog",
        'cataloger_script': project_root / "data_catalog" / "database_server_cataloger.py",
        'database_logfiles': project_root / "data_catalog" / "logfiles" / "database_server",
        'powerbi_logfiles': project_root / "data_catalog" / "logfiles" / "powerbi_semanticmodel", 
        'webapp': project_root / "webapp"
    }

def run_cataloger_with_progress(connection_id, databases=None):
    """Actually run the database server cataloger and track progress"""
    try:
        paths = get_project_paths()
        
        if not paths['cataloger_script'].exists():
            st.error(f"❌ Database server cataloger not found: {paths['cataloger_script']}")
            return False
        
        working_dir = str(paths['project_root'])
        
        # Build command
        if databases:
            db_list = ','.join(databases)
            command_line = f'venv\\Scripts\\python.exe data_catalog\\database_server_cataloger.py --connection-id {connection_id} --databases "{db_list}"'
        else:
            command_line = f'venv\\Scripts\\python.exe data_catalog\\database_server_cataloger.py --connection-id {connection_id}'
        
        # Start the process (NO debug output)
        process = subprocess.Popen(
            command_line,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=working_dir,
            shell=True
        )
        
        # Store process info
        st.session_state["cataloger_process"] = process
        st.session_state["cataloging_active"] = True
        st.session_state["cataloging_start_time"] = time.time()
        
        # The live progress monitoring will be handled by the separate section
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to start cataloging: {e}")
        return False

def check_cataloger_status():
    """Check if cataloger is still running"""
    if "cataloger_process" in st.session_state:
        process = st.session_state["cataloger_process"]
        if process.poll() is None:
            return "running"
        else:
            # Process finished
            st.session_state["cataloging_active"] = False
            if process.returncode == 0:
                return "completed"
            else:
                return "failed"
    return "not_started"

def get_catalog_run_log(run_id):
    """Get log content for a specific catalog run"""
    try:
        with engine.connect() as conn:
            # Get log filename for this run
            result = conn.execute(sa.text("""
                SELECT log_filename FROM catalog.catalog_runs WHERE id = :run_id
            """), {"run_id": run_id})
            
            log_info = result.fetchone()
            if not log_info or not log_info[0]:
                return "No log file found for this run"
            
            log_filename = log_info[0]
            
            # The log_filename might be relative to project root or absolute
            paths = get_project_paths()
            if not Path(log_filename).is_absolute():
                log_path = paths['project_root'] / log_filename
            else:
                log_path = Path(log_filename)
            
            if log_path.exists():
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                    # Return last 20 lines
                    return ''.join(lines[-20:])
            else:
                return f"Log file not found: {log_path}"
                
    except Exception as e:
        return f"Error reading log: {e}"

def get_current_run_info(connection_id):
    """Get current/recent run information for a connection"""
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text("""
                SELECT id, run_status, run_started_at, run_completed_at, log_filename,
                       databases_processed, schemas_processed, tables_processed, columns_processed
                FROM catalog.catalog_runs 
                WHERE connection_id = :conn_id 
                ORDER BY run_started_at DESC 
                LIMIT 1
            """), {"conn_id": connection_id})
            
            return result.fetchone()
    except Exception as e:
        return None

def get_latest_database_log():
    """Get the latest database server cataloging log content"""
    try:
        paths = get_project_paths()
        log_dir = paths['database_logfiles']  # This points to data_catalog/logfiles/database_server
        
        if log_dir.exists():
            log_files = sorted(log_dir.glob("catalog_extraction_*.log"), 
                             key=lambda x: x.stat().st_mtime, reverse=True)
            if log_files:
                with open(log_files[0], 'r') as f:
                    lines = f.readlines()
                    # Return last 25 lines
                    return ''.join(lines[-25:])
        return "No recent database server logs found"
    except Exception as e:
        return f"Error reading logs: {e}"


st.divider()

tab1, tab2 = st.tabs(["🚀 Execute Cataloging", "📊 Manage Catalog Runs"])

with tab1:
    st.subheader("Select connection to catalog")

    col_filter1, col_filter2 = st.columns(2)

    with col_filter1:
        # Connection type filter
        connection_type_options = ["All Types", "PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"]
        selected_connection_type = st.selectbox(
            "Connection Type:",
            options=connection_type_options,
            key="single_connection_type_filter"
        )

    with col_filter2:
        # Sort controls
        single_sort_by = st.selectbox(
            "Sort by:",
            ["ID", "Name", "Type", "Host"],
            index=1,  # Default to Name
            key="single_sort_by_filter"  # Make this key unique
        )

    # Fetch available connections with filter and sorting
    try:
        # Build where clause for connection type filter
        where_conditions = ["connection_type IN ('PostgreSQL', 'Azure SQL Server', 'Power BI Semantic Model')"]
        params = {}
        
        if selected_connection_type != "All Types":
            where_conditions.append("connection_type = :conn_type")
            params["conn_type"] = selected_connection_type
        
        where_clause = " WHERE " + " AND ".join(where_conditions)
        
        # Determine sort column and order
        sort_column_map = {
            "ID": "id",
            "Name": "name",
            "Type": "connection_type", 
            "Host": "host"
        }
        
        sort_column = sort_column_map[single_sort_by]
        sort_direction = "ASC"  # Always ascending for now
        
        # Single query with filter and sorting
        with engine.connect() as db_conn:
            query = f"""
                SELECT id, name, connection_type, host, port, username, password, database_name, folder_path
                FROM config.connections 
                {where_clause}
                ORDER BY {sort_column} {sort_direction}
            """
            result = db_conn.execute(sa.text(query), params)
            connections = result.fetchall()
            
        if connections:
            # Display connection count with filter info
            if selected_connection_type != "All Types":
                st.info(f"📊 Found {len(connections)} {selected_connection_type} connections")
            else:
                st.info(f"📊 Found {len(connections)} connections")
            
            # Create dropdown options with ID, Name, Type, and Host
            connection_options = []
            for conn in connections:
                if conn[2] == "Power BI Semantic Model":  # PowerBI connection
                    folder_display = Path(conn[8]).name if conn[8] else "No folder"
                    if single_sort_by == "ID":
                        connection_options.append(f"ID: {conn[0]} - {conn[1]} ({conn[2]}) - {folder_display}")
                    elif single_sort_by == "Name":
                        connection_options.append(f"{conn[1]} - ID: {conn[0]} ({conn[2]}) - {folder_display}")
                    elif single_sort_by == "Type":
                        connection_options.append(f"{conn[2]}: {conn[1]} - ID: {conn[0]} - {folder_display}")
                    else:  # Host
                        connection_options.append(f"PowerBI: {folder_display} - {conn[1]} - ID: {conn[0]}")
                else:  # Database connection
                    if single_sort_by == "ID":
                        connection_options.append(f"ID: {conn[0]} - {conn[1]} ({conn[2]}) - {conn[3]}:{conn[4]}")
                    elif single_sort_by == "Name":
                        connection_options.append(f"{conn[1]} - ID: {conn[0]} ({conn[2]}) - {conn[3]}:{conn[4]}")
                    elif single_sort_by == "Type":
                        connection_options.append(f"{conn[2]}: {conn[1]} - ID: {conn[0]} - {conn[3]}:{conn[4]}")
                    else:  # Host
                        connection_options.append(f"{conn[3]}:{conn[4]} - {conn[1]} - ID: {conn[0]} ({conn[2]})")
            
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
                
                # Check connection type
                connection_type = selected_conn_info[2]  # connection_type field
                
                # Display connection details
                with st.expander("📋 Selected Connection Details", expanded=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**ID:** {selected_conn_info[0]}")
                        st.write(f"**Name:** {selected_conn_info[1]}")
                        st.write(f"**Type:** {selected_conn_info[2]}")
                    with col2:
                        if connection_type == "Power BI Semantic Model":
                            st.write(f"**Folder Path:** {selected_conn_info[8] or 'Not specified'}")
                        else:
                            st.write(f"**Host:** {selected_conn_info[3]}")
                            st.write(f"**Port:** {selected_conn_info[4]}")
                            st.write(f"**Database:** {selected_conn_info[7] or 'Not specified'}")
                
                # === CONNECTION TYPE SPECIFIC SECTIONS ===
                if connection_type == "Power BI Semantic Model":
                    # PowerBI-specific interface
                    st.divider()
                    st.subheader("📊 PowerBI Project Configuration")
                    
                    folder_path = selected_conn_info[8]  # folder_path field
                    
                    if folder_path:
                        if os.path.exists(folder_path):
                            st.success(f"✅ Project folder found: {folder_path}")
                            
                            # List PowerBI project files in folder (including subfolders)
                            try:
                                pbip_files = list(Path(folder_path).rglob("*.pbip"))
                                tmdl_files = list(Path(folder_path).rglob("*.tmdl"))
                                json_files = list(Path(folder_path).rglob("*.json"))
                                dax_files = list(Path(folder_path).rglob("*.dax"))
                                pbism_files = list(Path(folder_path).rglob("*.pbism"))
                                pbir_files = list(Path(folder_path).rglob("*.pbir"))
                                
                                total_files = len(pbip_files) + len(tmdl_files) + len(json_files) + len(dax_files) + len(pbism_files) + len(pbir_files)
                                
                                if total_files > 0:
                                    st.info(f"📁 Found PowerBI project files (including subfolders):")
                                    if pbip_files:
                                        st.write(f"  • **{len(pbip_files)} .pbip files:** {', '.join([f.name for f in pbip_files])}")
                                    if tmdl_files:
                                        st.write(f"  • **{len(tmdl_files)} .tmdl files:** {', '.join([f.name for f in tmdl_files[:5]])}{'...' if len(tmdl_files) > 5 else ''}")
                                    if json_files:
                                        st.write(f"  • **{len(json_files)} .json files:** {', '.join([f.name for f in json_files[:5]])}{'...' if len(json_files) > 5 else ''}")
                                    if dax_files:
                                        st.write(f"  • **{len(dax_files)} .dax files:** {', '.join([f.name for f in dax_files[:5]])}{'...' if len(dax_files) > 5 else ''}")
                                    if pbism_files:
                                        st.write(f"  • **{len(pbism_files)} .pbism files:** {', '.join([f.name for f in pbism_files])}")
                                    if pbir_files:
                                        st.write(f"  • **{len(pbir_files)} .pbir files:** {', '.join([f.name for f in pbir_files])}")
                                else:
                                    st.warning("⚠️ No PowerBI project files found in the specified folder")
                                    st.info("Expected files: .pbip, .tmdl, .json, .dax, .pbism, or .pbir files (including subfolders)")
                                    
                            except Exception as e:
                                st.error(f"Error reading folder: {e}")
                                
                        else:
                            st.error(f"❌ Folder not found: {folder_path}")
                    else:
                        st.error("❌ No folder path specified in connection")
                    
                    # Action buttons for PowerBI
                    st.divider()
                    execution_ready = folder_path and os.path.exists(folder_path)
                    button_text = "🚀 Execute PowerBI Cataloging" if execution_ready else "⚠️ Fix Folder Path First"
                    
                    if st.button(button_text, key="execute_powerbi", type="primary", disabled=not execution_ready):
                        execute_powerbi_cataloging(selected_conn_info, folder_path)
                
                else:            
                    # === DATABASE SELECTION SECTION ===
                    st.divider()
                    st.subheader("Database selection (optional)")
                    
                    # Show current database_name from connection
                    current_db = selected_conn_info[7] or "All databases (not specified)"
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
            if connection_type != "Power BI Semantic Model":    
                st.divider()
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🧪 Test Connection", key="test_single", type="secondary"):
                        try:
                            with st.spinner(f"Testing connection to {selected_conn_info[1]}..."):
                                # Prepare connection info
                                connection_info = {
                                    "connection_type": selected_conn_info[2],  # Connection type
                                    "host": selected_conn_info[3],             # Host
                                    "port": selected_conn_info[4],             # Port
                                    "username": selected_conn_info[5],         # Username
                                    "password": selected_conn_info[6]          # Password
                                }
                                
                                # Determine cataloging mode
                                catalog_mode = st.session_state.get("catalog_mode", "Use connection setting")
                                
                                if catalog_mode == "Use connection setting":
                                    # Use preconfigured databases from the connection
                                    databases_to_test = [db.strip() for db in selected_conn_info[7].split(',')] if selected_conn_info[7] else None
                                
                                elif catalog_mode == "Select specific databases":
                                    # Use databases already selected for cataloging
                                    databases_to_test = st.session_state.get("selected_databases", None)
                                
                                # Call reusable test_connection function
                                test_results = test_connection(connection_info, databases_to_test)
                                
                                # Display results
                                if test_results:
                                    st.write("**Connection Test Results:**")
                                    for result in test_results:
                                        if "✅" in result:
                                            st.success(result)
                                        else:
                                            st.error(result)
                                else:
                                    # No databases selected, only server connection tested
                                    st.success("✅ Server connection: Success. No databases were selected, so only the server connection was tested.")
                        except Exception as e:
                            st.error(f"❌ Connection test failed: {e}")

                with col2:
                    # Not running - show execute button
                    execution_ready = True
                    
                    # Only check catalog_mode if we're in database mode (not PowerBI)
                    if connection_type != "Power BI Semantic Model":
                        catalog_mode = st.session_state.get("catalog_mode", "Use connection setting")
                        if catalog_mode == "Select specific databases":
                            if not st.session_state.get("selected_databases"):
                                execution_ready = False
                    
                    button_text = "🚀 Execute Cataloging" if execution_ready else "⚠️ Select Databases First"
                    
                    if st.button(button_text, key="execute_single", type="primary", disabled=not execution_ready):
                        try:
                            with st.spinner(f"Starting cataloging for {selected_conn_info[1]}..."):
                                # Prepare connection info
                                connection_info = {
                                    "connection_type": selected_conn_info[2],  # Connection type
                                    "host": selected_conn_info[3],             # Host
                                    "port": selected_conn_info[4],             # Port
                                    "username": selected_conn_info[5],         # Username
                                    "password": selected_conn_info[6]          # Password
                                }
                                # Define working_dir as the root directory of the project
                                working_dir = str(Path(__file__).resolve().parent.parent.parent / "data_catalog")

                                # Get databases to catalog
                                databases_to_catalog = None
                                if catalog_mode == "Select specific databases":
                                    databases_to_catalog = st.session_state.get("selected_databases", [])
                                elif selected_conn_info[7]:
                                    databases_to_catalog = [db.strip() for db in selected_conn_info[7].split(',')]
                                
                                # Build command for subprocess
                                if databases_to_catalog:
                                    db_arg = ','.join(databases_to_catalog)
                                    cmd = f'venv\\Scripts\\python.exe database_server_cataloger.py --connection-id {connection_id} --databases "{db_arg}"'
                                else:
                                    cmd = f'venv\\Scripts\\python.exe database_server_cataloger.py --connection-id {connection_id}'
                                
                                # Start subprocess
                                process = subprocess.Popen(
                                    cmd,
                                    cwd=working_dir,
                                    shell=True,
                                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE
                                )
                                
                                # Set session state and show success
                                st.session_state["cataloging_active"] = True
                                st.success(f"🚀 Cataloger started successfully for {selected_conn_info[1]}!")
                                st.info("👆 Switch to Live Cataloging view to monitor progress")
                                
                                time.sleep(1)
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Failed to start cataloger: {e}")
        else:
            st.warning("No connections found matching the selected criteria.")
            
    except Exception as e:
        st.error(f"❌ Failed to load connections: {e}")

    # === LIVE CATALOGING PROGRESS ===
    if st.session_state.get("cataloging_active"):
        cataloging_type = st.session_state.get("cataloging_type", "database")
        connection_id = st.session_state.get("monitoring_connection_id")
        
        # Check if the process is actually still running
        if connection_id:
            try:
                with engine.connect() as conn:
                    result = conn.execute(sa.text("""
                        SELECT run_status, log_filename FROM catalog.catalog_runs 
                        WHERE connection_id = :conn_id 
                        ORDER BY run_started_at DESC 
                        LIMIT 1
                    """), {"conn_id": connection_id})
                    
                    latest_run = result.fetchone()
                    if latest_run:
                        current_status, log_filename = latest_run
                        
                        # If the run is completed or failed, clear the active state
                        if current_status in ['completed', 'failed']:
                            st.session_state["cataloging_active"] = False
                            st.success(f"✅ Cataloging process completed with status: {current_status}")
                            st.info("🔄 Page will refresh to show final results")
                            time.sleep(2)
                            st.rerun()
                            
            except Exception as e:
                st.error(f"Error checking run status: {e}")
        
        if cataloging_type == "powerbi":
            st.warning("🔄 **POWERBI CATALOGING PROCESS IS ACTIVE!**")
        else:
            st.warning("🔄 **DATABASE CATALOGING PROCESS IS ACTIVE!**")
        
        # Simple controls - just stop monitoring and line selector
        col1, col2, col3 = st.columns([3, 2, 3])

        with col1:
            if st.button("🛑 Stop Monitoring"):
                st.session_state["cataloging_active"] = False
                st.session_state.pop("monitoring_connection_id", None)
                st.rerun()

        with col2:
            lines_to_show = st.selectbox("Lines:", [25, 50, 100, 200], index=1, key="live_log_lines")

        with col3:
            auto_refresh = st.checkbox("🔄 Auto-refresh (10s)", value=True, key="auto_refresh_enabled")
        
        # Information about alternative monitoring
        st.info("💡 **Tip:** You can also monitor this run and access log files in the **Run Management** tab → view run details")
    
        st.subheader("📋 Live Log File (Tail View)")
        
        
        # Get the actual log file path from the current run
        try:
            connection_id = st.session_state.get("monitoring_connection_id")
            relative_log_filename = None
            
            if connection_id:
                with engine.connect() as conn:
                    result = conn.execute(sa.text("""
                        SELECT log_filename, run_status FROM catalog.catalog_runs 
                        WHERE connection_id = :conn_id 
                        ORDER BY run_started_at DESC 
                        LIMIT 1
                    """), {"conn_id": connection_id})
                    
                    run_data = result.fetchone()
                    if run_data:
                        relative_log_filename, run_status = run_data
                        
                        # If process is completed but log file is None, show message
                        if not relative_log_filename and run_status in ['completed', 'failed']:
                            st.info("📋 Process completed. View the complete log in Run Management tab.")
            
            # Convert to absolute path
            log_file_path = get_absolute_log_path(relative_log_filename)
            
            # Show log file in tail mode
            if log_file_path and os.path.exists(log_file_path):
                # Read last N lines of log file
                with open(log_file_path, 'r') as f:
                    lines = f.readlines()
                    last_lines = lines[-lines_to_show:] if len(lines) > lines_to_show else lines
                
                # Reverse order (newest first) and add row numbers
                total_lines = len(lines)
                start_line_number = max(1, total_lines - len(last_lines) + 1)
                
                # Create numbered lines in reverse order (newest first)
                numbered_lines = []
                for i, line in enumerate(reversed(last_lines)):
                    line_number = total_lines - i
                    numbered_lines.append(f"{line_number:4d}: {line.rstrip()}")
                
                # Display in code block for better formatting
                log_content = '\n'.join(numbered_lines)
                
                # Show log statistics
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                with col_stat1:
                    st.metric("Total Lines", total_lines)
                with col_stat2:
                    st.metric("Showing Lines", len(last_lines))
                with col_stat3:
                    error_count = sum(1 for line in last_lines if '[ERROR]' in line)
                    st.metric("Errors in View", error_count)
                
                # Log content with custom styling
                st.markdown("**📄 Log Content (Newest First):**")
                st.code(log_content, language=None, line_numbers=False)
                
                # Show last activity info
                if last_lines:
                    last_line = last_lines[-1].strip()
                    if last_line:
                        st.info(f"**Latest Activity:** {last_line[:100]}...")
                

            else:
                if relative_log_filename:
                    st.warning(f"Log file not found: {log_file_path}")
                    st.info(f"Relative path from database: {relative_log_filename}")
                else:
                    st.info("Log file not available yet. The cataloging process may not have started.")
            
        except Exception as e:
            st.error(f"Error reading log file: {e}")
        
        if auto_refresh:
            time.sleep(10)
            st.rerun()

        # User explanation section
        with st.expander("ℹ️ How Live Monitoring Works", expanded=False):
            st.markdown("""
            **Understanding the Cataloging Process:**
            
            🔄 **Subprocess**: The cataloger runs as a separate process from Streamlit for stability and control.
            
            📋 **Log File**: Real-time progress is written to a log file that we monitor continuously.
            
            📄 **Tail View**: Shows the most recent log entries first (newest at top) with line numbers.
            
            🔄 **Updates**: Auto-refresh is enabled by default to show live updates. You can disable it if needed.
            
            📊 **Tab 2 Monitoring**: You can also monitor any catalog run (current or historical) in the **Run Management** tab by selecting a run and viewing its details and log files.
            
            💡 **Best Practice**: Use Tab 2 for comprehensive run management including stopping active processes.
            """)

    # === COMPLETED CATALOG RUN STATUS ===
    elif st.session_state.get("monitoring_connection_id") and not st.session_state.get("cataloging_active"):
        connection_id = st.session_state["monitoring_connection_id"]
        connection_name = st.session_state.get("monitoring_connection_name", "Unknown Connection")
        
        st.divider()
        st.subheader("📊 Latest Catalog Run Results")
        
        try:
            with engine.connect() as conn:
                result = conn.execute(sa.text("""
                    SELECT id, run_status, run_started_at, run_completed_at, log_filename,
                        databases_processed, schemas_processed, tables_processed, columns_processed, error_message
                    FROM catalog.catalog_runs 
                    WHERE connection_id = :conn_id 
                    ORDER BY run_started_at DESC 
                    LIMIT 1
                """), {"conn_id": connection_id})
                
                latest_run = result.fetchone()
                if latest_run:
                    run_id, status, started_at, completed_at, log_filename, db_count, schema_count, table_count, column_count, error_message = latest_run
                    
                    if status == "completed":
                        st.success(f"✅ **Latest catalog run completed for: {connection_name}**")
                    elif status == "failed":
                        st.error(f"❌ **Latest catalog run failed for: {connection_name}**")
                        
                        # ONLY SHOW DEBUG INFO FOR FAILED RUNS
                        with st.expander("🔧 **Debug Information**", expanded=True):
                            if error_message:
                                st.error(f"**Error:** {error_message}")
                            
                            # Show test button for failed runs
                            if st.button("🔍 Test Cataloger Command", key="test_cataloger_debug"):
                                paths = get_project_paths()
                                
                                # Test command
                                test_cmd = f'venv\\Scripts\\python.exe data_catalog\\database_server_cataloger.py --connection-id {connection_id} --databases "1247"'
                                
                                st.write(f"**Testing Command:** {test_cmd}")
                                
                                try:
                                    # Run and capture output
                                    result = subprocess.run(
                                        test_cmd,
                                        capture_output=True,
                                        text=True,
                                        cwd=str(paths['project_root']),
                                        shell=True,
                                        timeout=60  # 1 minute timeout
                                    )
                                    
                                    st.write(f"**Return Code:** {result.returncode}")
                                    if result.stdout:
                                        st.text_area("**Standard Output:**", result.stdout, height=200)
                                    if result.stderr:
                                        st.text_area("**Error Output:**", result.stderr, height=200)
                                        
                                except subprocess.TimeoutExpired:
                                    st.error("Command timed out after 60 seconds")
                                except Exception as e:
                                    st.error(f"Command failed: {e}")
                    elif status == "running":
                        # If still running, switch back to live monitoring
                        st.session_state["cataloging_active"] = True
                        st.rerun()
                    else:
                        st.info(f"🔄 **Catalog run status: {status} for: {connection_name}**")
                    
                    # Show run details in a nice layout
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("🔢 Run ID", run_id)
                        st.metric("📊 Databases", db_count or 0)
                    with col2:
                        st.metric("📂 Schemas", schema_count or 0)
                        st.metric("📋 Tables", table_count or 0)
                    with col3:
                        st.metric("📄 Columns", column_count or 0)
                        if completed_at and started_at:
                            duration = completed_at - started_at
                            st.metric("⏱️ Duration", f"{duration.total_seconds():.1f}s")
                    
                    # Show timestamps
                    st.write(f"**Started:** {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    if completed_at:
                        st.write(f"**Completed:** {completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Show log file info and content
                    if log_filename:
                        st.subheader(f"📋 Log File (Run {run_id})")
                        st.code(log_filename)
                        
                        # Show log content in expandable section
                        with st.expander(f"📄 View Log Content (Run {run_id})", expanded=False):
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                if st.button("🔄 Refresh Log", key=f"live_refresh_log_{run_id}"):
                                    st.rerun()
                            with col2:
                                log_lines = st.selectbox("Show lines:", [10, 25, 50, 100], index=1, key=f"live_log_lines_{run_id}")
                            
                            # Get log content
                            try:
                                if Path(log_filename).exists():
                                    with open(log_filename, 'r') as f:
                                        lines = f.readlines()
                                        recent_lines = ''.join(lines[-log_lines:])
                                        st.text_area(f"Last {log_lines} lines:", recent_lines, height=400, key=f"live_log_content_{run_id}")
                                else:
                                    st.error(f"Log file not found: {log_filename}")
                            except Exception as e:
                                st.error(f"Error reading log file: {e}")
                    else:
                        st.warning("No log file recorded for this run")
                else:
                    st.warning("No catalog runs found for this connection")
                    
        except Exception as e:
            st.error(f"Error getting run details: {e}")
        
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

with tab2:
    st.subheader("Catalog runs overview and management")
    
    # Show warning if cataloging is active but still allow access
    if st.session_state.get("cataloging_active"):
        st.warning("⚠️ **Live cataloging is currently active.** Exercise caution when managing runs to avoid interference.")
        st.info("💡 **Tip:** Use the 'Stop Monitoring' button in the Live Cataloging tab to safely stop monitoring before making changes.")
    
    # === FILTERS SECTION ===
    st.markdown("### 🔍 Filters")

    col_filter1, col_filter2, col_filter3 = st.columns(3)
    with col_filter1:
        # Connection filter
        try:
            with engine.connect() as conn:
                connections_result = conn.execute(sa.text("""
                    SELECT DISTINCT c.id, c.name 
                    FROM config.connections c
                    JOIN catalog.catalog_runs cr ON c.id = cr.connection_id
                    ORDER BY c.name
                """))
                connections = [("All Connections", 0)] + [(name, id) for id, name in connections_result.fetchall()]
        except Exception as e:
            connections = [("All Connections", 0)]
            st.error(f"Error loading connections: {e}")
        
        selected_connection = st.selectbox(
            "Connection:",
            options=connections,
            format_func=lambda x: x[0],
            key="mgmt_connection_filter"
        )
    
    with col_filter2:
        # Status filter
        status_options = ["All Statuses", "running", "completed", "failed"]
        selected_status = st.selectbox(
            "Status:",
            options=status_options,
            key="mgmt_status_filter"
        )
    
    with col_filter3:
        # Limit results
        limit_options = [10, 25, 50, 100]
        selected_limit = st.selectbox(
            "Show last:",
            options=limit_options,
            index=1,  # Default to 25
            key="mgmt_limit_filter"
        )
    
    # Add special warning for dangerous operations during live cataloging
    if st.session_state.get("cataloging_active"):
        with st.expander("⚠️ **Important:** Live Cataloging Safety", expanded=False):
            st.markdown("""
            **While live cataloging is active:**
            - ✅ **Safe:** View logs, check status, refresh data
            - ⚠️ **Caution:** Stopping specific runs may affect the monitored process
            - ❌ **Avoid:** Bulk operations that might interfere with active processes
            
            **Recommendation:** Use the Live Cataloging tab for monitoring active processes.
            """)

    # Build query with filters
    where_conditions = []
    params = {"limit": selected_limit}

    if selected_status != "All Statuses":
        where_conditions.append("cr.run_status = :status")
        status_value = selected_status
        if status_value:
            params["status"] = status_value

    if selected_connection[0] != "All Connections":
        where_conditions.append("c.id = :conn_id")
        params["conn_id"] = selected_connection[1]

    where_clause = " AND " + " AND ".join(where_conditions) if where_conditions else ""

    try:
        with engine.connect() as conn:
            # Get catalog runs with filters
            result = conn.execute(sa.text(f"""
                SELECT 
                    cr.id,
                    cr.connection_id,
                    c.name as connection_name,
                    cr.run_status,
                    cr.run_started_at,
                    cr.run_completed_at,
                    cr.databases_processed,
                    cr.schemas_processed,
                    cr.tables_processed,
                    cr.views_processed,
                    cr.columns_processed,
                    cr.error_message,
                    cr.log_filename,
                    cr.databases_to_catalog,
                    cr.databases_count,
                    cr.models_processed,
                    cr.measures_processed,
                    cr.relationships_processed,
                    cr.m_code_processed,
                    c.connection_type
                FROM catalog.catalog_runs cr
                JOIN config.connections c ON cr.connection_id = c.id
                WHERE 1=1 {where_clause}
                ORDER BY cr.run_started_at DESC
                LIMIT :limit
            """), params)
            
            runs = result.fetchall()
            
            if runs:
                # Display runs in a table format (removed bulk selection)
                runs_data = []
                for run in runs:
                    (run_id, conn_id, conn_name, status, started, completed, dbs_proc, schemas_proc, 
                    tables_proc, views_proc, columns_proc, error, log_file, databases_to_catalog, 
                    databases_count, models_proc, measures_proc, relationships_proc, m_code_proc, 
                    connection_type) = run

                    # Calculate duration
                    if completed and started:
                        duration = completed - started
                        duration_str = f"{duration.total_seconds():.1f}s"
                    else:
                        duration_str = "Running..." if status == "running" else "N/A"
                    
                    # Process databases_to_catalog for display
                    if databases_to_catalog and databases_to_catalog != "all":
                        try:
                            db_list = json.loads(databases_to_catalog)
                            db_display = f"{len(db_list)} DBs: {', '.join(db_list[:2])}{'...' if len(db_list) > 2 else ''}"
                        except:
                            db_display = databases_to_catalog[:30] + "..." if len(str(databases_to_catalog)) > 30 else databases_to_catalog
                    else:
                        db_display = "All DBs"
                    
                    # Truncate error message for display
                    error_display = error[:50] + "..." if error and len(error) > 50 else (error or "")
                    
                    # Check if this is a PowerBI run
                    is_powerbi = connection_type == "Power BI Semantic Model"
                    
                    if is_powerbi:
                        # PowerBI-specific data structure
                        runs_data.append({
                            "ID": run_id,
                            "Connection": conn_name,
                            "Type": connection_type,
                            "Status": status,
                            "Started": started.strftime('%m-%d %H:%M'),
                            "Duration": duration_str,
                            "Models": models_proc or 0,
                            "Tables": tables_proc or 0,
                            "Columns": columns_proc or 0,
                            "Measures": measures_proc or 0,
                            "Relationships": relationships_proc or 0,
                            "M-Code": m_code_proc or 0,
                            "Error": error_display
                        })
                    else:
                        # Database-specific data structure
                        # Process databases_to_catalog for display
                        if databases_to_catalog and databases_to_catalog != "all":
                            try:
                                db_list = json.loads(databases_to_catalog)
                                db_display = f"{len(db_list)} DBs: {', '.join(db_list[:2])}{'...' if len(db_list) > 2 else ''}"
                            except:
                                db_display = databases_to_catalog[:30] + "..." if len(str(databases_to_catalog)) > 30 else databases_to_catalog
                        else:
                            db_display = "All DBs"
                        
                        runs_data.append({
                            "ID": run_id,
                            "Connection": conn_name,
                            "Type": connection_type,
                            "Status": status,
                            "Started": started.strftime('%m-%d %H:%M'),
                            "Duration": duration_str,
                            "Target DBs": db_display,
                            "DB Count": databases_count or 0,
                            "DBs Done": dbs_proc or 0,
                            "DB Check": "✅" if (databases_count and dbs_proc and databases_count == dbs_proc) else ("❌" if (databases_count and dbs_proc and databases_count != dbs_proc) else "⏳"),
                            "Schemas": schemas_proc or 0,
                            "Tables": tables_proc or 0,
                            "Views": views_proc or 0,
                            "Columns": columns_proc or 0,
                            "Error": error_display
                        })


                # Display with dynamic column configuration based on run types
                st.dataframe(
                    runs_data,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ID": st.column_config.NumberColumn("Run ID", width="small"),
                        "Connection": st.column_config.TextColumn("Connection", width="medium"),
                        "Type": st.column_config.TextColumn("Type", width="medium"),
                        "Status": st.column_config.TextColumn("Status", width="small"),
                        "Started": st.column_config.TextColumn("Started", width="small"),
                        "Duration": st.column_config.TextColumn("Duration", width="small"),
                        # Database columns
                        "Target DBs": st.column_config.TextColumn("Target DBs", width="medium"),
                        "DB Count": st.column_config.NumberColumn("DB Count", width="small"),
                        "DBs Done": st.column_config.NumberColumn("DBs Done", width="small"),
                        "DB Check": st.column_config.TextColumn("DB ✓", width="small"),
                        "Schemas": st.column_config.NumberColumn("Schemas", width="small"),
                        "Views": st.column_config.NumberColumn("Views", width="small"),
                        # PowerBI columns
                        "Models": st.column_config.NumberColumn("Models", width="small"),
                        "Measures": st.column_config.NumberColumn("Measures", width="small"),
                        "Relationships": st.column_config.NumberColumn("Relationships", width="small"),
                        "M-Code": st.column_config.NumberColumn("M-Code", width="small"),
                        # Shared columns
                        "Tables": st.column_config.NumberColumn("Tables", width="small"),
                        "Columns": st.column_config.NumberColumn("Columns", width="small"),
                        "Error": st.column_config.TextColumn("Error", width="medium")
                    }
                )
                
                # Individual run details section
                st.divider()
                st.markdown("### 🔍 Run Details")

                # Run selection
                run_options = [(f"Run {run[0]} - {run[2]} ({run[3]})", run[0]) for run in runs]
                selected_run = st.selectbox(
                    "Select a run to view details:",
                    options=run_options,
                    format_func=lambda x: x[0],
                    key="selected_run_details"
                )

                if selected_run:
                    run_id = selected_run[1]
                    selected_run_data = next(run for run in runs if run[0] == run_id)
                    
                    # Run details display
                    col_details1, col_details2, col_details3 = st.columns(3)
                    
                    with col_details1:
                        st.write(f"**Run ID:** {selected_run_data[0]}")
                        st.write(f"**Connection:** {selected_run_data[2]}")
                        st.write(f"**Status:** {selected_run_data[3]}")
                        
                        # Add database target info
                        databases_to_catalog = selected_run_data[13]  # databases_to_catalog field
                        if databases_to_catalog and databases_to_catalog != "all":
                            try:
                                db_list = json.loads(databases_to_catalog)
                                st.write(f"**Target Databases:** {', '.join(db_list[:3])}{'...' if len(db_list) > 3 else ''}")
                            except:
                                st.write(f"**Target Databases:** {databases_to_catalog}")
                        else:
                            st.write("**Target Databases:** All available")
                    
                    with col_details2:
                        st.write(f"**Started:** {selected_run_data[4].strftime('%Y-%m-%d %H:%M:%S')}")
                        if selected_run_data[5]:
                            st.write(f"**Completed:** {selected_run_data[5].strftime('%Y-%m-%d %H:%M:%S')}")
                            # Calculate and show duration
                            duration = selected_run_data[5] - selected_run_data[4]
                            st.write(f"**Duration:** {duration.total_seconds():.1f}s")
                        
                        if selected_run_data[11]:  # error_message
                            st.write(f"**Error:** {selected_run_data[11][:100]}...")
                    
                    with col_details3:
                        # Check if this is a PowerBI run by connection type or other indicator
                        connection_type = selected_run_data[2] if len(selected_run_data) > 2 else None
                        is_powerbi_run = (connection_type == "Power BI Semantic Model" or 
                                        "powerbi_catalog_" in (selected_run_data[12] or ""))  # Check log filename
                        
                        if is_powerbi_run:
                            # PowerBI-specific metrics
                            models_processed = getattr(selected_run_data, 'models_processed', 1) if hasattr(selected_run_data, 'models_processed') else 1
                            measures_processed = getattr(selected_run_data, 'measures_processed', 0) if hasattr(selected_run_data, 'measures_processed') else 0
                            relationships_processed = getattr(selected_run_data, 'relationships_processed', 0) if hasattr(selected_run_data, 'relationships_processed') else 0
                            m_code_processed = getattr(selected_run_data, 'm_code_processed', 0) if hasattr(selected_run_data, 'm_code_processed') else 0
                            
                            st.write(f"**Models Processed:** {models_processed}")
                            st.write(f"**Tables:** {selected_run_data[6] or 0}")
                            st.write(f"**Columns:** {selected_run_data[10] or 0}")
                            st.write(f"**Measures:** {measures_processed}")
                            st.write(f"**Relationships:** {relationships_processed}")
                            st.write(f"**M-Code Partitions:** {m_code_processed}")
                            
                            # Show model completion status
                            if models_processed and models_processed >= 1:
                                st.success(f"✅ Semantic model cataloged successfully")
                            else:
                                st.warning(f"⚠️ Model cataloging incomplete")
                                
                        else:
                            # Database-specific metrics (existing code)
                            databases_count = selected_run_data[14] or 0  # databases_count field
                            databases_processed = selected_run_data[6] or 0  # databases_processed field
                            
                            st.write(f"**Databases Planned:** {databases_count}")
                            st.write(f"**Databases Processed:** {databases_processed}")
                            
                            # Show database check status
                            if databases_count and databases_processed:
                                if databases_count == databases_processed:
                                    st.success(f"✅ All {databases_processed} databases completed")
                                else:
                                    st.warning(f"⚠️ {databases_processed}/{databases_count} databases completed")
                            
                            st.write(f"**Schemas:** {selected_run_data[7] or 0}")
                            st.write(f"**Tables:** {selected_run_data[8] or 0}")
                            st.write(f"**Views:** {selected_run_data[9] or 0}")
                            st.write(f"**Columns:** {selected_run_data[10] or 0}")
                    
                    # Individual run actions
                    col_action1, col_action2, col_action3 = st.columns(3)
                    
                    with col_action1:
                        # Stop individual run (only if running)
                        is_running = selected_run_data[3] == "running"
                        if st.button(
                            f"🛑 Stop Run {run_id}", 
                            key=f"stop_{run_id}", 
                            type="secondary",
                            disabled=not is_running,
                            help="Stop this running process" if is_running else f"Cannot stop - status is {selected_run_data[3]}"
                        ):
                            try:
                                # First, actually terminate the subprocess using targeted function
                                with st.spinner("Stopping cataloger process..."):
                                    killed_count = kill_specific_cataloger_run(run_id)
                                
                                # Then update database status
                                conn.execute(sa.text("""
                                    UPDATE catalog.catalog_runs 
                                    SET run_status = 'failed',
                                        run_completed_at = CURRENT_TIMESTAMP,
                                        error_message = 'Manually stopped from Run Management'
                                    WHERE id = :run_id
                                """), {"run_id": run_id})
                                conn.commit()
                                
                                if killed_count > 0:
                                    st.success(f"🛑 Stopped run {run_id} - terminated {killed_count} process(es)")
                                else:
                                    st.warning(f"🛑 Updated run {run_id} status to failed, but no running processes found to terminate")
                                    st.info("The process may have already completed or was started from a different session")
                                
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to stop run: {e}")
                    
                    with col_action2:
                        if st.button(f"🔄 Refresh", key=f"refresh_{run_id}"):
                            st.rerun()
                    
                    with col_action3:
                        show_log = st.button(f"📋 Show Log", key=f"log_{run_id}")
                    
                    # Log file section (if requested)
                    if show_log:
                        log_file = selected_run_data[12]  # Note: should be index 12, not 11
                        absolute_log_path = get_absolute_log_path(log_file)
                        
                        if absolute_log_path and Path(absolute_log_path).exists():
                            try:
                                # Try UTF-8 first, then fall back to other encodings
                                try:
                                    with open(absolute_log_path, 'r', encoding='utf-8') as f:
                                        log_content = f.read()
                                except UnicodeDecodeError:
                                    # Try with utf-8 and error handling
                                    with open(absolute_log_path, 'r', encoding='utf-8', errors='replace') as f:
                                        log_content = f.read()
                                        st.warning("⚠️ Some characters in the log file were replaced due to encoding issues")
                                
                                st.divider()
                                st.subheader(f"📋 Log File: {Path(absolute_log_path).name}")
                                
                                # Controls for the log
                                log_col1, log_col2, log_col3 = st.columns([1, 1, 2])
                                
                                with log_col1:
                                    # Download button
                                    st.download_button(
                                        label=f"💾 Download Log",
                                        data=log_content,
                                        file_name=Path(absolute_log_path).name,
                                        mime="text/plain",
                                        key=f"download_log_{run_id}"
                                    )
                                
                                with log_col2:
                                    # Show options
                                    log_lines_to_show = st.selectbox(
                                        "Show:",
                                        ["All lines", "Last 100", "Last 50", "Last 25"],
                                        key=f"log_lines_{run_id}"
                                    )
                                
                                with log_col3:
                                    # Search in log
                                    search_term = st.text_input(
                                        "Search in log:",
                                        key=f"log_search_{run_id}",
                                        placeholder="Enter search term..."
                                    )
                                
                                # Process log content based on options
                                lines = log_content.split('\n')
                                
                                if log_lines_to_show != "All lines":
                                    line_count = int(log_lines_to_show.split()[-1])
                                    lines = lines[-line_count:]
                                
                                if search_term:
                                    lines = [line for line in lines if search_term.lower() in line.lower()]
                                    st.info(f"🔍 Found {len(lines)} lines containing '{search_term}'")
                                
                                # Reverse order (newest first) and add row numbers
                                total_lines = len(log_content.split('\n'))
                                start_line_number = max(1, total_lines - len(lines) + 1)
                                
                                # Create numbered lines in reverse order (newest first)
                                numbered_lines = []
                                for i, line in enumerate(reversed(lines)):
                                    line_number = total_lines - i if log_lines_to_show != "All lines" else len(lines) - i
                                    numbered_lines.append(f"{line_number:4d}: {line.rstrip()}")
                                
                                displayed_content = '\n'.join(numbered_lines)
                                
                                # Show the log in full width
                                st.text_area(
                                    f"📄 Log Content ({len(lines)} lines - Newest First)",
                                    displayed_content,
                                    height=600,
                                    key=f"log_display_{run_id}",
                                    help="Tip: Use Ctrl+F to search within the log content"
                                )
                                
                                # Log statistics
                                st.write("**Log Statistics:**")
                                col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                                with col_stat1:
                                    st.metric("Total Lines", len(log_content.split('\n')))
                                with col_stat2:
                                    st.metric("Displayed", len(lines))
                                with col_stat3:
                                    st.metric("File Size", f"{len(log_content):,} bytes")
                                with col_stat4:
                                    error_count = sum(1 for line in log_content.split('\n') if '[ERROR]' in line)
                                    st.metric("Errors", error_count)
                                    
                            except Exception as e:
                                st.error(f"Error reading log file: {e}")
                                st.info(f"Tried to access: {absolute_log_path}")
                                st.info(f"Original database path: {log_file}")
                        else:
                            st.warning("Log file not found")
                            if absolute_log_path:
                                st.info(f"Looked for: {absolute_log_path}")
                            st.info(f"Database stored path: {log_file}")

            else:
                st.info("No catalog runs found matching the selected filters.")

    except Exception as e:
        st.error(f"Error loading catalog runs: {e}")
 