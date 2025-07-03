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
from navigation import go_to_connection_manager
from data_catalog import connection_handler as ch


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
    try:
        connectors = ch.get_all_main_connectors()
        return [
            (c['id'], c['name'], c['connection_type'], c['host'], c['port'], c['username'], c['password'])
            for c in connectors
        ]
    except Exception as e:
        st.error(f"Failed to load connections: {e}")
        return []

# === PROGRESS MONITORING FUNCTIONS ===
def get_catalog_progress(connection_id):
    """Get cataloging progress from database"""
    try:
        conn = ch.get_catalog_connection()
        try:
            with conn.cursor() as cur:
                # Haal database op
                cur.execute("""
                    SELECT d.id, d.database_name, d.date_updated 
                    FROM catalog.catalog_databases d
                    JOIN catalog.catalog_runs cr ON d.catalog_run_id = cr.id
                    WHERE cr.connection_id = %s
                      AND d.date_deleted IS NULL 
                      AND d.is_current = true
                    ORDER BY d.date_created DESC
                    LIMIT 1
                """, (connection_id,))
                database = cur.fetchone()

                if not database:
                    return {"status": "not_started", "progress": 0}

                db_id = database[0]

                # Haal schema count op
                cur.execute("""
                    SELECT COUNT(*) FROM catalog.catalog_schemas 
                    WHERE database_id = %s AND date_deleted IS NULL AND is_current = true
                """, (db_id,))
                schema_count = cur.fetchone()[0] or 0

                # Haal tabel count op
                cur.execute("""
                    SELECT COUNT(*) FROM catalog.catalog_tables t
                    JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                    WHERE s.database_id = %s 
                      AND t.date_deleted IS NULL AND t.is_current = true
                      AND s.date_deleted IS NULL AND s.is_current = true
                """, (db_id,))
                table_count = cur.fetchone()[0] or 0

                # Haal kolom count op
                cur.execute("""
                    SELECT COUNT(*) FROM catalog.catalog_columns c
                    JOIN catalog.catalog_tables t ON c.table_id = t.id
                    JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                    WHERE s.database_id = %s
                      AND c.date_deleted IS NULL AND c.is_current = true
                      AND t.date_deleted IS NULL AND t.is_current = true
                      AND s.date_deleted IS NULL AND s.is_current = true
                """, (db_id,))
                column_count = cur.fetchone()[0] or 0

        finally:
            try:
                conn.close()
            except Exception:
                pass

        # Bereken voortgang
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
    try:
        conn = ch.get_catalog_connection()
        with conn.cursor() as cur:
            cur.execute("""
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
                WHERE cr.connection_id = %s
                AND d.date_deleted IS NULL 
                AND d.is_current = true
                GROUP BY d.database_name
                ORDER BY last_run DESC
            """, (connection_id,))
            databases = cur.fetchall()
            
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

    finally:
        try:
            conn.close()
        except Exception:
            pass


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
        conn = ch.get_catalog_connection()
        with conn.cursor() as cur:
            # Get log filename for this run
            cur.execute("""
                SELECT log_filename FROM catalog.catalog_runs WHERE id = %s
            """, (run_id,))
            log_info = cur.fetchone()
            if not log_info or not log_info[0]:
                return "No log file found for this run"

            log_filename = log_info[0]

        paths = get_project_paths()
        if not Path(log_filename).is_absolute():
            log_path = paths['project_root'] / log_filename
        else:
            log_path = Path(log_filename)

        if log_path.exists():
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                # Return last 20 lines
                return ''.join(lines[-20:])
        else:
            return f"Log file not found: {log_path}"

    except Exception as e:
        return f"Error reading log: {e}"

    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_current_run_info(connection_id):
    """Get current/recent run information for a connection"""
    try:
        conn = ch.get_catalog_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, run_status, run_started_at, run_completed_at, log_filename,
                       databases_processed, schemas_processed, tables_processed, columns_processed
                FROM catalog.catalog_runs 
                WHERE connection_id = %s
                ORDER BY run_started_at DESC 
                LIMIT 1
            """, (connection_id,))
            return cur.fetchone()
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

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

def render_test_status_badge(cfg_row):
    """Show the last test status for the selected config."""
    status = cfg_row[10] if len(cfg_row) > 10 else None
    tested_at = cfg_row[11] if len(cfg_row) > 11 else None
    notes = cfg_row[12] if len(cfg_row) > 12 else None

    if status == 'success':
        st.success(f"✅ Successfully tested on {tested_at.strftime('%Y-%m-%d %H:%M')}" if tested_at else "✅ Last test: success")
    elif status == 'fail':
        st.error(f"❌ Test failed on {tested_at.strftime('%Y-%m-%d %H:%M')}" if tested_at else "❌ Last test: fail")
        if notes:
            st.caption(f"📝 {notes}")
    else:
        st.warning("⚠️ This configuration has not been tested yet.")


def get_filtered_and_sorted_connections(selected_type, sort_key):
    connections = get_connections_for_dropdown()
    if not connections:
        st.warning("No connections found.")
        st.stop()
    if selected_type != "All Types":
        connections = [c for c in connections if c[2] == selected_type]
    sort_map = {
        "ID": lambda x: x[0],
        "Name": lambda x: x[1].lower(),
        "Type": lambda x: x[2],
        "Host": lambda x: x[3]
    }
    connections.sort(key=sort_map[sort_key])
    return connections


st.divider()

tab1, tab2 = st.tabs(["🚀 Execute Cataloging", "📊 Manage Catalog Runs"])

with tab1:
    st.subheader("Select connection to catalog")

    # === FILTER & SORT UI ===
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        selected_connection_type = st.selectbox(
            "Connection Type:",
            ["All Types", "PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"],
            key="single_connection_type_filter"
        )
    with col_filter2:
        single_sort_by = st.selectbox(
            "Sort by:",
            ["ID", "Name", "Type", "Host"],
            index=1,
            key="single_sort_by_filter"
        )

    # === CONNECTION SELECTION ===
    connections = get_filtered_and_sorted_connections(selected_connection_type, single_sort_by)
    connection_labels = [f"{c[1]} ({c[2]}) - {c[3]}:{c[4]} - ID: {c[0]}" for c in connections]
    selected_label = st.selectbox("Select main connection:", connection_labels)
    selected_conn_id = int(selected_label.split("ID: ")[1])
    selected_conn_info = next(c for c in connections if c[0] == selected_conn_id)

    # === FETCH CONFIGURATIONS FOR THIS CONNECTION ===
    conn = ch.get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id,
                    catalog_database_filter,
                    catalog_schema_filter,
                    catalog_table_filter,
                    include_views,
                    include_system_objects,
                    notes,
                    config_name,
                    last_test_status,
                    last_tested_at,
                    last_test_notes
                FROM config.catalog_connection_config
                WHERE connection_id = %s AND is_active = true
                ORDER BY id
            """, (selected_conn_id,))
            configs = cur.fetchall()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    # === NO CONFIGS AVAILABLE ===
    if not configs:
        st.warning("⚠️ No catalog configurations found for this connection.")
        if st.button("➕ Create New Configuration", key="create_new_catalog_config_no_configs"):
            go_to_connection_manager(mode="new", connection_id=selected_conn_info[0])
    else:
    # === CONFIG SELECTION UI ===
        config_labels = [
            f"ID: {cfg[0]} | Name: {cfg[7] or '⚠️ No name'} | DBs: {cfg[1] or 'ALL'} | Views: {'✅' if cfg[4] else '❌'}"
            for cfg in configs
        ]
        selected_cfg_label = st.selectbox("Select catalog configuration:", config_labels)
        selected_cfg_id = int(selected_cfg_label.split("ID: ")[1].split(" ")[0])
        selected_cfg = next(cfg for cfg in configs if cfg[0] == selected_cfg_id)

        # === STATUS BADGE ===
        render_test_status_badge(selected_cfg)

        # === CONFIG DETAILS ===
        with st.expander("📋 Catalog Configuration Details", expanded=False):
            st.write(f"**Connection ID:** {selected_conn_id}")
            st.write(f"**Connection Name:** {selected_conn_info[1]}")
            st.write(f"**Host:** {selected_conn_info[3]}:{selected_conn_info[4]}")
            st.write(f"**Database Filter:** {selected_cfg[1] or '*'}")
            st.write(f"**Schema Filter:** {selected_cfg[2] or '*'}")
            st.write(f"**Table Filter:** {selected_cfg[3] or '*'}")
            st.write(f"**Include Views:** {'✅' if selected_cfg[4] else '❌'}")
            st.write(f"**Include System Objects:** {'✅' if selected_cfg[5] else '❌'}")
            if selected_cfg[6]:
                st.write(f"**Notes:** {selected_cfg[6]}")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("➕ Create New Config", key="create_new_catalog_config"):
                # Navigates in 'new' mode – no config ID needed
                go_to_connection_manager(
                    mode="new",
                    connection_id=selected_conn_info[0]
                )

        with col2:
            if st.button("✏️ Edit Selected Config", key="edit_existing_catalog_config"):
                # Navigates in 'edit' mode – config ID is provided
                go_to_connection_manager(
                    mode="edit",
                    connection_id=selected_conn_info[0],
                    config_id=selected_cfg_id
                )

        # === REMINDER: TESTING IS DONE IN CONNECTION MANAGER ===
        st.divider()

        with st.info("ℹ️ To test the connection and filters, please go to the Connection Manager."):
            st.write(
                "Testing the connection (host, port, credentials) and validating filter settings "
                "is only available in the [Connection Manager]. This ensures test logic is centralized and consistent."
            )

            if st.button("➡️ Open Connection Manager", key="goto_connection_manager"):
                # Reuse same logic for navigation to edit screen
                go_to_connection_manager(
                    mode="edit",
                    connection_id=selected_conn_info[0],
                    config_id=selected_cfg_id
                )

        # === EXECUTION BUTTON ===
        st.divider()
        if st.button("🚀 Execute Cataloging", key="execute_single", type="primary"):
            try:
                with st.spinner("Starting cataloging..."):
                    working_dir = Path(__file__).resolve().parent.parent.parent / "DataNavigator"
                    python_executable = working_dir / "venv" / "Scripts" / "python.exe"

                    cmd = [
                        str(python_executable),
                        "-m", "data_catalog.database_server_cataloger",
                        "--connection-id", str(selected_conn_id),
                        "--catalog-config-id", str(selected_cfg_id)
                    ]

                    process = subprocess.Popen(
                        cmd,
                        cwd=str(working_dir),
                        shell=False,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )

                    # Zet hier direct de session_state aan zodat live monitor start
                    st.session_state["cataloging_active"] = True
                    st.session_state["monitoring_connection_id"] = selected_conn_id
                    st.session_state["monitoring_connection_name"] = selected_conn_info[1]

                    st.success(f"🚀 Started cataloging for {selected_conn_info[1]}")
                    st.rerun()

            except Exception as e:
                st.error(f"Failed to start cataloger: {e}")

        # === LIVE CATALOGING PROGRESS ===
        if st.session_state.get("cataloging_active"):
            cataloging_type = st.session_state.get("cataloging_type", "database")
            connection_id = st.session_state.get("monitoring_connection_id")
            
            # Check if the process is actually still running
            if connection_id:
                try:
                    conn = ch.get_catalog_connection()
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT run_status, log_filename FROM catalog.catalog_runs 
                                WHERE connection_id = %s
                                ORDER BY run_started_at DESC 
                                LIMIT 1
                            """, (connection_id,))
                            latest_run = cur.fetchone()
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
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
                    conn = ch.get_catalog_connection()
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT log_filename, run_status FROM catalog.catalog_runs 
                                WHERE connection_id = %s 
                                ORDER BY run_started_at DESC 
                                LIMIT 1
                            """, (connection_id,))
                            run_data = cur.fetchone()
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
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
                    st.code(log_content, language=None, line_numbers=True)
                    

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
                conn = ch.get_catalog_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT id, run_status, run_started_at, run_completed_at, log_filename,
                                databases_processed, schemas_processed, tables_processed, columns_processed, error_message
                            FROM catalog.catalog_runs 
                            WHERE connection_id = %s
                            ORDER BY run_started_at DESC 
                            LIMIT 1
                        """, (connection_id,))
                        latest_run = cur.fetchone()
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
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
            conn = ch.get_catalog_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT c.id, c.name 
                        FROM config.connections c
                        JOIN catalog.catalog_runs cr ON c.id = cr.connection_id
                        ORDER BY c.name
                    """)
                    rows = cur.fetchall()
                    # Let op: cur.fetchall() levert lijst van tuples (id, name)
                    # De oorspronkelijke code maakte een lijst van (name, id), dus hier terugdraaien naar (name, id)
                    connections = [("All Connections", 0)] + [(row[1], row[0]) for row in rows]
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
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
        conn = ch.get_catalog_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
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
                    LIMIT %s
                """, (params["limit"],))
                runs = cur.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass
            
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
 