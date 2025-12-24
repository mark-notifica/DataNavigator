"""
Run Cataloger - Extract database metadata into the catalog.
"""

import streamlit as st
import subprocess
import sys
import time
from storage import (
    get_catalog_servers, get_catalog_databases,
    get_latest_running_run, get_run_progress
)

st.set_page_config(
    page_title="Run Cataloger - DataNavigator",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 Run Cataloger")
st.markdown("Extract database metadata into the catalog")

st.divider()

# === EXISTING CATALOG ENTRIES ===
st.subheader("Existing Catalog Entries")
st.markdown("Select an existing entry to pre-fill the form, or enter new values below.")

try:
    existing_servers = get_catalog_servers()
    if existing_servers:
        # Build options list
        existing_options = ["-- New entry --"]
        server_db_map = {}

        for srv in existing_servers:
            databases = get_catalog_databases(srv['name'])
            for db in databases:
                label = f"{srv['name']} / {db['name']}"
                if srv['alias']:
                    label += f"  ({srv['alias']})"
                existing_options.append(label)
                server_db_map[label] = {
                    'server_name': srv['name'],
                    'server_alias': srv['alias'] or '',
                    'database': db['name'],
                    'host': srv.get('host', '')
                }

        selected_existing = st.selectbox(
            "Pre-fill from existing",
            existing_options,
            help="Select to re-run cataloger on an existing server/database"
        )

        # Set defaults based on selection
        if selected_existing != "-- New entry --":
            preset = server_db_map[selected_existing]
            default_server = preset['server_name']
            default_alias = preset['server_alias']
            default_database = preset['database']
            default_host = preset['host']
            st.success(f"Selected: {selected_existing} - fill in user/password below")
        else:
            default_server = ""
            default_alias = ""
            default_database = ""
            default_host = "localhost"
    else:
        default_server = ""
        default_alias = ""
        default_database = ""
        default_host = "localhost"
        st.info("No existing catalog entries found. Enter new connection details below.")
except Exception:
    default_server = ""
    default_alias = ""
    default_database = ""
    default_host = "localhost"

st.divider()

# Connection settings
st.subheader("Connection Settings")

col1, col2 = st.columns(2)

with col1:
    server_name = st.text_input("Server Name", value=default_server, help="Logical name for this server (e.g., VPS2)")
    server_alias = st.text_input("Server Alias (optional)", value=default_alias, help="Friendly name (e.g., Production)")
    ip_address = st.text_input("IP Address (optional)", value="", help="Server IP for reference")
    db_type = st.selectbox("Database Type", ["PostgreSQL"], help="Database system type")

with col2:
    host = st.text_input("Host", value=default_host, help="Connection host/IP")
    port = st.text_input("Port", value="5432", help="Connection port")
    database = st.text_input("Database Name", value=default_database, help="Name of database to catalog")
    user = st.text_input("Username", value="", help="Database user")
    password = st.text_input("Password", value="", type="password", help="Database password")

st.divider()

# Build command preview
if server_name and database and host and user:
    cmd_parts = [
        sys.executable, "run_db_catalog.py",
        "--server", server_name,
        "--database", database,
        "--host", host,
        "--port", port,
        "--user", user,
        "--password", "***"  # Hide password in preview
    ]
    if server_alias:
        cmd_parts.extend(["--alias", server_alias])
    if ip_address:
        cmd_parts.extend(["--ip", ip_address])
    cmd_parts.extend(["--dbtype", db_type])

    st.code(" ".join(cmd_parts), language="bash")

# Run button
st.subheader("Execute")

if st.button("🚀 Run Cataloger", type="primary", use_container_width=True):
    # Validate required fields
    if not server_name:
        st.error("Server Name is required")
    elif not database:
        st.error("Database Name is required")
    elif not host:
        st.error("Host is required")
    elif not user:
        st.error("Username is required")
    elif not password:
        st.error("Password is required")
    else:
        # Build actual command
        cmd = [
            sys.executable, "run_db_catalog.py",
            "--server", server_name,
            "--database", database,
            "--host", host,
            "--port", port,
            "--user", user,
            "--password", password,
            "--dbtype", db_type
        ]
        if server_alias:
            cmd.extend(["--alias", server_alias])
        if ip_address:
            cmd.extend(["--ip", ip_address])

        # Run cataloger with progress tracking
        with st.status("Running cataloger...", expanded=True) as status:
            st.write(f"Cataloging {server_name}/{database}")

            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )

                # Wait a moment for run to start
                time.sleep(1)

                # Find the run_id
                running_run = get_latest_running_run()
                run_id = running_run['run_id'] if running_run else None

                output_lines = []
                last_progress_check = 0

                while True:
                    line = process.stdout.readline()
                    if line:
                        output_lines.append(line.rstrip())

                    # Check progress every 5 seconds
                    current_time = time.time()
                    if run_id and current_time - last_progress_check >= 5:
                        progress = get_run_progress(run_id)
                        st.write(f"Progress: {progress['total']} nodes ({progress['created']} new, {progress['updated']} updated)")
                        last_progress_check = current_time

                    if not line and process.poll() is not None:
                        break

                # Final progress
                if run_id:
                    final_progress = get_run_progress(run_id)
                    st.write(f"Final: {final_progress['total']} nodes processed")

                if process.returncode == 0:
                    status.update(label="Cataloger completed!", state="complete", expanded=False)
                    st.success(f"Done! Created: {final_progress['created']}, Updated: {final_progress['updated']}")
                else:
                    status.update(label="Cataloger failed", state="error")
                    st.error(f"Failed with return code {process.returncode}")

                # Show output in expander
                with st.expander("Full Output", expanded=False):
                    st.code('\n'.join(output_lines), language="text")

            except Exception as e:
                status.update(label="Error", state="error")
                st.error(f"Error running cataloger: {e}")
                import traceback
                st.code(traceback.format_exc())

# Help section
st.divider()
with st.expander("Help"):
    st.markdown("""
    ### How to use

    1. **Server Name**: A logical name for the server (used in the catalog hierarchy)
    2. **Server Alias**: Optional friendly name (e.g., "Production", "Development")
    3. **Database Name**: The specific database to catalog
    4. **Host/Port/User/Password**: Connection credentials

    ### What it does

    The cataloger will:
    - Connect to the specified database
    - Extract all schemas, tables, views, and columns
    - Save the metadata to the catalog database
    - Track changes between runs (new, updated, deleted items)

    ### Command line equivalent

    You can also run the cataloger from the command line:
    ```bash
    python run_db_catalog.py --server VPS2 --database mydb --host localhost --port 5432 --user postgres --password secret
    ```
    """)
