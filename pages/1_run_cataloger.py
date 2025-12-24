"""
Run Cataloger - Extract database metadata into the catalog.
"""

import streamlit as st
import subprocess
import sys
import time
from storage import (
    get_catalog_servers, get_catalog_databases,
    get_latest_running_run, get_run_progress,
    get_all_runs, mark_run_failed
)

st.set_page_config(
    page_title="Run Cataloger - DataNavigator",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 Run Cataloger")
st.markdown("Extract database metadata into the catalog")

# === RUNNING JOBS MONITOR ===
running_run = get_latest_running_run()
if running_run:
    from datetime import datetime
    started = running_run['started_at']
    duration = datetime.now(started.tzinfo) - started if started else None
    duration_str = str(duration).split('.')[0] if duration else "unknown"

    with st.container():
        st.warning(f"**Cataloger running:** {running_run['source_label']} (started {duration_str} ago)")
        progress = get_run_progress(running_run['run_id'])
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Nodes", progress['total'])
        with col2:
            st.metric("New", progress['created'])
        with col3:
            st.metric("Updated", progress['updated'])
        with col4:
            if st.button("🔄 Refresh"):
                st.rerun()

        # Option to cancel/cleanup stale run
        with st.expander("Cleanup stale run"):
            st.caption(f"Run ID: {running_run['run_id']}")
            st.caption("If this run is stuck (no progress for a while), you can mark it as failed.")
            if st.button("Mark as failed", type="secondary"):
                from storage import get_catalog_connection
                conn = get_catalog_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE catalog.catalog_runs
                    SET status = 'failed', completed_at = NOW()
                    WHERE id = %s AND status = 'running'
                """, (running_run['run_id'],))
                conn.commit()
                cursor.close()
                conn.close()
                st.success("Run marked as failed")
                st.rerun()

st.divider()

# === MAIN TABS ===
tab_run, tab_history = st.tabs(["▶️ Run Cataloger", "📋 Run History"])

with tab_history:
    st.subheader("Catalog Run History")
    st.markdown("View all catalog runs and clean up stuck or stale runs.")

    runs = get_all_runs(limit=100)

    if not runs:
        st.info("No catalog runs found yet.")
    else:
        # Summary
        running_count = sum(1 for r in runs if r['status'] == 'running')
        completed_count = sum(1 for r in runs if r['status'] == 'completed')
        failed_count = sum(1 for r in runs if r['status'] == 'failed')

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Running", running_count)
        with col2:
            st.metric("Completed", completed_count)
        with col3:
            st.metric("Failed", failed_count)

        st.divider()

        # Bulk cleanup for stuck runs
        stuck_runs = [r for r in runs if r['status'] == 'running']
        if stuck_runs:
            with st.expander(f"⚠️ {len(stuck_runs)} runs still marked as 'running'", expanded=True):
                st.warning("These runs may be stuck if they've been running for a long time without progress.")

                from datetime import datetime

                for run in stuck_runs:
                    started = run['started_at']
                    if started:
                        duration = datetime.now(started.tzinfo) - started
                        duration_str = str(duration).split('.')[0]
                    else:
                        duration_str = "unknown"

                    col1, col2, col3 = st.columns([3, 2, 1])
                    with col1:
                        st.text(f"Run #{run['run_id']}: {run['source_label']}")
                    with col2:
                        st.text(f"Duration: {duration_str}")
                    with col3:
                        if st.button("Mark Failed", key=f"fail_{run['run_id']}"):
                            if mark_run_failed(run['run_id']):
                                st.success("Marked as failed")
                                st.rerun()

                st.divider()

                # Bulk action
                if st.button("Mark ALL as Failed", type="secondary"):
                    count = 0
                    for run in stuck_runs:
                        if mark_run_failed(run['run_id']):
                            count += 1
                    st.success(f"Marked {count} runs as failed")
                    st.rerun()

        # Full history table
        st.subheader("All Runs")

        import pandas as pd
        from datetime import datetime

        table_data = []
        for run in runs:
            started = run['started_at']
            completed = run['completed_at']

            if started and completed:
                duration = completed - started
                duration_str = str(duration).split('.')[0]
            elif started:
                duration = datetime.now(started.tzinfo) - started
                duration_str = f"{str(duration).split('.')[0]} (ongoing)"
            else:
                duration_str = "-"

            status_icon = {
                'running': '🔄',
                'completed': '✅',
                'failed': '❌'
            }.get(run['status'], '❓')

            table_data.append({
                'ID': run['run_id'],
                'Status': f"{status_icon} {run['status']}",
                'Source': run['source_label'],
                'Started': started.strftime('%Y-%m-%d %H:%M') if started else '-',
                'Duration': duration_str,
                'Created': run['created'],
                'Updated': run['updated']
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_run:
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
            progress_container = st.empty()
            progress_container.info(f"Starting cataloger for {server_name}/{database}...")

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

                    # Check progress every 2 seconds
                    current_time = time.time()
                    if run_id and current_time - last_progress_check >= 2:
                        progress = get_run_progress(run_id)
                        progress_container.info(
                            f"**Running:** {progress['total']} nodes processed "
                            f"({progress['created']} new, {progress['updated']} updated)"
                        )
                        last_progress_check = current_time

                    if not line and process.poll() is not None:
                        break

                # Final result
                if run_id:
                    final_progress = get_run_progress(run_id)
                else:
                    final_progress = {'created': 0, 'updated': 0, 'total': 0}

                if process.returncode == 0:
                    progress_container.success(
                        f"**Completed!** {final_progress['total']} nodes processed "
                        f"({final_progress['created']} new, {final_progress['updated']} updated)"
                    )
                else:
                    progress_container.error(f"Failed with return code {process.returncode}")

                # Show output in expander
                with st.expander("Full Output", expanded=process.returncode != 0):
                    st.code('\n'.join(output_lines), language="text")

            except Exception as e:
                progress_container.error(f"Error running cataloger: {e}")
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
