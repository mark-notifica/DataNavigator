"""
Run Cataloger - Extract database metadata into the catalog.
"""

import streamlit as st
import subprocess
import sys

st.set_page_config(
    page_title="Run Cataloger - DataNavigator",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 Run Cataloger")
st.markdown("Extract database metadata into the catalog")

st.divider()

# Connection settings
st.subheader("Connection Settings")

col1, col2 = st.columns(2)

with col1:
    server_name = st.text_input("Server Name", value="", help="Logical name for this server (e.g., VPS2)")
    server_alias = st.text_input("Server Alias (optional)", value="", help="Friendly name (e.g., Production)")
    ip_address = st.text_input("IP Address (optional)", value="", help="Server IP for reference")
    db_type = st.selectbox("Database Type", ["PostgreSQL"], help="Database system type")

with col2:
    host = st.text_input("Host", value="localhost", help="Connection host/IP")
    port = st.text_input("Port", value="5432", help="Connection port")
    database = st.text_input("Database Name", value="", help="Name of database to catalog")
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

        # Run with output
        with st.spinner("Running cataloger..."):
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )

                if result.returncode == 0:
                    st.success("Cataloger completed successfully!")
                    st.subheader("Output")
                    st.code(result.stdout, language="text")
                else:
                    st.error("Cataloger failed")
                    if result.stdout:
                        st.subheader("Output")
                        st.code(result.stdout, language="text")
                    if result.stderr:
                        st.subheader("Errors")
                        st.code(result.stderr, language="text")

            except subprocess.TimeoutExpired:
                st.error("Cataloger timed out after 5 minutes")
            except Exception as e:
                st.error(f"Error running cataloger: {e}")

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
