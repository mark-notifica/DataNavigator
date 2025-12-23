"""
Catalog Browser - View and edit catalog descriptions.
"""

import streamlit as st
import pandas as pd
from storage import (
    get_catalog_servers,
    get_catalog_databases,
    get_catalog_schemas,
    get_catalog_tables_for_database,
    get_catalog_columns,
    get_table_node_id,
    get_column_node_id,
    update_node_description
)

st.set_page_config(
    page_title="Catalog - DataNavigator",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Catalog")
st.markdown("Browse and edit catalog descriptions")

# Sidebar
st.sidebar.header("Navigation")

try:
    # === SERVER SELECTION ===
    servers = get_catalog_servers()

    if not servers:
        st.warning("No servers in catalog. Run extraction first.")
        st.stop()

    # Format: "VPS2 (Local Dev)" of alleen "VPS2" als geen alias
    server_options = {
        s['name'] if not s['alias'] else f"{s['name']} ({s['alias']})": s['name']
        for s in servers
    }
    selected_server_display = st.sidebar.selectbox("Server", list(server_options.keys()))
    selected_server = server_options[selected_server_display]

    # === DATABASE SELECTION ===
    databases = get_catalog_databases(selected_server)

    if not databases:
        st.warning(f"No databases found for {selected_server}")
        st.stop()

    # Get table count per database
    database_options = {}
    for db in databases:
        tables = get_catalog_tables_for_database(selected_server, db['name'])
        count = len(tables)
        label = f"{db['name']} ({count} tables)"
        database_options[label] = db['name']

    selected_db_display = st.sidebar.selectbox("Database", list(database_options.keys()))
    selected_database = database_options[selected_db_display]

    st.sidebar.divider()

    # === TABLE SELECTION ===
    tables = get_catalog_tables_for_database(selected_server, selected_database)

    if not tables:
        st.warning("No tables in catalog for this database.")
        st.stop()

    # Schema filter
    all_schemas = sorted(list(set(t['schema'] for t in tables)))
    schema_options = ["All schemas"] + all_schemas
    selected_schema = st.sidebar.selectbox("Schema", schema_options)

    if selected_schema != "All schemas":
        tables = [t for t in tables if t['schema'] == selected_schema]

    # Search filter
    search = st.sidebar.text_input("🔍 Filter tables", "")

    if search:
        tables = [t for t in tables if search.lower() in t['table'].lower()
                  or search.lower() in t['schema'].lower()]

    if not tables:
        st.sidebar.warning("No tables match filter")
        st.stop()

    table_options = [f"{t['schema']}.{t['table']}" for t in tables]
    selected = st.sidebar.selectbox("Table", table_options)

    # Parse selected table
    schema, table = selected.split('.')

    # Find table info
    table_info = next(
        (t for t in tables if t['schema'] == schema and t['table'] == table),
        None
    )

    # Main content
    st.header(f"{selected_server_display} / {selected_database}")

    # === SERVER DESCRIPTION EDITING ===
    server_info = next((s for s in servers if s['name'] == selected_server), None)
    server_node_id = server_info['node_id'] if server_info else None
    current_server_desc = server_info['description'] if server_info else ''

    with st.expander("Server Description"):
        new_server_desc = st.text_area(
            "Description",
            value=current_server_desc,
            key=f"server_desc_{selected_server}",
            height=68
        )
        if st.button("Save Server Description"):
            if server_node_id:
                update_node_description(server_node_id, new_server_desc)
                st.success("Saved!")
                st.rerun()

    # === DATABASE DESCRIPTION EDITING ===
    db_info = next((d for d in databases if d['name'] == selected_database), None)
    db_node_id = db_info['node_id'] if db_info else None
    current_db_desc = db_info['description'] if db_info else ''

    with st.expander("Database Description"):
        new_db_desc = st.text_area(
            "Description",
            value=current_db_desc,
            key=f"db_desc_{selected_database}",
            height=68
        )
        if st.button("Save Database Description"):
            if db_node_id:
                update_node_description(db_node_id, new_db_desc)
                st.success("Saved!")
                st.rerun()

    # === SCHEMA DESCRIPTION EDITING ===
    # Only show if a specific schema is selected
    if selected_schema != "All schemas":
        schemas = get_catalog_schemas(selected_server, selected_database)
        schema_info = next((s for s in schemas if s['name'] == selected_schema), None)
        schema_node_id = schema_info['node_id'] if schema_info else None
        current_schema_desc = schema_info['description'] if schema_info else ''

        with st.expander(f"Schema Description: {selected_schema}"):
            new_schema_desc = st.text_area(
                "Description",
                value=current_schema_desc,
                key=f"schema_desc_{selected_schema}",
                height=68
            )
            if st.button("Save Schema Description"):
                if schema_node_id:
                    update_node_description(schema_node_id, new_schema_desc)
                    st.success("Saved!")
                    st.rerun()

    st.subheader(f"Table: {selected}")

    # === TABLE DESCRIPTION EDITING ===
    table_node_id = get_table_node_id(schema, table)
    current_table_desc = table_info['description'] if table_info else ''

    new_table_desc = st.text_area(
        "Table Description",
        value=current_table_desc,
        key=f"table_desc_{schema}_{table}",
        height=80
    )

    if st.button("Save Table Description"):
        if table_node_id:
            update_node_description(table_node_id, new_table_desc)
            st.success("Saved!")
            st.rerun()
        else:
            st.error("Table not found in catalog")

    st.divider()

    # === COLUMNS SECTION ===
    st.subheader("Columns")

    columns = get_catalog_columns(schema, table)

    df = pd.DataFrame(columns)
    df.columns = ['Column', 'Type', 'Nullable', 'Description']
    st.dataframe(df, use_container_width=True)

    st.info(f"{len(columns)} columns")

    # === COLUMN DESCRIPTION EDITING ===
    st.divider()
    st.subheader("Edit Column Descriptions")

    column_names = [c['column'] for c in columns]
    selected_column = st.selectbox("Select column to edit", column_names)

    col_info = next(
        (c for c in columns if c['column'] == selected_column),
        None
    )
    current_col_desc = col_info['description'] if col_info else ''

    new_col_desc = st.text_area(
        f"Description for `{selected_column}`",
        value=current_col_desc,
        key=f"col_desc_{schema}_{table}_{selected_column}",
        height=80
    )

    if st.button("Save Column Description"):
        col_node_id = get_column_node_id(schema, table, selected_column)
        if col_node_id:
            update_node_description(col_node_id, new_col_desc)
            st.success("Saved!")
            st.rerun()
        else:
            st.error("Column not found in catalog")

except Exception as e:
    st.error(f"Error: {e}")
    st.info("Check database connection or run catalog extraction first")
