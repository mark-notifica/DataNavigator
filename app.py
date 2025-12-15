"""
DataNavigator v1 - Main Streamlit App
Simple data catalog viewer with description editing.
"""

import streamlit as st
import pandas as pd
from storage import (
    get_catalog_servers,
    get_catalog_databases,
    get_catalog_tables_for_database,
    get_catalog_columns,
    get_table_node_id,
    get_column_node_id,
    update_node_description
)

# Page config
st.set_page_config(
    page_title="DataNavigator v1",
    page_icon="📚",
    layout="wide"
)

# Title
st.title("📚 DataNavigator v1")
st.markdown("Simple data catalog - view tables and columns")

# Sidebar
st.sidebar.header("Navigation")

try:
    # === SERVER SELECTION ===
    servers = get_catalog_servers()

    if not servers:
        st.warning("No servers in catalog. Run extraction first.")
        st.stop()

    server_options = [s['name'] for s in servers]
    selected_server = st.sidebar.selectbox("Server", server_options)

    # === DATABASE SELECTION ===
    databases = get_catalog_databases(selected_server)

    if not databases:
        st.warning(f"No databases found for {selected_server}")
        st.stop()

    database_options = [d['name'] for d in databases]
    selected_database = st.sidebar.selectbox("Database", database_options)

    st.sidebar.divider()

    # === TABLE SELECTION ===
    tables = get_catalog_tables_for_database(selected_server, selected_database)

    if not tables:
        st.warning("No tables in catalog for this database.")
        st.stop()

    table_options = [f"{t['schema']}.{t['table']}" for t in tables]
    selected = st.sidebar.selectbox("Table", table_options)

    # Parse selected table
    schema, table = selected.split('.')

    # Find table description
    table_info = next(
        (t for t in tables if t['schema'] == schema and t['table'] == table),
        None
    )

    # Main content
    st.header(f"{selected_server} / {selected_database}")
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

    st.info(f"📊 {len(columns)} columns")

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
