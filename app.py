"""
DataNavigator v1 - Main Streamlit App
Simple data catalog viewer.
"""

import streamlit as st
import pandas as pd
from storage import get_catalog_tables, get_catalog_columns

# Page config
st.set_page_config(
    page_title="DataNavigator v1",
    page_icon="📚",
    layout="wide"
)

# Title
st.title("📚 DataNavigator v1")
st.markdown("Simple data catalog - view tables and columns")

# Sidebar for table selection
st.sidebar.header("Tables")

try:
    # Get all tables from catalog DB
    tables = get_catalog_tables()

    if not tables:
        st.warning("No tables in catalog. Run `python run_db_catalog.py` first.")
        st.stop()

    # Create list of table names for selectbox
    table_options = [f"{t['schema']}.{t['table']}" for t in tables]

    # Select table
    selected = st.sidebar.selectbox("Select a table", table_options)

    # Parse selected table
    schema, table = selected.split('.')

    # Find table description
    table_info = next((t for t in tables if t['schema'] == schema and t['table'] == table), None)

    # Main content
    st.header(f"Table: {selected}")

    # Show table description
    if table_info and table_info['description']:
        st.markdown(f"*{table_info['description']}*")
    else:
        st.caption("No description yet")

    # Get columns from catalog DB
    columns = get_catalog_columns(schema, table)

    # Display columns as dataframe
    df = pd.DataFrame(columns)
    df.columns = ['Column', 'Type', 'Nullable', 'Description']
    st.dataframe(df, use_container_width=True)

    # Show count
    st.info(f"📊 {len(columns)} columns")

except Exception as e:
    st.error(f"Error: {e}")
    st.info("Check database connection or run catalog extraction first")