"""
DataNavigator v1 - Main Streamlit App
Simple data catalog viewer.
"""

import streamlit as st
import pandas as pd
from extractor import get_all_tables, get_columns

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
    # Get all tables
    tables = get_all_tables()

    if not tables:
        st.warning("No tables found in database")
        st.stop()

    # Create list of table names for selectbox
    table_names = [f"{t['schema']}.{t['table']}" for t in tables]

    # Select table
    selected_table = st.sidebar.selectbox(
        "Select a table",
        table_names
    )

    # Parse selected table
    schema, table = selected_table.split('.')

    # Main content
    st.header(f"Table: {selected_table}")

    # Get columns
    columns = get_columns(table, schema)

    # Display columns as dataframe
    df = pd.DataFrame(columns)
    st.dataframe(df, use_container_width=True)

    # Show count
    st.info(f"📊 {len(columns)} columns")

except Exception as e:
    st.error(f"Error: {e}")
    st.info("Make sure your .env file has correct database credentials")