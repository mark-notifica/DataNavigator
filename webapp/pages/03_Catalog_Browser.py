import streamlit as st
import pandas as pd
import sqlalchemy as sa
import yaml
from pathlib import Path
import os
import re

st.title("Catalog Browser")

@st.cache_resource
def get_engine():
    config_path = Path(__file__).resolve().parents[2] / 'data_catalog' / 'db_config.yaml'
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    env_pattern = re.compile(r'^\${(.+)}$')
    for key, val in list(cfg.items()):
        if isinstance(val, str):
            m = env_pattern.match(val)
            if m:
                cfg[key] = os.getenv(m.group(1), val)
    url = sa.engine.URL.create(
        'postgresql+psycopg2',
        username=cfg.get('user'),
        password=cfg.get('password'),
        host=cfg.get('host', 'localhost'),
        port=cfg.get('port', 5432),
        database=cfg.get('database')
    )
    return sa.create_engine(url)

engine = get_engine()

@st.cache_data
def load_databases():
    q = """SELECT id, server_name, database_name FROM metadata.catalog_databases WHERE curr_id='Y' ORDER BY server_name, database_name"""
    with engine.connect() as conn:
        return pd.read_sql(sa.text(q), conn)

try:
    db_df = load_databases()
except Exception as e:
    st.error(f"Fout bij ophalen van databases: {e}")
    st.stop()

search_db = st.text_input("Zoek database")
filtered_db = db_df[db_df.database_name.str.contains(search_db, case=False, na=False)]

if filtered_db.empty:
    st.info("Geen databases gevonden")
    st.stop()

db_options = {f"{row.server_name}.{row.database_name}": row.id for row in filtered_db.itertuples()}
selected_db = st.selectbox("Database", list(db_options.keys()))
db_id = db_options[selected_db]

@st.cache_data
def load_schemas(db_id):
    q = """SELECT id, schema_name FROM metadata.catalog_schemas WHERE database_id=:db_id AND curr_id='Y' ORDER BY schema_name"""
    with engine.connect() as conn:
        return pd.read_sql(sa.text(q), conn, params={'db_id': db_id})

schema_df = load_schemas(db_id)
search_schema = st.text_input("Zoek schema")
schema_filtered = schema_df[schema_df.schema_name.str.contains(search_schema, case=False, na=False)]

if schema_filtered.empty:
    st.info("Geen schemas gevonden")
    st.stop()

schema_options = {row.schema_name: row.id for row in schema_filtered.itertuples()}
selected_schema = st.selectbox("Schema", list(schema_options.keys()))
schema_id = schema_options[selected_schema]

@st.cache_data
def load_tables(schema_id):
    q = """SELECT id, table_name, table_type FROM metadata.catalog_tables WHERE schema_id=:schema_id AND curr_id='Y' ORDER BY table_name"""
    with engine.connect() as conn:
        return pd.read_sql(sa.text(q), conn, params={'schema_id': schema_id})

table_df = load_tables(schema_id)
search_table = st.text_input("Zoek tabel")
table_filtered = table_df[table_df.table_name.str.contains(search_table, case=False, na=False)]

if table_filtered.empty:
    st.info("Geen tabellen gevonden")
    st.stop()

table_options = {row.table_name: row.id for row in table_filtered.itertuples()}
selected_table = st.selectbox("Tabel", list(table_options.keys()))
table_id = table_options[selected_table]

@st.cache_data
def load_columns(table_id):
    q = """SELECT column_name, data_type, is_nullable, column_default, ordinal_position FROM metadata.catalog_columns WHERE table_id=:table_id AND curr_id='Y' ORDER BY ordinal_position"""
    with engine.connect() as conn:
        return pd.read_sql(sa.text(q), conn, params={'table_id': table_id})

columns_df = load_columns(table_id)
search_column = st.text_input("Zoek kolom")
columns_filtered = columns_df[columns_df.column_name.str.contains(search_column, case=False, na=False)]

st.dataframe(columns_filtered)
