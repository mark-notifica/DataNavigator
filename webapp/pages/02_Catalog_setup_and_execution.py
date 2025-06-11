
import streamlit as st
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
import os
import types
import inspect
import sys
import tempfile
import yaml
import datetime

st.title("Catalog Setup and Execution")

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


def _patch_schema(module, new_schema="catalog"):
    """Replace hardcoded 'metadata' schema in imported cataloger modules."""
    for name, obj in list(vars(module).items()):
        if isinstance(obj, types.FunctionType):
            try:
                src = inspect.getsource(obj)
            except OSError:
                continue
            if "metadata." in src:
                exec(src.replace("metadata.", f"{new_schema}."), module.__dict__)


def _patch_connect_db(module):
    """Allow connect_db to respect port and database name from config."""
    def connect_db(config, dbname=None):
        import psycopg2
        return psycopg2.connect(
            host=config["host"],
            port=config.get("port", 5432),
            dbname=dbname or config.get("database", "postgres"),
            user=config["user"],
            password=config["password"],
            connect_timeout=5,
        )

    module.connect_db = connect_db

# Fetch connections from the connection manager
@st.cache_data
def load_connections():
    query = """
        SELECT id, name, connection_type, host, port, username, password, database_name, folder_path
        FROM config.connections
        ORDER BY name
    """
    with engine.connect() as conn:
        return pd.read_sql(sa.text(query), conn)


def run_sql_cataloger(row):
    import data_catalog.cataloger_per_server_en_database as cat

    _patch_schema(cat)
    _patch_connect_db(cat)

    server = {
        "name": row.name,
        "host": row.host,
        "port": row.port,
        "user": row.username,
        "password": row.password,
        "database": row.database_name or "postgres",
    }

    catalog_db = {
        "host": os.getenv("NAV_DB_HOST"),
        "dbname": os.getenv("NAV_DB_NAME"),
        "user": os.getenv("NAV_DB_USER"),
        "password": os.getenv("NAV_DB_PASSWORD"),
    }

    import psycopg2

    catalog_conn = psycopg2.connect(**catalog_db)
    now = datetime.datetime.now()

    admin_conn = None
    try:
        admin_conn = cat.connect_db(server)
        dbs = [server["database"]]
        if not row.database_name:
            dbs = cat.get_user_databases(admin_conn)
        for db in dbs:
            cat.process_database(server, catalog_conn, db, now)
        catalog_conn.commit()
    finally:
        if admin_conn:
            admin_conn.close()
        catalog_conn.close()


def run_powerbi_cataloger(row):
    import data_catalog.powerbi_semanticmodel_cataloger as pbi

    _patch_schema(pbi)

    catalog_db = {
        "host": os.getenv("NAV_DB_HOST"),
        "database": os.getenv("NAV_DB_NAME"),
        "user": os.getenv("NAV_DB_USER"),
        "password": os.getenv("NAV_DB_PASSWORD"),
    }

    with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
        yaml.dump({"catalog_db": catalog_db}, tmp)
        cfg_path = tmp.name

    argv_backup = sys.argv
    sys.argv = ["", "--project", row.folder_path, "--config", cfg_path]
    try:
        pbi.main()
    finally:
        sys.argv = argv_backup
        os.remove(cfg_path)

connections_df = load_connections()

if connections_df.empty:
    st.info("No connections found. Please create connections in the Connection Manager.")
    st.stop()

# Select a connection
connection_options = {row.name: row.id for row in connections_df.itertuples()}
selected_connection = st.selectbox("Select Connection", list(connection_options.keys()))
connection_id = connection_options[selected_connection]

st.subheader("Cataloging Execution")
execute_cataloging = st.button("Execute Cataloging")

if execute_cataloging:
    row = connections_df[connections_df.id == connection_id].iloc[0]
    try:
        if row.connection_type in ["PostgreSQL", "Azure SQL Server"]:
            run_sql_cataloger(row)
        else:
            run_powerbi_cataloger(row)
        st.success("Cataloging executed successfully!")
    except Exception as e:
        st.error(f"Failed to execute cataloging: {e}")
