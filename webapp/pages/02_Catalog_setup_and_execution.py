
import streamlit as st
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
import os

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

# Fetch connections from the connection manager
@st.cache_data
def load_connections():
    query = """
        SELECT id, name, connection_type, host, port, username, database_name, folder_path
        FROM config.connections
        ORDER BY name
    """
    with engine.connect() as conn:
        return pd.read_sql(sa.text(query), conn)

connections_df = load_connections()

if connections_df.empty:
    st.info("No connections found. Please create connections in the Connection Manager.")
    st.stop()

# Select a connection
connection_options = {row.name: row.id for row in connections_df.itertuples()}
selected_connection = st.selectbox("Select Connection", list(connection_options.keys()))
connection_id = connection_options[selected_connection]

# Plan and execute cataloging
st.subheader("Cataloging Execution")
execute_cataloging = st.button("Execute Cataloging")

if execute_cataloging:
    try:
        # Example logic for cataloging execution
        with engine.connect() as conn:
            conn.execute(sa.text("CALL cataloging_procedure(:connection_id)"), {"connection_id": connection_id})
        st.success("Cataloging executed successfully!")
    except Exception as e:
        st.error(f"Failed to execute cataloging: {e}")