import streamlit as st
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
import os
import sys
import pyodbc

# Print Python executable path
print("Python executable:", sys.executable)
st.write(st.__version__)

# Load environment variables for the DataNavigator database
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# Connect to the DataNavigator database using NAV_* variables
db_url = sa.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("NAV_DB_USER"),
    password=os.getenv("NAV_DB_PASSWORD"),
    host=os.getenv("NAV_DB_HOST"),
    port=os.getenv("NAV_DB_PORT"),
    database=os.getenv("NAV_DB_NAME")
)
engine = sa.create_engine(db_url)

st.markdown("""
    <script>
        for (let input of window.parent.document.querySelectorAll("input")) {
            input.setAttribute("autocomplete", "off");
        }
    </script>
""", unsafe_allow_html=True)

st.title("Create connection to a data source")

# Dropdown for connection type
connection_type = st.selectbox(
    "Select Connection Type",
    ["PostgreSQL", "Azure SQL Server", "Power BI Semantic Model"]
)

# Reset fields als reset flag actief is (voordat widgets worden getekend)
if st.session_state.get("reset_fields", False):
    for key in ["connection_name", "host", "port", "username", "password", "database", "folder_path"]:
        st.session_state[key] = ""
    st.session_state["reset_fields"] = False

# Input fields: altijd met key, waarde wordt automatisch uit session_state gehaald
def render_input_fields():
    st.text_input("Connection Name", key="connection_name", autocomplete="off")
    st.text_input("Host", key="host", autocomplete="off")
    st.text_input("Port", key="port", autocomplete="off")
    st.text_input("Username", key="username", autocomplete="off")
    st.text_input("Password", type="password", key="password", autocomplete="off")
    st.text_input("Database Name", key="database", autocomplete="off")
    st.text_input("Folder Path", key="folder_path", placeholder="Enter the folder path for Power BI models", autocomplete="off")

render_input_fields()

# Reset knop om handmatig te testen
if st.button("Reset Fields"):
    st.session_state["reset_fields"] = True
    st.rerun()

# Test Connection
if st.button("Test Connection"):
    try:
        if connection_type == "PostgreSQL":
            driver = "postgresql+psycopg2"
            url = sa.engine.URL.create(
                drivername=driver,
                username=st.session_state["username"],
                password=st.session_state["password"],
                host=st.session_state["host"],
                port=st.session_state["port"],
                database=st.session_state["database"]
            )
        else:  # Azure SQL Server
            driver = "mssql+pyodbc"
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={st.session_state['host']};"
                f"PORT={st.session_state['port']};"
                f"UID={st.session_state['username']};"
                f"PWD={st.session_state['password']};"
                f"DATABASE={st.session_state['database']}"
            )
            url = sa.engine.URL.create(
                drivername=driver,
                query={"odbc_connect": connection_string}
            )

        test_engine = sa.create_engine(url)
        with test_engine.connect():
            st.success(f"Successfully connected to {connection_type}!")
    except pyodbc.Error:
        st.error("ODBC Driver is missing or misconfigured. Please install the required driver.")
    except Exception as e:
        st.error(f"Failed to connect: {e}")

# Save Connection Button
if st.button("Save Connection"):
    try:
        with engine.begin() as db_conn:  # Gebruik begin() voor auto-commit
            db_conn.execute(
                sa.text("""
                    INSERT INTO config.connections (
                        name, connection_type, host, port, username, password, database_name
                    ) VALUES (
                        :name, :connection_type, :host, :port, :username, :password, :database_name
                    )
                """),
                {
                    "name": st.session_state["connection_name"],
                    "connection_type": connection_type,
                    "host": st.session_state["host"],
                    "port": st.session_state["port"],
                    "username": st.session_state["username"],
                    "password": st.session_state["password"],
                    "database_name": st.session_state["database"].strip() or None
                }
            )
        st.success("Connection saved")
        st.session_state["reset_fields"] = True
        st.rerun()
    except Exception as e:
        st.error(f"Save failed: {e}")

# Save Folder Path
if st.button("Save Folder Path"):
    try:
        with engine.begin() as db_conn:
            db_conn.execute(
                sa.text("""
                    INSERT INTO config.connections (name, connection_type, folder_path)
                    VALUES (:name, :connection_type, :folder_path)
                """),
                {
                    "name": st.session_state["connection_name"],
                    "connection_type": connection_type,
                    "folder_path": st.session_state["folder_path"]
                }
            )
        st.success("Folder path saved successfully!")

        st.session_state["folder_path"] = ""
        st.rerun()

    except Exception as e:
        st.error(f"Failed to save folder path: {e}")
