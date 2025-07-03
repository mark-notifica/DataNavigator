import streamlit as st
from pathlib import Path
import sqlalchemy as sa
import pyodbc
import fnmatch
from typing import Optional
from datetime import datetime
from pytz import UTC



def get_main_connection_test_status(engine, connection_id: int) -> dict:
    with engine.connect() as conn:
        result = conn.execute(sa.text("""
            SELECT 
                last_test_status,
                last_tested_at,
                last_test_notes
            FROM config.connections
            WHERE id = :id
        """), {"id": connection_id}).fetchone()

    if result:
        return {
            "status": result.last_test_status,
            "tested_at": result.last_tested_at,
            "notes": result.last_test_notes
        }
    else:
        return {
            "status": None,
            "tested_at": None,
            "notes": None
        }

def get_catalog_config_test_status(engine, config_id: int) -> dict:
    with engine.connect() as conn:
        result = conn.execute(sa.text("""
            SELECT 
                last_test_status,
                last_tested_at,
                last_test_notes
            FROM config.catalog_connection_config
            WHERE id = :id
        """), {"id": config_id}).fetchone()

    if result:
        return {
            "status": result.last_test_status,
            "tested_at": result.last_tested_at,
            "notes": result.last_test_notes
        }
    else:
        return {
            "status": None,
            "tested_at": None,
            "notes": None
        }

def get_ai_config_test_status(engine, config_id: int) -> dict:
    with engine.connect() as conn:
        result = conn.execute(sa.text("""
            SELECT 
                last_test_status,
                last_tested_at,
                last_test_notes
            FROM config.ai_analyzer_connection_config
            WHERE id = :id
        """), {"id": config_id}).fetchone()

    if result:
        return {
            "status": result.last_test_status,
            "tested_at": result.last_tested_at,
            "notes": result.last_test_notes
        }
    else:
        return {
            "status": None,
            "tested_at": None,
            "notes": None
        }


def test_main_connection(connection_info: dict, engine) -> list[str]:
    """
    Test a main database connection.
    Updates the test result into config.connections.
    """

    connection_id = connection_info.get("id")
    results = test_connection(connection_info)
    status = "success" if any(r.startswith("✅") for r in results) else "failed"
    notes = "\n".join(results)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE config.connections
            SET last_test_status = :status,
                last_tested_at = :tested_at,
                last_test_notes = :notes
            WHERE id = :id
        """), {
            "status": status,
            "tested_at": datetime.now(UTC),
            "notes": notes,
            "id": connection_id
        })

    return results


def test_catalog_config(connection_info: dict, config_info: dict, engine) -> list[str]:
    """
    Test a catalog configuration in the context of its connection.
    Updates the test result into config.catalog_connection_config.
    """

    # Database filter uit config_info lezen (comma separated string)
    db_filter = config_info.get("catalog_database_filter", "")
    databases = None
    if db_filter.strip():
        databases = [db.strip() for db in db_filter.split(",") if db.strip()]

    results = test_connection(connection_info, databases=databases)

    status = "success" if any(r.startswith("✅") for r in results) else "failed"
    notes = "\n".join(results)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE config.catalog_connection_config
            SET last_test_status = :status,
                last_tested_at = :tested_at,
                last_test_notes = :notes
            WHERE id = :id
        """), {
            "status": status,
            "tested_at": datetime.now(UTC),
            "notes": notes,
            "id": config_info["id"]
        })

    return results


def test_ai_config(connection_info: dict, ai_config_info: dict, engine) -> list[str]:
    """
    Test an AI configuration in the context of its connection.
    Updates the test result into config.ai_analyzer_connection_config.
    """

    # Haal databasefilter op uit ai_config_info (verwacht 1 database naam)
    databases = None
    db_filter = ai_config_info.get("ai_database_filter")
    if db_filter and db_filter.strip():
        databases = [db_filter.strip()]

    results = test_connection(connection_info, databases=databases)

    status = "success" if any(r.startswith("✅") for r in results) else "failed"
    notes = "\n".join(results)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE config.ai_analyzer_connection_config
            SET last_test_status = :status,
                last_tested_at = :tested_at,
                last_test_notes = :notes
            WHERE id = :id
        """), {
            "status": status,
            "tested_at": datetime.now(UTC),
            "notes": notes,
            "id": ai_config_info["id"]
        })

    return results


def get_connection_info_by_id(engine, connection_id):
    with engine.connect() as conn:
        result = conn.execute(sa.text("""
            SELECT connection_type, host, port, username, password,folder_path, is_active
            FROM config.connections
            WHERE id = :id
        """), {"id": connection_id}).fetchone()

    if result:
        return dict(result._mapping)
    return None

def apply_compact_styling():
    """Apply compact styling to make UI elements smaller and more refined"""
    css_file = Path(__file__).parent / "styles" / "custom.css"
    
    #     # Debug information
    # st.write(f"**CSS Debug:**")
    # st.write(f"- shared_utils.py location: {Path(__file__)}")
    # st.write(f"- Looking for CSS at: {css_file}")
    # st.write(f"- CSS file exists: {css_file.exists()}")

    if css_file.exists():
        try:
            with open(css_file) as f:
                css_content = f.read()
            # st.write(f"- CSS file size: {len(css_content)} characters")
            st.markdown(f'<style>{css_content}</style>', unsafe_allow_html=True)
            # st.success("✅ External CSS file loaded successfully")
        except Exception as e:
            st.error(f"❌ Error reading CSS file: {e}")
    else:
        # Fallback compact styling
        st.markdown("""
        <style>
            /* Main container */
            .main .block-container {
                padding-top: 1rem !important;
                padding-bottom: 1rem !important;
                padding-left: 1rem !important;
                padding-right: 1rem !important;
                max-width: 95% !important;
            }
            
            /* Headers */
            h1 { font-size: 1.6rem !important; margin-bottom: 0.6rem !important; }
            h2 { font-size: 1.3rem !important; margin-bottom: 0.5rem !important; }
            h3 { font-size: 1.1rem !important; margin-bottom: 0.4rem !important; }
            
            /* Compact elements */
            .element-container { margin-bottom: 0.3rem !important; }
            
            /* Buttons */
            .stButton > button {
                padding: 0.25rem 0.6rem !important;
                font-size: 0.85rem !important;
                height: 2rem !important;
                border-radius: 0.25rem !important;
            }
            
            /* Form elements */
            .stSelectbox > div > div { min-height: 2rem !important; }
            .stTextInput > div > div > input { height: 2rem !important; }
            .stNumberInput > div > div > input { height: 2rem !important; }
            .stTextArea textarea { min-height: 100px !important; font-size: 0.85rem !important; }
            
            /* Metrics */
            [data-testid="metric-container"] {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                padding: 0.3rem 0.5rem;
                border-radius: 0.25rem;
                margin: 0.1rem 0;
            }
            [data-testid="metric-container"] label { font-size: 0.8rem !important; }
            [data-testid="metric-container"] [data-testid="metric-value"] { font-size: 1.1rem !important; }
            
            /* Expanders */
            .streamlit-expanderHeader {
                font-size: 0.9rem !important;
                padding: 0.25rem 0.5rem !important;
                background-color: #f8f9fa !important;
            }
            
            /* Alerts */
            .stAlert { padding: 0.4rem 0.6rem !important; margin: 0.2rem 0 !important; }
            
            /* Tables */
            .dataframe { font-size: 0.8rem !important; }
            
            /* Sidebar */
            .css-1d391kg { padding-top: 1rem !important; }
        </style>
        """, unsafe_allow_html=True)

def test_connection(connection_info: dict, databases=None) -> list[str]:
    """
    Test connection to server and optionally filtered databases.

    Args:
        connection_info (dict): expects connection_type, host, port, username, password
        databases (str | list[str] | None): comma-separated string, list, or None

    Returns:
        list[str]: result strings with ✅ or ❌
    """

    # Normalize input
    if isinstance(databases, str):
        databases = [d.strip() for d in databases.split(",") if d.strip()]
    elif databases is None:
        # Probeer catalog filter uit config te halen
        filter_str = connection_info.get("catalog_database_filter")
        if filter_str:
            databases = [d.strip() for d in filter_str.split(",") if d.strip()]
        else:
            databases = []        

    results = []
    connection_type = connection_info["connection_type"]
    host = connection_info["host"]
    port = connection_info["port"]
    username = connection_info["username"]
    password = connection_info["password"]
    include_views = connection_info.get("include_views", False)
    include_system_objects = connection_info.get("include_system_objects", False)

    try:
        if connection_type == "PostgreSQL":
            # Server test
            base_url = sa.engine.URL.create(
                drivername="postgresql+psycopg2",
                username=username,
                password=password,
                host=host,
                port=port,
                database="postgres"
            )
            sa.create_engine(base_url).connect().close()
            results.append("✅ Server-level connection successful (PostgreSQL)")

            if databases:
                # Haal alle databases op indien filter
                if len(databases) == 1 and not any(char in databases[0] for char in ["*", "%", "_"]):
                    # Één concrete databasenaam
                    test_dbs = databases
                else:
                    # Fetch list of dbs and filter
                    with sa.create_engine(base_url).connect() as conn:
                        rows = conn.execute(sa.text("SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn")).fetchall()
                    db_names = [r[0] for r in rows]
                    patterns = [d.lower().replace("*", "%") for d in databases]
                    test_dbs = [db for db in db_names if any(fnmatch.fnmatchcase(db.lower(), p.replace("%", "*")) for p in patterns)]

                for db in test_dbs:
                    try:
                        db_url = sa.engine.URL.create(
                            drivername="postgresql+psycopg2",
                            username=username,
                            password=password,
                            host=host,
                            port=port,
                            database=db
                        )
                        engine = sa.create_engine(db_url)
                        with engine.connect() as conn:
                            table_count = conn.execute(sa.text("""
                                SELECT COUNT(*) FROM information_schema.tables 
                                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') 
                                AND table_type = 'BASE TABLE'
                            """)).scalar() or 0

                            view_count = 0
                            if include_views:
                                view_count = conn.execute(sa.text("""
                                    SELECT COUNT(*) FROM information_schema.tables 
                                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') 
                                    AND table_type = 'VIEW'
                                """)).scalar() or 0

                        results.append(f"✅ Database '{db}': OK (Tables: {table_count}, Views: {view_count})")
                    except Exception as e:
                        results.append(f"❌ Database '{db}': {e}")

        elif connection_type == "Azure SQL Server":
            driver = "ODBC Driver 18 for SQL Server"
            base_conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};UID={username};PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"
            )
            pyodbc.connect(base_conn_str).close()
            results.append("✅ Server-level connection successful (Azure SQL Server)")

            if databases:
                if len(databases) == 1 and not any(char in databases[0] for char in ["*", "%", "_"]):
                    test_dbs = databases
                else:
                    with pyodbc.connect(base_conn_str, timeout=5) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")  # exclude system DBs
                        db_names = [row[0] for row in cursor.fetchall()]
                    patterns = [d.lower().replace("*", "%") for d in databases]
                    test_dbs = [db for db in db_names if any(fnmatch.fnmatchcase(db.lower(), p.replace("%", "*")) for p in patterns)]

                for db in test_dbs:
                    try:
                        db_conn_str = (
                            f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={db};"
                            f"UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"
                        )
                        with pyodbc.connect(db_conn_str, timeout=5) as conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
                            """)
                            table_count = cursor.fetchone()[0]

                            view_count = 0
                            if include_views:
                                cursor.execute("""
                                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS
                                """)
                                view_count = cursor.fetchone()[0]

                        results.append(f"✅ Database '{db}': OK (Tables: {table_count}, Views: {view_count})")
                    except Exception as e:
                        results.append(f"❌ Database '{db}': {e}")
        else:
            results.append(f"❌ Unsupported connection type: {connection_type}")
    except Exception as e:
        results.append(f"❌ Server-level connection failed: {e}")

    return results
