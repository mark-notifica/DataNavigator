import streamlit as st
from pathlib import Path
import sqlalchemy as sa
import pyodbc

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

def test_connection(connection_info, databases=None):
    """
    Test the connection to the server or specific databases.
    
    Args:
        connection_info (dict): Connection details (host, port, username, password, etc.).
        databases (list): List of databases to test. If None, test the server connection.
    
    Returns:
        list: Results of the test for each database or server.
    """
    results = []
    try:
        if connection_info['connection_type'] == "PostgreSQL":
            driver = "postgresql+psycopg2"
            if databases:
                # Test each database
                for db_name in databases:
                    try:
                        url = sa.engine.URL.create(
                            drivername=driver,
                            username=connection_info['username'],
                            password=connection_info['password'],
                            host=connection_info['host'],
                            port=connection_info['port'],
                            database=db_name
                        )
                        test_engine = sa.create_engine(url)
                        with test_engine.connect() as test_conn:
                            test_conn.execute(sa.text("SELECT 1"))
                        results.append(f"✅ {db_name}: Success")
                        test_engine.dispose()
                    except Exception as db_error:
                        results.append(f"❌ {db_name}: {str(db_error)}")
            else:
                # Test server connection
                url = sa.engine.URL.create(
                    drivername=driver,
                    username=connection_info['username'],
                    password=connection_info['password'],
                    host=connection_info['host'],
                    port=connection_info['port'],
                    database="postgres"  # Default system database
                )
                test_engine = sa.create_engine(url)
                with test_engine.connect() as test_conn:
                    test_conn.execute(sa.text("SELECT 1"))
                results.append("✅ Server connection: Success")
                test_engine.dispose()
        
        elif connection_info['connection_type'] == "Azure SQL Server":
            driver = "mssql+pyodbc"
            if databases:
                # Test each database
                for db_name in databases:
                    try:
                        connection_string = (
                            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                            f"SERVER={connection_info['host']},{connection_info['port']};"
                            f"DATABASE={db_name};"
                            f"UID={connection_info['username']};"
                            f"PWD={connection_info['password']};"
                            f"Encrypt=yes;"
                            f"TrustServerCertificate=no;"
                            f"Connection Timeout=30;"
                        )
                        test_conn = pyodbc.connect(connection_string)
                        test_conn.close()
                        results.append(f"✅ {db_name}: Success")
                    except Exception as db_error:
                        results.append(f"❌ {db_name}: {str(db_error)}")
            else:
                # Test server connection
                connection_string = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={connection_info['host']},{connection_info['port']};"
                    f"UID={connection_info['username']};"
                    f"PWD={connection_info['password']};"
                    f"Encrypt=yes;"
                    f"TrustServerCertificate=no;"
                    f"Connection Timeout=30;"
                )
                test_conn = pyodbc.connect(connection_string)
                test_conn.close()
                results.append("✅ Server connection: Success")
    except Exception as e:
        results.append(f"❌ Connection test failed: {e}")
    
    return results