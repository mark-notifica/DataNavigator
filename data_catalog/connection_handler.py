import os
import psycopg2
import pyodbc
import sqlalchemy as sa
import logging
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Setup logger
logger = logging.getLogger(__name__)

CATALOG_DB_CONFIG = {
    'host': os.getenv('NAV_DB_HOST'),
    'port': os.getenv('NAV_DB_PORT'),
    'database': os.getenv('NAV_DB_NAME'),
    'user': os.getenv('NAV_DB_USER'),
    'password': os.getenv('NAV_DB_PASSWORD')
}


def get_catalog_connection():
    """Maakt een verbinding met de catalogusdatabase en logt resultaat"""
    try:
        conn = psycopg2.connect(**CATALOG_DB_CONFIG)
        logger.debug(f"Verbonden met catalogus: {CATALOG_DB_CONFIG['host']}:{CATALOG_DB_CONFIG['port']}/{CATALOG_DB_CONFIG['database']}")
        return conn
    except Exception as e:
        logger.error(f"Fout bij verbinden met catalogus: {e}")
        raise


def get_source_connections():
    """
    Leest alle actieve connecties uit config.connections (catalog-database).
    Geeft een lijst van dicts terug.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, connection_type, host, port, username, password, database_name
                FROM config.connections
                WHERE is_active = TRUE
            """)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            logger.debug(f"{len(rows)} actieve connecties opgehaald uit catalogus.")
            return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def get_connection_by_server_name(server_name: str):
    """
    Zoekt connectie-informatie op uit config.connections o.b.v. server_name (a.k.a. name).
    """
    all_connections = get_source_connections()
    for conn in all_connections:
        if conn["name"] == server_name or conn["host"] == server_name:
            logger.debug(f"Connectie gevonden voor server_name '{server_name}'")
            return conn
    logger.warning(f"Geen connectie gevonden voor server_name '{server_name}'")
    raise ValueError(f"Geen connectie gevonden voor server_name '{server_name}'")


def build_sqlalchemy_engine(conn_info: dict):
    """Geeft een SQLAlchemy engine terug obv connectie-informatie"""
    driver = conn_info["connection_type"]
    if driver == "PostgreSQL":
        url = sa.engine.URL.create(
            drivername="postgresql+psycopg2",
            username=conn_info["username"],
            password=conn_info["password"],
            host=conn_info["host"],
            port=conn_info["port"],
            database=conn_info["database_name"] or "postgres"
        )
        logger.debug(f"SQLAlchemy engine aangemaakt voor PostgreSQL: {conn_info['host']}/{conn_info['database_name']}")
        return sa.create_engine(url)

    elif driver == "Azure SQL Server":
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={conn_info['host']},{conn_info['port']};"
            f"DATABASE={conn_info['database_name']};"
            f"UID={conn_info['username']};PWD={conn_info['password']};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        url = sa.engine.URL.create(
            drivername="mssql+pyodbc",
            query={"odbc_connect": connection_string}
        )
        logger.debug(f"SQLAlchemy engine aangemaakt voor Azure SQL Server: {conn_info['host']}/{conn_info['database_name']}")
        return sa.create_engine(url)

    else:
        logger.error(f"Onbekend connection_type: {driver}")
        raise ValueError(f"Onbekend connection_type: {driver}")


def connect_to_source_database(conn_info: dict):
    """
    Bouwt een directe connectie met de brondatabase, niet via SQLAlchemy (alleen voor pandas.read_sql e.d.)
    """
    try:
        if conn_info["connection_type"] == "PostgreSQL":
            conn = psycopg2.connect(
                dbname=conn_info["database_name"],
                user=conn_info["username"],
                password=conn_info["password"],
                host=conn_info["host"],
                port=conn_info["port"]
            )
            logger.debug(f"Verbinding met PostgreSQL: {conn_info['host']}/{conn_info['database_name']}")
            return conn

        elif conn_info["connection_type"] == "Azure SQL Server":
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={conn_info['host']},{conn_info['port']};"
                f"DATABASE={conn_info['database_name']};"
                f"UID={conn_info['username']};PWD={conn_info['password']};"
                f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            conn = pyodbc.connect(connection_string)
            logger.debug(f"Verbinding met Azure SQL Server: {conn_info['host']}/{conn_info['database_name']}")
            return conn

        else:
            logger.error(f"Onbekend connection_type: {conn_info['connection_type']}")
            raise ValueError(f"Onbekend connection_type: {conn_info['connection_type']}")

    except Exception as e:
        logger.error(f"Fout bij verbinden met brondatabase ({conn_info.get('name')}): {e}")
        raise
