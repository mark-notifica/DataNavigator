import os
import psycopg2
import psycopg2.extras
import pyodbc
import sqlalchemy as sa
import logging
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

logger = logging.getLogger(__name__)

CATALOG_DB_CONFIG = {
    'host': os.getenv('NAV_DB_HOST'),
    'port': os.getenv('NAV_DB_PORT'),
    'database': os.getenv('NAV_DB_NAME'),
    'user': os.getenv('NAV_DB_USER'),
    'password': os.getenv('NAV_DB_PASSWORD')
}


def get_catalog_connection():
    """Maakt verbinding met de catalogusdatabase DataNavigator en logt resultaat."""
    try:
        conn = psycopg2.connect(**CATALOG_DB_CONFIG)
        logger.debug(
            f"Verbonden met catalogus: {CATALOG_DB_CONFIG['host']}:{CATALOG_DB_CONFIG['port']}/{CATALOG_DB_CONFIG['database']}"
        )
        return conn
    except Exception as e:
        logger.error(f"Fout bij verbinden met catalogus: {e}")
        raise


def get_all_main_connectors() -> list[dict]:
    """Leest alle actieve hoofdconnecties uit config.connections."""
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM config.connections
                WHERE is_active = TRUE
                  AND connection_type IN ('PostgreSQL', 'Azure SQL Server')
            """)
            rows = cur.fetchall()
            logger.debug(f"{len(rows)} actieve hoofdconnecties opgehaald.")
            return rows
    finally:
        conn.close()


def get_main_connector_by_name(name: str) -> dict:
    """
    Haalt één hoofdconnectie op op basis van unieke naam.
    Raise ValueError als niet gevonden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM config.connections
                WHERE is_active = TRUE
                  AND name = %s
                  AND connection_type IN ('PostgreSQL', 'Azure SQL Server')
                LIMIT 1
            """, (name,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Geen actieve hoofdconnectie gevonden met name '{name}'")
            return row
    finally:
        conn.close()

def get_main_connector_by_id(connection_id: int) -> dict:
    """
    Haalt één hoofdconnectie op op basis van ID.
    Raise ValueError als niet gevonden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM config.connections
                WHERE is_active = TRUE
                  AND id = %s
                  AND connection_type IN ('PostgreSQL', 'Azure SQL Server')
                LIMIT 1
            """, (connection_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Geen actieve hoofdconnectie gevonden met ID '{connection_id}'")
            return row
    finally:
        conn.close()

def get_all_catalog_configs() -> list[dict]:
    """Leest alle actieve catalogusconfiguraties uit config.catalog_connection_config."""
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, cat.catalog_database_filter, cat.config_name
                FROM config.connections c
                JOIN config.catalog_connection_config cat ON c.id = cat.connection_id
                WHERE c.is_active = TRUE
                  AND cat.is_active = TRUE
                  AND cat.use_for_catalog = TRUE
            """)
            rows = cur.fetchall()
            logger.debug(f"{len(rows)} actieve catalogusconfiguraties opgehaald.")
            return rows
    finally:
        conn.close()

def get_specific_connection(connection_id: int) -> dict:
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM config.connections
                WHERE id = %s AND is_active = TRUE
            """, (connection_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Geen connectie gevonden met id {connection_id}")
            return row
    finally:
        conn.close()

def get_catalog_config_by_main_connector_name(name: str) -> dict:
    """
    Haalt één catalogusconfiguratie op op basis van hoofdconnectie naam.
    Raise ValueError als niet gevonden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, cat.catalog_database_filter, cat.config_name
                FROM config.connections c
                JOIN config.catalog_connection_config cat ON c.id = cat.connection_id
                WHERE c.is_active = TRUE
                  AND cat.is_active = TRUE
                  AND cat.use_for_catalog = TRUE
                  AND c.name = %s
                LIMIT 1
            """, (name,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Geen catalog-config gevonden voor hoofdconnectie '{name}'")
            return row
    finally:
        conn.close()

def get_catalog_config_by_main_connector_id(connection_id: int) -> dict | None:
    """
    Haalt één actieve catalogusconfiguratie op op basis van hoofdconnectie-ID.
    Geeft een dict terug of None als niet gevonden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, cat.catalog_database_filter, cat.catalog_schema_filter, cat.catalog_table_filter,
                       cat.include_views, cat.include_system_objects, cat.config_name
                FROM config.connections c
                JOIN config.catalog_connection_config cat ON c.id = cat.connection_id
                WHERE c.is_active = TRUE
                  AND cat.is_active = TRUE
                  AND cat.use_for_catalog = TRUE
                  AND c.id = %s
                LIMIT 1
            """, (connection_id,))
            return cur.fetchone()
    finally:
        conn.close()

def get_catalog_config_by_id(catalog_conn, catalog_config_id: int) -> dict | None:
    """Haal één catalogusconfiguratie op o.b.v. catalog_config_id."""
    with catalog_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT *
            FROM config.catalog_connection_config
            WHERE id = %s
              AND is_active = TRUE
        """, (catalog_config_id,))
        row = cur.fetchone()
        return row

def get_all_ai_configs() -> list[dict]:
    """Leest alle actieve AI-analyzerconfiguraties uit config.ai_analyzer_connection_config."""
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, ai.ai_database_filter, ai.config_name
                FROM config.connections c
                JOIN config.ai_analyzer_connection_config ai ON c.id = ai.connection_id
                WHERE c.is_active = TRUE
                  AND ai.is_active = TRUE
                  AND ai.use_for_ai = TRUE
            """)
            rows = cur.fetchall()
            logger.debug(f"{len(rows)} actieve AI-analyzerconfiguraties opgehaald.")
            return rows
    finally:
        conn.close()


def get_ai_config_by_main_connector_name(name: str) -> dict:
    """
    Haalt één AI-analyzerconfiguratie op op basis van hoofdconnectie naam.
    Raise ValueError als niet gevonden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, ai.ai_database_filter, ai.config_name
                FROM config.connections c
                JOIN config.ai_analyzer_connection_config ai ON c.id = ai.connection_id
                WHERE c.is_active = TRUE
                  AND ai.is_active = TRUE
                  AND ai.use_for_ai = TRUE
                  AND c.name = %s
                LIMIT 1
            """, (name,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Geen AI-config gevonden voor hoofdconnectie '{name}'")
            return row
    finally:
        conn.close()

def get_ai_config_by_main_connector_id(connection_id: int) -> dict | None:
    """
    Haalt één actieve AI-analyzerconfiguratie op op basis van hoofdconnectie-ID.
    Geeft een dict terug of None als niet gevonden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, ai.ai_database_filter, ai.ai_schema_filter, ai.config_name
                FROM config.connections c
                JOIN config.ai_analyzer_connection_config ai ON c.id = ai.connection_id
                WHERE c.is_active = TRUE
                  AND ai.is_active = TRUE
                  AND ai.use_for_ai = TRUE
                  AND c.id = %s
                LIMIT 1
            """, (connection_id,))
            return cur.fetchone()
    finally:
        conn.close()

# def get_ai_config_by_id(config_id: int) -> dict | None:
#     """
#     Haalt een AI-analyzerconfiguratie op op basis van config ID.
#     Geeft een dict terug of None als niet gevonden.
#     """
#     conn = get_catalog_connection()
#     try:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute("""
#                 SELECT ai.*
#                 FROM config.ai_analyzer_connection_config ai
#                 WHERE ai.id = %s
#                   AND ai.is_active = TRUE
#                   AND ai.use_for_ai = TRUE
#                 LIMIT 1
#             """, (config_id,))
#             return cur.fetchone()
#     finally:
#         conn.close()

def build_sqlalchemy_engine(conn_info: dict, database_name: str = None):
    """
    Geeft een SQLAlchemy engine terug obv connectie-informatie.
    Optioneel kun je een database_name meegeven (handig bij 1:n-relaties).
    """
    driver = conn_info["connection_type"]
    host = conn_info["host"]
    port = conn_info["port"]
    username = conn_info["username"]
    password = conn_info["password"]

    if driver == "PostgreSQL":
        db = database_name or "postgres"
        url = sa.engine.URL.create(
            drivername="postgresql+psycopg2",
            username=username,
            password=password,
            host=host,
            port=port,
            database=db
        )
        logger.debug(f"SQLAlchemy engine aangemaakt voor PostgreSQL: {host}/{db}")
        return sa.create_engine(url)

    elif driver == "Azure SQL Server":
        db = database_name or "master"
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={db};"
            f"UID={username};PWD={password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        url = sa.engine.URL.create(
            drivername="mssql+pyodbc",
            query={"odbc_connect": connection_string}
        )
        logger.debug(f"SQLAlchemy engine aangemaakt voor Azure SQL Server: {host}/{db}")
        return sa.create_engine(url)

    else:
        logger.error(f"Onbekend connection_type: {driver}")
        raise ValueError(f"Onbekend connection_type: {driver}")


def connect_to_source_database(conn_info: dict, database_name: str = None):
    """
    Bouwt een directe connectie met een opgegeven database op de brondatabase server.
    Vereist database_name als parameter vanwege 1:n-relaties.
    """
    try:
        db = (
            database_name or
            conn_info.get("database_name") or
            ("postgres" if conn_info["connection_type"] == "PostgreSQL" else "master")
        )

        if not database_name and not conn_info.get("database_name"):
            logger.warning(f"⚠️ Geen database_name expliciet opgegeven; fallback gebruikt: {db}")
            
        if conn_info["connection_type"] == "PostgreSQL":
            conn = psycopg2.connect(
                dbname=db,
                user=conn_info["username"],
                password=conn_info["password"],
                host=conn_info["host"],
                port=conn_info["port"]
            )
            logger.debug(f"Verbinding met PostgreSQL: {conn_info['host']}/{db}")
            return conn

        elif conn_info["connection_type"] == "Azure SQL Server":
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={conn_info['host']},{conn_info['port']};"
                f"DATABASE={db};"
                f"UID={conn_info['username']};PWD={conn_info['password']};"
                f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            conn = pyodbc.connect(connection_string)
            logger.debug(f"Verbinding met Azure SQL Server: {conn_info['host']}/{db}")
            return conn

        else:
            logger.error(f"Onbekend connection_type: {conn_info['connection_type']}")
            raise ValueError(f"Onbekend connection_type: {conn_info['connection_type']}")

    except Exception as e:
        logger.error(f"Fout bij verbinden met brondatabase ({conn_info.get('name')}): {e}")
        raise

def get_databases_on_server(connection_info):
    """Get list of all databases on the server"""
    logger.info(f"Discovering databases on server: {connection_info['host']}")
    
    # Connect to master/system database to enumerate databases
    master_connection_info = connection_info.copy()
    
    if connection_info['connection_type'] == 'PostgreSQL':
        master_connection_info['database_name'] = 'postgres'
    elif connection_info['connection_type'] == 'Azure SQL Server':
        master_connection_info['database_name'] = 'master'
    else:
        logger.error(f"Unsupported connection type: {connection_info['connection_type']}")
        return []
    
    master_conn = connect_to_source_database(master_connection_info)
    if not master_conn:
        logger.error(f"Could not connect to master database on {connection_info['host']}")
        return []
    
    try:
        if connection_info['connection_type'] == 'PostgreSQL':
            # PostgreSQL query to get databases
            with master_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT datname 
                    FROM pg_database 
                    WHERE datistemplate = false 
                    AND datname NOT IN ('postgres', 'template0', 'template1')
                    ORDER BY datname
                """)
                databases = [row[0] for row in cursor.fetchall()]
                
        elif connection_info['connection_type'] == 'Azure SQL Server':
            # Azure SQL Server query to get databases
            with master_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT name 
                    FROM sys.databases 
                    WHERE database_id > 4  -- Skip system databases (master, tempdb, model, msdb)
                    AND state = 0  -- Only online databases
                    AND is_read_only = 0  -- Skip read-only databases
                    ORDER BY name
                """)
                databases = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(databases)} databases: {', '.join(databases)}")
        return databases
        
    except Exception as e:
        logger.error(f"Error getting database list from {connection_info['host']}: {e}")
        return []
    finally:
        master_conn.close()


# ------------- Backwards compatibility wrappers -------------

def get_connection_by_name(name: str) -> dict:
    """Wrapper, oude naam vervangen door nieuwe, gebaseerd op unieke naam."""
    return get_main_connector_by_name(name)

def get_catalog_config_by_name(name: str) -> dict:
    """Wrapper naar catalog config op basis van naam."""
    return get_catalog_config_by_main_connector_name(name)

def get_ai_config_by_name(name: str) -> dict:
    """Wrapper naar ai config op basis van naam."""
    return get_ai_config_by_main_connector_name(name)
