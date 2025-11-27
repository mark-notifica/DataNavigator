import os
import logging
from functools import lru_cache
from typing import Optional, Dict, Any, Tuple, List

import psycopg2
import psycopg2.extras
import pyodbc
import sqlalchemy as sa
from dotenv import load_dotenv
from data_catalog.db import q_all,q_one,exec_tx
from sqlalchemy import create_engine
from typing import Optional
import pandas as pd


# ---------------------------------------
# Load environment + logger
# ---------------------------------------
load_dotenv()
logger = logging.getLogger(__name__)

# ---------------------------------------
# Catalog DB connection via env
# ---------------------------------------
CATALOG_DB_CONFIG = {
    "host": os.getenv("NAV_DB_HOST"),
    "port": os.getenv("NAV_DB_PORT"),
    "database": os.getenv("NAV_DB_NAME"),
    "user": os.getenv("NAV_DB_USER"),
    "password": os.getenv("NAV_DB_PASSWORD"),
}
def get_catalog_connection():
    """
    PostgreSQL connectie naar de catalogusdatabase (DataNavigator).
    Wordt gebruikt voor queries naar config/metadata-tabellen.
    """
    try:
        conn = psycopg2.connect(**CATALOG_DB_CONFIG)
        logger.debug(
            f"Verbonden met catalogus: {CATALOG_DB_CONFIG['host']}:{CATALOG_DB_CONFIG['port']}/{CATALOG_DB_CONFIG['database']}"
        )
        return conn
    except Exception as e:
        logger.error(f"Fout bij verbinden met catalogus: {e}")
        raise

# ------------------------------------------------------------------
# Compatibility shim: expose get_specific_connection via connection_handler
# Actual implementation resides in data_catalog.dw_cataloger
# ------------------------------------------------------------------
def get_specific_connection(connection_id: int):
    try:
        from data_catalog.dw_cataloger import get_specific_connection as _real
    except ImportError as e:
        raise ImportError("dw_cataloger.get_specific_connection niet beschikbaar") from e
    result = _real(connection_id)
    # dw_cataloger returns a list of one RealDict; normalize to dict
    if isinstance(result, list) and result:
        return dict(result[0])
    return result

# ---------------------------------------
# Low-level helpers: fetch uit config.connections
# ---------------------------------------

def get_all_main_connectors() -> list[dict]:
    rows = q_all("""
        SELECT *
        FROM   config.connections
        WHERE  is_active = TRUE
          AND  connection_type IN ('PostgreSQL', 'Azure SQL Server')
    """)
    return [dict(r._mapping) for r in rows]


def get_main_connector_by_name(name: str) -> Dict[str, Any]:
    """Haalt één actieve hoofdconnectie op o.b.v. unieke naam."""
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM config.connections
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

def get_main_connector_by_id(connection_id: int) -> Dict[str, Any]:
    """Haalt één actieve hoofdconnectie op o.b.v. ID."""
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM config.connections
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


# ---------------------------------------
# Engine/URL builders
# ---------------------------------------

def _build_sqlalchemy_url(conn_info: Dict[str, Any], database_name: Optional[str] = None) -> sa.engine.URL | str:
    """
    Bouw een SQLAlchemy URL voor PostgreSQL of Azure SQL Server.
    """
    driver = (conn_info.get("connection_type") or "").strip()
    host = (conn_info.get("host") or "").strip()
    port = conn_info.get("port")
    username = (conn_info.get("username") or "").strip()
    password = (conn_info.get("password") or "").strip()

    if driver == "PostgreSQL":
        db = (database_name or conn_info.get("database_name") or "postgres").strip()
        return sa.engine.URL.create(
            drivername="postgresql+psycopg2",
            username=username,
            password=password,
            host=host,
            port=port,
            database=db
        )

    if driver == "Azure SQL Server":
        db = (database_name or conn_info.get("database_name") or "master").strip()
        # Encrypt aan, TrustServerCertificate policy-bewust (hier 'no'; pas aan indien gewenst).
        odbc = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={db};"
            f"UID={username};PWD={password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        return sa.engine.URL.create(drivername="mssql+pyodbc", query={"odbc_connect": odbc})

    raise ValueError(f"Onbekend connection_type: {driver}")


def build_sqlalchemy_engine(conn_info: Dict[str, Any], database_name: Optional[str] = None):
    """
    Compat helper: bouw een SQLAlchemy Engine op basis van conn_info.
    Wordt gebruikt door tests en older call-sites in samples.

    Parameters:
        conn_info: dictionary met o.a. connection_type, host, port, username, password
        database_name: optionele override voor database
    """
    url = _build_sqlalchemy_url(conn_info, database_name=database_name)
    return create_engine(url, pool_pre_ping=True, future=True)

@lru_cache(maxsize=128)
def get_engine_for_connection(conn_id: int, database_name: Optional[str] = None):
    """
    Gecachete SQLAlchemy engine per (conn_id, database_name).
    """
    conn_info = get_main_connector_by_id(conn_id)
    url = _build_sqlalchemy_url(conn_info, database_name=database_name)
    return create_engine(url, pool_pre_ping=True, future=True)

def dispose_engine(conn_id: int, database_name: Optional[str] = None):
    """Dispose & cache clear (na cred-wijzigingen)."""
    try:
        engine = get_engine_for_connection(conn_id, database_name)
        engine.dispose()
    finally:
        get_engine_for_connection.cache_clear()


# ---------------------------------------
# Directe bronverbinding (psycopg2/pyodbc) – voor bestaande call-sites
# ---------------------------------------

def connect_to_source_database(conn_info: Dict[str, Any], database_name: Optional[str] = None):
    """
    Bouwt een directe connectie (psycopg2/pyodbc) voor bestaande codepaths.
    Gebruik bij nieuwe code bij voorkeur SQLAlchemy engines.
    """
    try:
        driver = conn_info["connection_type"]
        db = database_name or conn_info.get("database_name") or ("postgres" if driver == "PostgreSQL" else "master")

        if database_name is None and not conn_info.get("database_name"):
            logger.warning(f"⚠️ Geen database_name expliciet opgegeven; fallback gebruikt: {db}")

        if driver == "PostgreSQL":
            conn = psycopg2.connect(
                dbname=db,
                user=conn_info["username"],
                password=conn_info["password"],
                host=conn_info["host"],
                port=conn_info["port"]
            )
            logger.debug(f"Verbinding met PostgreSQL: {conn_info['host']}/{db}")
            return conn

        if driver == "Azure SQL Server":
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

        raise ValueError(f"Onbekend connection_type: {driver}")

    except Exception as e:
        logger.error(f"Fout bij verbinden met brondatabase ({conn_info.get('name')}): {e}")
        raise

# ---------------------------------------
# Discovery
# ---------------------------------------

def get_databases_on_server(connection_info: Dict[str, Any]) -> List[str]:
    """
    Haal lijst met databases op voor een server (alleen user DBs).
    """
    logger.info(f"Discovering databases on server: {connection_info['host']}")

    master_info = dict(connection_info)
    master_info["database_name"] = "postgres" if connection_info["connection_type"] == "PostgreSQL" else "master"

    master_conn = connect_to_source_database(master_info)
    if not master_conn:
        logger.error(f"Could not connect to master database on {connection_info['host']}")
        return []

    try:
        if connection_info["connection_type"] == "PostgreSQL":
            with master_conn.cursor() as cur:
                cur.execute("""
                    SELECT datname
                    FROM pg_database
                    WHERE datistemplate = false
                      AND datname NOT IN ('postgres','template0','template1')
                    ORDER BY datname
                """)
                return [r[0] for r in cur.fetchall()]

        # Azure SQL Server
        with master_conn.cursor() as cur:
            cur.execute("""
                SELECT name
                FROM sys.databases
                WHERE database_id > 4
                  AND state = 0
                  AND is_read_only = 0
                ORDER BY name
            """)
            return [r[0] for r in cur.fetchall()]

    except Exception as e:
        logger.error(f"Error getting database list from {connection_info['host']}: {e}")
        return []
    finally:
        master_conn.close()

# ---------------------------------------
# Test helpers – koppelen aan config_handler (catalog-varianten)
# ---------------------------------------

def _ping_db(conn_id: int) -> Tuple[bool, str]:
    """Uitvoeren van een simpele SELECT 1 via SQLAlchemy."""
    try:
        engine = get_engine_for_connection(conn_id)
        with engine.connect() as c:
            c.exec_driver_sql("SELECT 1")
        return True, "OK"
    except Exception as ex:
        return False, f"{type(ex).__name__}: {ex}"

def test_dw_catalog_with_config(cfg_id: int, conn_id: int, set_status: bool = True) -> Tuple[bool, str]:
    """
    Test de hoofdverbinding en schrijf (optioneel) last_test_* naar config.dw_catalog_config.
    """
    ok, msg = _ping_db(conn_id)
    if set_status:
        from .config_crud import set_dw_catalog_last_test_result
        set_dw_catalog_last_test_result(cfg_id, "OK" if ok else "ERROR", msg)
    return ok, msg

def test_pbi_catalog_with_config(cfg_id: int, conn_id: int, set_status: bool = True) -> Tuple[bool, str]:
    """
    Test de hoofdverbinding en schrijf (optioneel) last_test_* naar config.pbi_catalog_config.
    """
    ok, msg = _ping_db(conn_id)
    if set_status:
        from .config_crud import set_pbi_catalog_last_test_result
        set_pbi_catalog_last_test_result(cfg_id, "OK" if ok else "ERROR", msg)
    return ok, msg

def test_dl_catalog_with_config(cfg_id: int, conn_id: int, set_status: bool = True) -> Tuple[bool, str]:
    """
    Test de hoofdverbinding en schrijf (optioneel) last_test_* naar config.dl_catalog_config.
    """
    ok, msg = _ping_db(conn_id)
    if set_status:
        from .config_crud import set_dl_catalog_last_test_result
        set_dl_catalog_last_test_result(cfg_id, "OK" if ok else "ERROR", msg)
    return ok, msg

# ---------------------------------------
# Backwards compatibility wrappers
# ---------------------------------------

def get_connection_by_name(name: str) -> Dict[str, Any]:
    return get_main_connector_by_name(name)

# Historische naamgeving behouden:
def get_catalog_config_by_name(name: str):
    """
    Laat bij voorkeur config_handler.* gebruiken i.p.v. connection_handler.
    Wrapper blijft bestaan voor oude call-sites (haalt alleen hoofdconnectie op).
    """
    return get_main_connector_by_name(name)

def get_ai_config_by_name(name: str):
    """
    Laat bij voorkeur config_handler.* (pbi_ai/dl_ai/dw_ai) gebruiken.
    Deze wrapper retourneert enkel de hoofdconnectie (oude compat).
    """
    return get_main_connector_by_name(name)

# ---------------------------------------
# MAIN CONNECTION CRUD (config.connections)
# ---------------------------------------


def load_mapping_df() -> pd.DataFrame:
    rows = q_all("""
        SELECT connection_type
             , data_source_category
             , display_name
             , short_code
        FROM   config.connection_type_registry
        WHERE  is_active = true
        ORDER  BY display_name
    """)
    return pd.DataFrame([dict(r._mapping) for r in rows])

def list_connections_df(include_orphans: bool = False) -> pd.DataFrame:
    if include_orphans:
        rows = q_all("""
            SELECT c.id
                 , c.connection_name
                 , c.connection_type
                 , r.display_name
                 , r.data_source_category
                 , r.short_code
                 , c.is_active
                 , c.created_at
                 , c.updated_at
                 , c.last_test_status
                 , c.last_tested_at
                 , c.last_test_notes
            FROM   config.connections c
            LEFT   JOIN config.connection_type_registry r
              ON   r.connection_type = c.connection_type
            WHERE  c.deleted_at IS NULL
            ORDER  BY c.id DESC
        """)
    else:
        rows = q_all("""
            SELECT c.id
                 , c.connection_name
                 , c.connection_type
                 , r.display_name
                 , r.data_source_category
                 , r.short_code
                 , c.is_active
                 , c.created_at
                 , c.updated_at
                 , c.last_test_status
                 , c.last_tested_at
                 , c.last_test_notes
            FROM   config.connections c
            JOIN   config.connection_type_registry r
              ON   r.connection_type = c.connection_type
            WHERE  c.deleted_at IS NULL
            ORDER  BY c.id DESC
        """)
    return pd.DataFrame([dict(r._mapping) for r in rows])

def upsert_connection_row(conn_id: Optional[int], connection_name: str, connection_type: str) -> int:
    exists = q_one("""
        SELECT 1
        FROM   config.connection_type_registry
        WHERE  connection_type = :t
         AND   is_active       = true
    """, {"t": connection_type})
    if not exists:
        raise ValueError(f"Unknown or inactive connection_type: {connection_type}")

    name_taken = q_one("""
        SELECT 1
        FROM   config.connections
        WHERE  connection_name = :n
         AND   deleted_at IS NULL
         AND  (:id IS NULL OR id <> :id)
    """, {"n": connection_name.strip(), "id": conn_id})
    if name_taken:
        raise ValueError(f"connection_name '{connection_name}' bestaat al")

    if conn_id:
        row = q_one("""
            UPDATE config.connections
               SET connection_name = :n
                 , connection_type = :t
                 , updated_at      = CURRENT_TIMESTAMP
            WHERE  id = :id
              AND  deleted_at IS NULL
            RETURNING id
        """, {"n": connection_name.strip(), "t": connection_type, "id": conn_id})
    else:
        row = q_one("""
            INSERT INTO config.connections
            ( connection_name
            , connection_type
            )
            VALUES
            ( :n
            , :t
            )
            RETURNING id
        """, {"n": connection_name.strip(), "t": connection_type})
    return int(row[0])

def set_connection_last_test_result(conn_id: int, status: Optional[str], notes: Optional[str]) -> None:
    exec_tx("""
        UPDATE config.connections
           SET last_test_status = :s
             , last_tested_at   = CURRENT_TIMESTAMP
             , last_test_notes  = :notes
             , updated_at       = CURRENT_TIMESTAMP
         WHERE id = :id
           AND deleted_at IS NULL
    """, {"id": conn_id, "s": (status or "").strip() or None, "notes": (notes or "").strip() or None})

def clear_connection_last_test_result(conn_id: int) -> None:
    exec_tx("""
        UPDATE config.connections
           SET last_test_status = NULL
             , last_tested_at   = NULL
             , last_test_notes  = NULL
             , updated_at       = CURRENT_TIMESTAMP
         WHERE id = :id
           AND deleted_at IS NULL
    """, {"id": conn_id})

def deactivate_connection(conn_id: int) -> None:
    exec_tx("""
        UPDATE config.connections
           SET is_active  = false
             , updated_at = CURRENT_TIMESTAMP
         WHERE id = :id
           AND deleted_at IS NULL
    """, {"id": conn_id})

def reactivate_connection(conn_id: int) -> None:
    exec_tx("""
        UPDATE config.connections
           SET is_active  = true
             , updated_at = CURRENT_TIMESTAMP
         WHERE id = :id
           AND deleted_at IS NULL
    """, {"id": conn_id})

def soft_delete_connection(conn_id: int) -> None:
    exec_tx("""
        UPDATE config.connections
           SET deleted_at = CURRENT_TIMESTAMP
             , updated_at  = CURRENT_TIMESTAMP
         WHERE id = :id
           AND deleted_at IS NULL
    """, {"id": conn_id})

def get_connection_row_by_id(conn_id: int) -> dict | None:
    row = q_one("""
        SELECT c.id
             , c.connection_name
             , c.connection_type
             , c.is_active
             , c.created_at
             , c.updated_at
             , c.last_test_status
             , c.last_tested_at
             , c.last_test_notes
        FROM   config.connections c
        WHERE  c.id = :id
          AND  c.deleted_at IS NULL
        LIMIT  1
    """, {"id": conn_id})
    return dict(row._mapping) if row else None

def restore_soft_deleted_connection(conn_id: int) -> None:
    exec_tx("""
        UPDATE config.connections
           SET deleted_at = NULL
             , updated_at = CURRENT_TIMESTAMP
         WHERE id = :id
    """, {"id": conn_id})

# ---------------------------------------
# CONNECTION DETAILS CRUD (config.*_connection_details)
# ---------------------------------------

# ---------------- Secrets helpers ----------------
def fetch_secret(ref_key: Optional[str]) -> Optional[str]:
    if not ref_key:
        return None
    row = q_one("""
        SELECT secret_value
        FROM   security.secrets_plain
        WHERE  ref_key = :k
    """, {"k": ref_key})
    return (row and row[0]) or None

def save_secret(ref_key: str, value: str):
    exec_tx("""
        INSERT INTO security.secrets_plain
        ( ref_key , secret_value )
        VALUES (:k , :v)
        ON CONFLICT (ref_key) DO UPDATE
        SET  secret_value = EXCLUDED.secret_value
         ,  updated_at   = now()
    """, {"k": ref_key, "v": value})

def _norm(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None

def _details_exists(table: str, connection_id: int) -> bool:
    row = q_one(f"""
        SELECT 1
        FROM   {table}
        WHERE  connection_id = :id
        LIMIT  1
    """, {"id": connection_id})
    return bool(row)

def fetch_dw_details(connection_id: int, with_secret: bool = False) -> dict | None:
    print(f"[fetch_dw_details] called for {connection_id}")
    row = q_one("""
        SELECT d.connection_id
             , d.engine_type
             , d.host
             , d.port
             , d.default_database
             , d.username
             , d.ssl_mode
             , d.secret_ref
             , d.updated_at
        FROM   config.dw_connection_details d
        WHERE  d.connection_id = :id
    """, {"id": connection_id})
    if not row:
        return None
    d = dict(row._mapping)
    if with_secret:
        d["secret_value"] = fetch_secret(d.get("secret_ref"))
    return d


def insert_dw_details(*args, **kwargs) -> None:
    # mag alleen als record nog niet bestaat
    connection_id = args[0] if args else kwargs["connection_id"]
    if _details_exists("config.dw_connection_details", connection_id):
        raise ValueError(f"dw_connection_details voor connection_id={connection_id} bestaat al")
    upsert_dw_details(*args, **kwargs)


def update_dw_details(*args, **kwargs) -> None:
    # mag alleen als record al bestaat
    connection_id = args[0] if args else kwargs["connection_id"]
    if not _details_exists("config.dw_connection_details", connection_id):
        raise ValueError(f"dw_connection_details voor connection_id={connection_id} bestaat nog niet")
    upsert_dw_details(*args, **kwargs)

def upsert_dw_details(connection_id: int
                    , engine_type: str
                    , host: Optional[str]
                    , port: Optional[str | int]
                    , default_database: Optional[str]
                    , username: Optional[str]
                    , ssl_mode: Optional[str]
                    , password_plain: Optional[str]) -> None:
    # Normalisatie
    et = _norm(engine_type)
    h  = _norm(host)
    db = _norm(default_database)
    u  = _norm(username)
    ssl= _norm(ssl_mode)

    # Port cast
    if isinstance(port, str):
        p = int(port.strip()) if port.strip() else None
    elif isinstance(port, int):
        p = port
    else:
        p = None

    # (Optioneel) whitelist engine_type
    if et not in ("PostgreSQL", "Azure SQL Server"):
        raise ValueError(f"Unsupported engine_type: {et}")

    # Secret opslaan (vooraf), zodat DB en vault consistent blijven
    secret_ref = None
    if _norm(password_plain):
        secret_ref = f"connection/{connection_id}/db_password"
        save_secret(secret_ref, password_plain.strip())

    exec_tx("""
        INSERT INTO config.dw_connection_details
        ( connection_id
        , engine_type
        , host
        , port
        , default_database
        , username
        , ssl_mode
        , secret_ref
        )
        VALUES
        ( :id
        , :et
        , :h
        , :p
        , :db
        , :u
        , :ssl
        , :sref
        )
        ON CONFLICT (connection_id) DO UPDATE
           SET engine_type      = EXCLUDED.engine_type
             , host             = EXCLUDED.host
             , port             = EXCLUDED.port
             , default_database = EXCLUDED.default_database
             , username         = EXCLUDED.username
             , ssl_mode         = EXCLUDED.ssl_mode
             , secret_ref       = COALESCE(EXCLUDED.secret_ref
                                          , config.dw_connection_details.secret_ref)
             , updated_at       = CURRENT_TIMESTAMP
    """, {"id": connection_id, "et": et, "h": h, "p": p, "db": db, "u": u, "ssl": ssl, "sref": secret_ref})

def fetch_pbi_local_details(connection_id: int) -> dict | None:
    row = q_one("""
        SELECT d.connection_id
             , d.folder_path
             , d.updated_at
        FROM   config.pbi_local_connection_details d
        WHERE  d.connection_id = :id
    """, {"id": connection_id})
    return dict(row._mapping) if row else None


def fetch_pbi_service_details(connection_id: int, with_secret: bool = False) -> dict | None:
    row = q_one("""
        SELECT d.connection_id
             , d.tenant_id
             , d.client_id
             , d.auth_method
             , d.secret_ref
             , d.default_workspace_id
             , d.default_workspace_name
             , d.updated_at
        FROM   config.pbi_service_connection_details d
        WHERE  d.connection_id = :id
    """, {"id": connection_id})
    if not row:
        return None
    d = dict(row._mapping)
    if with_secret:
        d["secret_value"] = fetch_secret(d.get("secret_ref"))
    return d


def insert_pbi_local_details(connection_id: int, folder_path: str) -> None:
    # mag alleen als er geen local én geen service record is
    if _details_exists("config.pbi_local_connection_details", connection_id) \
       or _details_exists("config.pbi_service_connection_details", connection_id):
        raise ValueError("Er bestaat al een PBI details record (local of service) voor deze connection")
    # hergebruik jouw upsert met mode-switch
    upsert_pbi_local_details(connection_id, folder_path)


def update_pbi_local_details(connection_id: int, folder_path: str) -> None:
    if not _details_exists("config.pbi_local_connection_details", connection_id):
        raise ValueError("PBI local details bestaan nog niet voor deze connection")
    upsert_pbi_local_details(connection_id, folder_path)


def insert_pbi_service_details(**kwargs) -> None:
    connection_id = kwargs["connection_id"]
    if _details_exists("config.pbi_local_connection_details", connection_id) \
       or _details_exists("config.pbi_service_connection_details", connection_id):
        raise ValueError("Er bestaat al een PBI details record (local of service) voor deze connection")
    upsert_pbi_service_details(**kwargs)


def update_pbi_service_details(**kwargs) -> None:
    connection_id = kwargs["connection_id"]
    if not _details_exists("config.pbi_service_connection_details", connection_id):
        raise ValueError("PBI service details bestaan nog niet voor deze connection")
    upsert_pbi_service_details(**kwargs)


def upsert_pbi_local_details(connection_id: int, folder_path: str) -> None:
    fp = _norm(folder_path) or ""   # leeg pad toestaan → opslaan als lege string

    # Eén transactie: verwijder service, insert/update local
    exec_tx("""
        DELETE
          FROM config.pbi_service_connection_details
         WHERE connection_id = :id;

        INSERT INTO config.pbi_local_connection_details
        ( connection_id
        , folder_path
        )
        VALUES
        ( :id
        , :fp
        )
        ON CONFLICT (connection_id) DO UPDATE
           SET folder_path = EXCLUDED.folder_path
             , updated_at  = CURRENT_TIMESTAMP;
    """, {"id": connection_id, "fp": fp})


def upsert_pbi_service_details(connection_id: int
                             , tenant_id: Optional[str]
                             , client_id: Optional[str]
                             , auth_method: str
                             , secret_value: Optional[str]
                             , default_workspace_id: Optional[str]
                             , default_workspace_name: Optional[str]) -> None:
    tid  = _norm(tenant_id)
    cid  = _norm(client_id)
    auth = _norm(auth_method) or "DEVICE_CODE"
    dwid = _norm(default_workspace_id)
    dwn  = _norm(default_workspace_name)

    # (Optioneel) whitelist auth_method
    # allowed = {"DEVICE_CODE", "CLIENT_SECRET", "MSI"}
    # if auth not in allowed: raise ValueError(...)

    secret_ref = None
    if _norm(secret_value):
        secret_ref = f"connection/{connection_id}/pbi_client_secret"
        save_secret(secret_ref, secret_value.strip())

    # Eén transactie: verwijder local, insert/update service
    exec_tx("""
        DELETE
          FROM config.pbi_local_connection_details
         WHERE connection_id = :id;

        INSERT INTO config.pbi_service_connection_details
        ( connection_id
        , tenant_id
        , client_id
        , auth_method
        , secret_ref
        , default_workspace_id
        , default_workspace_name
        )
        VALUES
        ( :id
        , :tid
        , :cid
        , :auth
        , :sref
        , :dwid
        , :dwn
        )
        ON CONFLICT (connection_id) DO UPDATE
           SET tenant_id             = EXCLUDED.tenant_id
             , client_id             = EXCLUDED.client_id
             , auth_method           = EXCLUDED.auth_method
             , secret_ref            = COALESCE(EXCLUDED.secret_ref
                                              , config.pbi_service_connection_details.secret_ref)
             , default_workspace_id  = EXCLUDED.default_workspace_id
             , default_workspace_name= EXCLUDED.default_workspace_name
             , updated_at            = CURRENT_TIMESTAMP;
    """, {"id": connection_id, "tid": tid, "cid": cid, "auth": auth,
          "sref": secret_ref, "dwid": dwid, "dwn": dwn})

def fetch_dl_details(connection_id: int, with_secret: bool = False) -> dict | None:
    row = q_one("""
        SELECT d.connection_id
             , d.storage_type
             , d.endpoint_url
             , d.bucket_or_container
             , d.base_path
             , d.auth_method
             , d.secret_ref
             , d.updated_at
        FROM   config.dl_connection_details d
        WHERE  d.connection_id = :id
    """, {"id": connection_id})
    if not row:
        return None
    d = dict(row._mapping)
    if with_secret:
        d["secret_value"] = fetch_secret(d.get("secret_ref"))
    return d


def insert_dl_details(*args, **kwargs) -> None:
    connection_id = args[0] if args else kwargs["connection_id"]
    if _details_exists("config.dl_connection_details", connection_id):
        raise ValueError(f"dl_connection_details voor connection_id={connection_id} bestaat al")
    upsert_dl_details(*args, **kwargs)


def update_dl_details(*args, **kwargs) -> None:
    connection_id = args[0] if args else kwargs["connection_id"]
    if not _details_exists("config.dl_connection_details", connection_id):
        raise ValueError(f"dl_connection_details voor connection_id={connection_id} bestaat nog niet")
    upsert_dl_details(*args, **kwargs)


def upsert_dl_details(connection_id: int
                    , storage_type: str
                    , endpoint_url: Optional[str]
                    , bucket_or_container: Optional[str]
                    , base_path: Optional[str]
                    , auth_method: Optional[str]
                    , access_key_or_secret: Optional[str]) -> None:
    st   = _norm(storage_type)
    ep   = _norm(endpoint_url)
    boc  = _norm(bucket_or_container)
    bp   = _norm(base_path)
    am   = _norm(auth_method)

    # (Optioneel) whitelist storage_type/auth_method
    # st_allowed = {"S3", "AZURE_BLOB", "AZURE_DFS", "MINIO", "GCS"}
    # if st not in st_allowed: raise ValueError(...)
    # am_allowed = {"ACCESS_KEY", "SAS", "MSI", "ANON"}
    # if am and am not in am_allowed: raise ValueError(...)

    secret_ref = None
    if _norm(access_key_or_secret):
        secret_ref = f"connection/{connection_id}/dl_secret"
        save_secret(secret_ref, access_key_or_secret.strip())

    exec_tx("""
        INSERT INTO config.dl_connection_details
        ( connection_id
        , storage_type
        , endpoint_url
        , bucket_or_container
        , base_path
        , auth_method
        , secret_ref
        )
        VALUES
        ( :id
        , :st
        , :ep
        , :boc
        , :bp
        , :am
        , :sref
        )
        ON CONFLICT (connection_id) DO UPDATE
           SET storage_type        = EXCLUDED.storage_type
             , endpoint_url        = EXCLUDED.endpoint_url
             , bucket_or_container = EXCLUDED.bucket_or_container
             , base_path           = EXCLUDED.base_path
             , auth_method         = EXCLUDED.auth_method
             , secret_ref          = COALESCE(EXCLUDED.secret_ref
                                            , config.dl_connection_details.secret_ref)
             , updated_at          = CURRENT_TIMESTAMP
    """, {"id": connection_id, "st": st, "ep": ep, "boc": boc, "bp": bp, "am": am, "sref": secret_ref})

def fetch_connection_type_registry(*, active_only: bool = True) -> List[Dict]:
    """
    Load the live registry of supported connection types from
    config.connection_type_registry.
    """
    if active_only:
        sql = """
            SELECT connection_type,
                   data_source_category,
                   display_name,
                   is_active,
                   created_at,
                   short_code
            FROM   config.connection_type_registry
            WHERE  is_active
            ORDER  BY data_source_category, display_name
        """
        rows = q_all(sql)
    else:
        sql = """
            SELECT connection_type,
                   data_source_category,
                   display_name,
                   is_active,
                   created_at,
                   short_code
            FROM   config.connection_type_registry
            ORDER  BY data_source_category, display_name
        """
        rows = q_all(sql)

    return [dict(r._mapping) for r in rows] if rows else []


# ------------------------------------------------------------------
# Compatibility shim for legacy imports in tests expecting
# data_catalog.connection_handler.get_ai_config_by_id
# Actual implementation lives in ai_analyzer.catalog_access.dw_config_reader
# ------------------------------------------------------------------
def get_ai_config_by_id(ai_config_id: int):  # noqa: D401
    """Proxy naar dw_config_reader.get_ai_config_by_id (compat for tests)."""
    try:
        from ai_analyzer.catalog_access.dw_config_reader import get_ai_config_by_id as _real
    except ImportError as e:  # pragma: no cover
        raise ImportError("dw_config_reader module niet gevonden voor get_ai_config_by_id") from e
    return _real(ai_config_id)

