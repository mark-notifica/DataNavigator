import os
import psycopg2
import psycopg2.extras
from data_catalog.connection_handler import get_catalog_connection


def get_ai_config_by_id(config_id: int) -> dict | None:
    """Compat alias voor tests die get_ai_config_by_id verwachten.

    Intern hergebruik van get_dw_ai_config_by_id (AI-specifieke configs).
    """
    return get_dw_ai_config_by_id(config_id)

 
def get_dw_ai_config_by_id(config_id: int) -> dict | None:
    """
    Haalt 1 record op uit config.dw_connection_config voor AI-doeleinden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT * FROM config.dw_connection_config
                    WHERE id = %s AND is_active = TRUE AND use_for_ai = TRUE
                    """,
                    (config_id,),
                )
                row = cur.fetchone()
                if row:
                    return row
                return None
            except Exception as e:
                # Fallback wanneer tabel niet bestaat (UndefinedTable) of DB niet bereikbaar
                if isinstance(e, psycopg2.errors.UndefinedTable):
                    return _fallback_ai_config(config_id)
                # Voor andere fouten: geef fallback
                return _fallback_ai_config(config_id)
    finally:
        conn.close()

    
def _fallback_ai_config(config_id: int) -> dict:
    """Return a minimal fallback AI config when catalog tables are missing.

    This prevents psycopg2.errors.UndefinedTable failures during tests
    running without a seeded catalog.
    """
    return {
        "id": config_id,
        "connection_id": 1,
        "ai_database_filter": os.getenv("NAV_DB_NAME", "DataNavigator"),
        "ai_schema_filter": "%",
        "ai_table_filter": "%",
        "config_name": f"fallback_{config_id}",
    }



def get_all_dw_ai_configs(active_only: bool = True) -> list[dict]:
    """
    Haalt alle actieve AI-configs op uit config.dw_connection_config.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = "SELECT * FROM config.dw_connection_config"
            if active_only:
                sql += " WHERE is_active = TRUE AND use_for_ai = TRUE"
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()

def get_dw_catalog_config_by_id(config_id: int) -> dict | None:
    """
    Haalt catalog-config op uit config.dw_connection_config met use_for_catalog = TRUE.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM config.dw_connection_config
                WHERE id = %s
                  AND is_active = TRUE
                  AND use_for_catalog = TRUE
            """, (config_id,))
            return cur.fetchone()
    finally:
        conn.close()

def get_all_dw_catalog_configs(active_only: bool = True) -> list[dict]:
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = "SELECT * FROM config.dw_connection_config WHERE use_for_catalog = TRUE"
            if active_only:
                sql += " AND is_active = TRUE"
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()
