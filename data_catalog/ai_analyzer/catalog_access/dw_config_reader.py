from data_catalog.connection_handler import get_catalog_connection
import psycopg2.extras

def get_dw_ai_config_by_id(config_id: int) -> dict | None:
    """
    Haalt 1 record op uit config.dw_connection_config voor AI-doeleinden.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM config.dw_connection_config
                WHERE id = %s AND is_active = TRUE AND use_for_ai = TRUE
            """, (config_id,))
            return cur.fetchone()
    finally:
        conn.close()


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
