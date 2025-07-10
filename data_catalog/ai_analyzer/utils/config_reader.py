from data_catalog.connection_handler import get_catalog_connection
import psycopg2.extras

def get_ai_config_by_id(config_id: int) -> dict | None:
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM config.ai_analyzer_connection_config
                WHERE id = %s AND is_active = TRUE AND use_for_ai = TRUE
            """, (config_id,))
            return cur.fetchone()
    finally:
        conn.close()

def get_all_ai_configs(active_only: bool = True) -> list[dict]:
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = "SELECT * FROM config.ai_analyzer_connection_config"
            if active_only:
                sql += " WHERE is_active = TRUE AND use_for_ai = TRUE"
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()