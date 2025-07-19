from data_catalog.connection_handler import get_catalog_connection
import psycopg2.extras

def get_powerbi_connection_config_by_id(config_id: int) -> dict | None:
    conn = get_catalog_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM config.connections
                WHERE id = %s
                  AND is_active = TRUE
                  AND data_source_category = 'POWERBI'
            """, (config_id,))
            return cur.fetchone()
    finally:
        conn.close()
