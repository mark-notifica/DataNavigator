from data_catalog.database_server_cataloger import get_catalog_connection

def get_table_analysis(server_name: str, database_name: str, schema_name: str, table_name: str) -> dict:
    """Haalt individuele tabelanalyse op"""
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT analysis_json
                FROM catalog.catalog_table_analysis
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name = %s
                ORDER BY analyzed_at DESC
                LIMIT 1
            """, (server_name, database_name, schema_name, table_name))
            row = cur.fetchone()
            if row:
                return row[0]
            return {}
    finally:
        conn.close()


def get_batch_analysis(server_name: str, database_name: str, schema_name: str, prefix: str) -> dict:
    """Haalt batch-analyse op voor groep tabellen"""
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT analysis_json
                FROM catalog.catalog_batch_analysis
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND prefix = %s
                ORDER BY analyzed_at DESC
                LIMIT 1
            """, (server_name, database_name, schema_name, prefix))
            row = cur.fetchone()
            if row:
                return row[0]
            return {}
    finally:
        conn.close()