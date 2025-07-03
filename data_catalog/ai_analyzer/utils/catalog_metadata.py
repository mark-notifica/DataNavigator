from typing import List, Dict, Optional
from data_catalog.connection_handler import get_catalog_connection

def get_table_metadata(server_name: str,
                       database_name: str,
                       schema_name: str,
                       table_name: str) -> Optional[Dict]:
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.id AS database_id, d.server_name, d.database_name,
                       s.id AS schema_id, s.schema_name,
                       t.id AS table_id, t.table_name, t.table_type
                FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                  AND t.table_name = %s
                LIMIT 1
            """, (server_name, database_name, schema_name, table_name))
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()

def get_column_metadata(table_id: int) -> List[Dict]:
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id AS column_id, column_name, data_type, ordinal_position
                FROM catalog.catalog_columns
                WHERE table_id = %s
                ORDER BY ordinal_position
            """, (table_id,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()

def get_schema_metadata(server_name: str,
                        database_name: str,
                        schema_name: str) -> Optional[Dict]:
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id AS schema_id, s.schema_name, d.id AS database_id,
                       d.server_name, d.database_name
                FROM catalog.catalog_schemas s
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                LIMIT 1
            """, (server_name, database_name, schema_name))
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()
        
# TODO: vergelijkbare functies voor view metadata, relatie suggesties, classificatie historie, layers etc.
