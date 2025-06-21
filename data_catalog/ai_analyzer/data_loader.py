from data_catalog.database_server_cataloger import get_catalog_connection, connect_to_source_database
from data_catalog.ai_analyzer.database_server_cataloger_extension import get_connection_by_server_name
import pandas as pd

def get_tables_for_source(source_key: str):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT server_name, database_name, schema_name, table_name
                FROM catalog.catalog_table
                WHERE server_name = %s
            """
            cur.execute(query, (source_key,))
            rows = cur.fetchall()
            return [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    finally:
        conn.close()

def get_metadata(table: dict):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT column_name, data_type
                FROM catalog.catalog_column
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name = %s
                ORDER BY ordinal_position
            """
            cur.execute(query, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"]
            ))
            rows = cur.fetchall()
            return [{"name": r[0], "type": r[1]} for r in rows]
    finally:
        conn.close()

def get_sample_data(table: dict, metadata: list):
    try:
        conn_info = get_connection_by_server_name(table["server_name"])
        conn = connect_to_source_database(conn_info)
        if conn is None:
            return []

        col_list = ", ".join([f'"{col["name"]}"' for col in metadata])
        query = f'SELECT {col_list} FROM "{table["schema_name"]}"."{table["table_name"]}" LIMIT 20'
        df = pd.read_sql(query, conn)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"[ERROR] Sample ophalen mislukt voor {table['table_name']}: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def get_tables_for_pattern(server_name, database_name, schema_name, prefix):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT server_name, database_name, schema_name, table_name
                FROM catalog.catalog_table
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name ILIKE %s
            """
            cur.execute(query, (server_name, database_name, schema_name, prefix + '%'))
            rows = cur.fetchall()
            return [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    finally:
        conn.close()