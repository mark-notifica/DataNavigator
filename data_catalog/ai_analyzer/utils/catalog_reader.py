from data_catalog.connection_handler import get_catalog_connection
from typing import List, Dict, Optional


def get_metadata(table: dict) -> List[Dict[str, str]]:
    """
    Haalt kolomnamen en datatypes op uit de catalogus voor een opgegeven tabel.

    :param table: Dictionary met server_name, database_name, schema_name en table_name
    :return: Lijst van dictionaries met 'name' en 'type'
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT col.column_name, col.data_type
                FROM catalog.catalog_columns col
                JOIN catalog.catalog_tables t ON col.table_id = t.id
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                AND d.database_name = %s
                AND s.schema_name = %s
                AND t.table_name = %s
                ORDER BY col.ordinal_position
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"]
            ))
            rows = cur.fetchall()
            return [{"name": r[0], "type": r[1]} for r in rows]
    finally:
        conn.close()


def get_tables_for_pattern(server_name: str, database_name: str, schema_name: str, prefix: str) -> List[dict]:
    """
    Zoekt tabellen in de catalogus op basis van een prefix (ILIKE) binnen één schema.

    :return: Lijst van dictionaries met server/database/schema/table
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.server_name, d.database_name, s.schema_name, t.table_name
                FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                  AND t.table_name ILIKE %s
            """, (server_name, database_name, schema_name, prefix + '%'))
            rows = cur.fetchall()
            return [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    finally:
        conn.close()


def get_view_definition(table: dict) -> Optional[str]:
    """
    Haalt de opgeslagen viewdefinitie op uit catalog.catalog_view_definitions
    via JOINs op tables, schemas en databases.

    :param table: Dictionary met server_name, database_name, schema_name, table_name
    :return: Viewdefinitie als string, of None als niet gevonden
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT v.definition_definition
                FROM catalog.catalog_view_definitions v
                JOIN catalog.catalog_tables t ON v.table_id = t.id
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                  AND t.table_name = %s
                LIMIT 1
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"]
            ))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()

def get_catalog_tables_for_server(server_name: str) -> List[dict]:
    """
    Haalt alle tabellen op voor een opgegeven server.

    :return: Lijst van dictionaries met server/database/schema/table
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.server_name, d.database_name, s.schema_name, t.table_name
                FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
            """, (server_name,))
            rows = cur.fetchall()
            return [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    finally:
        conn.close()
