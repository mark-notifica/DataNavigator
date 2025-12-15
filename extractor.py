"""
Data extraction functions.
Extracts metadata from SOURCE database.
"""

from connection import get_source_connection, get_connection


def get_all_schemas(host=None, port=None, database=None, user=None, password=None):
    """
    Get list of all user schemas (excluding system schemas).
    """
    if host:
        conn = get_connection(host, port, database, user, password)
    else:
        conn = get_source_connection()

    cursor = conn.cursor()

    query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN (
            'pg_catalog',
            'information_schema',
            'pg_toast'
        )
        ORDER BY schema_name;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    schemas = [row[0] for row in results]

    cursor.close()
    conn.close()

    return schemas


def get_all_tables(host=None, port=None, database=None, user=None, password=None, schema=None):
    """
    Get list of all tables.
    """
    if host:
        conn = get_connection(host, port, database, user, password)
    else:
        conn = get_source_connection()

    cursor = conn.cursor()

    if schema:
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        cursor.execute(query, (schema,))
    else:
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN (
                'pg_catalog',
                'information_schema',
                'pg_toast'
            )
            ORDER BY table_schema, table_name;
        """
        cursor.execute(query)

    results = cursor.fetchall()

    tables = [
        {'schema': row[0], 'table': row[1]}
        for row in results
    ]

    cursor.close()
    conn.close()

    return tables


def get_columns(table_name, schema='public', host=None, port=None, database=None, user=None, password=None):
    """
    Get columns for a specific table.
    """
    if host:
        conn = get_connection(host, port, database, user, password)
    else:
        conn = get_source_connection()

    cursor = conn.cursor()

    query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        ORDER BY ordinal_position;
    """

    cursor.execute(query, (schema, table_name))
    results = cursor.fetchall()

    columns = [
        {
            'column': row[0],
            'type': row[1],
            'nullable': (row[2] == 'YES')
        }
        for row in results
    ]

    cursor.close()
    conn.close()

    return columns
