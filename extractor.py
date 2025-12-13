"""
Data extraction functions.
Extracts metadata from SOURCE database.
"""

from connection import get_source_connection


def get_all_schemas():
    """
    Get list of all user schemas (excluding system schemas).

    Returns:
        list of strings: ['grip', 'prepare', ...]
    """
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


def get_all_tables(schema=None):
    """
    Get list of all tables.

    Args:
        schema: Specific schema to query, or None for all schemas

    Returns:
        list of dicts: [{'schema': 'grip', 'table': 'grip_job'}, ...]
    """
    conn = get_source_connection()
    cursor = conn.cursor()

    if schema:
        # Get tables from specific schema
        query = """
            SELECT
                table_schema,
                table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        cursor.execute(query, (schema,))
    else:
        # Get tables from all user schemas
        query = """
            SELECT
                table_schema,
                table_name
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


def get_columns(table_name, schema='public'):
    """
    Get columns for a specific table.

    Returns:
        list of dicts: [{'column': 'id', 'type': 'integer', 'nullable': False}, ...]
    """
    conn = get_source_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            column_name,
            data_type,
            is_nullable
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


if __name__ == "__main__":
    # Test schema extraction
    print("=== SCHEMAS ===")
    schemas = get_all_schemas()
    print(f"Found {len(schemas)} schemas:")
    for s in schemas:
        print(f"  - {s}")

    # Test table extraction (all schemas)
    print("\n=== ALL TABLES ===")
    tables = get_all_tables()  # No schema = all schemas
    print(f"Found {len(tables)} tables:")
    for table in tables[:10]:  # Show first 10
        print(f"  - {table['schema']}.{table['table']}")
    if len(tables) > 10:
        print(f"  ... and {len(tables) - 10} more")

    # Test column extraction on first table
    if tables:
        first = tables[0]
        print(f"\n=== COLUMNS IN {first['schema']}.{first['table']} ===")
        columns = get_columns(first['table'], first['schema'])
        print(f"Found {len(columns)} columns:")
        for col in columns:
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            print(f"  - {col['column']} ({col['type']}) {nullable}")
