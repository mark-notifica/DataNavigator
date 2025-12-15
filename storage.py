"""
Storage functions for catalog metadata.
Saves extracted data to the CATALOG database.
"""

from connection import get_catalog_connection


def get_or_create_server_node(server_name, server_alias=''):
    """
    Get or create the server node. Returns node_id.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    qualified_name = server_name

    # Try to find existing
    cursor.execute("""
        SELECT node_id FROM catalog.nodes
        WHERE object_type_code = 'DB_SERVER' AND qualified_name = %s
    """, (qualified_name,))

    row = cursor.fetchone()
    if row:
        cursor.close()
        conn.close()
        return row[0]

    # Create new
    cursor.execute("""
        INSERT INTO catalog.nodes (object_type_code, name, qualified_name, description_short)
        VALUES ('DB_SERVER', %s, %s, %s)
        RETURNING node_id
    """, (server_name, qualified_name, server_alias if server_alias else None))

    node_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()
    return node_id


def get_or_create_database_node(server_node_id, server_name, database_name):
    """
    Get or create the database node. Returns node_id.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    qualified_name = f"{server_name}/{database_name}"

    # Try to find existing
    cursor.execute("""
        SELECT node_id FROM catalog.nodes
        WHERE object_type_code = 'DB_DATABASE' AND qualified_name = %s
    """, (qualified_name,))

    row = cursor.fetchone()
    if row:
        cursor.close()
        conn.close()
        return row[0]

    # Create new
    cursor.execute("""
        INSERT INTO catalog.nodes (object_type_code, name, qualified_name)
        VALUES ('DB_DATABASE', %s, %s)
        RETURNING node_id
    """, (database_name, qualified_name))

    node_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO catalog.node_database (node_id, server_node_id, server_name, database_name)
        VALUES (%s, %s, %s, %s)
    """, (node_id, server_node_id, server_name, database_name))

    conn.commit()
    cursor.close()
    conn.close()
    return node_id


def save_schema(database_node_id, schema_name):
    """
    Save a schema. Returns node_id.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT qualified_name FROM catalog.nodes WHERE node_id = %s
    """, (database_node_id,))
    db_qualified = cursor.fetchone()[0]
    qualified_name = f"{db_qualified}/{schema_name}"

    # Check if exists
    cursor.execute("""
        SELECT node_id FROM catalog.nodes
        WHERE object_type_code = 'DB_SCHEMA' AND qualified_name = %s
    """, (qualified_name,))

    row = cursor.fetchone()
    if row:
        cursor.close()
        conn.close()
        return row[0]

    # Create new
    cursor.execute("""
        INSERT INTO catalog.nodes (object_type_code, name, qualified_name)
        VALUES ('DB_SCHEMA', %s, %s)
        RETURNING node_id
    """, (schema_name, qualified_name))

    node_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO catalog.node_schema (node_id, database_node_id, schema_name)
        VALUES (%s, %s, %s)
    """, (node_id, database_node_id, schema_name))

    conn.commit()
    cursor.close()
    conn.close()
    return node_id


def save_table(schema_node_id, table_name, table_type='TABLE', view_definition=None):
    """
    Save a table or view. Returns node_id.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    object_type_code = 'DB_VIEW' if table_type == 'VIEW' else 'DB_TABLE'

    cursor.execute("""
        SELECT qualified_name FROM catalog.nodes WHERE node_id = %s
    """, (schema_node_id,))
    schema_qualified = cursor.fetchone()[0]
    qualified_name = f"{schema_qualified}/{table_name}"

    # Check if exists
    cursor.execute("""
        SELECT node_id FROM catalog.nodes
        WHERE object_type_code = %s AND qualified_name = %s
    """, (object_type_code, qualified_name))

    row = cursor.fetchone()
    if row:
        cursor.close()
        conn.close()
        return row[0]

    # Create new
    cursor.execute("""
        INSERT INTO catalog.nodes (object_type_code, name, qualified_name)
        VALUES (%s, %s, %s)
        RETURNING node_id
    """, (object_type_code, table_name, qualified_name))

    node_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO catalog.node_table (node_id, schema_node_id, table_name, table_type, view_definition)
        VALUES (%s, %s, %s, %s, %s)
    """, (node_id, schema_node_id, table_name, table_type, view_definition))

    conn.commit()
    cursor.close()
    conn.close()
    return node_id


def save_column(table_node_id, column_name, data_type, is_nullable):
    """
    Save a column. Returns node_id.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT qualified_name FROM catalog.nodes WHERE node_id = %s
    """, (table_node_id,))
    table_qualified = cursor.fetchone()[0]
    qualified_name = f"{table_qualified}/{column_name}"

    # Check if exists
    cursor.execute("""
        SELECT node_id FROM catalog.nodes
        WHERE object_type_code = 'DB_COLUMN' AND qualified_name = %s
    """, (qualified_name,))

    row = cursor.fetchone()
    if row:
        cursor.close()
        conn.close()
        return row[0]

    # Create new
    cursor.execute("""
        INSERT INTO catalog.nodes (object_type_code, name, qualified_name)
        VALUES ('DB_COLUMN', %s, %s)
        RETURNING node_id
    """, (column_name, qualified_name))

    node_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO catalog.node_column (node_id, table_node_id, column_name, data_type, is_nullable)
        VALUES (%s, %s, %s, %s, %s)
    """, (node_id, table_node_id, column_name, data_type, is_nullable))

    conn.commit()
    cursor.close()
    conn.close()
    return node_id


def save_full_catalog(server_name, server_alias, database_name, schemas, tables_by_schema, table_types, view_definitions, columns_by_table):
    """
    Save complete catalog extraction.
    """
    print(f"Saving catalog for {server_name}/{database_name}...")

    # Server node first
    server_node_id = get_or_create_server_node(server_name, server_alias)
    print(f"  Server node: {server_node_id}")

    # Database node
    db_node_id = get_or_create_database_node(server_node_id, server_name, database_name)
    print(f"  Database node: {db_node_id}")

    schema_nodes = {}
    for schema in schemas:
        schema_nodes[schema] = save_schema(db_node_id, schema)
    print(f"  Saved {len(schemas)} schemas")

    table_nodes = {}
    table_count = 0
    view_count = 0
    for schema, tables in tables_by_schema.items():
        if schema not in schema_nodes:
            continue
        for table in tables:
            ttype = table_types.get((schema, table), 'TABLE')
            view_def = view_definitions.get((schema, table))
            table_nodes[(schema, table)] = save_table(schema_nodes[schema], table, ttype, view_def)
            table_count += 1
            if ttype == 'VIEW':
                view_count += 1
    print(f"  Saved {table_count} tables/views ({view_count} views)")

    col_count = 0
    for (schema, table), columns in columns_by_table.items():
        if (schema, table) not in table_nodes:
            continue
        table_node_id = table_nodes[(schema, table)]
        for col in columns:
            save_column(table_node_id, col['column'], col['type'], col['nullable'])
            col_count += 1
    print(f"  Saved {col_count} columns")

    print("✅ Catalog saved!")

# === READ FUNCTIONS ===


def get_catalog_tables():
    """
    Get all tables from catalog database.
    Returns list of dicts with schema, table, and description.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            s.schema_name,
            t.table_name,
            tn.description_short
        FROM catalog.node_table t
        JOIN catalog.nodes tn ON t.node_id = tn.node_id
        JOIN catalog.node_schema s ON t.schema_node_id = s.node_id
        ORDER BY s.schema_name, t.table_name
    """)

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {'schema': r[0], 'table': r[1], 'description': r[2] or ''}
        for r in results
    ]


def get_catalog_columns(schema_name, table_name):
    """
    Get columns for a specific table from catalog database.
    Returns list of dicts with column info and description.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            cn.description_short
        FROM catalog.node_column c
        JOIN catalog.nodes cn ON c.node_id = cn.node_id
        JOIN catalog.node_table t ON c.table_node_id = t.node_id
        JOIN catalog.nodes tn ON t.node_id = tn.node_id
        JOIN catalog.node_schema s ON t.schema_node_id = s.node_id
        WHERE s.schema_name = %s AND t.table_name = %s
        ORDER BY c.column_name
    """, (schema_name, table_name))

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {
            'column': r[0],
            'type': r[1],
            'nullable': r[2],
            'description': r[3] or ''
        }
        for r in results
    ]


def get_table_node_id(schema_name, table_name):
    """Get the node_id for a table. Returns None if not found."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT t.node_id
        FROM catalog.node_table t
        JOIN catalog.node_schema s ON t.schema_node_id = s.node_id
        WHERE s.schema_name = %s AND t.table_name = %s
    """, (schema_name, table_name))

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    return row[0] if row else None


if __name__ == "__main__":
    # Quick test - just verify connection works
    conn = get_catalog_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM catalog.object_type")
    print(f"Object types in DB: {cursor.fetchone()[0]}")
    cursor.close()
    conn.close()
