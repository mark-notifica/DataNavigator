"""
Storage functions for catalog metadata.
Saves extracted data to the CATALOG database.
"""

from connection_db_postgres import get_catalog_connection


def start_catalog_run(run_type, source_label, server_node_id=None):
    """Start a new catalog run. Returns run_id."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO catalog.catalog_runs (run_type, connection_id, source_label, status)
        VALUES (%s, %s, %s, 'running')
        RETURNING id
    """, (run_type, server_node_id or 0, source_label))

    run_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return run_id


def finish_catalog_run(run_id, nodes_created, nodes_updated, nodes_deleted, status='completed', error_message=None):
    """Mark a catalog run as finished."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE catalog.catalog_runs
        SET completed_at = NOW(),
            status = %s,
            nodes_created = %s,
            nodes_updated = %s,
            nodes_deleted = %s,
            objects_total = %s,
            error_message = %s
        WHERE id = %s
    """, (status, nodes_created, nodes_updated, nodes_deleted,
          nodes_created + nodes_updated, error_message, run_id))

    conn.commit()
    cursor.close()
    conn.close()


def mark_deleted_nodes(source_prefix, run_id):
    """
    Mark nodes not seen in this run as deleted.
    Returns count of deleted nodes.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE catalog.nodes
        SET deleted_in_run_id = %s, deleted_at = NOW()
        WHERE qualified_name LIKE %s
        AND last_seen_run_id != %s
        AND last_seen_run_id IS NOT NULL
        AND deleted_at IS NULL
    """, (run_id, f"{source_prefix}%", run_id))

    deleted_count = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    return deleted_count


def get_or_create_server_node(server_name, server_alias='', run_id=None,
                              ip_address=None, database_type=None, host=None):
    """
    Get or create the server node. Returns (node_id, was_created).
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
        node_id = row[0]
        # Update nodes
        cursor.execute("""
            UPDATE catalog.nodes
            SET updated_at = NOW(), last_seen_run_id = %s
            WHERE node_id = %s
        """, (run_id, node_id))
        # Upsert node_server
        cursor.execute("""
            INSERT INTO catalog.node_server
            (node_id, server_name, server_alias, ip_address, database_type, host)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (node_id) DO UPDATE SET
                server_alias = COALESCE(EXCLUDED.server_alias, catalog.node_server.server_alias),
                ip_address = COALESCE(EXCLUDED.ip_address, catalog.node_server.ip_address),
                database_type = COALESCE(EXCLUDED.database_type, catalog.node_server.database_type),
                host = COALESCE(EXCLUDED.host, catalog.node_server.host)
        """, (node_id, server_name, server_alias or None, ip_address, database_type, host))
        conn.commit()
        cursor.close()
        conn.close()
        return node_id, False

    # Create new node
    cursor.execute("""
        INSERT INTO catalog.nodes
        (object_type_code, name, qualified_name, description,
         created_in_run_id, last_seen_run_id)
        VALUES ('DB_SERVER', %s, %s, %s, %s, %s)
        RETURNING node_id
    """, (server_name, qualified_name, server_alias or None, run_id, run_id))

    node_id = cursor.fetchone()[0]

    # Create node_server detail
    cursor.execute("""
        INSERT INTO catalog.node_server
        (node_id, server_name, server_alias, ip_address, database_type, host)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (node_id, server_name, server_alias or None, ip_address, database_type, host))

    conn.commit()
    cursor.close()
    conn.close()
    return node_id, True


def get_or_create_database_node(server_node_id, server_name, database_name, run_id=None):
    """
    Get or create the database node. Returns (node_id, was_created).
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
        node_id = row[0]
        cursor.execute("""
            UPDATE catalog.nodes
            SET updated_at = NOW(), last_seen_run_id = %s
            WHERE node_id = %s
        """, (run_id, node_id))
        conn.commit()
        cursor.close()
        conn.close()
        return node_id, False

    # Create new
    cursor.execute("""
        INSERT INTO catalog.nodes (object_type_code, name, qualified_name, created_in_run_id, last_seen_run_id)
        VALUES ('DB_DATABASE', %s, %s, %s, %s)
        RETURNING node_id
    """, (database_name, qualified_name, run_id, run_id))

    node_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO catalog.node_database (node_id, server_node_id, server_name, database_name)
        VALUES (%s, %s, %s, %s)
    """, (node_id, server_node_id, server_name, database_name))

    conn.commit()
    cursor.close()
    conn.close()
    return node_id, True


def save_schema(database_node_id, schema_name, run_id=None):
    """
    Save a schema (upsert). Returns (node_id, was_created).
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
        # UPDATE existing
        node_id = row[0]
        cursor.execute("""
            UPDATE catalog.nodes
            SET updated_at = NOW(), last_seen_run_id = %s
            WHERE node_id = %s
        """, (run_id, node_id))

        was_created = False
    else:
        # INSERT new
        cursor.execute("""
            INSERT INTO catalog.nodes (object_type_code, name, qualified_name, created_in_run_id, last_seen_run_id)
            VALUES ('DB_SCHEMA', %s, %s, %s, %s)
            RETURNING node_id
        """, (schema_name, qualified_name, run_id, run_id))

        node_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO catalog.node_schema (node_id, database_node_id, schema_name)
            VALUES (%s, %s, %s)
        """, (node_id, database_node_id, schema_name))

        was_created = True

    conn.commit()
    cursor.close()
    conn.close()
    return node_id, was_created


def save_table(schema_node_id, table_name, table_type='TABLE', view_definition=None, run_id=None):
    """
    Save a table or view (upsert). Returns (node_id, was_created).
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
        # UPDATE existing
        node_id = row[0]
        cursor.execute("""
            UPDATE catalog.nodes
            SET updated_at = NOW(), last_seen_run_id = %s
            WHERE node_id = %s
        """, (run_id, node_id))

        cursor.execute("""
            UPDATE catalog.node_table
            SET table_type = %s, view_definition = %s
            WHERE node_id = %s
        """, (table_type, view_definition, node_id))

        was_created = False
    else:
        # INSERT new
        cursor.execute("""
            INSERT INTO catalog.nodes (object_type_code, name, qualified_name, created_in_run_id, last_seen_run_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING node_id
        """, (object_type_code, table_name, qualified_name, run_id, run_id))

        node_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO catalog.node_table (node_id, schema_node_id, table_name, table_type, view_definition)
            VALUES (%s, %s, %s, %s, %s)
        """, (node_id, schema_node_id, table_name, table_type, view_definition))

        was_created = True

    conn.commit()
    cursor.close()
    conn.close()
    return node_id, was_created


def save_column(table_node_id, column_name, data_type, is_nullable, run_id=None):
    """
    Save a column (upsert). Returns (node_id, was_created).
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
        # UPDATE existing
        node_id = row[0]
        cursor.execute("""
            UPDATE catalog.nodes
            SET updated_at = NOW(), last_seen_run_id = %s
            WHERE node_id = %s
        """, (run_id, node_id))

        cursor.execute("""
            UPDATE catalog.node_column
            SET data_type = %s, is_nullable = %s
            WHERE node_id = %s
        """, (data_type, is_nullable, node_id))

        was_created = False
    else:
        # INSERT new
        cursor.execute("""
            INSERT INTO catalog.nodes (object_type_code, name, qualified_name, created_in_run_id, last_seen_run_id)
            VALUES ('DB_COLUMN', %s, %s, %s, %s)
            RETURNING node_id
        """, (column_name, qualified_name, run_id, run_id))

        node_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO catalog.node_column (node_id, table_node_id, column_name, data_type, is_nullable)
            VALUES (%s, %s, %s, %s, %s)
        """, (node_id, table_node_id, column_name, data_type, is_nullable))

        was_created = True

    conn.commit()
    cursor.close()
    conn.close()
    return node_id, was_created


def save_full_catalog(server_name, server_alias, ip_address, database_type,
                      database_name, schemas, tables_by_schema, table_types,
                      view_definitions, columns_by_table, host=None):
    """
    Save complete catalog extraction with upsert logic.
    """
    source_label = f"{server_name}/{database_name}"
    print(f"Saving catalog for {source_label}...")

    created_count = 0
    updated_count = 0

    # Start run
    run_id = start_catalog_run('DATABASE', source_label)
    print(f"  Run ID: {run_id}")

    server_node_id, was_created = get_or_create_server_node(
        server_name, server_alias, run_id, ip_address, database_type, host
    )
    if was_created:
        created_count += 1
    else:
        updated_count += 1
    print(f"  Server node: {server_node_id}")

    # Database node
    db_node_id, was_created = get_or_create_database_node(
        server_node_id, server_name, database_name, run_id
    )
    if was_created:
        created_count += 1
    else:
        updated_count += 1
    print(f"  Database node: {db_node_id}")

    # Schemas
    schema_nodes = {}
    for schema in schemas:
        node_id, was_created = save_schema(db_node_id, schema, run_id)
        schema_nodes[schema] = node_id
        if was_created:
            created_count += 1
        else:
            updated_count += 1
    print(f"  Processed {len(schemas)} schemas")

    # Tables
    table_nodes = {}
    for schema, tables in tables_by_schema.items():
        if schema not in schema_nodes:
            continue
        for table in tables:
            ttype = table_types.get((schema, table), 'TABLE')
            view_def = view_definitions.get((schema, table))
            node_id, was_created = save_table(
                schema_nodes[schema], table, ttype, view_def, run_id
            )
            table_nodes[(schema, table)] = node_id
            if was_created:
                created_count += 1
            else:
                updated_count += 1
    print(f"  Processed {len(table_nodes)} tables/views")

    # Columns
    col_count = 0
    for (schema, table), columns in columns_by_table.items():
        if (schema, table) not in table_nodes:
            continue
        table_node_id = table_nodes[(schema, table)]
        for col in columns:
            _, was_created = save_column(
                table_node_id, col['column'], col['type'], col['nullable'], run_id
            )
            if was_created:
                created_count += 1
            else:
                updated_count += 1
            col_count += 1
    print(f"  Processed {col_count} columns")

    # Mark deleted
    deleted_count = mark_deleted_nodes(source_label, run_id)
    print(f"  Marked {deleted_count} nodes as deleted")

    # Finish run
    finish_catalog_run(run_id, created_count, updated_count, deleted_count)

    print(f"✅ Catalog saved! Created: {created_count}, "
          f"updated: {updated_count}, deleted: {deleted_count}")

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
            tn.description
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
            cn.description
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


def get_column_node_id(schema_name, table_name, column_name):
    """Get node_id for a column."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT c.node_id
        FROM catalog.node_column c
        JOIN catalog.node_table t ON c.table_node_id = t.node_id
        JOIN catalog.node_schema s ON t.schema_node_id = s.node_id
        WHERE s.schema_name = %s
          AND t.table_name = %s
          AND c.column_name = %s
    """, (schema_name, table_name, column_name))

    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def get_catalog_servers():
    """Get all servers from catalog."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT n.node_id, n.name, s.server_alias, n.description, s.host
        FROM catalog.nodes n
        LEFT JOIN catalog.node_server s ON n.node_id = s.node_id
        WHERE n.object_type_code = 'DB_SERVER'
          AND n.deleted_at IS NULL
        ORDER BY n.name
    """)

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {
            'node_id': r[0],
            'name': r[1],
            'alias': r[2] or '',
            'description': r[3] or '',
            'host': r[4] or ''
        }
        for r in results
    ]


def get_catalog_databases(server_name):
    """Get all databases for a server."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT n.node_id, d.database_name, n.description
        FROM catalog.node_database d
        JOIN catalog.nodes n ON d.node_id = n.node_id
        JOIN catalog.nodes sn ON d.server_node_id = sn.node_id
        WHERE sn.name = %s
          AND n.deleted_at IS NULL
        ORDER BY d.database_name
    """, (server_name,))

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {'node_id': r[0], 'name': r[1], 'description': r[2] or ''}
        for r in results
    ]


def get_catalog_schemas(server_name, database_name):
    """Get all schemas for a server/database."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT n.node_id, s.schema_name, n.description
        FROM catalog.node_schema s
        JOIN catalog.nodes n ON s.node_id = n.node_id
        JOIN catalog.node_database d ON s.database_node_id = d.node_id
        JOIN catalog.nodes dn ON d.node_id = dn.node_id
        WHERE d.server_name = %s
          AND d.database_name = %s
          AND n.deleted_at IS NULL
        ORDER BY s.schema_name
    """, (server_name, database_name))

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {'node_id': r[0], 'name': r[1], 'description': r[2] or ''}
        for r in results
    ]


def get_catalog_tables_for_database(server_name, database_name):
    """Get tables filtered by server and database."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            s.schema_name,
            t.table_name,
            tn.description
        FROM catalog.node_table t
        JOIN catalog.nodes tn ON t.node_id = tn.node_id
        JOIN catalog.node_schema s ON t.schema_node_id = s.node_id
        JOIN catalog.nodes sn ON s.node_id = sn.node_id
        JOIN catalog.node_database d ON s.database_node_id = d.node_id
        WHERE d.server_name = %s
          AND d.database_name = %s
          AND tn.deleted_at IS NULL
        ORDER BY s.schema_name, t.table_name
    """, (server_name, database_name))

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {'schema': r[0], 'table': r[1], 'description': r[2] or ''}
        for r in results
    ]


def update_node_description(node_id, description):
    """Update description for a node."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE catalog.nodes
        SET description = %s,
            description_status = 'draft',
            updated_at = NOW()
        WHERE node_id = %s
    """, (description, node_id))

    conn.commit()
    cursor.close()
    conn.close()


def get_latest_running_run():
    """Get the latest running catalog run, if any."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, source_label, started_at
        FROM catalog.catalog_runs
        WHERE status = 'running'
        ORDER BY started_at DESC
        LIMIT 1
    """)

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return {'run_id': row[0], 'source_label': row[1], 'started_at': row[2]}
    return None


def get_run_progress(run_id):
    """Get progress stats for a running catalog run."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    # Count nodes created/updated in this run
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE created_in_run_id = %s) as created,
            COUNT(*) FILTER (WHERE last_seen_run_id = %s AND created_in_run_id != %s) as updated
        FROM catalog.nodes
        WHERE last_seen_run_id = %s
    """, (run_id, run_id, run_id, run_id))

    row = cursor.fetchone()

    # Get run status
    cursor.execute("""
        SELECT status, started_at, completed_at
        FROM catalog.catalog_runs
        WHERE id = %s
    """, (run_id,))

    run_row = cursor.fetchone()

    cursor.close()
    conn.close()

    return {
        'created': row[0] if row else 0,
        'updated': row[1] if row else 0,
        'total': (row[0] or 0) + (row[1] or 0) if row else 0,
        'status': run_row[0] if run_row else 'unknown',
        'started_at': run_row[1] if run_row else None,
        'completed_at': run_row[2] if run_row else None
    }


if __name__ == "__main__":
    # Quick test - just verify connection works
    conn = get_catalog_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM catalog.object_type")
    print(f"Object types in DB: {cursor.fetchone()[0]}")
    cursor.close()
    conn.close()
