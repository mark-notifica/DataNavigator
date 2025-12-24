"""
Export and import catalog descriptions.
Export to CSV for AI enrichment, import back with generated descriptions.
Also supports exporting view DDL for AI analysis.
"""

import csv
from io import StringIO
from connection_db_postgres import get_catalog_connection


def export_view_ddl(server_name=None, database_name=None, schema_name=None,
                    include_columns=True, output_format='sql'):
    """
    Export view DDL definitions for AI analysis.

    Args:
        server_name: Filter by server (optional)
        database_name: Filter by database (optional)
        schema_name: Filter by schema (optional)
        include_columns: Include column info in output (default True)
        output_format: 'sql' for pure DDL, 'markdown' for documented format

    Returns:
        String with view definitions
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    # Get views with their DDL
    query = """
        SELECT
            n.qualified_name,
            n.name as view_name,
            n.description,
            t.view_definition,
            s.schema_name,
            d.database_name,
            srv.name as server_name
        FROM catalog.nodes n
        JOIN catalog.node_table t ON n.node_id = t.node_id
        JOIN catalog.node_schema s ON t.schema_node_id = s.node_id
        JOIN catalog.nodes sn ON s.node_id = sn.node_id
        JOIN catalog.node_database d ON s.database_node_id = d.node_id
        JOIN catalog.nodes dn ON d.node_id = dn.node_id
        JOIN catalog.node_server srv_t ON d.server_node_id = srv_t.node_id
        JOIN catalog.nodes srv ON srv_t.node_id = srv.node_id
        WHERE n.object_type_code = 'DB_VIEW'
          AND n.deleted_at IS NULL
          AND t.view_definition IS NOT NULL
    """
    params = []

    if server_name:
        query += " AND srv.name = %s"
        params.append(server_name)

    if database_name:
        query += " AND d.database_name = %s"
        params.append(database_name)

    if schema_name:
        query += " AND s.schema_name = %s"
        params.append(schema_name)

    query += " ORDER BY srv.name, d.database_name, s.schema_name, n.name"

    cursor.execute(query, params)
    views = cursor.fetchall()

    # Get columns for each view if requested
    view_columns = {}
    if include_columns:
        view_node_ids = []
        cursor.execute("""
            SELECT n.node_id, n.qualified_name
            FROM catalog.nodes n
            WHERE n.object_type_code = 'DB_VIEW'
              AND n.deleted_at IS NULL
        """)
        view_map = {row[1]: row[0] for row in cursor.fetchall()}

        for view in views:
            qualified_name = view[0]
            if qualified_name in view_map:
                view_node_ids.append(view_map[qualified_name])

        if view_node_ids:
            cursor.execute("""
                SELECT
                    c.table_node_id,
                    c.column_name,
                    c.data_type,
                    cn.description
                FROM catalog.node_column c
                JOIN catalog.nodes cn ON c.node_id = cn.node_id
                WHERE c.table_node_id = ANY(%s)
                  AND cn.deleted_at IS NULL
                ORDER BY c.table_node_id, c.column_name
            """, (view_node_ids,))

            for row in cursor.fetchall():
                table_node_id = row[0]
                if table_node_id not in view_columns:
                    view_columns[table_node_id] = []
                view_columns[table_node_id].append({
                    'name': row[1],
                    'type': row[2],
                    'description': row[3] or ''
                })

    cursor.close()
    conn.close()

    # Build output
    if output_format == 'markdown':
        return _format_ddl_markdown(views, view_columns, view_map if include_columns else {})
    else:
        return _format_ddl_sql(views, view_columns, view_map if include_columns else {})


def _format_ddl_sql(views, view_columns, view_map):
    """Format as pure SQL with comments."""
    output = []
    output.append("-- View DDL Export")
    output.append("-- Generated for AI analysis")
    output.append("")

    for view in views:
        qualified_name, view_name, description, ddl, schema, db, server = view
        output.append("-- ============================================")
        output.append(f"-- View: {schema}.{view_name}")
        output.append(f"-- Location: {server}/{db}/{schema}")
        if description:
            output.append(f"-- Description: {description}")
        output.append("-- ============================================")
        output.append("")
        output.append(f"CREATE OR REPLACE VIEW {schema}.{view_name} AS")
        output.append(ddl)
        output.append(";")
        output.append("")

        # Add column info as comments
        if qualified_name in view_map:
            node_id = view_map[qualified_name]
            if node_id in view_columns:
                output.append("-- Columns:")
                for col in view_columns[node_id]:
                    desc = f" -- {col['description']}" if col['description'] else ""
                    output.append(f"--   {col['name']}: {col['type']}{desc}")
                output.append("")

    return "\n".join(output)


def _format_ddl_markdown(views, view_columns, view_map):
    """Format as markdown documentation."""
    output = []
    output.append("# View DDL Export")
    output.append("")
    output.append("This document contains view definitions for AI analysis.")
    output.append("")

    current_server = None
    current_db = None

    for view in views:
        qualified_name, view_name, description, ddl, schema, db, server = view

        # Add server/database headers
        if server != current_server:
            output.append(f"# Server: {server}")
            output.append("")
            current_server = server
            current_db = None

        if db != current_db:
            output.append(f"## Database: {db}")
            output.append("")
            current_db = db

        output.append(f"### {schema}.{view_name}")
        output.append("")

        if description:
            output.append(f"**Description:** {description}")
            output.append("")

        output.append("**DDL:**")
        output.append("```sql")
        output.append(f"CREATE OR REPLACE VIEW {schema}.{view_name} AS")
        output.append(ddl)
        output.append("```")
        output.append("")

        # Add column table
        if qualified_name in view_map:
            node_id = view_map[qualified_name]
            if node_id in view_columns and view_columns[node_id]:
                output.append("**Columns:**")
                output.append("")
                output.append("| Column | Type | Description |")
                output.append("|--------|------|-------------|")
                for col in view_columns[node_id]:
                    output.append(f"| {col['name']} | {col['type']} | {col['description']} |")
                output.append("")

    return "\n".join(output)


def export_ddl_to_file(filepath, **kwargs):
    """Export view DDL to a file."""
    content = export_view_ddl(**kwargs)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    # Count views
    view_count = content.count('CREATE OR REPLACE VIEW')
    print(f"Exported {view_count} view definitions to {filepath}")
    return view_count


def export_for_description(server_name=None, database_name=None,
                           include_described=False, object_types=None):
    """
    Export catalog nodes for AI description generation.

    Args:
        server_name: Filter by server (optional)
        database_name: Filter by database (optional)
        include_described: If False, only export nodes without description
        object_types: List of types to include, e.g. ['DB_SERVER', 'DB_DATABASE', 'DB_SCHEMA', 'DB_TABLE', 'DB_VIEW', 'DB_COLUMN']
                     Default: tables, views, columns

    Returns:
        CSV string ready to paste into AI
    """
    if object_types is None:
        object_types = ['DB_TABLE', 'DB_VIEW', 'DB_COLUMN']

    conn = get_catalog_connection()
    cursor = conn.cursor()

    # Build query with optional filters
    query = """
        SELECT
            n.node_id,
            n.object_type_code,
            n.qualified_name,
            n.description,
            CASE
                WHEN n.object_type_code = 'DB_COLUMN' THEN c.data_type
                ELSE NULL
            END as extra_info
        FROM catalog.nodes n
        LEFT JOIN catalog.node_column c ON n.node_id = c.node_id
        WHERE n.object_type_code = ANY(%s)
          AND n.deleted_at IS NULL
    """
    params = [object_types]

    if not include_described:
        query += " AND (n.description IS NULL OR n.description = '')"

    if server_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"{server_name}/%")

    if database_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"%/{database_name}/%")

    query += " ORDER BY n.qualified_name"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Build CSV
    output = StringIO()
    writer = csv.writer(output, delimiter=';')

    # Header
    writer.writerow(['node_id', 'object_type', 'qualified_name', 'data_type',
                     'current_description', 'new_description'])

    # Data rows
    for row in rows:
        node_id, obj_type, qual_name, curr_desc, extra = row
        writer.writerow([
            node_id,
            obj_type,
            qual_name,
            extra or '',  # data_type for columns
            curr_desc or '',
            ''  # new_description - to be filled by AI
        ])

    return output.getvalue()


def export_to_file(filepath, **kwargs):
    """Export to a CSV file."""
    csv_content = export_for_description(**kwargs)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(csv_content)

    # Count rows (minus header)
    row_count = csv_content.count('\n') - 1
    print(f"Exported {row_count} nodes to {filepath}")
    return row_count


def import_descriptions(csv_content, dry_run=False, mode='add_and_update'):
    """
    Import descriptions from CSV.

    Args:
        csv_content: CSV string with new_description column filled
        dry_run: If True, only validate and report what would be updated
        mode: Import mode:
            - 'add_only': Only add descriptions where none exists
            - 'add_and_update': Add new and update existing (default)
            - 'overwrite_all': Update all, including clearing with [CLEAR]

    Returns:
        Dict with counts and details for UI display
    """
    reader = csv.DictReader(StringIO(csv_content), delimiter=';')

    results = {
        'updated': 0,
        'added': 0,
        'cleared': 0,
        'skipped_empty': 0,
        'skipped_unchanged': 0,
        'skipped_mode': 0,
        'errors': [],
        'changes': []  # List of changes for dry run display
    }

    # First, collect all node_ids we need to check
    rows_to_process = []
    for row in reader:
        node_id = row.get('node_id', '').strip()
        new_desc = row.get('new_description', '').strip()
        current_desc = row.get('current_description', '').strip()

        if not node_id:
            results['errors'].append("Missing node_id in row")
            continue

        try:
            rows_to_process.append({
                'node_id': int(node_id),
                'new_desc': new_desc,
                'current_desc': current_desc,
                'qualified_name': row.get('qualified_name', '')
            })
        except ValueError:
            results['errors'].append(f"Invalid node_id: {node_id}")

    # Get current descriptions from database for comparison
    if rows_to_process:
        conn = get_catalog_connection()
        cursor = conn.cursor()

        node_ids = [r['node_id'] for r in rows_to_process]
        cursor.execute("""
            SELECT node_id, description
            FROM catalog.nodes
            WHERE node_id = ANY(%s)
        """, (node_ids,))

        db_descriptions = {row[0]: row[1] or '' for row in cursor.fetchall()}
        cursor.close()
        conn.close()
    else:
        db_descriptions = {}

    # Process each row
    updates = []
    for row in rows_to_process:
        node_id = row['node_id']
        new_desc = row['new_desc']
        db_current = db_descriptions.get(node_id, '')
        qualified_name = row['qualified_name']

        # Handle empty new_description
        if not new_desc:
            results['skipped_empty'] += 1
            continue

        # Handle [CLEAR] special value
        is_clear = new_desc.upper() == '[CLEAR]'
        if is_clear:
            if mode != 'overwrite_all':
                results['skipped_mode'] += 1
                continue
            new_desc = ''  # Clear the description

        # Check if unchanged
        if new_desc == db_current:
            results['skipped_unchanged'] += 1
            continue

        # Check mode restrictions
        has_existing = bool(db_current)
        if mode == 'add_only' and has_existing:
            results['skipped_mode'] += 1
            continue

        # Determine change type
        if is_clear:
            change_type = 'clear'
            results['cleared'] += 1
        elif has_existing:
            change_type = 'update'
            results['updated'] += 1
        else:
            change_type = 'add'
            results['added'] += 1

        updates.append((node_id, new_desc))
        results['changes'].append({
            'node_id': node_id,
            'qualified_name': qualified_name,
            'type': change_type,
            'old': db_current[:50] + ('...' if len(db_current) > 50 else ''),
            'new': new_desc[:50] + ('...' if len(new_desc) > 50 else '')
        })

    if dry_run:
        total = results['added'] + results['updated'] + results['cleared']
        print(f"DRY RUN: Would process {total} nodes")
        print(f"  Add: {results['added']}, Update: {results['updated']}, Clear: {results['cleared']}")
        print(f"  Skipped - empty: {results['skipped_empty']}, unchanged: {results['skipped_unchanged']}, mode: {results['skipped_mode']}")
        for change in results['changes'][:10]:
            print(f"  [{change['type']}] {change['node_id']}: '{change['old']}' -> '{change['new']}'")
        if len(results['changes']) > 10:
            print(f"  ... and {len(results['changes']) - 10} more")
        return results

    # Actually update
    if updates:
        conn = get_catalog_connection()
        cursor = conn.cursor()

        for node_id, new_desc in updates:
            try:
                status = 'ai_generated' if new_desc else 'draft'
                cursor.execute("""
                    UPDATE catalog.nodes
                    SET description = %s,
                        description_status = %s,
                        updated_at = NOW()
                    WHERE node_id = %s
                """, (new_desc if new_desc else None, status, node_id))
            except Exception as e:
                results['errors'].append(f"Error updating {node_id}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

    total = results['added'] + results['updated'] + results['cleared']
    print(f"Processed {total} nodes")
    print(f"  Added: {results['added']}, Updated: {results['updated']}, Cleared: {results['cleared']}")
    if results['errors']:
        print(f"Errors: {len(results['errors'])}")
        for err in results['errors'][:5]:
            print(f"  {err}")

    return results


def import_from_file(filepath, dry_run=False):
    """Import from a CSV file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        csv_content = f.read()
    return import_descriptions(csv_content, dry_run=dry_run)


# === CLI ===

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Export/import catalog descriptions')
    subparsers = parser.add_subparsers(dest='command', help='Command')

    # Export command
    export_parser = subparsers.add_parser('export', help='Export nodes for description')
    export_parser.add_argument('-o', '--output', default='catalog_export.csv',
                               help='Output file path')
    export_parser.add_argument('--server', help='Filter by server name')
    export_parser.add_argument('--database', help='Filter by database name')
    export_parser.add_argument('--include-described', action='store_true',
                               help='Include nodes that already have descriptions')
    export_parser.add_argument('--types', nargs='+',
                               default=['DB_TABLE', 'DB_VIEW', 'DB_COLUMN'],
                               help='Object types to export')

    # Import command
    import_parser = subparsers.add_parser('import', help='Import descriptions from CSV')
    import_parser.add_argument('-i', '--input', required=True,
                               help='Input CSV file')
    import_parser.add_argument('--dry-run', action='store_true',
                               help='Validate only, do not update')

    # DDL export command
    ddl_parser = subparsers.add_parser('ddl', help='Export view DDL for AI analysis')
    ddl_parser.add_argument('-o', '--output', default='views_ddl.sql',
                            help='Output file path')
    ddl_parser.add_argument('--server', help='Filter by server name')
    ddl_parser.add_argument('--database', help='Filter by database name')
    ddl_parser.add_argument('--schema', help='Filter by schema name')
    ddl_parser.add_argument('--format', choices=['sql', 'markdown'], default='sql',
                            help='Output format: sql or markdown')
    ddl_parser.add_argument('--no-columns', action='store_true',
                            help='Exclude column information')

    args = parser.parse_args()

    if args.command == 'export':
        export_to_file(
            args.output,
            server_name=args.server,
            database_name=args.database,
            include_described=args.include_described,
            object_types=args.types
        )
    elif args.command == 'import':
        import_from_file(args.input, dry_run=args.dry_run)
    elif args.command == 'ddl':
        export_ddl_to_file(
            args.output,
            server_name=args.server,
            database_name=args.database,
            schema_name=args.schema,
            include_columns=not args.no_columns,
            output_format=args.format
        )
    else:
        parser.print_help()
