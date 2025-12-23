"""
Export and import catalog descriptions.
Export to CSV for AI enrichment, import back with generated descriptions.
"""

import csv
from io import StringIO
from connection_db_postgres import get_catalog_connection


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


def import_descriptions(csv_content, dry_run=False):
    """
    Import descriptions from CSV.

    Args:
        csv_content: CSV string with new_description column filled
        dry_run: If True, only validate and report what would be updated

    Returns:
        Dict with counts: {'updated': n, 'skipped': n, 'errors': [...]}
    """
    reader = csv.DictReader(StringIO(csv_content), delimiter=';')

    results = {'updated': 0, 'skipped': 0, 'errors': []}
    updates = []

    for row in reader:
        node_id = row.get('node_id', '').strip()
        new_desc = row.get('new_description', '').strip()

        if not node_id:
            results['errors'].append("Missing node_id in row")
            continue

        if not new_desc:
            results['skipped'] += 1
            continue

        try:
            updates.append((int(node_id), new_desc))
        except ValueError:
            results['errors'].append(f"Invalid node_id: {node_id}")

    if dry_run:
        results['updated'] = len(updates)
        print(f"DRY RUN: Would update {len(updates)} nodes")
        for node_id, desc in updates[:5]:
            print(f"  {node_id}: {desc[:50]}...")
        if len(updates) > 5:
            print(f"  ... and {len(updates) - 5} more")
        return results

    # Actually update
    conn = get_catalog_connection()
    cursor = conn.cursor()

    for node_id, new_desc in updates:
        try:
            cursor.execute("""
                UPDATE catalog.nodes
                SET description = %s,
                    description_status = 'ai_generated',
                    updated_at = NOW()
                WHERE node_id = %s
            """, (new_desc, node_id))
            results['updated'] += 1
        except Exception as e:
            results['errors'].append(f"Error updating {node_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Updated {results['updated']} nodes")
    if results['skipped']:
        print(f"Skipped {results['skipped']} (no new description)")
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
    else:
        parser.print_help()
