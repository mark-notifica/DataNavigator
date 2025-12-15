"""
Run a full catalog extraction and save to database.
"""
import argparse
from extractor import get_all_schemas, get_all_tables, get_columns
from storage import save_full_catalog


def run_extraction(server_name, server_alias, database_name, host, port, user, password):
    print(f"Extracting from {server_name}/{database_name}...")
    if server_alias:
        print(f"  Server alias: {server_alias}")

    # 1. Get schemas
    schemas = get_all_schemas(host, port, database_name, user, password)
    print(f"Found {len(schemas)} schemas")

    # 2. Get tables per schema
    tables_by_schema = {}
    all_tables = get_all_tables(host, port, database_name, user, password)
    for t in all_tables:
        schema = t['schema']
        table = t['table']
        if schema not in tables_by_schema:
            tables_by_schema[schema] = []
        tables_by_schema[schema].append(table)
    print(f"Found {len(all_tables)} tables")

    # 3. Get columns per table
    columns_by_table = {}
    for t in all_tables:
        cols = get_columns(t['table'], t['schema'], host, port, database_name, user, password)
        columns_by_table[(t['schema'], t['table'])] = cols
    print("Extracted columns for all tables")

    # 4. Save everything
    save_full_catalog(
        server_name,
        server_alias,
        database_name,
        schemas,
        tables_by_schema,
        columns_by_table
    )
    print("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Catalog a database')
    parser.add_argument('--server', required=True, help='Server name (e.g., VPS2)')
    parser.add_argument('--alias', default='', help='Server alias (e.g., Production)')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--host', required=True, help='Connection host/IP')
    parser.add_argument('--port', default='5432', help='Connection port')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')

    args = parser.parse_args()

    run_extraction(
        args.server,
        args.alias,
        args.database,
        args.host,
        args.port,
        args.user,
        args.password
    )
