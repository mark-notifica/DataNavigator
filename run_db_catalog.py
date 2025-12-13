"""
Run a full catalog extraction and save to database.
"""
print("Starting script...")

from extractor import get_all_schemas, get_all_tables, get_columns
print("Extractor imported")
from storage import save_full_catalog
print("Storage imported")
import os
from dotenv import load_dotenv

load_dotenv()


def run_extraction():
    # Get server/database from your env
    server_name = os.getenv("SOURCE_DB_HOST", "localhost")
    database_name = os.getenv("SOURCE_DB_NAME", "1054")

    print(f"Extracting from {server_name}/{database_name}...")

    # 1. Get schemas
    schemas = get_all_schemas()
    print(f"Found {len(schemas)} schemas")

    # 2. Get tables per schema
    tables_by_schema = {}
    all_tables = get_all_tables()
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
        cols = get_columns(t['table'], t['schema'])
        columns_by_table[(t['schema'], t['table'])] = cols
    print(f"Extracted columns for all tables")

    # 4. Save everything
    save_full_catalog(server_name, database_name, schemas, tables_by_schema, columns_by_table)


if __name__ == "__main__":
    run_extraction()