import psycopg2
import psycopg2.extras
import yaml
import datetime
import argparse
import logging
from logging.handlers import RotatingFileHandler
import os

os.makedirs('logfiles', exist_ok=True)

# Setup logging
log_filename = f"logfiles/catalog_extraction_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=5)
console_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[log_handler, console_handler]
)

summary = {
    'databases_added': 0,
    'schemas_added': 0,
    'tables_added': 0,
    'tables_updated': 0,
    'columns_added': 0,
    'columns_updated': 0,
    'databases_deleted': 0,
    'schemas_deleted': 0,
    'tables_deleted': 0,
    'columns_deleted': 0
}

def mark_deleted_databases(cur, seen_set, now, server_name):
    # Only databases for this server
    cur.execute("""
        SELECT database_name, server_name
        FROM metadata.catalog_databases main
        WHERE main.curr_id = 'Y' AND main.server_name = %s;
    """, (server_name,))
    existing = {tuple(row) for row in cur.fetchall()}
    to_delete = existing - seen_set
    for key_tuple in to_delete:
        cur.execute("""
            UPDATE metadata.catalog_databases AS main
            SET curr_id = 'N', date_updated = %s, date_deleted = %s
            WHERE main.curr_id = 'Y' AND main.database_name = %s AND main.server_name = %s;
        """, (now, now, *key_tuple))
    return len(to_delete)

def mark_deleted_schemas(cur, seen_set, now, db_id):
    cur.execute("""
        SELECT schema_name, database_id
        FROM metadata.catalog_schemas
        WHERE curr_id = 'Y' AND database_id = %s;
    """, (db_id,))
    existing = {tuple(row) for row in cur.fetchall()}
    to_delete = existing - seen_set
    for key_tuple in to_delete:
        cur.execute("""
            UPDATE metadata.catalog_schemas
            SET curr_id = 'N', date_updated = %s, date_deleted = %s
            WHERE curr_id = 'Y' AND schema_name = %s AND database_id = %s;
        """, (now, now, *key_tuple))
    return len(to_delete)

def mark_deleted_tables(cur, seen_set, now, schema_ids):
    # Only tables for these schemas
    cur.execute("""
        SELECT table_name, schema_id
        FROM metadata.catalog_tables main
        WHERE main.curr_id = 'Y' AND main.schema_id = ANY(%s);
    """, (list(schema_ids),))
    existing = {tuple(row) for row in cur.fetchall()}
    to_delete = existing - seen_set
    for key_tuple in to_delete:
        cur.execute("""
            UPDATE metadata.catalog_tables AS main
            SET curr_id = 'N', date_updated = %s, date_deleted = %s
            WHERE main.curr_id = 'Y' AND main.table_name = %s AND main.schema_id = %s;
        """, (now, now, *key_tuple))
    return len(to_delete)

def mark_deleted_columns(cur, seen_set, now, table_ids):
    # Only columns for these tables
    cur.execute("""
        SELECT column_name, table_id
        FROM metadata.catalog_columns main
        WHERE main.curr_id = 'Y' AND main.table_id = ANY(%s);
    """, (list(table_ids),))
    existing = {tuple(row) for row in cur.fetchall()}
    to_delete = existing - seen_set
    for key_tuple in to_delete:
        cur.execute("""
            UPDATE metadata.catalog_columns AS main
            SET curr_id = 'N', date_updated = %s, date_deleted = %s
            WHERE main.curr_id = 'Y' AND main.column_name = %s AND main.table_id = %s;
        """, (now, now, *key_tuple))
    return len(to_delete)

def load_config(path='servers_config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def connect_db(config, dbname=None):
    return psycopg2.connect(
        host=config["host"],
        port=5432,
        dbname=dbname or "postgres",
        user=config["user"],
        password=config["password"]
    )

def get_user_databases(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT datname FROM pg_database
            WHERE datistemplate = false AND datname NOT IN ('postgres');
        """)
        return [row[0] for row in cur.fetchall()]

def get_schemas(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema');
        """)
        return [row[0] for row in cur.fetchall()]

def get_tables(conn, schema):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s;
        """, (schema,))
        return cur.fetchall()

def get_columns(conn, schema, table):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """, (schema, table))
        return cur.fetchall()

def upsert_database(cur, dbname, server_name, now):
    cur.execute("""
        SELECT id FROM metadata.catalog_databases
        WHERE database_name = %s AND server_name = %s AND curr_id = 'Y';
    """, (dbname, server_name))
    existing = cur.fetchone()

    if not existing:
        cur.execute("""
            INSERT INTO metadata.catalog_databases (database_name, server_name, date_created, curr_id)
            VALUES (%s, %s, %s, 'Y') RETURNING id;
        """, (dbname, server_name, now))
        summary['databases_added'] += 1
        return cur.fetchone()[0]
    else:
        return existing[0]

def upsert_schema(cur, schema_name, db_id, now):
    cur.execute("""
        SELECT id FROM metadata.catalog_schemas
        WHERE schema_name = %s AND database_id = %s AND curr_id = 'Y';
    """, (schema_name, db_id))
    existing = cur.fetchone()

    if not existing:
        cur.execute("""
            INSERT INTO metadata.catalog_schemas (schema_name, database_id, date_created, curr_id)
            VALUES (%s, %s, %s, 'Y') RETURNING id;
        """, (schema_name, db_id, now))
        summary['schemas_added'] += 1
        return cur.fetchone()[0]
    else:
        return existing[0]

def upsert_table(cur, schema_id, table_name, table_type, now):
    cur.execute("""
        SELECT id, table_type FROM metadata.catalog_tables
        WHERE schema_id = %s AND table_name = %s AND curr_id = 'Y';
    """, (schema_id, table_name))
    existing = cur.fetchone()

    if existing:
        existing_id, existing_type = existing
        if existing_type == table_type:
            return existing_id
        cur.execute("""
            UPDATE metadata.catalog_tables
            SET curr_id = 'N', date_updated = %s
            WHERE id = %s;
        """, (now, existing_id))
        summary['tables_updated'] += 1
    else:
        summary['tables_added'] += 1

    cur.execute("""
        INSERT INTO metadata.catalog_tables (
            schema_id, table_name, table_type, date_created, curr_id
        ) VALUES (%s, %s, %s, %s, 'Y') RETURNING id;
    """, (schema_id, table_name, table_type, now))
    return cur.fetchone()[0]

def upsert_column(cur, table_id, column, now):
    cur.execute("""
        SELECT id, data_type, is_nullable, column_default, ordinal_position
        FROM metadata.catalog_columns
        WHERE table_id = %s AND column_name = %s AND curr_id = 'Y';
    """, (table_id, column['column_name']))
    existing = cur.fetchone()

    if existing:
        existing_id, dt, nullable, default, pos = existing
        if (dt == column['data_type'] and
            nullable == (column['is_nullable'] == 'YES') and
            default == column['column_default'] and
            pos == column['ordinal_position']):
            return existing_id
        cur.execute("""
            UPDATE metadata.catalog_columns
            SET curr_id = 'N', date_updated = %s
            WHERE id = %s;
        """, (now, existing_id))
        summary['columns_updated'] += 1
    else:
        summary['columns_added'] += 1

    cur.execute("""
        INSERT INTO metadata.catalog_columns (
            table_id, column_name, data_type, is_nullable, column_default,
            ordinal_position, date_created, curr_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'Y') RETURNING id;
    """, (
        table_id,
        column['column_name'],
        column['data_type'],
        column['is_nullable'] == 'YES',
        column['column_default'],
        column['ordinal_position'],
        now
    ))
    return cur.fetchone()[0]

def run():
    logging.info("Script starting")
    parser = argparse.ArgumentParser()
    parser.add_argument('--server')
    parser.add_argument('--dbname')
    args = parser.parse_args()
    logging.info(f"Arguments received: server={args.server}, dbname={args.dbname}")

    config = load_config()
    logging.info("Config loaded successfully")
    logging.debug(f"Config contents: {config}")  # Be careful with sensitive data
    try:
        catalog_conn = psycopg2.connect(**config['catalog_db'])
        logging.info("Successfully connected to catalog database")
    except Exception as e:
        logging.error(f"Failed to connect to catalog database: {e}")
        raise

    now = datetime.datetime.now()

    for server in config['servers']:
        logging.info(f"Checking server {server['name']}")
        if args.server and server['name'] != args.server:
            logging.info(f"Skipping server {server['name']} (doesn't match filter)")
            continue

        seen_dbs = set()
        try:
            admin_conn = connect_db(server)
            logging.info(f"Connected to server {server['name']}")
            dbs = get_user_databases(admin_conn)
            logging.info(f"Found databases: {dbs}")
            if not dbs:
                logging.warning("No databases found on server")
        except Exception as e:
            logging.error(f"Failed to process server {server['name']}: {e}")
            continue

        try:
            with catalog_conn.cursor() as cur:
                for dbname in dbs:
                    if args.dbname and dbname != args.dbname:
                        continue

                    seen_schemas = set()
                    schema_ids = set()
                    table_ids = set()
                    db_id = upsert_database(cur, dbname, server['name'], now)
                    seen_dbs.add((dbname, server['name']))

                    db_conn = connect_db(server, dbname)
                    schemas = get_schemas(db_conn)

                    for schema in schemas:
                        seen_tables = set()
                        schema_id = upsert_schema(cur, schema, db_id, now)
                        seen_schemas.add((schema, db_id))
                        schema_ids.add(schema_id)

                        tables = get_tables(db_conn, schema)
                        for tbl in tables:
                            seen_columns = set()
                            table_id = upsert_table(cur, schema_id, tbl['table_name'], tbl['table_type'], now)
                            seen_tables.add((tbl['table_name'], schema_id))
                            table_ids.add(table_id)

                            columns = get_columns(db_conn, schema, tbl['table_name'])
                            for col in columns:
                                upsert_column(cur, table_id, col, now)
                                seen_columns.add((col['column_name'], table_id))

                            # After all columns for this table
                            summary['columns_deleted'] += mark_deleted_columns(cur, seen_columns, now, list(table_ids))

                        # After all tables for this schema
                        summary['tables_deleted'] += mark_deleted_tables(cur, seen_tables, now, list(schema_ids))

                    # After all schemas for this database
                    summary['schemas_deleted'] += mark_deleted_schemas(cur, seen_schemas, now, db_id)

                    db_conn.close()

                # After all databases for this server
                summary['databases_deleted'] += mark_deleted_databases(cur, seen_dbs, now, server['name'])

                catalog_conn.commit()
        except Exception as e:
            logging.error(f"Error during upsert for a database on {server['host']}: {e}")
        finally:
            admin_conn.close()
            logging.info(f"Closed connection to server {server['name']}")


    catalog_conn.close()
    logging.info("Catalog update complete with version tracking.")
    print("\nSummary:")
    for key, val in summary.items():
        print(f"{key.replace('_', ' ').title()}: {val}")

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        raise
