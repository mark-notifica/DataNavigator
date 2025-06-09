import psycopg2
import psycopg2.extras
import yaml
import datetime
import argparse
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import re

# Setup logging
os.makedirs('logfiles', exist_ok=True)
log_filename = f"logfiles/catalog_extraction_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=5)
console_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[log_handler, console_handler]
)

logger = logging.getLogger(__name__)

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

ENV_PATTERN = re.compile(r'^\${(.+)}$')

def _resolve_env(obj):
    if isinstance(obj, dict):
        return {k: _resolve_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env(v) for v in obj]
    if isinstance(obj, str):
        m = ENV_PATTERN.match(obj)
        if m:
            return os.getenv(m.group(1), obj)
    return obj

def load_config(path='servers_config.yaml'):
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    return _resolve_env(data)

def connect_db(config, dbname=None):
    return psycopg2.connect(
        host=config["host"],
        port=5432,
        dbname=dbname or "postgres",
        user=config["user"],
        password=config["password"],
        connect_timeout=5
    )

def get_user_databases(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT datname FROM pg_database
            WHERE datistemplate = false AND datname NOT IN ('postgres')
        """)
        return [row[0] for row in cur.fetchall()]

def get_schemas(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
        """)
        return [row[0] for row in cur.fetchall()]

def get_tables(conn, schema):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
        """, (schema,))
        return cur.fetchall()

def get_columns(conn, schema, table):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return cur.fetchall()

def upsert_database(cur, dbname, server_name, now):
    cur.execute("""
        SELECT id FROM metadata.catalog_databases
        WHERE database_name = %s AND server_name = %s AND curr_id = 'Y'
    """, (dbname, server_name))
    existing = cur.fetchone()

    if not existing:
        cur.execute("""
            INSERT INTO metadata.catalog_databases (database_name, server_name, date_created, curr_id)
            VALUES (%s, %s, %s, 'Y') RETURNING id
        """, (dbname, server_name, now))
        summary['databases_added'] += 1
        return cur.fetchone()[0]
    return existing[0]

def upsert_schema(cur, schema_name, db_id, now):
    cur.execute("""
        SELECT id FROM metadata.catalog_schemas
        WHERE schema_name = %s AND database_id = %s AND curr_id = 'Y'
    """, (schema_name, db_id))
    existing = cur.fetchone()

    if not existing:
        cur.execute("""
            INSERT INTO metadata.catalog_schemas (schema_name, database_id, date_created, curr_id)
            VALUES (%s, %s, %s, 'Y') RETURNING id
        """, (schema_name, db_id, now))
        summary['schemas_added'] += 1
        return cur.fetchone()[0]
    return existing[0]

def upsert_table(cur, schema_id, table_name, table_type, now):
    cur.execute("""
        SELECT id, table_type FROM metadata.catalog_tables
        WHERE schema_id = %s AND table_name = %s AND curr_id = 'Y'
    """, (schema_id, table_name))
    existing = cur.fetchone()

    if existing:
        existing_id, existing_type = existing
        if existing_type == table_type:
            return existing_id
        cur.execute("""
            UPDATE metadata.catalog_tables
            SET curr_id = 'N', date_updated = %s
            WHERE id = %s
        """, (now, existing_id))
        summary['tables_updated'] += 1
    else:
        summary['tables_added'] += 1

    cur.execute("""
        INSERT INTO metadata.catalog_tables (schema_id, table_name, table_type, date_created, curr_id)
        VALUES (%s, %s, %s, %s, 'Y') RETURNING id
    """, (schema_id, table_name, table_type, now))
    return cur.fetchone()[0]

def upsert_column(cur, table_id, column, now):
    cur.execute("""
        SELECT id, data_type, is_nullable, column_default, ordinal_position
        FROM metadata.catalog_columns
        WHERE table_id = %s AND column_name = %s AND curr_id = 'Y'
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
            WHERE id = %s
        """, (now, existing_id))
        summary['columns_updated'] += 1
    else:
        summary['columns_added'] += 1

    cur.execute("""
        INSERT INTO metadata.catalog_columns (
            table_id, column_name, data_type, is_nullable, column_default,
            ordinal_position, date_created, curr_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'Y') RETURNING id
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

def mark_deleted_databases(cur, seen_set, now, server_name, processed_dbs=None):
    try:
        query = """
            SELECT database_name, server_name 
            FROM metadata.catalog_databases
            WHERE curr_id = 'Y' AND server_name = %s
        """
        params = [server_name]
        
        if processed_dbs:
            query += " AND database_name = ANY(%s)"
            params.append(processed_dbs)
        
        cur.execute(query, params)
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for dbname, srvname in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_databases
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND database_name = %s AND server_name = %s
            """, (now, now, dbname, srvname))
        
        count = len(to_delete)
        logger.info(f"Marked {count} databases as deleted (Scope: {'current run' if processed_dbs else 'all'})")
        return count
    except Exception as e:
        logger.error(f"Failed to mark deleted databases: {e}")
        raise

def mark_deleted_schemas(cur, seen_set, now, db_id, processed_schemas=None):
    try:
        query = """
            SELECT schema_name, database_id
            FROM metadata.catalog_schemas
            WHERE curr_id = 'Y' AND database_id = %s
        """
        params = [db_id]
        
        if processed_schemas:
            query += " AND schema_name = ANY(%s)"
            params.append(processed_schemas)
        
        cur.execute(query, params)
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for schema_name, dbid in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_schemas
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND schema_name = %s AND database_id = %s
            """, (now, now, schema_name, dbid))
        
        count = len(to_delete)
        logger.info(f"Marked {count} schemas as deleted")
        return count
    except Exception as e:
        logger.error(f"Failed to mark deleted schemas: {e}")
        raise

def mark_deleted_tables(cur, seen_set, now, schema_ids, processed_tables=None):
    try:
        query = """
            SELECT table_name, schema_id
            FROM metadata.catalog_tables
            WHERE curr_id = 'Y' AND schema_id = ANY(%s)
        """
        params = [list(schema_ids)]
        
        if processed_tables:
            query += " AND table_name = ANY(%s)"
            params.append(processed_tables)
        
        cur.execute(query, params)
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for table_name, sid in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_tables
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND table_name = %s AND schema_id = %s
            """, (now, now, table_name, sid))
        
        count = len(to_delete)
        logger.info(f"Marked {count} tables as deleted")
        return count
    except Exception as e:
        logger.error(f"Failed to mark deleted tables: {e}")
        raise

def mark_deleted_columns(cur, seen_set, now, table_ids, processed_columns=None):
    try:
        query = """
            SELECT column_name, table_id
            FROM metadata.catalog_columns
            WHERE curr_id = 'Y' AND table_id = ANY(%s)
        """
        params = [list(table_ids)]
        
        if processed_columns:
            query += " AND column_name = ANY(%s)"
            params.append(processed_columns)
        
        cur.execute(query, params)
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for col_name, tid in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_columns
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND column_name = %s AND table_id = %s
            """, (now, now, col_name, tid))
        
        count = len(to_delete)
        logger.info(f"Marked {count} columns as deleted")
        return count
    except Exception as e:
        logger.error(f"Failed to mark deleted columns: {e}")
        raise

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server')
    parser.add_argument('--dbname')
    args = parser.parse_args()

    config = load_config()
    catalog_conn = psycopg2.connect(**config['catalog_db'])
    now = datetime.datetime.now()

    for server in config['servers']:
        if args.server and server['name'] != args.server:
            continue

        seen_dbs = set()
        try:
            admin_conn = connect_db(server)
            dbs = get_user_databases(admin_conn)
            
            if args.dbname:  # Filter to specific DB if requested
                if args.dbname not in dbs:
                    logger.error(f"Database {args.dbname} not found")
                    continue
                dbs = [args.dbname]

            with catalog_conn.cursor() as cur:
                processed_dbs = []
                
                for dbname in dbs:
                    processed_dbs.append(dbname)
                    seen_schemas = set()
                    schema_ids = set()
                    
                    try:
                        db_conn = connect_db(server, dbname)
                        db_id = upsert_database(cur, dbname, server['name'], now)
                        seen_dbs.add((dbname, server['name']))
                        
                        schemas = get_schemas(db_conn)
                        processed_schemas = []
                        
                        for schema in schemas:
                            processed_schemas.append(schema)
                            seen_tables = set()
                            table_ids = set()
                            
                            schema_id = upsert_schema(cur, schema, db_id, now)
                            seen_schemas.add((schema, db_id))
                            schema_ids.add(schema_id)
                            
                            tables = get_tables(db_conn, schema)
                            processed_tables = []
                            
                            for tbl in tables:
                                processed_tables.append(tbl['table_name'])
                                seen_columns = set()
                                
                                table_id = upsert_table(cur, schema_id, tbl['table_name'], tbl['table_type'], now)
                                seen_tables.add((tbl['table_name'], schema_id))
                                table_ids.add(table_id)
                                
                                columns = get_columns(db_conn, schema, tbl['table_name'])
                                processed_columns = [c['column_name'] for c in columns]
                                
                                for col in columns:
                                    upsert_column(cur, table_id, col, now)
                                    seen_columns.add((col['column_name'], table_id))
                                
                                # Mark deleted columns
                                deleted_cols = mark_deleted_columns(
                                    cur, seen_columns, now, [table_id],
                                    processed_columns if args.dbname else None
                                )
                                summary['columns_deleted'] += deleted_cols
                            
                            # Mark deleted tables
                            deleted_tables = mark_deleted_tables(
                                cur, seen_tables, now, [schema_id],
                                processed_tables if args.dbname else None
                            )
                            summary['tables_deleted'] += deleted_tables
                        
                        # Mark deleted schemas
                        deleted_schemas = mark_deleted_schemas(
                            cur, seen_schemas, now, db_id,
                            processed_schemas if args.dbname else None
                        )
                        summary['schemas_deleted'] += deleted_schemas
                        
                    except Exception as e:
                        logger.error(f"Error processing {dbname}: {e}")
                        catalog_conn.rollback()
                    finally:
                        if 'db_conn' in locals():
                            db_conn.close()
                
                # Mark deleted databases
                deleted_dbs = mark_deleted_databases(
                    cur, seen_dbs, now, server['name'],
                    processed_dbs if args.dbname else None
                )
                summary['databases_deleted'] += deleted_dbs
                
                catalog_conn.commit()
                
        except Exception as e:
            logger.error(f"Error processing server {server['name']}: {e}")
        finally:
            if 'admin_conn' in locals():
                admin_conn.close()

    catalog_conn.close()
    logger.info("Catalog update complete")
    print("\nSummary:")
    for key, val in summary.items():
        print(f"{key.replace('_', ' ').title()}: {val}")

if __name__ == '__main__':
    run()