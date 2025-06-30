import psycopg2
import psycopg2.extras
import yaml
import re
import datetime
import argparse
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

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

def mark_deleted_databases(cur, seen_set, now, server_name):
    try:
        cur.execute("""
            SELECT database_name, server_name
            FROM metadata.catalog_databases
            WHERE curr_id = 'Y' AND server_name = %s;
        """, (server_name,))
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for key_tuple in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_databases
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND database_name = %s AND server_name = %s;
            """, (now, now, *key_tuple))
        
        logger.info(f"Marked {len(to_delete)} databases as deleted for server {server_name}")
        return len(to_delete)
    except Exception as e:
        logger.error(f"Error marking deleted databases: {e}")
        raise

def mark_deleted_schemas(cur, seen_set, now, db_id):
    try:
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
        
        logger.info(f"Marked {len(to_delete)} schemas as deleted for database ID {db_id}")
        return len(to_delete)
    except Exception as e:
        logger.error(f"Error marking deleted schemas: {e}")
        raise

def mark_deleted_tables(cur, seen_set, now, schema_ids):
    try:
        cur.execute("""
            SELECT table_name, schema_id
            FROM metadata.catalog_tables
            WHERE curr_id = 'Y' AND schema_id = ANY(%s);
        """, (list(schema_ids),))
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for key_tuple in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_tables
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND table_name = %s AND schema_id = %s;
            """, (now, now, *key_tuple))
        
        logger.info(f"Marked {len(to_delete)} tables as deleted for schema IDs {schema_ids}")
        return len(to_delete)
    except Exception as e:
        logger.error(f"Error marking deleted tables: {e}")
        raise

def mark_deleted_columns(cur, seen_set, now, table_ids):
    try:
        cur.execute("""
            SELECT column_name, table_id
            FROM metadata.catalog_columns
            WHERE curr_id = 'Y' AND table_id = ANY(%s);
        """, (list(table_ids),))
        existing = {tuple(row) for row in cur.fetchall()}
        to_delete = existing - seen_set
        
        for key_tuple in to_delete:
            cur.execute("""
                UPDATE metadata.catalog_columns
                SET curr_id = 'N', date_updated = %s, date_deleted = %s
                WHERE curr_id = 'Y' AND column_name = %s AND table_id = %s;
            """, (now, now, *key_tuple))
        
        logger.info(f"Marked {len(to_delete)} columns as deleted for table IDs {table_ids}")
        return len(to_delete)
    except Exception as e:
        logger.error(f"Error marking deleted columns: {e}")
        raise

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
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
            if not config:
                raise ValueError("Config file is empty")
            return _resolve_env(config)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        raise

def connect_db(config, dbname=None):
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=5432,
            dbname=dbname or "postgres",
            user=config["user"],
            password=config["password"],
            connect_timeout=5
        )
        logger.info(f"Connected to {config['host']}, database: {dbname or 'postgres'}")
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to {config['host']}: {e}")
        raise


def get_user_databases(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datistemplate = false 
                AND datname NOT IN ('postgres', 'template0', 'template1');
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting user databases: {e}")
        raise

def get_schemas(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting schemas: {e}")
        raise

def get_tables(conn, schema):
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s;
            """, (schema,))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Error getting tables for schema {schema}: {e}")
        raise

def get_columns(conn, schema, table):
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable, 
                       column_default, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Error getting columns for {schema}.{table}: {e}")
        raise

def upsert_database(cur, dbname, server_name, now):
    try:
        cur.execute("""
            SELECT id FROM metadata.catalog_databases
            WHERE database_name = %s AND server_name = %s AND curr_id = 'Y';
        """, (dbname, server_name))
        existing = cur.fetchone()

        if not existing:
            cur.execute("""
                INSERT INTO metadata.catalog_databases 
                (database_name, server_name, date_created, curr_id)
                VALUES (%s, %s, %s, 'Y') RETURNING id;
            """, (dbname, server_name, now))
            db_id = cur.fetchone()[0]
            summary['databases_added'] += 1
            logger.info(f"Added new database: {dbname} on server {server_name}")
            return db_id
        else:
            logger.debug(f"Database exists: {dbname} on server {server_name}")
            return existing[0]
    except Exception as e:
        logger.error(f"Error upserting database {dbname}: {e}")
        raise

def upsert_schema(cur, schema_name, db_id, now):
    try:
        cur.execute("""
            SELECT id FROM metadata.catalog_schemas
            WHERE schema_name = %s AND database_id = %s AND curr_id = 'Y';
        """, (schema_name, db_id))
        existing = cur.fetchone()

        if not existing:
            cur.execute("""
                INSERT INTO metadata.catalog_schemas 
                (schema_name, database_id, date_created, curr_id)
                VALUES (%s, %s, %s, 'Y') RETURNING id;
            """, (schema_name, db_id, now))
            schema_id = cur.fetchone()[0]
            summary['schemas_added'] += 1
            logger.info(f"Added new schema: {schema_name} in database ID {db_id}")
            return schema_id
        else:
            logger.debug(f"Schema exists: {schema_name} in database ID {db_id}")
            return existing[0]
    except Exception as e:
        logger.error(f"Error upserting schema {schema_name}: {e}")
        raise

def upsert_table(cur, schema_id, table_name, table_type, now):
    try:
        cur.execute("""
            SELECT id, table_type FROM metadata.catalog_tables
            WHERE schema_id = %s AND table_name = %s AND curr_id = 'Y';
        """, (schema_id, table_name))
        existing = cur.fetchone()

        if existing:
            existing_id, existing_type = existing
            if existing_type == table_type:
                logger.debug(f"Table unchanged: {table_name} in schema ID {schema_id}")
                return existing_id
            
            cur.execute("""
                UPDATE metadata.catalog_tables
                SET curr_id = 'N', date_updated = %s
                WHERE id = %s;
            """, (now, existing_id))
            summary['tables_updated'] += 1
            logger.info(f"Updated table: {table_name} in schema ID {schema_id}")
        else:
            summary['tables_added'] += 1

        cur.execute("""
            INSERT INTO metadata.catalog_tables (
                schema_id, table_name, table_type, date_created, curr_id
            ) VALUES (%s, %s, %s, %s, 'Y') RETURNING id;
        """, (schema_id, table_name, table_type, now))
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Error upserting table {table_name}: {e}")
        raise

def upsert_column(cur, table_id, column, now):
    try:
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
                logger.debug(f"Column unchanged: {column['column_name']} in table ID {table_id}")
                return existing_id
            
            cur.execute("""
                UPDATE metadata.catalog_columns
                SET curr_id = 'N', date_updated = %s
                WHERE id = %s;
            """, (now, existing_id))
            summary['columns_updated'] += 1
            logger.info(f"Updated column: {column['column_name']} in table ID {table_id}")
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
    except Exception as e:
        logger.error(f"Error upserting column {column['column_name']}: {e}")
        raise

def run():
    logger.info("Script starting")
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='PostgreSQL Catalog Extractor')
    parser.add_argument('--server', help='Process only this server')
    parser.add_argument('--dbname', help='Process only this database')
    args = parser.parse_args()
    logger.info(f"Arguments: server={args.server}, dbname={args.dbname}")

    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")
        
        # Validate configuration
        if 'catalog_db' not in config:
            raise ValueError("Missing 'catalog_db' in configuration")
        if 'servers' not in config or not isinstance(config['servers'], list):
            raise ValueError("Missing or invalid 'servers' in configuration")

        # Connect to catalog database
        catalog_conn = None
        try:
            catalog_conn = psycopg2.connect(**config['catalog_db'])
            logger.info("Connected to catalog database")
            
            now = datetime.datetime.now()
            processed_servers = 0

            # Process each server
            for server in config['servers']:
                server_name = server.get('name', 'unnamed')
                logger.info(f"Processing server: {server_name}")

                # Skip if server filter doesn't match
                if args.server and server_name != args.server:
                    logger.info(f"Skipping server {server_name} (doesn't match filter)")
                    continue

                admin_conn = None
                seen_dbs = set()
                
                try:
                    # Connect to admin database (postgres)
                    admin_conn = connect_db(server)
                    logger.info(f"Connected to server {server_name}")

                    # Get list of databases
                    dbs = get_user_databases(admin_conn)
                    logger.info(f"Found {len(dbs)} databases on server {server_name}")
                    
                    if not dbs:
                        logger.warning(f"No databases found on server {server_name}")
                        continue

                    # Process each database
                    with catalog_conn.cursor() as cur:
                        for dbname in dbs:
                            # Skip if database filter doesn't match
                            if args.dbname and dbname != args.dbname:
                                logger.info(f"Skipping database {dbname} (doesn't match filter)")
                                continue

                            db_conn = None
                            try:
                                logger.info(f"Processing database: {dbname}")
                                
                                # Connect to target database
                                db_conn = connect_db(server, dbname)
                                
                                # Upsert database record
                                db_id = upsert_database(cur, dbname, server_name, now)
                                seen_dbs.add((dbname, server_name))
                                
                                # Get and process schemas
                                schemas = get_schemas(db_conn)
                                seen_schemas = set()
                                schema_ids = set()
                                
                                for schema in schemas:
                                    logger.info(f"Processing schema: {schema}")
                                    
                                    # Upsert schema record
                                    schema_id = upsert_schema(cur, schema, db_id, now)
                                    seen_schemas.add((schema, db_id))
                                    schema_ids.add(schema_id)
                                    
                                    # Get and process tables
                                    tables = get_tables(db_conn, schema)
                                    seen_tables = set()
                                    table_ids = set()
                                    
                                    for tbl in tables:
                                        logger.debug(f"Processing table: {tbl['table_name']}")
                                        
                                        # Upsert table record
                                        table_id = upsert_table(cur, schema_id, tbl['table_name'], tbl['table_type'], now)
                                        seen_tables.add((tbl['table_name'], schema_id))
                                        table_ids.add(table_id)
                                        
                                        # Get and process columns
                                        columns = get_columns(db_conn, schema, tbl['table_name'])
                                        seen_columns = set()
                                        
                                        for col in columns:
                                            # Upsert column record
                                            upsert_column(cur, table_id, col, now)
                                            seen_columns.add((col['column_name'], table_id))
                                        
                                        # Mark deleted columns for this table
                                        deleted_cols = mark_deleted_columns(cur, seen_columns, now, [table_id])
                                        summary['columns_deleted'] += deleted_cols
                                        if deleted_cols:
                                            logger.info(f"Marked {deleted_cols} columns as deleted for table {tbl['table_name']}")
                                    
                                    # Mark deleted tables for this schema
                                    deleted_tables = mark_deleted_tables(cur, seen_tables, now, [schema_id])
                                    summary['tables_deleted'] += deleted_tables
                                    if deleted_tables:
                                        logger.info(f"Marked {deleted_tables} tables as deleted for schema {schema}")
                                
                                # Mark deleted schemas for this database
                                deleted_schemas = mark_deleted_schemas(cur, seen_schemas, now, db_id)
                                summary['schemas_deleted'] += deleted_schemas
                                if deleted_schemas:
                                    logger.info(f"Marked {deleted_schemas} schemas as deleted for database {dbname}")
                                
                            except Exception as e:
                                logger.error(f"Error processing database {dbname}: {e}")
                                if catalog_conn:
                                    catalog_conn.rollback()
                                continue
                            finally:
                                if db_conn:
                                    db_conn.close()
                                    logger.debug(f"Closed connection to database {dbname}")

                        # Mark deleted databases for this server
                        deleted_dbs = mark_deleted_databases(cur, seen_dbs, now, server_name)
                        summary['databases_deleted'] += deleted_dbs
                        if deleted_dbs:
                            logger.info(f"Marked {deleted_dbs} databases as deleted for server {server_name}")
                        
                        # Commit changes for this server
                        catalog_conn.commit()
                        logger.info(f"Completed processing server {server_name}")
                        processed_servers += 1

                except Exception as e:
                    logger.error(f"Error processing server {server_name}: {e}")
                    if catalog_conn:
                        catalog_conn.rollback()
                    continue
                finally:
                    if admin_conn:
                        admin_conn.close()
                        logger.debug(f"Closed connection to server {server_name}")

            # Final summary
            logger.info(f"Processing complete. Processed {processed_servers} servers.")
            print("\nSummary:")
            for key, val in summary.items():
                print(f"{key.replace('_', ' ').title()}: {val}")

        except Exception as e:
            logger.error(f"Catalog database error: {e}")
            raise
        finally:
            if catalog_conn:
                catalog_conn.close()
                logger.info("Closed connection to catalog database")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    run()