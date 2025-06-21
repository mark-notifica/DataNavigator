import psycopg2
import psycopg2.extras
import pyodbc
# import yaml
import datetime
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import re
from pathlib import Path
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Setup logging conditionally
if __name__ == "__main__":
    # Only setup initial logging if this is the main script
    script_dir = Path(__file__).parent
    log_dir = script_dir / 'logfiles' / 'database_server'
    log_dir.mkdir(parents=True, exist_ok=True)

    # Don't setup logging here - let the main() function handle it
    logger = logging.getLogger(__name__)

summary = {
    'databases_added': 0, 'databases_updated': 0,
    'schemas_added': 0, 'schemas_updated': 0,
    'tables_added': 0, 'tables_updated': 0,
    'views_added': 0, 'views_updated': 0,
    'view_definitions_added': 0, 'view_definitions_updated': 0,
    'columns_added': 0, 'columns_updated': 0,
    'databases_deleted': 0, 'schemas_deleted': 0, 
    'tables_deleted': 0, 'views_deleted': 0, 
    'view_definitions_deleted': 0, 'columns_deleted': 0
}

# DataNavigator database connection configuration
CATALOG_DB_CONFIG = {
    'host': os.getenv('NAV_DB_HOST'),
    'port': os.getenv('NAV_DB_PORT'),
    'database': os.getenv('NAV_DB_NAME'),  
    'user': os.getenv('NAV_DB_USER'),
    'password': os.getenv('NAV_DB_PASSWORD')
}

# Catalog schema name
CATALOG_SCHEMA = 'catalog'

def get_catalog_connection():
    """Get connection to the DataNavigator catalog database"""
    try:
        conn = psycopg2.connect(**CATALOG_DB_CONFIG)
        logger.info(f"Connected to DataNavigator catalog database: {CATALOG_DB_CONFIG['host']}:{CATALOG_DB_CONFIG['port']}/{CATALOG_DB_CONFIG['database']}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to catalog database: {e}")
        raise

def get_source_connections():
    """Get all source database connections from config.connections table"""
    catalog_conn = get_catalog_connection()
    try:
        with catalog_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, name, connection_type, host, port, username, password, database_name 
                FROM config.connections 
                WHERE connection_type IN ('PostgreSQL', 'Azure SQL Server')
                ORDER BY id
            """)
            connections = cursor.fetchall()
            logger.info(f"Found {len(connections)} source database connections")
            return connections
    finally:
        catalog_conn.close()

def connect_to_source_database(connection_info):
    """Connect to a source database using connection info from config.connections"""
    try:
        if connection_info['connection_type'] == 'PostgreSQL':
            # For PostgreSQL, connect to specific database or default to 'postgres'
            database_name = connection_info['database_name'] or 'postgres'
            conn = psycopg2.connect(
                host=connection_info['host'],
                port=connection_info['port'],
                database=database_name,
                user=connection_info['username'],
                password=connection_info['password']
            )
            logger.info(f"Connected to PostgreSQL database: {database_name} on {connection_info['host']}")
            
        elif connection_info['connection_type'] == 'Azure SQL Server':
            # For Azure SQL Server, connect to specific database or default to 'master'
            database_name = connection_info['database_name'] or 'master'
            
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={connection_info['host']};"
                f"PORT={connection_info['port']};"
                f"DATABASE={database_name};"
                f"UID={connection_info['username']};"
                f"PWD={connection_info['password']};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            
            conn = pyodbc.connect(connection_string)
            logger.info(f"Connected to Azure SQL Server database: {database_name} on {connection_info['host']}")
        
        else:
            logger.error(f"Unknown connection type: {connection_info['connection_type']}")
            return None
        
        return conn
        
    except Exception as e:
        logger.error(f"Failed to connect to source database {connection_info['name']}: {e}")
        return None

def get_connection_by_server_name(server_name: str):
    """Haalt connectie-info uit config.connections o.b.v. server_name"""
    all_conns = get_source_connections()
    for conn in all_conns:
        if conn["name"] == server_name:
            return conn
    raise ValueError(f"Geen connectie gevonden voor server_name '{server_name}'")

# Remove or update the old env resolution functions since we're using .env now
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

# Update any existing functions that reference the old schema to use 'catalog'

def setup_logging_with_run_id(catalog_run_id=None):
    """Setup logging with optional run ID in filename"""
    # Clear any existing handlers first
    logger = logging.getLogger(__name__)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Clear root logger handlers too
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create logfiles directory structure
    script_dir = Path(__file__).parent  # data_catalog directory
    log_dir = script_dir / 'logfiles' / 'database_server'
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Include run ID in log filename if available
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    if catalog_run_id:
        log_filename = log_dir / f"catalog_extraction_{timestamp}_run_{catalog_run_id}.log"
    else:
        log_filename = log_dir / f"catalog_extraction_{timestamp}.log"
    
    # Setup new handlers
    log_handler = RotatingFileHandler(str(log_filename), maxBytes=5*1024*1024, backupCount=5)
    console_handler = logging.StreamHandler()
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[log_handler, console_handler],
        force=True  # Force reconfiguration
    )
    
    # Return path relative to PROJECT ROOT (not data_catalog)
    # Go up one level from script_dir to get to project root
    project_root = script_dir.parent
    relative_path = log_filename.relative_to(project_root)
    
    return str(relative_path)  # Returns: data_catalog/logfiles/database_server/catalog_extraction_xxx.log

def main():
    """Main cataloging process"""
    
    # Add argument parsing
    parser = argparse.ArgumentParser(description='Catalog database metadata')
    parser.add_argument('--connection-id', type=int, help='Specific connection ID to catalog')
    parser.add_argument('--databases', type=str, help='Comma-separated list of databases to catalog')
    args = parser.parse_args()
    
    logger.info("Starting catalog extraction process")
    
    # Parse databases argument
    databases_to_catalog = None
    if args.databases:
        databases_to_catalog = [db.strip() for db in args.databases.split(',')]
        logger.info(f"Specific databases requested: {databases_to_catalog}")
    
    # Get source database connections
    if args.connection_id:
        # Catalog only specific connection
        logger.info(f"Cataloging specific connection ID: {args.connection_id}")
        source_connections = get_specific_connection(args.connection_id)
    else:
        # Catalog all connections
        logger.info("Cataloging all connections")
        source_connections = get_source_connections()
    
    if not source_connections:
        logger.warning("No source database connections found")
        return
    
    # Initialize summary for all connections
    summary = {
        'databases_added': 0, 'databases_updated': 0, 'schemas_added': 0, 'schemas_updated': 0,
        'tables_added': 0, 'tables_updated': 0, 'views_added': 0, 'views_updated': 0,
        'view_definitions_added': 0, 'view_definitions_updated': 0, 'columns_added': 0, 'columns_updated': 0,
        'databases_deleted': 0, 'schemas_deleted': 0, 'tables_deleted': 0, 'views_deleted': 0,
        'view_definitions_deleted': 0, 'columns_deleted': 0
    }
    
    # Process each source database connection
    for connection_info in source_connections:
        logger.info(f"Processing connection: {connection_info['name']}")
        
        # Start catalog run for this connection with database information
        catalog_conn = get_catalog_connection()
        try:
            # Pass databases_to_catalog to start_catalog_run
            catalog_run_id = start_catalog_run(catalog_conn, connection_info, databases_to_catalog)
            catalog_conn.commit()
            logger.info(f"Catalog run {catalog_run_id} committed and ready")
        except Exception as e:
            catalog_conn.rollback()
            logger.error(f"Failed to create catalog run: {e}")
            catalog_run_id = None
        finally:
            catalog_conn.close()
        
        if catalog_run_id:
            # Setup logging with run ID
            setup_logging_with_run_id(catalog_run_id)
            
            try:
                # Process the connection with catalog_run_id and specific databases
                connection_summary = catalog_connection_databases(connection_info, catalog_run_id, databases_to_catalog)
                
                # Merge connection summary into overall summary
                for key in summary:
                    summary[key] += connection_summary.get(key, 0)
                
                # Mark run as completed
                complete_conn = get_catalog_connection()
                try:
                    complete_catalog_run(complete_conn, catalog_run_id, connection_summary)
                    complete_conn.commit()
                except Exception as e:
                    logger.error(f"Failed to complete catalog run {catalog_run_id}: {e}")
                    complete_conn.rollback()
                finally:
                    complete_conn.close()
                    
            except Exception as e:
                # Mark run as failed
                fail_conn = get_catalog_connection()
                try:
                    fail_catalog_run(fail_conn, catalog_run_id, str(e))
                    fail_conn.commit()
                except:
                    fail_conn.rollback()
                finally:
                    fail_conn.close()
                logger.error(f"Failed to catalog connection {connection_info['name']}: {e}")
        else:
            logger.error(f"Failed to start catalog run for {connection_info['name']}")
    
    # Log summary
    logger.info("Catalog extraction completed")
    logger.info(f"Summary: {summary}")

# def process_single_connection(connection_info, databases_to_catalog=None):
#     """Process a single connection with specified databases"""
#     catalog_conn = get_catalog_connection()
    
#     try:
#         # Start catalog run with database information
#         run_id = start_catalog_run(catalog_conn, connection_info, databases_to_catalog)
        
#         # ... rest of your cataloging logic ...
        
#     except Exception as e:
#         logger.error(f"Error processing connection {connection_info['name']}: {e}")
#     finally:
#         catalog_conn.close()

def catalog_connection_databases(connection_info, catalog_run_id, databases_to_catalog=None):
    """Catalog databases for a connection based on database_name field or provided list"""
    
    # Step 1: Get list of all databases on the server
    all_databases = get_databases_on_server(connection_info)
    
    if not all_databases:
        logger.warning(f"No databases found on server {connection_info['host']}")
        return {}
    
    # Step 2: Determine which databases to catalog
    if databases_to_catalog:
        # Use databases provided from command line argument
        logger.info(f"Using databases from command line: {databases_to_catalog}")
        filtered_databases = []
        for requested_db in databases_to_catalog:
            if requested_db in all_databases:
                filtered_databases.append(requested_db)
                logger.info(f"Database '{requested_db}' found on server - will catalog")
            else:
                logger.warning(f"Database '{requested_db}' not found on server {connection_info['host']}")
        databases_to_process = filtered_databases
        
    elif connection_info['database_name']:
        # Use databases from connection configuration
        logger.info(f"Using databases from connection config: {connection_info['database_name']}")
        requested_databases = [db.strip() for db in connection_info['database_name'].split(',')]
        filtered_databases = []
        for requested_db in requested_databases:
            if requested_db in all_databases:
                filtered_databases.append(requested_db)
                logger.info(f"Database '{requested_db}' found on server - will catalog")
            else:
                logger.warning(f"Database '{requested_db}' not found on server {connection_info['host']}")
        databases_to_process = filtered_databases
        
    else:
        # Catalog all databases
        databases_to_process = all_databases
        logger.info(f"No specific databases requested - cataloging all {len(databases_to_process)} databases")
    
    if not databases_to_process:
        logger.error(f"No valid databases to catalog on server {connection_info['host']}")
        return {}
    
    # Step 3: Catalog each database in the filtered list
    logger.info(f"Cataloging {len(databases_to_process)} database(s): {', '.join(databases_to_process)}")
    
    # Initialize summary for this connection
    connection_summary = {
        'databases_added': 0, 'databases_updated': 0, 'schemas_added': 0, 'schemas_updated': 0,
        'tables_added': 0, 'tables_updated': 0, 'views_added': 0, 'views_updated': 0,
        'view_definitions_added': 0, 'view_definitions_updated': 0, 'columns_added': 0, 'columns_updated': 0,
        'databases_deleted': 0, 'schemas_deleted': 0, 'tables_deleted': 0, 'views_deleted': 0,
        'view_definitions_deleted': 0, 'columns_deleted': 0
    }
    
    try:
        for database_name in databases_to_process:
            logger.info(f"Processing database: {database_name}")
            
            db_connection_info = connection_info.copy()
            db_connection_info['database_name'] = database_name
            
            source_conn = connect_to_source_database(db_connection_info)
            if source_conn:
                try:
                    # Catalog the database and get summary (if your catalog_single_database returns summary)
                    catalog_single_database(source_conn, db_connection_info, catalog_run_id)
                    logger.info(f"Successfully cataloged database: {database_name}")
                    connection_summary['databases_added'] += 1  # Track processed databases
                finally:
                    source_conn.close()
            else:
                logger.error(f"Failed to connect to database: {database_name}")
        
        logger.info(f"Connection summary: {connection_summary}")
        return connection_summary
            
    except Exception as e:
        logger.error(f"Failed during cataloging: {e}")
        
        try:
            catalog_conn = get_catalog_connection()
            fail_catalog_run(catalog_conn, catalog_run_id, str(e))
            catalog_conn.close()
        except Exception as fail_error:
            logger.error(f"Failed to mark run as failed: {fail_error}")
        
        raise

def get_databases_on_server(connection_info):
    """Get list of all databases on the server"""
    logger.info(f"Discovering databases on server: {connection_info['host']}")
    
    # Connect to master/system database to enumerate databases
    master_connection_info = connection_info.copy()
    
    if connection_info['connection_type'] == 'PostgreSQL':
        master_connection_info['database_name'] = 'postgres'
    elif connection_info['connection_type'] == 'Azure SQL Server':
        master_connection_info['database_name'] = 'master'
    else:
        logger.error(f"Unsupported connection type: {connection_info['connection_type']}")
        return []
    
    master_conn = connect_to_source_database(master_connection_info)
    if not master_conn:
        logger.error(f"Could not connect to master database on {connection_info['host']}")
        return []
    
    try:
        if connection_info['connection_type'] == 'PostgreSQL':
            # PostgreSQL query to get databases
            with master_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT datname 
                    FROM pg_database 
                    WHERE datistemplate = false 
                    AND datname NOT IN ('postgres', 'template0', 'template1')
                    ORDER BY datname
                """)
                databases = [row[0] for row in cursor.fetchall()]
                
        elif connection_info['connection_type'] == 'Azure SQL Server':
            # Azure SQL Server query to get databases
            with master_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT name 
                    FROM sys.databases 
                    WHERE database_id > 4  -- Skip system databases (master, tempdb, model, msdb)
                    AND state = 0  -- Only online databases
                    AND is_read_only = 0  -- Skip read-only databases
                    ORDER BY name
                """)
                databases = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(databases)} databases: {', '.join(databases)}")
        return databases
        
    except Exception as e:
        logger.error(f"Error getting database list from {connection_info['host']}: {e}")
        return []
    finally:
        master_conn.close()

def start_catalog_run(catalog_conn, connection_info, databases_to_catalog=None):
    """Start a new catalog run and return the run ID"""
    with catalog_conn.cursor() as cursor:
        # Determine databases info
        if databases_to_catalog is None or len(databases_to_catalog) == 0:
            databases_info = "all"
            databases_count = None  # Will be determined later when we get the actual database list
        else:
            databases_info = json.dumps(databases_to_catalog)  # Store as JSON array
            databases_count = len(databases_to_catalog)
        
        cursor.execute("""
            INSERT INTO catalog.catalog_runs 
            (connection_id, connection_name, connection_type, connection_host, connection_port, 
             databases_to_catalog, databases_count, run_started_at, run_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'running')
            RETURNING id
        """, (
            connection_info['id'],
            connection_info['name'],
            connection_info['connection_type'],
            connection_info['host'],
            connection_info['port'],
            databases_info,
            databases_count
        ))
        
        run_id = cursor.fetchone()[0]
        
        # Setup logging with run ID (returns relative path)
        relative_log_filename = setup_logging_with_run_id(run_id)
        
        # Store relative log filename in database
        cursor.execute("""
            UPDATE catalog.catalog_runs 
            SET log_filename = %s
            WHERE id = %s
        """, (relative_log_filename, run_id))
        
        # If databases_to_catalog was "all", get the actual count now
        if databases_info == "all":
            try:
                actual_databases = get_databases_on_server(connection_info)
                actual_count = len(actual_databases)
                cursor.execute("""
                    UPDATE catalog.catalog_runs 
                    SET databases_count = %s,
                        databases_to_catalog = %s
                    WHERE id = %s
                """, (actual_count, json.dumps(actual_databases), run_id))
            except Exception as e:
                logger.warning(f"Could not determine exact database count: {e}")
        
        catalog_conn.commit()
        
        logger.info(f"Started catalog run {run_id} for connection {connection_info['name']}")
        logger.info(f"Databases to catalog: {databases_info}")
        logger.info(f"Expected database count: {databases_count}")
        logger.info(f"Relative log file path: {relative_log_filename}")
        return run_id

def complete_catalog_run(catalog_conn, run_id, summary):
    """Mark catalog run as completed with summary stats"""
    logger.info(f"Starting completion of catalog run {run_id}")
    
    try:
        with catalog_conn.cursor() as cursor:
            # Mark as completed
            cursor.execute("""
                UPDATE catalog.catalog_runs 
                SET run_completed_at = CURRENT_TIMESTAMP,
                    run_status = 'completed'
                WHERE id = %s
            """, (run_id,))
            
            # Check if update was successful
            if cursor.rowcount == 0:
                logger.warning(f"No catalog run found with id {run_id} to complete")
                return
            
            # Commit the transaction
            catalog_conn.commit()
            logger.info(f"Successfully committed completion status for catalog run {run_id}")
            
            # Get final counts for logging only
            cursor.execute("""
                SELECT databases_processed, schemas_processed, tables_processed, views_processed, columns_processed
                FROM catalog.catalog_runs 
                WHERE id = %s
            """, (run_id,))
            
            result = cursor.fetchone()
            if result:
                databases_processed, schemas_processed, tables_processed, views_processed, columns_processed = result
                logger.info(f"Completed catalog run {run_id} - processed {databases_processed} database(s), {schemas_processed} schema(s), {tables_processed} tables, {views_processed} views, {columns_processed} columns")
            else:
                logger.info(f"Completed catalog run {run_id}")
                
            logger.info(f"Summary stats: {summary}")
            
    except Exception as e:
        logger.error(f"Failed to complete catalog run {run_id}: {str(e)}")
        try:
            catalog_conn.rollback()
            logger.info(f"Rolled back transaction for catalog run {run_id}")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {str(rollback_error)}")
        raise

def fail_catalog_run(catalog_conn, run_id, error_message):
    """Mark catalog run as failed with error message"""
    with catalog_conn.cursor() as cursor:
        cursor.execute("""
            UPDATE catalog.catalog_runs 
            SET run_completed_at = CURRENT_TIMESTAMP,
                run_status = 'failed',
                error_message = %s
            WHERE id = %s
        """, (str(error_message), run_id))
        logger.error(f"Failed catalog run {run_id}: {error_message}")

def upsert_database_temporal(catalog_conn, connection_info, catalog_run_id):
    """Insert or create new version of database with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id, server_name FROM catalog.catalog_databases 
            WHERE database_name = %s AND server_name = %s 
            AND date_deleted IS NULL AND is_current = true
        """, (connection_info.get('database_name', ''), connection_info['host']))
        
        result = cursor.fetchone()
        
        if result:
            current_id, current_server = result
            
            # Check if anything actually changed
            if current_server != connection_info['host']:
                # Something changed - create new version
                
                # 1. Mark current record as no longer current
                cursor.execute("""
                    UPDATE catalog.catalog_databases 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new current version
                cursor.execute("""
                    INSERT INTO catalog.catalog_databases 
                    (database_name, server_name, date_created, is_current, catalog_run_id)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, true, %s)
                    RETURNING id
                """, (
                    connection_info.get('database_name', connection_info['name']), 
                    connection_info['host'],
                    catalog_run_id
                ))
                
                database_id = cursor.fetchone()[0]
                summary['databases_updated'] += 1
                logger.debug(f"Created new version of database: {connection_info.get('database_name', connection_info['name'])}")
                
            else:
                # No changes - don't update anything
                database_id = current_id
                
        else:
            # Insert new database (first time seeing it)
            cursor.execute("""
                INSERT INTO catalog.catalog_databases 
                (database_name, server_name, date_created, is_current, catalog_run_id)
                VALUES (%s, %s, CURRENT_TIMESTAMP, true, %s)
                RETURNING id
            """, (
                connection_info.get('database_name', connection_info['name']), 
                connection_info['host'],
                catalog_run_id
            ))
            
            database_id = cursor.fetchone()[0]
            summary['databases_added'] += 1
            logger.debug(f"Added new database: {connection_info.get('database_name', connection_info['name'])}")
        
        return database_id

def catalog_single_database(source_conn, connection_info, catalog_run_id):
    """Catalog a single specific database with catalog run tracking"""
    logger.info(f"Starting catalog of database: {connection_info['database_name']} on {connection_info['host']}")
    
    catalog_conn = get_catalog_connection()
    
    try:
        # Initialize local progress tracking
        progress = {
            'databases_processed': 0,
            'schemas_processed': 0,
            'tables_processed': 0,
            'views_processed': 0,
            'columns_processed': 0
        }
        
        # 1. Insert/Update database entry
        database_id = upsert_database_temporal(catalog_conn, connection_info, catalog_run_id)
        # logger.info(f"✅ Database processed: {connection_info['database_name']}")
        
        # update proccessed databases count
        update_run_progress(catalog_conn, catalog_run_id, progress)
        logger.info(f"Processed database: {connection_info['database_name']}")
        
        # 2. Get schemas from source database
        schemas = get_source_schemas(source_conn)
        logger.info(f"Found {len(schemas)} schemas in {connection_info['database_name']}")
        
        # 3. Process each schema
        for schema_name in schemas:
            logger.info(f"Processing schema: {schema_name}")
            schema_id = upsert_schema_temporal(catalog_conn, database_id, schema_name, catalog_run_id)
            
            # update processed schemas count
            update_run_progress(catalog_conn, catalog_run_id, progress)
            logger.info(f"Processed schema: {schema_name}")
            
            # 4. Get tables from schema
            tables = get_source_tables(source_conn, schema_name)
            logger.info(f"Found {len(tables)} tables in schema {schema_name}")
            
            # DEBUG: Log table types for first few objects
            for i, table_info in enumerate(tables[:5]):
                logger.debug(f"Table {i+1}: {table_info['table_name']} = '{table_info['table_type']}'")

            # 5. Process each table
            for table_info in tables:
                table_type = table_info['table_type'].upper().strip()
                logger.debug(f"Processing table of type: {table_type} - {schema_name}.{table_info['table_name']}")
                table_id = upsert_table_temporal(catalog_conn, schema_id, table_info, catalog_run_id)
                
                if table_type in ('VIEW', 'V'):
                    # For views: ONLY process view definition, skip columns/row count
                    if table_info.get('view_definition'):
                        logger.debug(f"Processing view definition for: {schema_name}.{table_info['table_name']}")
                        upsert_view_definition_temporal(catalog_conn, table_id, table_info['view_definition'], catalog_run_id)
                    else:
                        logger.warning(f"View {table_info['table_name']} has no definition")
                    progress['views_processed'] += 1
                        
                elif table_type in ('BASE TABLE', 'TABLE', 'U'):
                    # For tables: ONLY process columns and row count, skip view definition
                    # 6. Get columns from table
                    columns = get_source_columns(source_conn, schema_name, table_info['table_name'])
                    
                    # 7. Process each column
                    for column_info in columns:
                        upsert_column_temporal(catalog_conn, table_id, column_info, catalog_run_id)
                        progress['columns_processed'] += 1
                    
                    # 8. Get row count estimate
                    row_count = get_table_row_count(source_conn, schema_name, table_info['table_name'], connection_info['connection_type'])
                    if row_count is not None:
                        update_table_row_count_temporal(catalog_conn, table_id, row_count, catalog_run_id)
                    
                    progress['tables_processed'] += 1
                else:
                    logger.warning(f"Unknown table_type '{table_type}' for {schema_name}.{table_info['table_name']}")
                    
                if (progress['tables_processed'] + progress['views_processed']) % 10 == 0:
                    update_run_progress(catalog_conn, catalog_run_id, progress)
                    logger.debug(f"Progress update: {progress['tables_processed']} tables, {progress['views_processed']} views, {progress['columns_processed']} columns")
            
            # Update progress after processing all tables in schema
            progress['schemas_processed'] += 1 
            update_run_progress(catalog_conn, catalog_run_id, progress)
            logger.info(f"Completed schema {schema_name}: {len(tables)} objects")
       
        # ✅ ADD DATABASE COMPLETION HERE
        progress['databases_processed'] += 1
        
        # Final progress update
        update_run_progress(catalog_conn, catalog_run_id, progress)
        logger.info(f"✅ Database completed: {connection_info['database_name']}")
        logger.info(f"Final stats: {progress['schemas_processed']} schemas, {progress['tables_processed']} tables, {progress['views_processed']} views, {progress['columns_processed']} columns")
        
        catalog_conn.commit()
        logger.info(f"Successfully cataloged database: {connection_info['database_name']}")
        
    except Exception as e:
        # Mark run as failed
        try:
            fail_catalog_run(catalog_conn, catalog_run_id, str(e))
            catalog_conn.rollback()
        except:
            pass  # Don't fail on cleanup failure
        logger.error(f"Error cataloging database {connection_info['database_name']}: {e}")
        raise
        
    finally:
        catalog_conn.close()

def get_specific_connection(connection_id):
    """Get a specific connection by ID"""
    catalog_conn = get_catalog_connection()
    try:
        with catalog_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, name, connection_type, host, port, username, password, database_name 
                FROM config.connections 
                WHERE id = %s AND connection_type IN ('PostgreSQL', 'Azure SQL Server')
            """, (connection_id,))  # This should be a tuple with the connection_id
            connections = cursor.fetchall()
            if connections:
                logger.info(f"Found connection: {connections[0]['name']} (ID: {connection_id})")
            else:
                logger.warning(f"No connection found with ID: {connection_id}")
            return connections
    finally:
        catalog_conn.close()

def upsert_schema_temporal(catalog_conn, database_id, schema_name, catalog_run_id):
    """Insert or create new version of schema with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id FROM catalog.catalog_schemas 
            WHERE database_id = %s AND schema_name = %s 
            AND date_deleted IS NULL AND is_current = true
        """, (database_id, schema_name))
        
        result = cursor.fetchone()
        
        if result:
            current_id = result[0]
            # Schema exists and no changes - do nothing!
            schema_id = current_id
        else:
            # Insert new schema (first time seeing it)
            cursor.execute("""
                INSERT INTO catalog.catalog_schemas (database_id, schema_name, date_created, is_current, catalog_run_id)
                VALUES (%s, %s, CURRENT_TIMESTAMP, true, %s)
                RETURNING id
            """, (database_id, schema_name, catalog_run_id))
            
            schema_id = cursor.fetchone()[0]
            summary['schemas_added'] += 1
            logger.debug(f"Added new schema: {schema_name}")
        
        return schema_id

def upsert_table_temporal(catalog_conn, schema_id, table_info, catalog_run_id):
    """Insert or create new version of table with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id, table_type FROM catalog.catalog_tables 
            WHERE schema_id = %s AND table_name = %s 
            AND date_deleted IS NULL AND is_current = true
        """, (schema_id, table_info['table_name']))
        
        result = cursor.fetchone()
        
        if result:
            current_id, current_table_type = result
            
            # Check if table type changed
            if current_table_type != table_info.get('table_type'):
                # Table type changed - create new version
                
                # 1. Mark current record as no longer current
                cursor.execute("""
                    UPDATE catalog.catalog_tables 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new current version
                cursor.execute("""
                    INSERT INTO catalog.catalog_tables (schema_id, table_name, table_type, date_created, is_current, catalog_run_id)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, true, %s)
                    RETURNING id
                """, (schema_id, table_info['table_name'], table_info.get('table_type'), catalog_run_id))
                
                table_id = cursor.fetchone()[0]
                summary['tables_updated'] += 1
                logger.debug(f"Created new version of table: {table_info['table_name']}")
                
            else:
                # No changes - don't update anything
                table_id = current_id
                
        else:
            # Insert new table (first time seeing it)
            cursor.execute("""
                INSERT INTO catalog.catalog_tables (schema_id, table_name, table_type, date_created, is_current, catalog_run_id)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, true, %s)
                RETURNING id
            """, (schema_id, table_info['table_name'], table_info.get('table_type'), catalog_run_id))
            
            table_id = cursor.fetchone()[0]
            summary['tables_added'] += 1
            logger.debug(f"Added new table: {table_info['table_name']}")
        
        return table_id

def upsert_view_definition_temporal(catalog_conn, table_id, view_definition, catalog_run_id):
    """Insert or create new version of view definition with temporal versioning"""
    if not view_definition or not view_definition.strip():
        return None
        
    with catalog_conn.cursor() as cursor:
        # Calculate hash for change detection
        import hashlib
        definition_hash = hashlib.sha256(view_definition.encode()).hexdigest()
        
        # Check if current record exists
        cursor.execute("""
            SELECT id, definition_hash FROM catalog.catalog_view_definitions 
            WHERE table_id = %s 
            AND date_deleted IS NULL AND is_current = true
        """, (table_id,))
        
        result = cursor.fetchone()
        
        if result:
            current_id, current_hash = result
            
            # Check if definition actually changed
            if current_hash != definition_hash:
                # Definition changed - create new version
                
                # 1. Mark current record as no longer current
                cursor.execute("""
                    UPDATE catalog.catalog_view_definitions 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new version
                cursor.execute("""
                    INSERT INTO catalog.catalog_view_definitions 
                    (table_id, view_definition, definition_hash, catalog_run_id, is_current)
                    VALUES (%s, %s, %s, %s, true)
                    RETURNING id
                """, (table_id, view_definition, definition_hash, catalog_run_id))
                
                new_id = cursor.fetchone()[0]
                logger.debug(f"Updated view definition for table_id {table_id} (new version: {new_id})")
                summary['view_definitions_updated'] += 1
                return new_id
            else:
                # No changes - return existing ID
                logger.debug(f"No changes for view definition table_id {table_id}")
                return current_id
        else:
            # New view definition - insert it
            cursor.execute("""
                INSERT INTO catalog.catalog_view_definitions 
                (table_id, view_definition, definition_hash, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, true)
                RETURNING id
            """, (table_id, view_definition, definition_hash, catalog_run_id))
            
            new_id = cursor.fetchone()[0]
            logger.debug(f"Added new view definition for table_id {table_id} (ID: {new_id})")
            summary['view_definitions_added'] += 1
            return new_id

def mark_deleted_view_definitions(catalog_conn, schema_id, current_views, catalog_run_id):
    """Mark view definitions as deleted if their views no longer exist"""
    with catalog_conn.cursor() as cursor:
        current_view_names = [v['table_name'] for v in current_views if v['table_type'] == 'VIEW']
        
        if current_view_names:
            placeholders = ','.join(['%s'] * len(current_view_names))
            cursor.execute(f"""
                SELECT vd.id, t.table_name 
                FROM catalog.catalog_view_definitions vd
                JOIN catalog.catalog_tables t ON vd.table_id = t.id
                WHERE t.schema_id = %s 
                AND t.table_type = 'VIEW'
                AND t.table_name NOT IN ({placeholders})
                AND vd.is_current = true 
                AND vd.date_deleted IS NULL
            """, [schema_id] + current_view_names)
        else:
            # No views found - mark all current view definitions as deleted
            cursor.execute("""
                SELECT vd.id, t.table_name 
                FROM catalog.catalog_view_definitions vd
                JOIN catalog.catalog_tables t ON vd.table_id = t.id
                WHERE t.schema_id = %s 
                AND t.table_type = 'VIEW'
                AND vd.is_current = true 
                AND vd.date_deleted IS NULL
            """, (schema_id,))
        
        deleted_definitions = cursor.fetchall()
        
        for def_id, view_name in deleted_definitions:
            cursor.execute("""
                UPDATE catalog.catalog_view_definitions 
                SET is_current = false,
                    date_deleted = CURRENT_TIMESTAMP,
                    deleted_by_catalog_run_id = %s
                WHERE id = %s
            """, (catalog_run_id, def_id))
            
            logger.debug(f"Marked view definition for {view_name} as deleted")
            summary['view_definitions_deleted'] += 1

def upsert_column_temporal(catalog_conn, table_id, column_info, catalog_run_id):
    """Insert or create new version of column with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id, data_type, is_nullable, column_default, ordinal_position FROM catalog.catalog_columns 
            WHERE table_id = %s AND column_name = %s 
            AND date_deleted IS NULL AND is_current = true
        """, (table_id, column_info['column_name']))
        
        result = cursor.fetchone()
        
        if result:
            current_id, current_data_type, current_nullable, current_default, current_position = result
            
            # Check if anything changed
            changed = (
                current_data_type != column_info['data_type'] or
                current_nullable != column_info['is_nullable'] or
                current_default != column_info['column_default'] or
                current_position != column_info['ordinal_position']
            )
            
            if changed:
                # Column definition changed - create new version
                
                # 1. Mark current record as no longer current
                cursor.execute("""
                    UPDATE catalog.catalog_columns 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new current version
                cursor.execute("""
                    INSERT INTO catalog.catalog_columns 
                    (table_id, column_name, data_type, is_nullable, column_default, ordinal_position, date_created, is_current, catalog_run_id)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, true, %s)
                """, (
                    table_id,
                    column_info['column_name'],
                    column_info['data_type'],
                    column_info['is_nullable'],
                    column_info['column_default'],
                    column_info['ordinal_position'],
                    catalog_run_id
                ))
                
                summary['columns_updated'] += 1
                logger.debug(f"Created new version of column: {column_info['column_name']}")
                
            # If no changes, do nothing
                
        else:
            # Insert new column (first time seeing it)
            cursor.execute("""
                INSERT INTO catalog.catalog_columns 
                (table_id, column_name, data_type, is_nullable, column_default, ordinal_position, date_created, is_current, catalog_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, true, %s)
            """, (
                table_id,
                column_info['column_name'],
                column_info['data_type'],
                column_info['is_nullable'],
                column_info['column_default'],
                column_info['ordinal_position'],
                catalog_run_id
            ))
            
            summary['columns_added'] += 1
            logger.debug(f"Added new column: {column_info['column_name']}")

def update_table_row_count_temporal(catalog_conn, table_id, row_count, catalog_run_id):
    """Update table row count with catalog run tracking"""
    with catalog_conn.cursor() as cursor:
        # Update main table
        cursor.execute("""
            UPDATE catalog.catalog_tables 
            SET row_count_estimated = %s, 
                row_count_updated = CURRENT_TIMESTAMP
            WHERE id = %s AND is_current = true
        """, (row_count, table_id))
        
        # Log in rowcounts table with run reference
        cursor.execute("""
            INSERT INTO catalog.catalog_table_rowcounts (table_id, row_count_estimated, collected_at, catalog_run_id)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
        """, (table_id, row_count, catalog_run_id))

# Add helper functions to extract data from source databases
def get_source_schemas(source_conn):
    """Get list of schemas from source database"""
    # Check if it's a pyodbc connection (Azure SQL) or psycopg2 (PostgreSQL)
    if hasattr(source_conn, 'cursor') and 'pyodbc' in str(type(source_conn)):
        # Azure SQL Server using pyodbc
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
                ORDER BY schema_name
            """)
            return [row[0] for row in cursor.fetchall()]
    else:
        # PostgreSQL using psycopg2
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
            """)
            return [row[0] for row in cursor.fetchall()]

def get_source_tables(source_conn, schema_name):
    """Get list of tables AND views from source schema, including view definitions"""
    if hasattr(source_conn, 'cursor') and 'pyodbc' in str(type(source_conn)):
        # Azure SQL Server
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    t.table_name, 
                    t.table_type,
                    v.view_definition
                FROM information_schema.tables t
                LEFT JOIN information_schema.views v 
                    ON t.table_name = v.table_name 
                    AND t.table_schema = v.table_schema
                WHERE t.table_schema = ?
                AND t.table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY t.table_name
            """, schema_name)
            
            return [{
                'table_name': row[0], 
                'table_type': row[1],
                'view_definition': row[2] if row[2] else None
            } for row in cursor.fetchall()]
    else:
        # PostgreSQL
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    t.table_name, 
                    t.table_type,
                    v.definition as view_definition
                FROM information_schema.tables t
                LEFT JOIN pg_views v 
                    ON t.table_name = v.viewname 
                    AND t.table_schema = v.schemaname
                WHERE t.table_schema = %s
                AND t.table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY t.table_name
            """, (schema_name,))
            
            return [{
                'table_name': row[0], 
                'table_type': row[1],
                'view_definition': row[2] if row[2] else None
            } for row in cursor.fetchall()]

def get_source_columns(source_conn, schema_name, table_name):
    """Get list of columns from source table"""
    if hasattr(source_conn, 'cursor') and 'pyodbc' in str(type(source_conn)):
        # Azure SQL Server
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default, ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, schema_name, table_name)
            return [
                {
                    'column_name': row[0],
                    'data_type': row[1],
                    'is_nullable': row[2] == 'YES',
                    'column_default': row[3],
                    'ordinal_position': row[4]
                }
                for row in cursor.fetchall()
            ]
    else:
        # PostgreSQL
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default, ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            return [
                {
                    'column_name': row[0],
                    'data_type': row[1],
                    'is_nullable': row[2] == 'YES',
                    'column_default': row[3],
                    'ordinal_position': row[4]
                }
                for row in cursor.fetchall()
            ]

def get_table_row_count(source_conn, schema_name, table_name, connection_type):
    """Get estimated row count for table (database-specific implementation)"""
    
    if connection_type == 'PostgreSQL':
        return get_table_row_count_postgresql(source_conn, schema_name, table_name)
    elif connection_type == 'Azure SQL Server':
        return get_table_row_count_sqlserver(source_conn, schema_name, table_name)
    else:
        logger.warning(f"Row count not implemented for connection type: {connection_type}")
        return None

def get_table_row_count_postgresql(source_conn, schema_name, table_name):
    """Get estimated row count for PostgreSQL table"""
    try:
        with source_conn.cursor() as cursor:
            # First try pg_stat_user_tables (fast but requires ANALYZE)
            cursor.execute("""
                SELECT n_tup_ins - n_tup_del as row_count
                FROM pg_stat_user_tables 
                WHERE schemaname = %s AND relname = %s
            """, (schema_name, table_name))
            result = cursor.fetchone()
            
            if result and result[0] is not None:
                return result[0]
            
            # Fallback to pg_class (less accurate but doesn't require ANALYZE)
            cursor.execute("""
                SELECT c.reltuples::bigint as row_count
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
            """, (schema_name, table_name))
            result = cursor.fetchone()
            return result[0] if result and result[0] else 0
            
    except Exception as e:
        logger.warning(f"Could not get row count for {schema_name}.{table_name}: {e}")
        return None

def get_table_row_count_sqlserver(source_conn, schema_name, table_name):
    """Get estimated row count for SQL Server table"""
    try:
        with source_conn.cursor() as cursor:
            cursor.execute("""
                SELECT SUM(p.rows) as row_count
                FROM sys.tables t
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ? AND t.name = ? 
                AND p.index_id IN (0,1)
            """, schema_name, table_name)
            result = cursor.fetchone()
            return result[0] if result and result[0] else 0
    except Exception as e:
        logger.warning(f"Could not get row count for {schema_name}.{table_name}: {e}")
        return None

def update_run_progress(catalog_conn, catalog_run_id, progress):
    """Update catalog run progress in real-time"""
    try:
        with catalog_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE catalog.catalog_runs 
                SET databases_processed = %s,
                    schemas_processed = %s,
                    tables_processed = %s,
                    views_processed = %s,
                    columns_processed = %s
                WHERE id = %s
            """, (
                progress['databases_processed'],
                progress['schemas_processed'], 
                progress['tables_processed'],      
                progress['views_processed'],       
                progress['columns_processed'],
                catalog_run_id
            ))
            catalog_conn.commit()
    except Exception as e:
        logger.warning(f"Failed to update progress: {e}")

if __name__ == "__main__":
    main()