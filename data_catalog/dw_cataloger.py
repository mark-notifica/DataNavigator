import psycopg2
import psycopg2.extras
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import datetime
import argparse
from dotenv import load_dotenv

from data_catalog.connection_handler import (
    get_main_connector_by_name,
    get_catalog_config_by_main_connector_id,
    connect_to_source_database,
    get_databases_on_server,
    get_catalog_connection,
    get_catalog_config_by_id
)

# Load environment variables
load_dotenv()

# Setup logger
logger = logging.getLogger(__name__)

# Catalog schema name
CATALOG_SCHEMA = 'catalog'

# Setup logging directory if run as main script
if __name__ == "__main__":
    script_dir = Path(__file__).parent
    log_dir = script_dir / 'logfiles' / 'database_server'
    log_dir.mkdir(parents=True, exist_ok=True)

def load_connection_and_config(name_or_host):
    """
    Haalt de hoofdconnectie op en de bijbehorende catalog config (indien aanwezig).
    Returned tuple: (main_conn_info, catalog_config) of (dict, dict|None)
    """
    main_conn_info = get_main_connector_by_name(name_or_host)
    catalog_config = get_catalog_config_by_main_connector_id(main_conn_info['id'])
    return main_conn_info, catalog_config

def resolve_filters(main_conn_info, catalog_config):
    """
    Zet catalog filters om in lijsten. Leeg of None betekent 'alles'.
    """
    # databases
    if catalog_config and catalog_config.get('catalog_database_filter'):
        db_filter = [db.strip() for db in catalog_config['catalog_database_filter'].split(',') if db.strip()]
    else:
        db_filter = None  # alles
    
    # schema's
    if catalog_config and catalog_config.get('catalog_schema_filter'):
        schema_filter = [s.strip() for s in catalog_config['catalog_schema_filter'].split(',') if s.strip()]
    else:
        schema_filter = None
    
    # tabellen
    if catalog_config and catalog_config.get('catalog_table_filter'):
        table_filter = [t.strip() for t in catalog_config['catalog_table_filter'].split(',') if t.strip()]
    else:
        table_filter = None

    include_views = catalog_config.get('include_views', False) if catalog_config else False
    include_system_objects = catalog_config.get('include_system_objects', False) if catalog_config else False

    return db_filter, schema_filter, table_filter, include_views, include_system_objects

def get_summary_template():
    return {
        'databases_added': 0, 'databases_updated': 0, 'databases_deleted': 0, 'databases_processed': 0, 'databases_unchanged': 0,
        'schemas_added': 0, 'schemas_updated': 0, 'schemas_deleted': 0, 'schemas_processed': 0, 'schemas_unchanged': 0,
        'tables_added': 0, 'tables_updated': 0, 'tables_deleted': 0, 'tables_processed': 0, 'tables_unchanged': 0,
        'views_added': 0, 'views_updated': 0, 'views_deleted': 0, 'views_processed': 0, 'views_unchanged': 0,
        'view_definitions_added': 0, 'view_definitions_updated': 0, 'view_definitions_deleted': 0, 'view_definitions_processed': 0, 'view_definitions_unchanged': 0,
        'columns_added': 0, 'columns_updated': 0, 'columns_deleted': 0, 'columns_processed': 0, 'columns_unchanged': 0
    }


def catalog_multiple_databases(
    connection_info,
    databases_to_catalog,
    schema_filter=None,
    table_filter=None,
    catalog_config_id=None,
    include_views=False,
    include_system_objects=False
):
    """Catalog multiple databases for a given connection."""
    summary = get_summary_template()  # Initialize summary for the entire run
    progress = initialize_progress()  # Initialize progress for the entire run

    try:
        catalog_conn = get_catalog_connection()
        catalog_run_id = start_catalog_run(
            catalog_conn,
            connection_info,
            databases_to_catalog=databases_to_catalog,
            catalog_config_id=catalog_config_id
        )
        catalog_conn.commit()

        setup_logging_with_run_id(catalog_run_id)

        for database_name in databases_to_catalog:
            try:
                logger.info(f"Starting cataloging for database: {database_name}")

                db_connection_info = connection_info.copy()
                db_connection_info['database_name'] = database_name

                source_conn = connect_to_source_database(db_connection_info, database_name)
                if not source_conn:
                    logger.error(f"Failed to connect to source database: {database_name}")
                    summary['databases_deleted'] += 1
                    continue

                catalog_single_database(
                    source_conn,
                    db_connection_info,
                    catalog_run_id,
                    schema_filter=schema_filter,
                    table_filter=table_filter,
                    summary=summary,
                    progress=progress,
                    include_views=include_views,
                    include_system_objects=include_system_objects
                )

                update_run_progress(catalog_conn, catalog_run_id, progress)

            except Exception as e:
                logger.error(f"Failed to catalog database {database_name}: {e}")
                summary['databases_deleted'] += 1

        logger.info(f"Completed cataloging for all databases in connection {connection_info['name']}")

        complete_catalog_run(catalog_conn, catalog_run_id, summary)
        catalog_conn.commit()

    except Exception as e:
        logger.error(f"Failed to complete catalog run: {e}")
        fail_catalog_run(catalog_conn, catalog_run_id, str(e))
        catalog_conn.rollback()
    finally:
        catalog_conn.close()

    return summary

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
    """Main cataloging process, based on connection ID and catalog config ID."""

    parser = argparse.ArgumentParser(description='Catalog database metadata')
    parser.add_argument('--connection-id', type=int, required=True, help='ID van de hoofdconnectie om te gebruiken')
    parser.add_argument('--catalog-config-id', type=int, required=True, help='ID van de catalogusconfiguratie met filters')
    args = parser.parse_args()

    logger.info("Starting catalog extraction process")

    # Haal de hoofdconnectie op
    source_connections = get_specific_connection(args.connection_id)
    if not source_connections:
        logger.error(f"Geen connectie gevonden voor id {args.connection_id}")
        return
    connection_info = source_connections[0]

    # Haal de catalogusconfiguratie op
    with get_catalog_connection() as catalog_conn:
        catalog_config = get_catalog_config_by_id(catalog_conn, args.catalog_config_id)
    if not catalog_config:
        logger.error(f"Geen catalog configuratie gevonden voor id {args.catalog_config_id}")
        return

    # Parse database filter naar lijst of None (alles)
    databases_to_catalog = parse_comma_separated_values(catalog_config['catalog_database_filter'])
    if databases_to_catalog is None:
        # Geen filter? Dan alle databases ophalen
        databases_to_catalog = get_databases_on_server(connection_info)

    # Schema- en table-filter hetzelfde
    schema_filter = parse_comma_separated_values(catalog_config['catalog_schema_filter']) if catalog_config['catalog_schema_filter'] else None
    table_filter = parse_comma_separated_values(catalog_config['catalog_table_filter']) if catalog_config['catalog_table_filter'] else None

    # Call cataloging
    summary = catalog_multiple_databases(
        connection_info,
        databases_to_catalog,
        schema_filter=schema_filter,
        table_filter=table_filter,
        catalog_config_id=args.catalog_config_id,
        include_views=catalog_config.get('include_views', False),
        include_system_objects=catalog_config.get('include_system_objects', False)
    )
    
    log_final_summary(summary, schema_filter, table_filter)

def parse_comma_separated_values(value):
    """Parse comma-separated string to list, or None als leeg."""
    return [item.strip() for item in value.split(",") if item.strip()] if value else None

def resolve_databases_to_catalog(connection_info, databases_to_catalog):
    """Resolve databases to catalog based on user input or connection config."""
    if databases_to_catalog is None:
        if connection_info.get('database_name'):
            databases_to_catalog = parse_comma_separated_values(connection_info['database_name'])
            logger.info(f"Using databases from connection config: {databases_to_catalog}")
        else:
            databases_to_catalog = get_databases_on_server(connection_info)
            logger.info(f"No databases specified, defaulting to all databases: {databases_to_catalog}")
    return databases_to_catalog

def update_summary(summary, connection_summary):
    """Update the main summary with connection-specific summary."""
    for key in summary:
        summary[key] += connection_summary.get(key, 0)

def log_final_summary(summary, schema_filter, table_filter):
    """Log the final summary and applied filters."""
    logger.info("Catalog extraction completed")
    # Log applied filters
    if schema_filter:
        logger.info(f"Schema filter applied: {schema_filter}")
    else:
        logger.info("No schema filter applied. Processing all schemas.")
    
    if table_filter:
        logger.info(f"Table filter applied: {table_filter}")
    else:
        logger.info("No table filter applied. Processing all tables.")
    
    logger.info("Final Summary:")
    
    # Log schemas summary
    logger.info("------ Schemas ------")
    logger.info(f"schemas_processed: {summary['schemas_processed']}")
    logger.info(f"schemas_added: {summary['schemas_added']}")
    logger.info(f"schemas_updated: {summary['schemas_updated']}")
    logger.info(f"schemas_deleted: {summary['schemas_deleted']}")
    logger.info(f"schemas_unchanged: {summary['schemas_unchanged']}")
    
    # Log tables summary
    logger.info("------ Tables ------")
    logger.info(f"tables_processed: {summary['tables_processed']}")
    logger.info(f"tables_added: {summary['tables_added']}")
    logger.info(f"tables_updated: {summary['tables_updated']}")
    logger.info(f"tables_deleted: {summary['tables_deleted']}")
    logger.info(f"tables_unchanged: {summary['tables_unchanged']}")
    
    # Log views summary
    logger.info("------ Views ------")
    logger.info(f"views_processed: {summary['views_processed']}")
    logger.info(f"views_added: {summary['views_added']}")
    logger.info(f"views_updated: {summary['views_updated']}")
    logger.info(f"views_deleted: {summary['views_deleted']}")
    logger.info(f"views_unchanged: {summary['views_unchanged']}")
    
    # Log view definitions summary
    logger.info("------ View Definitions ------")
    logger.info(f"view_definitions_processed: {summary['view_definitions_processed']}")
    logger.info(f"view_definitions_added: {summary['view_definitions_added']}")
    logger.info(f"view_definitions_updated: {summary['view_definitions_updated']}")
    logger.info(f"view_definitions_deleted: {summary['view_definitions_deleted']}")
    logger.info(f"view_definitions_unchanged: {summary['view_definitions_unchanged']}")
    
    # Log columns summary
    logger.info("------ Columns ------")
    logger.info(f"columns_processed: {summary['columns_processed']}")
    logger.info(f"columns_added: {summary['columns_added']}")
    logger.info(f"columns_updated: {summary['columns_updated']}")
    logger.info(f"columns_deleted: {summary['columns_deleted']}")
    logger.info(f"columns_unchanged: {summary['columns_unchanged']}")


def start_catalog_run(catalog_conn, connection_info, databases_to_catalog=None, catalog_config_id=None):
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
            INSERT INTO catalog.dw_catalog_runs 
            (connection_id, catalog_config_id, connection_name, connection_type, connection_host, connection_port, 
            databases_to_catalog, databases_count, run_started_at, run_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'running')
            RETURNING id
        """, (
            connection_info['id'],
            catalog_config_id,
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
            UPDATE catalog.dw_catalog_runs 
            SET log_filename = %s
            WHERE id = %s
        """, (relative_log_filename, run_id))
        
        # If databases_to_catalog was "all", get the actual count now
        if databases_info == "all":
            try:
                actual_databases = get_databases_on_server(connection_info)
                actual_count = len(actual_databases)
                cursor.execute("""
                    UPDATE catalog.dw_catalog_runs 
                    SET databases_count = %s,
                        databases_to_catalog = %s
                    WHERE id = %s
                """, (actual_count, json.dumps(actual_databases), run_id))
            except Exception as e:
                logger.warning(f"Could not determine exact database count: {e}")
        
        catalog_conn.commit()
        
        logger.info(f"Started dw catalog run {run_id} for connection {connection_info['name']}")
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
                UPDATE catalog.dw_catalog_runs 
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
                FROM catalog.dw_catalog_runs 
                WHERE id = %s
            """, (run_id,))
            
            result = cursor.fetchone()
            if result:
                databases_processed, schemas_processed, tables_processed, views_processed, columns_processed = result
                logger.info(f"Completed catalog run {run_id} - processed {databases_processed} database(s), {schemas_processed} schema(s), {tables_processed} tables, {views_processed} views, {columns_processed} columns")
            else:
                logger.info(f"Completed catalog run {run_id}")
                
            # logger.info(f"Summary stats: {summary}")
            
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
            UPDATE catalog.dw_catalog_runs 
            SET run_completed_at = CURRENT_TIMESTAMP,
                run_status = 'failed',
                error_message = %s
            WHERE id = %s
        """, (str(error_message), run_id))
        logger.error(f"Failed catalog run {run_id}: {error_message}")

def upsert_database_temporal(catalog_conn, connection_info, catalog_run_id,summary):
    """Insert or create new version of database with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id, server_name FROM catalog.dw_databases 
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
                    UPDATE catalog.dw_databases 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new current version
                cursor.execute("""
                    INSERT INTO catalog.dw_databases 
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
                INSERT INTO catalog.dw_databases 
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


def catalog_single_database(
    source_conn,
    connection_info,
    catalog_run_id,
    schema_filter=None,
    table_filter=None,
    summary=None,
    progress=None,
    include_views=False,
    include_system_objects=False
):
    """Catalog a single specific database with catalog run tracking."""
    if summary is None:
        summary = get_summary_template()
    if progress is None:
        progress = initialize_progress()

    logger.info(f"Starting catalog of database: {connection_info['database_name']} on {connection_info['host']}")

    catalog_conn = get_catalog_connection()

    try:
        database_id = upsert_database_temporal(catalog_conn, connection_info, catalog_run_id, summary)
        update_run_progress(catalog_conn, catalog_run_id, progress)

        if not source_conn:
            logger.error(f"Failed to find connection for database: {connection_info['database_name']}")
            return summary

        schemas = get_source_schemas(source_conn)
        if schema_filter:
            schemas = [schema for schema in schemas if schema in schema_filter]
        if not schemas:
            logger.warning(f"No schemas found in database: {connection_info['database_name']} matching the filter.")
            return summary
        logger.info(f"Found {len(schemas)} schemas in {connection_info['database_name']} matching the filter.")

        for schema_name in schemas:
            process_schema(
                catalog_conn,
                source_conn,
                schema_name,
                database_id,
                catalog_run_id,
                progress,
                summary,
                table_filter=table_filter,
                include_views=include_views,
                include_system_objects=include_system_objects
            )

        summary['databases_processed'] += 1
        progress['databases_processed'] += 1

        update_run_progress(catalog_conn, catalog_run_id, progress)

        catalog_conn.commit()

    except Exception as e:
        fail_catalog_run(catalog_conn, catalog_run_id, str(e))
        catalog_conn.rollback()
        raise

    finally:
        catalog_conn.close()
        if source_conn:
            source_conn.close()

    return summary
            
def initialize_progress():
    """Initialize progress tracking dictionary."""
    return {
        'databases_processed': 0,
        'schemas_processed': 0,
        'tables_processed': 0,
        'views_processed': 0,
        'columns_processed': 0
    }

def process_schema(
    catalog_conn,
    source_conn,
    schema_name,
    database_id,
    catalog_run_id,
    progress,
    summary,
    table_filter=None,
    include_views=False,
    include_system_objects=False
):
    """Process a single schema."""
    logger.info(f"Processing schema: {schema_name}")
    schema_id = upsert_schema_temporal(catalog_conn, database_id, schema_name, catalog_run_id, summary)
    update_run_progress(catalog_conn, catalog_run_id, progress)

    # Increment schema counters
    summary['schemas_processed'] += 1
    progress['schemas_processed'] += 1
    # Check if schema was unchanged
    if summary['schemas_added'] == 0 and summary['schemas_updated'] == 0 and summary['schemas_deleted'] == 0:
        summary['schemas_unchanged'] += 1

    # Process tables and views met filter en flags
    tables = get_source_tables(
        source_conn,
        schema_name,
        table_filter=table_filter,
        include_views=include_views,
        include_system_objects=include_system_objects
    )
    if not tables:
        logger.warning(f"No tables found in schema: {schema_name} matching the filter.")
        return
    logger.info(f"Found {len(tables)} tables in schema {schema_name} matching the filter.")

    process_tables_and_views(catalog_conn, source_conn, schema_id, tables, schema_name, catalog_run_id, progress, summary)

    # Update progress after processing the schema
    update_run_progress(catalog_conn, catalog_run_id, progress)

def process_tables_and_views(catalog_conn, source_conn, schema_id, tables, schema_name, catalog_run_id, progress, summary):
    """Process tables and views for a schema."""
    view_definitions_batch = []
    current_view_names = []

    for table_info in tables:
        table_type = table_info['table_type'].upper().strip()
        table_id = upsert_table_temporal(catalog_conn, schema_id, table_info, catalog_run_id, summary)

        if table_type in ('VIEW', 'V'):
            current_view_names.append(table_info['table_name'])

            process_columns(catalog_conn, source_conn, schema_name, table_info, table_id, catalog_run_id, progress, summary)
            
            if table_info.get('view_definition'):
                view_definitions_batch.append((table_id, table_info['view_definition']))
            else:
                logger.warning(f"View {table_info['table_name']} has no definition")
        elif table_type in ('BASE TABLE', 'TABLE', 'U'):
            process_columns(catalog_conn, source_conn, schema_name, table_info, table_id, catalog_run_id, progress, summary)
        else:
            logger.warning(f"Unknown table_type '{table_type}' for {schema_name}.{table_info['table_name']}")

    # Process batch of view definitions
    if view_definitions_batch:
        process_view_definitions_batch(catalog_conn, view_definitions_batch, catalog_run_id, progress, summary)

    # Mark deleted view definitions
    mark_deleted_view_definitions_batch(catalog_conn, schema_id, current_view_names, catalog_run_id,summary)
    
    # Calculate views_unchanged
    summary['views_unchanged'] = (
        summary['views_processed']
        - summary['views_added']
        - summary['views_updated']
        - summary['views_deleted']
    )
    # Update progress after processing tables and views
    update_run_progress(catalog_conn, catalog_run_id, progress)

def process_view_definitions_batch(catalog_conn, view_definitions_batch, catalog_run_id, progress, summary):
    """Process a batch of view definitions."""
    try:
        logger.debug(f"Processing batch of {len(view_definitions_batch)} view definitions")
        
        # Perform batch processing
        upsert_view_definitions_batch(catalog_conn, view_definitions_batch, catalog_run_id,summary)
        
        # Update progress and summary
        progress['views_processed'] += len(view_definitions_batch)
        summary['views_processed'] += len(view_definitions_batch)

        logger.debug(f"Processed batch of {len(view_definitions_batch)} view definitions")
    except Exception as e:
        logger.error(f"Failed to process batch of view definitions: {e}")
        catalog_conn.rollback()

def process_columns(catalog_conn, source_conn, schema_name, table_info, table_id, catalog_run_id, progress, summary):
    """Process columns for a table."""
    connection_type = table_info.get('connection_type', 'PostgreSQL')  # Default to PostgreSQL if missing
    columns = get_source_columns(source_conn, schema_name, table_info['table_name'])
    for column_info in columns:
        upsert_column_temporal(catalog_conn, table_id, column_info, catalog_run_id,summary)
        progress['columns_processed'] += 1
        summary['columns_processed'] += 1 

                # Check if column was unchanged
        if summary['columns_added'] == 0 and summary['columns_updated'] == 0 and summary['columns_deleted'] == 0:
            summary['columns_unchanged'] += 1

def get_specific_connection(connection_id):
    """Get a specific connection by ID. Raises ValueError if not exactly one found."""
    catalog_conn = get_catalog_connection()
    try:
        with catalog_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, name, connection_type, host, port, username, password
                FROM config.connections 
                WHERE id = %s AND connection_type IN ('PostgreSQL', 'Azure SQL Server')
            """, (connection_id,))
            connections = cursor.fetchall()

            if not connections:
                logger.warning(f"No connection found with ID: {connection_id}")
                raise ValueError(f"No connection found with ID {connection_id}")

            if len(connections) > 1:
                logger.error(f"Multiple connections found with ID: {connection_id}")
                raise ValueError(f"Multiple connections found with ID {connection_id}, but expected exactly one.")

            logger.info(f"Found connection: {connections[0]['name']} (ID: {connection_id})")
            return connections
    finally:
        catalog_conn.close()

def upsert_schema_temporal(catalog_conn, database_id, schema_name, catalog_run_id, summary):
    """Insert or update schema with temporal versioning."""
    with catalog_conn.cursor() as cursor:
        cursor.execute("""
            SELECT id FROM catalog.dw_schemas
            WHERE database_id = %s AND schema_name = %s AND is_current = true
        """, (database_id, schema_name))
        result = cursor.fetchone()

        if result:
            schema_id = result[0]
            # No changes, do nothing
        else:
            # Insert new schema
            cursor.execute("""
                INSERT INTO catalog.dw_schemas (database_id, schema_name, date_created, is_current, catalog_run_id)
                VALUES (%s, %s, CURRENT_TIMESTAMP, true, %s)
                RETURNING id
            """, (database_id, schema_name, catalog_run_id))
            schema_id = cursor.fetchone()[0]
            summary['schemas_added'] += 1  # Update the passed summary
            logger.debug(f"Added new schema: {schema_name}")

        return schema_id

def upsert_table_temporal(catalog_conn, schema_id, table_info, catalog_run_id, summary):
    """Insert or create new version of table with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id, table_type FROM catalog.dw_tables 
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
                    UPDATE catalog.dw_tables 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new current version
                cursor.execute("""
                    INSERT INTO catalog.dw_tables (schema_id, table_name, table_type, date_created, is_current, catalog_run_id)
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
                INSERT INTO catalog.dw_tables (schema_id, table_name, table_type, date_created, is_current, catalog_run_id)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, true, %s)
                RETURNING id
            """, (schema_id, table_info['table_name'], table_info.get('table_type'), catalog_run_id))
            
            table_id = cursor.fetchone()[0]
            summary['tables_added'] += 1
            logger.debug(f"Added new table: {table_info['table_name']}")
        
        return table_id


def get_all_view_definitions(source_conn, schema_name):
    """Retrieve all view definitions for a schema in a single query."""
    with source_conn.cursor() as cursor:
        cursor.execute("""
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = %s
        """, (schema_name,))
        return cursor.fetchall()

def upsert_view_definitions_batch(catalog_conn, table_id_definitions, catalog_run_id, summary):
    """Insert or update view definitions in batch with temporal versioning."""
    if not table_id_definitions:
        return None

    with catalog_conn.cursor() as cursor:
        import hashlib

        # Prepare batch data for hash calculation and change detection
        batch_data = []
        for table_id, view_definition in table_id_definitions:
            if not view_definition or not view_definition.strip():
                logger.warning(f"View definition for table_id {table_id} is empty or missing.")
                continue

            # Calculate hash for change detection
            definition_hash = hashlib.sha256(view_definition.encode()).hexdigest()
            batch_data.append((table_id, view_definition, definition_hash))

        # Retrieve existing hashes for all table IDs in the batch
        table_ids = [data[0] for data in batch_data]
        placeholders = ','.join(['%s'] * len(table_ids))
        cursor.execute(f"""
            SELECT vd.table_id, vd.definition_hash, t.table_name 
            FROM catalog.dw_view_definitions vd
            JOIN catalog.dw_tables t ON vd.table_id = t.id
            WHERE vd.table_id IN ({placeholders}) 
            AND vd.date_deleted IS NULL 
            AND vd.is_current = true
        """, table_ids)

        existing_definitions = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}  # Map table_id -> (definition_hash, table_name)

        # Prepare data for updates and inserts
        updates = []
        inserts = []
        for table_id, view_definition, definition_hash in batch_data:
            existing_data = existing_definitions.get(table_id)
            if existing_data:
                current_hash, table_name = existing_data
                if current_hash != definition_hash:
                    # Prepare update data
                    updates.append((table_id, view_definition, definition_hash, catalog_run_id, table_id))
                    summary['view_definitions_updated'] += 1  # Update summary for updated views
                    logger.debug(f"Updated view definition for table '{table_name}' (type: view) with table_id {table_id}")
                else:
                    summary['view_definitions_unchanged'] += 1  # Update summary for unchanged views
                    logger.debug(f"No changes for view definition for table '{table_name}' (type: view) with table_id {table_id}")
            else:
                # Prepare insert data
                inserts.append((table_id, view_definition, definition_hash, catalog_run_id))
                summary['view_definitions_added'] += 1  # Update summary for added views
                logger.debug(f"Added new view definition for table_id {table_id}")
        
        # Update the total number of view definitions processed
        summary['view_definitions_processed'] += len(batch_data)
        
        # Execute batch updates
        if updates:
            cursor.executemany("""
                UPDATE catalog.dw_view_definitions 
                SET view_definition = %s, definition_hash = %s, catalog_run_id = %s, is_current = true, date_updated = CURRENT_TIMESTAMP
                WHERE table_id = %s
            """, updates)

        # Execute batch inserts
        if inserts:
            cursor.executemany("""
                INSERT INTO catalog.dw_view_definitions 
                (table_id, view_definition, definition_hash, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, true)
            """, inserts)

def mark_deleted_view_definitions_batch(catalog_conn, schema_id, current_view_names, catalog_run_id,summary):
    """Mark view definitions as deleted in batch if their views no longer exist."""
    with catalog_conn.cursor() as cursor:
        if current_view_names:
            placeholders = ','.join(['%s'] * len(current_view_names))
            cursor.execute(f"""
                SELECT vd.id, t.table_name 
                FROM catalog.dw_view_definitions vd
                JOIN catalog.dw_tables t ON vd.table_id = t.id
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
                FROM catalog.dw_view_definitions vd
                JOIN catalog.dw_tables t ON vd.table_id = t.id
                WHERE t.schema_id = %s 
                AND t.table_type = 'VIEW'
                AND vd.is_current = true 
                AND vd.date_deleted IS NULL
            """, (schema_id,))
        
        deleted_definitions = cursor.fetchall()
        
        # Update all deleted definitions in a batch
        for def_id, view_name in deleted_definitions:
            cursor.execute("""
                UPDATE catalog.dw_view_definitions 
                SET is_current = false,
                    date_deleted = CURRENT_TIMESTAMP,
                    deleted_by_catalog_run_id = %s
                WHERE id = %s
            """, (catalog_run_id, def_id))
            
            logger.debug(f"Marked view definition for {view_name} as deleted")
        
        # Update summary with the total number of deleted definitions
        summary['view_definitions_deleted'] += len(deleted_definitions)

def upsert_column_temporal(catalog_conn, table_id, column_info, catalog_run_id,summary):
    """Insert or create new version of column with temporal versioning"""
    with catalog_conn.cursor() as cursor:
        # Check if current record exists
        cursor.execute("""
            SELECT id, data_type, is_nullable, column_default, ordinal_position FROM catalog.dw_columns 
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
                    UPDATE catalog.dwg_columns 
                    SET is_current = false,
                        date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_id,))
                
                # 2. Insert new current version
                cursor.execute("""
                    INSERT INTO catalog.dw_columns 
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
                INSERT INTO catalog.dw_columns 
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
            UPDATE catalog.dw_tables 
            SET row_count_estimated = %s, 
                row_count_updated = CURRENT_TIMESTAMP
            WHERE id = %s AND is_current = true
        """, (row_count, table_id))
        
        # Log in rowcounts table with run reference
        cursor.execute("""
            INSERT INTO catalog.dw_table_rowcounts (table_id, row_count_estimated, collected_at, catalog_run_id)
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

def get_source_tables(
    source_conn,
    schema_name,
    table_filter=None,
    include_views=False,
    include_system_objects=False
):
    """
    Get list of tables and optionally views from source schema, applying filters.

    :param source_conn: database connection
    :param schema_name: schema to query
    :param table_filter: list of table names to include (None = all)
    :param include_views: boolean, whether to include views
    :param include_system_objects: boolean, whether to include system tables
    :return: list of dicts with keys: table_name, table_type, view_definition, connection_type
    """

    if hasattr(source_conn, 'cursor') and 'pyodbc' in str(type(source_conn)):
        connection_type = 'Azure SQL Server'
        with source_conn.cursor() as cursor:
            # Basis query
            query = """
                SELECT
                    t.table_name,
                    t.table_type,
                    v.view_definition
                FROM information_schema.tables t
                LEFT JOIN information_schema.views v
                    ON t.table_name = v.table_name
                    AND t.table_schema = v.table_schema
                WHERE t.table_schema = ?
            """

            params = [schema_name]

            # Filter op table_type
            allowed_types = ["BASE TABLE"]
            if include_views:
                allowed_types.append("VIEW")

            if not include_system_objects:
                # Exclude system tables (bijvoorbeeld 'sys' en 'INFORMATION_SCHEMA' objecten)
                # Voor Azure SQL Server meestal via schema_name uitgesloten, maar extra check kan
                query += " AND t.table_type IN ({})".format(
                    ','.join('?' for _ in allowed_types)
                )
                params.extend(allowed_types)
                # Optioneel: exclude system tables by name patterns if nodig

            else:
                # Include all, dus ook system objects
                query += " AND t.table_type IN ({})".format(
                    ','.join('?' for _ in allowed_types)
                )
                params.extend(allowed_types)

            # Filter op specifieke tabelnamen
            if table_filter:
                placeholders = ','.join('?' for _ in table_filter)
                query += f" AND t.table_name IN ({placeholders})"
                params.extend(table_filter)

            query += " ORDER BY t.table_name"
            cursor.execute(query, params)

            return [{
                'table_name': row[0],
                'table_type': row[1],
                'view_definition': row[2] if row[2] else None,
                'connection_type': connection_type
            } for row in cursor.fetchall()]

    else:
        connection_type = 'PostgreSQL'
        with source_conn.cursor() as cursor:
            # Basis query
            query = """
                SELECT
                    t.table_name,
                    t.table_type,
                    v.definition as view_definition
                FROM information_schema.tables t
                LEFT JOIN pg_views v
                    ON t.table_name = v.viewname
                    AND t.table_schema = v.schemaname
                WHERE t.table_schema = %s
            """

            params = [schema_name]

            allowed_types = ["BASE TABLE"]
            if include_views:
                allowed_types.append("VIEW")

            query += " AND t.table_type = ANY (%s)"
            params.append(allowed_types)

            # Filter op specifieke tabelnamen
            if table_filter:
                query += " AND t.table_name = ANY (%s)"
                params.append(table_filter)

            query += " ORDER BY t.table_name"
            cursor.execute(query, params)

            return [{
                'table_name': row[0],
                'table_type': row[1],
                'view_definition': row[2] if row[2] else None,
                'connection_type': connection_type
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
                UPDATE catalog.dw_catalog_runs 
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