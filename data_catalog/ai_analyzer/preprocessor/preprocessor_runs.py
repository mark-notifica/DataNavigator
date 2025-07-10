from data_catalog.connection_handler import get_catalog_connection

def start_preprocessor_run(
    run_name: str,
    description: str,
    filter_server_name: str,
    filter_database_name: str,
    filter_schema_name: str,
    filter_table_name_prefix: str = None,
    author: str = None,
    config_source: str = None,
    log_path: str = None,
    preprocessor_type: str = None,           # ⬅️ nieuw
    columns_profiled: int = None             # ⬅️ optioneel
) -> int:
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
            insert into catalog.preprocessor_runs (
                run_name
              , description
              , filter_server_name
              , filter_database_name
              , filter_schema_name
              , filter_table_name_prefix
              , author
              , config_source
              , log_path
              , status
              , started_at
              , preprocessor_type
              , columns_profiled
            )
            values (
                %(run_name)s
              , %(description)s
              , %(filter_server_name)s
              , %(filter_database_name)s
              , %(filter_schema_name)s
              , %(filter_table_name_prefix)s
              , %(author)s
              , %(config_source)s
              , %(log_path)s
              , 'started'
              , now()
              , %(preprocessor_type)s
              , %(columns_profiled)s
            )
            returning id;
            """
            cur.execute(query, {
                "run_name": run_name,
                "description": description,
                "filter_server_name": filter_server_name,
                "filter_database_name": filter_database_name,
                "filter_schema_name": filter_schema_name,
                "filter_table_name_prefix": filter_table_name_prefix,
                "author": author,
                "config_source": config_source,
                "log_path": log_path,
                "preprocessor_type": preprocessor_type,
                "columns_profiled": columns_profiled
            })
            run_id = cur.fetchone()[0]
            conn.commit()
            return run_id
    finally:
        conn.close()

def complete_preprocessor_run(
    run_id: int,
    status: str = "completed",
    tables_processed: int = None,
    columns_profiled: int = None,
    notes: str = None
):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
            update catalog.preprocessor_runs
            set
              status = %(status)s
            , completed_at = now()
            , updated_at = now()
            , tables_processed = coalesce(%(tables_processed)s, tables_processed)
            , columns_profiled = coalesce(%(columns_profiled)s, columns_profiled)
            , notes = coalesce(%(notes)s, notes)
            where id = %(run_id)s
            """
            cur.execute(query, {
                "run_id": run_id,
                "status": status,
                "tables_processed": tables_processed,
                "columns_profiled": columns_profiled,
                "notes": notes
            })
        conn.commit()
    finally:
        conn.close()

def mark_preprocessor_run_aborted(run_id: int, notes: str = None):
    complete_preprocessor_run(
        run_id = run_id,
        status = "aborted",
        notes = notes
    )
    
def mark_preprocessor_run_failed(run_id: int, notes: str = None):
    complete_preprocessor_run(
        run_id = run_id,
        status = "failed",
        notes = notes
    )