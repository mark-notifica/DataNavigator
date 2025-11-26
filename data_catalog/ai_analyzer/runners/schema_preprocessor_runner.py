from data_catalog.ai_analyzer.preprocessor.schema.graph_builder import main_graph_build
from ai_analyzer.model_logic.dw_ai_config_utils import resolve_ai_config_and_connection

def run_schema_preprocessor_by_config(ai_config_id: int, author: str = "system"):
    """
    Voert de graph builder uit op basis van AI-configuratie (en dus niet catalogusconfig).
    """
    import logging

    try:
        ai_config, conn_info = resolve_ai_config_and_connection(ai_config_id)
    except Exception as e:
        logging.error(f"[ABORT] {e}")
        return

    filter_db = ai_config.get("filter_database_name")
    filter_schema = ai_config.get("filter_schema_name")
    run_name = f"schema_preprocessor:{filter_db}.{filter_schema}"

    with conn_info["engine"].connect() as db_cursor:
        run_id = insert_schema_preprocessor_run(
            db_cursor,
            server_name = conn_info.get("server") or conn_info.get("server_name") or "UNKNOWN",
            database_name=filter_db,
            schema_name=filter_schema,
            run_name=run_name,
            description="Preprocessing via graph_builder",
            author=author,
            connection_id=conn_info.get("connection_id"),
            config_source="ai_config"
        )

        try:
            count = main_graph_build(
                db_cursor,
                server_name=conn_info["server"],
                database_name=filter_db,
                schema_name=filter_schema,
                schema_preprocessor_run_id=run_id,
                ai_config=ai_config
            )
            logging.info(f"[DONE] Graph build klaar met {count} relaties.")
            complete_schema_preprocessor_run(db_cursor, run_id, status="completed")

        except Exception as e:
            logging.exception(f"[FOUT] Graph builder faalt: {e}")
            complete_schema_preprocessor_run(db_cursor, run_id, status="failed")

def insert_schema_preprocessor_run(db_cursor, server_name, database_name, schema_name,
                                    run_name=None, description=None, author="system",
                                    connection_id=None, config_source="default"):
    """
    Start een nieuwe schema_preprocessor_run en retourneert het run_id.
    """
    insert_query = """
    INSERT INTO catalog.schema_preprocessor_runs (
        run_name,
        description,
        filter_server_name,
        filter_database_name,
        filter_schema_name,
        status,
        author,
        connection_id,
        config_source
    )
    VALUES (%s, %s, %s, %s, %s, 'started', %s, %s, %s)
    RETURNING id
    """
    db_cursor.execute(insert_query, (
        run_name,
        description,
        server_name,
        database_name,
        schema_name,
        author,
        connection_id,
        config_source
    ))
    return db_cursor.fetchone()[0]

def complete_schema_preprocessor_run(db_cursor, run_id, status="completed"):
    db_cursor.execute("""
        UPDATE catalog.schema_preprocessor_runs
        SET status = %s,
            completed_at = now(),
            updated_at = now()
        WHERE id = %s
    """, (status, run_id))