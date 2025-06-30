import logging
from datetime import datetime
from data_catalog.database_server_cataloger import get_catalog_connection
from ai_analyzer.postprocessor.output_writer import (
    store_ai_column_descriptions,
    store_ai_table_description,
    store_ai_schema_description
)


def extract_descriptions_from_results(run_id: int, author="ai_result_extractor"):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    analysis_type,
                    result_json
                FROM catalog.catalog_ai_analysis_results
                WHERE run_id = %s
            """, (run_id,))
            rows = cur.fetchall()

        for row in rows:
            server, database, schema, table, analysis_type, result_json = row
            result = result_json if isinstance(result_json, dict) else None
            if not result:
                continue

            tbl = {
                "server_name": server,
                "database_name": database,
                "schema_name": schema,
                "table_name": table
            }

            if analysis_type == "table_description" and "summary" in result:
                store_ai_table_description(run_id, tbl, result, author, description_type="short_summary")
            elif analysis_type == "view_definition_analysis" and "summary" in result:
                store_ai_table_description(run_id, tbl, result, author, description_type="view_definition")
            elif analysis_type == "column_classification" and "column_classification" in result:
                store_ai_column_descriptions(run_id, tbl, result["column_classification"], author)
            elif analysis_type.startswith("schema_") and "schema_summary" in result:
                store_ai_schema_description(run_id, server, database, schema, result, author, analysis_type)
            else:
                logging.info(f"[SKIP] Geen beschrijving opgeslagen voor {server}.{database}.{schema}.{table} ({analysis_type})")
    finally:
        conn.close()