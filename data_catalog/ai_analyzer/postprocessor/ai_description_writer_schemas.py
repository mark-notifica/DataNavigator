from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection
from ai_analyzer.utils.catalog_metadata import get_schema_metadata

def store_schema_description(run_id, server, database, schema, result_json, author="ai_analyzer", description_type="schema_context"):
    schema_meta = get_schema_metadata(server, database, schema)
    if not schema_meta:
        raise ValueError(f"Schema metadata niet gevonden: {server}.{database}.{schema}")

    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.now()

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            # Zet oudere descriptions op inactive
            cur.execute("""
                UPDATE catalog.catalog_schema_descriptions
                SET is_current = FALSE,
                    date_updated = %s,
                    author_updated = %s
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND description_type = %s
                  AND is_current = TRUE
            """, (now, author, server, database, schema, description_type))

            # Voeg nieuwe description toe
            cur.execute("""
                INSERT INTO catalog.catalog_schema_descriptions (
                    schema_id, server_name, database_name, schema_name,
                    analysis_run_id, description, description_type, source,
                    is_current, date_created, date_updated, author_created
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'AI', TRUE, %s, %s, %s)
            """, (
                schema_meta["schema_id"], server, database, schema,
                run_id, summary.strip(), description_type, now, now, author
            ))
            conn.commit()
    finally:
        conn.close()
