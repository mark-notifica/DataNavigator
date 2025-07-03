from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection
from ai_analyzer.utils.catalog_metadata import get_table_metadata, get_column_metadata

def write_table_description(run_id, server, database, schema, table_name, result_json, author="ai_analyzer"):
    table = get_table_metadata(server, database, schema, table_name)
    if not table:
        raise ValueError(f"Tabel metadata niet gevonden: {server}.{database}.{schema}.{table_name}")

    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.now()

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            # Voorbeeld schrijven description
            cur.execute("""
                INSERT INTO catalog.catalog_table_descriptions (
                    table_id, server_name, database_name, schema_name, table_name,
                    description, description_type, source, is_current,
                    date_created, date_updated, author_created, ai_classified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 'short_summary', 'AI', TRUE, %s, %s, %s, %s)
                ON CONFLICT (server_name, database_name, schema_name, table_name, description_type, is_current)
                DO UPDATE SET description=EXCLUDED.description, date_updated=EXCLUDED.date_updated, author_updated=EXCLUDED.author_created
            """, (
                table["table_id"], table["server_name"], table["database_name"], table["schema_name"], table["table_name"],
                summary.strip(), now, now, author, now
            ))
            conn.commit()
    finally:
        conn.close()

def write_column_descriptions(run_id, table, column_classification, author="ai_analyzer"):
    columns = get_column_metadata(table["table_id"])
    # Zet kolomnaam -> column_id map
    col_map = {col["column_name"]: col["column_id"] for col in columns}

    now = datetime.now()
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            for col_name, info in column_classification.items():
                column_id = col_map.get(col_name)
                if not column_id:
                    continue
                # Insert kolombeschrijving
                cur.execute("""
                    INSERT INTO catalog.catalog_column_descriptions (
                        column_id, server_name, database_name, schema_name, table_name, column_name,
                        analysis_run_id, classification, confidence, notes,
                        author_created, is_current, date_created, date_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    column_id, table["server_name"], table["database_name"], table["schema_name"], table["table_name"], col_name,
                    run_id, info.get("classification"), info.get("confidence"), info.get("notes"),
                    author, now, now
                ))
            conn.commit()
    finally:
        conn.close()
