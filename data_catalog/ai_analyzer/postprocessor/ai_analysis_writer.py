from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection

def store_ai_table_description(run_id: int, table: dict, result_json: dict,
                               author="ai_analyzer", description_type="short_summary"):
    """
    Slaat een tabelbeschrijving op inclusief table_id.
    :param table: dict met o.a. server_name, database_name, schema_name, table_name, table_id
    """
    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.now()

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_table_descriptions (
                    table_id,
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    description,
                    description_type,
                    source,
                    is_current,
                    date_created,
                    date_updated,
                    author_created,
                    ai_table_type,
                    ai_classified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'AI', TRUE, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT uq_table_description_unique_current DO UPDATE
                SET description = EXCLUDED.description,
                    description_type = EXCLUDED.description_type,
                    date_updated = EXCLUDED.date_updated,
                    author_updated = EXCLUDED.author_created,
                    ai_table_type = EXCLUDED.ai_table_type,
                    ai_classified_at = EXCLUDED.ai_classified_at
            """, (
                table.get("table_id"),
                table.get("server_name"),
                table.get("database_name"),
                table.get("schema_name"),
                table.get("table_name"),
                summary.strip(),
                description_type,
                now,
                now,
                author,
                table.get("table_type"),
                now
            ))
            conn.commit()
    finally:
        conn.close()


def store_ai_column_descriptions(run_id: int, table: dict, column_classification: dict, author="ai_analyzer"):
    """
    Slaat kolomclassificaties op inclusief column_id.
    :param table: dict met o.a. server_name, database_name, schema_name, table_name, table_id
    :param column_classification: dict met kolomnamen als keys en classificatie info als values
    """
    now = datetime.now()
    conn = get_catalog_connection()

    try:
        with conn.cursor() as cur:
            for column_name, info in column_classification.items():
                # Eerst de column_id ophalen op basis van table_id en column_name
                cur.execute("""
                    SELECT id FROM catalog.catalog_columns
                    WHERE table_id = %s AND column_name = %s
                    LIMIT 1
                """, (table.get("table_id"), column_name))
                row = cur.fetchone()
                if not row:
                    # Kolom niet gevonden, evt. loggen en overslaan
                    continue
                column_id = row[0]

                # Check of classificatie al bestaat voor deze kolom en run
                cur.execute("""
                    SELECT 1 FROM catalog.catalog_column_descriptions
                    WHERE server_name = %s
                      AND database_name = %s
                      AND schema_name = %s
                      AND table_name = %s
                      AND column_name = %s
                      AND analysis_run_id = %s
                      AND is_current = TRUE
                """, (
                    table.get("server_name"),
                    table.get("database_name"),
                    table.get("schema_name"),
                    table.get("table_name"),
                    column_name,
                    run_id
                ))
                if cur.fetchone():
                    continue

                cur.execute("""
                    INSERT INTO catalog.catalog_column_descriptions (
                        column_id,
                        server_name,
                        database_name,
                        schema_name,
                        table_name,
                        column_name,
                        analysis_run_id,
                        classification,
                        confidence,
                        notes,
                        author_created,
                        is_current,
                        date_created,
                        date_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
                """, (
                    column_id,
                    table.get("server_name"),
                    table.get("database_name"),
                    table.get("schema_name"),
                    table.get("table_name"),
                    column_name,
                    run_id,
                    info.get("classification"),
                    info.get("confidence"),
                    info.get("notes"),
                    author,
                    now,
                    now
                ))
            conn.commit()
    finally:
        conn.close()
