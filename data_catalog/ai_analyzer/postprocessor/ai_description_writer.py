import json
from datetime import datetime, timezone
from data_catalog.connection_handler import get_catalog_connection

def store_ai_table_description(run_id, table: dict, result_json: dict, author="ai_analyzer", description_type="short_summary"):
    """
    Slaat beschrijving van een BASE TABLE of VIEW op in catalog_table_descriptions.
    - Gebruik description_type om het type tekst te onderscheiden, bv:
        - 'short_summary'
        - 'view_definition'
        - 'dwh_layer_explanation'
    """
    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.now(timezone.utc)  # Tijd in UTC met timezone-aware object

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_table_descriptions (
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
                VALUES (%s, %s, %s, %s, %s, %s, 'AI', TRUE, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uq_table_description_unique_current DO NOTHING
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"],
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

def store_ai_column_descriptions(run_id, table: dict, column_classification: dict, author="ai_analyzer"):
    """
    Slaat classificatie van kolommen op in catalog.catalog_column_descriptions,
    alleen als deze classificatie nog niet bestaat voor deze kolom + run.
    """
    conn = get_catalog_connection()
    inserted_count = 0
    skipped_count = 0
    now = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            for column_name, info in column_classification.items():
                # Check of classificatie al bestaat voor deze kolom en run
                cur.execute("""
                    SELECT 1 FROM catalog.catalog_column_descriptions
                    WHERE server_name = %s
                      AND database_name = %s
                      AND schema_name = %s
                      AND table_name = %s
                      AND column_name = %s
                      AND analysis_run_id = %s
                """, (
                    table["server_name"],
                    table["database_name"],
                    table["schema_name"],
                    table["table_name"],
                    column_name,
                    run_id
                ))
                if cur.fetchone():
                    skipped_count += 1
                    continue

                cur.execute("""
                    INSERT INTO catalog.catalog_column_descriptions (
                        server_name,
                        database_name,
                        schema_name,
                        table_name,
                        column_name,
                        analysis_run_id,
                        classification,
                        confidence,
                        notes,
                        author,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    table["server_name"],
                    table["database_name"],
                    table["schema_name"],
                    table["table_name"],
                    column_name,
                    run_id,
                    info.get("classification"),
                    info.get("confidence"),
                    info.get("notes"),
                    author,
                    now
                ))
                inserted_count += 1

            conn.commit()
    finally:
        conn.close()

    # Simpele stdout-log — je kunt hier desgewenst `logging` toevoegen
    print(f"🔍 Kolomclassificaties opgeslagen voor {table['table_name']}: {inserted_count} toegevoegd, {skipped_count} overgeslagen.")
