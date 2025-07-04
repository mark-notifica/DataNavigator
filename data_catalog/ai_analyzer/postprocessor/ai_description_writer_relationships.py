from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection

def write_relationship_suggestion(
    run_id: int,
    server: str,
    database: str,
    schema: str,
    source_table: str,
    target_table: str,
    source_column: str = None,
    target_column: str = None,
    relationship_type: str = "unknown",
    confidence_score: float = None,
    description: str = "",
    source: str = "AI",
    author: str = "system"
):
    """
    Slaat een relatievoorstel op tussen twee tabellen en optioneel kolommen.
    """
    now = datetime.now()
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_relationship_suggestions (
                    run_id,
                    server_address,
                    database_name,
                    schema_name,
                    source_table,
                    source_column,
                    target_table,
                    target_column,
                    relationship_type,
                    confidence_score,
                    description,
                    is_current,
                    source,
                    date_created,
                    date_updated,
                    author_created,
                    author_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s)
            """, (
                run_id,
                server,
                database,
                schema,
                source_table,
                source_column,
                target_table,
                target_column,
                relationship_type,
                confidence_score,
                description,
                source,
                now,
                now,
                author,
                author
            ))
            conn.commit()
    finally:
        conn.close()