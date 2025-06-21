from data_catalog.database_server_cataloger import get_catalog_connection
from datetime import datetime

def store_relationship_suggestion(
    server_address: str,
    database_name: str,
    schema_name: str,
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
    Slaat een relatievoorstel op tussen twee tabellen (optioneel ook kolommen).
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_relationship_suggestions (
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
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s)
            """, (
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
                source,
                datetime.utcnow(),
                datetime.utcnow(),
                author,
                author
            ))
            conn.commit()
    finally:
        conn.close()