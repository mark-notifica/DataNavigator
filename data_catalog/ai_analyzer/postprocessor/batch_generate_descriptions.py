import json
import logging
from data_catalog.connection_handler import get_catalog_connection

from ai_analyzer.postprocessor.ai_description_writer_tables import (
    store_table_description,
    store_column_descriptions
)
from ai_analyzer.postprocessor.ai_description_writer_schemas import (
    store_schema_description
)
from ai_analyzer.utils.catalog_metadata import get_table_metadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def batch_generate_descriptions(run_id: int, author: str = "ai_analyzer"):
    logging.info(f"Start batch beschrijving generatie voor run_id={run_id}")
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            # Selecteer alle pending AI-resultaten
            cur.execute("""
                SELECT run_id, server_name, database_name, schema_name, table_name, result_json
                FROM catalog.catalog_ai_analysis_results
                WHERE run_id = %s
                  AND description_status = 'pending'
            """, (run_id,))
            rows = cur.fetchall()

            logging.info(f"{len(rows)} AI-resultaten gevonden om te verwerken")

            for row in rows:
                run_id, server, db, schema, table_name, result_json = row
                result = json.loads(result_json)

                # Bepaal type entity op basis van presence van table_name
                try:
                    if table_name:  # Table of view
                        table = get_table_metadata(server, db, schema, table_name)
                        if not table:
                            logging.warning(f"Tabel metadata niet gevonden voor {server}.{db}.{schema}.{table_name}, skip")
                            continue

                        # Opslaan tabelbeschrijving en kolomclassificaties
                        if result.get("summary") or result.get("insights_summary"):
                            store_table_description(run_id, server, db, schema, table_name, result, author)

                        if "column_classification" in result:
                            store_column_descriptions(run_id, table, result["column_classification"], author)

                    else:  # Schema beschrijving
                        if result.get("summary") or result.get("insights_summary"):
                            store_schema_description(run_id, server, db, schema, result, author)

                    # Update status naar done
                    cur.execute("""
                        UPDATE catalog.catalog_ai_analysis_results
                        SET description_status = 'done'
                        WHERE run_id = %s
                          AND server_name = %s
                          AND database_name = %s
                          AND schema_name = %s
                          AND (table_name = %s OR (table_name IS NULL AND %s IS NULL))
                    """, (run_id, server, db, schema, table_name, table_name))
                    conn.commit()

                    logging.info(f"[OK] Beschrijving opgeslagen voor {server}.{db}.{schema}.{table_name or '[schema]'}")

                except Exception as e:
                    logging.error(f"[FAILED] Fout bij verwerken van {server}.{db}.{schema}.{table_name}: {e}")

                    cur.execute("""
                        UPDATE catalog.catalog_ai_analysis_results
                        SET description_status = 'failed'
                        WHERE run_id = %s
                          AND server_name = %s
                          AND database_name = %s
                          AND schema_name = %s
                          AND (table_name = %s OR (table_name IS NULL AND %s IS NULL))
                    """, (run_id, server, db, schema, table_name, table_name))
                    conn.commit()
    finally:
        conn.close()
        logging.info(f"Batch beschrijving generatie voor run_id={run_id} afgerond")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Gebruik: python batch_generate_descriptions.py <run_id> [author]")
        sys.exit(1)

    run_id = int(sys.argv[1])
    author = sys.argv[2] if len(sys.argv) > 2 else "ai_analyzer"

    batch_generate_descriptions(run_id, author)
