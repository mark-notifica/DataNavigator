from data_catalog.database_server_cataloger import get_catalog_connection
import json
import os

def store_analysis_result(table: dict, result: dict):
    """Standaard opslag als JSON-bestand"""
    output_dir = "/mnt/data/ai_analyzer/output"
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{table['table_name']}_analysis.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(result, f, indent=2)


def store_batch_analysis_result(batch_info: dict, result: dict):
    """
    Slaat batch-analyse op in de catalogus in tabel: catalog.catalog_batch_analysis
    batch_info: dict met server_name, database_name, schema_name, prefix
    result: dict met analyse-output
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_batch_analysis (
                    server_name,
                    database_name,
                    schema_name,
                    prefix,
                    analysis_json
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                batch_info["server_name"],
                batch_info["database_name"],
                batch_info["schema_name"],
                batch_info["prefix"],
                json.dumps(result)
            ))
            conn.commit()
    finally:
        conn.close()

def store_table_analysis_result(table: dict, result: dict):
    """
    Slaat analyse op in catalog.catalog_table_analysis
    table: dict met server_name, database_name, schema_name, table_name
    result: dict met analyse-output
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_table_analysis (
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    analysis_json
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"],
                json.dumps(result)
            ))
            conn.commit()
    finally:
        conn.close()