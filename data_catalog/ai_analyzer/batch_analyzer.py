from data_catalog.ai_analyzer.data_loader import get_metadata, get_sample_data
from data_catalog.ai_analyzer.prompt_builder import build_batch_prompt
from data_catalog.ai_analyzer.openai_client import analyze_with_openai
from data_catalog.ai_analyzer.output_writer import store_analysis_result, store_batch_analysis_result
from data_catalog.database_server_cataloger import get_catalog_connection

def get_tables_for_pattern(server_name, database_name, schema_name, prefix):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT server_name, database_name, schema_name, table_name
                FROM catalog.catalog_table
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name ILIKE %s
            """
            cur.execute(query, (server_name, database_name, schema_name, prefix + '%'))
            rows = cur.fetchall()
            return [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    finally:
        conn.close()

def run_batch_analysis(server_name, database_name, schema_name, prefix):
    print(f"Start batch-analyse voor {server_name}.{database_name}.{schema_name}.{prefix}*")
    tables = get_tables_for_pattern(server_name, database_name, schema_name, prefix)

    batch = []
    for table in tables:
        metadata = get_metadata(table)
        sample = get_sample_data(table, metadata)

        batch.append({
            "table_name": table["table_name"],
            "columns": metadata,
            "sample_rows": sample
        })

    prompt = build_batch_prompt(batch)
    response = analyze_with_openai(prompt)

    result = {
        "server": server_name,
        "database": database_name,
        "schema": schema_name,
        "prefix": prefix,
        "analysis": response
    }

    store_analysis_result(
        {"table_name": f"{schema_name}.{prefix}_batch"}, result
    )
    store_batch_analysis_result(
        {
            "server_name": server_name,
            "database_name": database_name,
            "schema_name": schema_name,
            "prefix": prefix
        }, response
    )
    print("Batch-analyse voltooid.")