import json
from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection
from ai_analyzer.utils.catalog_reader import get_column_id

def generate_run_name(analysis_type: str, author: str = None) -> str:
    """
    Genereert een standaard run_name zoals:
    'analyse_20250704_0932_column_classification_mharing'
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    name_parts = [
        "analyse",
        timestamp,
        analysis_type.lower()
    ]
    if author:
        name_parts.append(author.lower().replace("@", "").replace(" ", "_"))
    return "_".join(name_parts)


def create_analysis_run_entry(server, database, schema, prefix, analysis_type, author, is_dry_run=False, run_name=None, description=None, connection_id=None, ai_config_id=None):
    """
    Maakt een nieuwe entry aan in catalog_ai_analysis_runs en retourneert de nieuwe run_id.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            if not run_name:
                run_name = generate_run_name(analysis_type, author)

            cur.execute("""
                INSERT INTO catalog.catalog_ai_analysis_runs (
                      run_name
                    , analysis_type
                    , description
                    , filter_server_name
                    , filter_database_name
                    , filter_schema_name
                    , filter_table_name_prefix
                    , author
                    , is_dry_run
                    , connection_id
                    , ai_config_id    
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
                RETURNING id
            """, (
                run_name,
                analysis_type,
                description,
                server,
                database,
                schema,
                prefix,
                author,
                is_dry_run, 
                connection_id, 
                ai_config_id
            ))
            run_id = cur.fetchone()[0]
            conn.commit()
            return run_id
    finally:
        conn.close()


def mark_analysis_run_complete(run_id: int):
    """
    Markeert een run als voltooid en vult completed_at in.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET status = 'completed',
                    completed_at = NOW()
                WHERE id = %s
            """, (run_id,))
            conn.commit()
    finally:
        conn.close()

def finalize_run_with_token_totals(run_id: int):
    """
    Aggregeert tokengebruik uit catalog_ai_analysis_results
    en schrijft totaalresultaten weg in catalog_ai_analysis_runs.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(prompt_tokens), 0),
                    COALESCE(SUM(completion_tokens), 0),
                    COALESCE(SUM(total_tokens), 0),
                    COALESCE(SUM(estimated_cost_usd), 0)
                FROM catalog.catalog_ai_analysis_results
                WHERE run_id = %s
            """, (run_id,))
            prompt_tokens, completion_tokens, total_tokens, cost = cur.fetchone()

            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET prompt_tokens = %s,
                    completion_tokens = %s,
                    total_tokens = %s,
                    estimated_cost_usd = %s
                WHERE id = %s
            """, (prompt_tokens, completion_tokens, total_tokens, cost, run_id))

            conn.commit()
            return {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "estimated_cost_usd": float(cost)
            }
    finally:
        conn.close()


def mark_analysis_run_failed(run_id: int, reason: str = None):
    """
    Markeert een run als 'failed' met optionele notitie.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET status = 'failed',
                    notes = %s,
                    completed_at = NOW()
                WHERE id = %s
            """, (reason, run_id))
            conn.commit()
    finally:
        conn.close()


def mark_analysis_run_aborted(run_id: int, reason: str = None):
    """
    Markeert een run als 'aborted' en sluit hem netjes af.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET status = 'aborted',
                    notes = %s,
                    completed_at = NOW()
                WHERE id = %s
            """, (reason, run_id))
            conn.commit()
    finally:
        conn.close()


def store_ai_table_analysis(run_id: int, table: dict, result: dict, analysis_type: str):
    """
    Slaat AI-analyse op inclusief table_id, column_id (indien van toepassing), schema_id en database_id.
    Bij column_classification wordt per kolom een regel opgeslagen.
    """
    now = datetime.now()
    conn = get_catalog_connection()

    try:
        with conn.cursor() as cur:
            # Speciale behandeling voor kolomanalyse (meerdere kolommen per resultaat)
            if analysis_type == "column_classification" and "column_classification" in result:
                for column_name, info in result["column_classification"].items():
                    column_id = get_column_id(
                        table_id=table.get("table_id"),
                        column_name=column_name
                    )

                    row_data = (
                        run_id,
                        table.get("database_id"),
                        table.get("schema_id"),
                        table.get("table_id"),
                        column_id,
                        table.get("server_name"),
                        table.get("database_name"),
                        table.get("schema_name"),
                        table.get("table_name"),
                        analysis_type,
                        json.dumps(info),  # Alleen deze kolom
                        info.get("status", "ok"),
                        info.get("score"),
                        info.get("insights_summary") or info.get("summary"),
                        info.get("prompt_tokens"),
                        info.get("completion_tokens"),
                        info.get("total_tokens"),
                        info.get("estimated_cost_usd"),
                        now,
                        # vaste waarden
                        False,  # description_generated
                        'pending'
                    )

                    cur.execute("""
                        INSERT INTO catalog.catalog_ai_analysis_results (
                            run_id,
                            database_id,
                            schema_id,
                            table_id,
                            column_id,
                            server_name,
                            database_name,
                            schema_name,
                            table_name,
                            analysis_type,
                            result_json,
                            status,
                            score,
                            insights_summary,
                            prompt_tokens,
                            completion_tokens,
                            total_tokens,
                            estimated_cost_usd,
                            created_at,
                            description_generated,
                            description_status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, row_data)

            else:
                # Standaard analyse (1 regel per tabel)
                cur.execute("""
                    INSERT INTO catalog.catalog_ai_analysis_results (
                        run_id,
                        database_id,
                        schema_id,
                        table_id,
                        column_id,
                        server_name,
                        database_name,
                        schema_name,
                        table_name,
                        analysis_type,
                        result_json,
                        status,
                        score,
                        insights_summary,
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                        estimated_cost_usd,
                        created_at,
                        description_generated,
                        description_status
                    )
                    VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, 'pending')
                """, (
                    run_id,
                    table.get("database_id"),
                    table.get("schema_id"),
                    table.get("table_id"),
                    table.get("server_name"),
                    table.get("database_name"),
                    table.get("schema_name"),
                    table.get("table_name"),
                    result.get("analysis_type"),
                    json.dumps(result),
                    result.get("status", "ok"),
                    result.get("score"),
                    result.get("insights_summary") or result.get("summary"),
                    result.get("prompt_tokens"),
                    result.get("completion_tokens"),
                    result.get("total_tokens"),
                    result.get("estimated_cost_usd"),
                    now
                ))

            conn.commit()

    finally:
        conn.close()
