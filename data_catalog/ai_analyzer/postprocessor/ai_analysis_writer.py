import json
from sqlalchemy import text
from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection
from ai_analyzer.catalog_access.catalog_reader import get_column_id
import logging

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

def update_log_path_for_run(run_id: int, log_path: str):
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            logging.info(f"[DEBUG] DB-update: run_id={run_id}, log_path={log_path}")
            cur.execute(
                "UPDATE catalog.catalog_ai_analysis_runs SET log_path = %s WHERE id = %s",
                (log_path, run_id)
            )
            conn.commit()
            logging.info("[DEBUG] Commit uitgevoerd voor update_log_path_for_run()")
    except Exception as e:
        logging.warning(f"[FOUT] Kan log_path bijwerken voor run {run_id}: {e}")

def create_analysis_run_entry(
    server: str,
    database: str,
    schema: str,
    prefix: str,
    analysis_type: str,
    author: str,
    is_dry_run: bool,
    connection_id: int,
    ai_config_id: int,
    model_used: str = None,
    temperature: float = None,
    max_tokens: int = None,
    model_config_source: str = None,
    run_name: str = None,
    description: str = None
) -> int:

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
                    , model_used
                    , temperature
                    , max_tokens
                    , model_config_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                ai_config_id,
                model_used,
                temperature,
                max_tokens,
                model_config_source
            ))
            run_id = cur.fetchone()[0]
            conn.commit()
            logging.info(f"[RUN CREATED] Nieuwe AI-analyse aangemaakt: run_id={run_id}, name='{run_name}'")
            return run_id
    finally:
        conn.close()

def get_token_totals_for_run(run_id: int) -> dict:
    """
    Haalt de geaggregeerde token-totalen en kosten op uit de results-tabel.
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
            return {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "estimated_cost_usd": float(cost)
            }
    finally:
        conn.close()

def finalize_and_complete_run(run_id: int):
    """
    Haalt totalen op uit results-tabel, slaat ze op in runs-tabel
    en markeert de run als voltooid.
    """
    totals = get_token_totals_for_run(run_id)

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET prompt_tokens = %s,
                    completion_tokens = %s,
                    total_tokens = %s,
                    estimated_cost_usd = %s,
                    status = 'completed',
                    completed_at = NOW()
                WHERE id = %s
            """, (
                totals["prompt_tokens"],
                totals["completion_tokens"],
                totals["total_tokens"],
                totals["estimated_cost_usd"],
                run_id
            ))
            conn.commit()

            logging.info(
                f"[RUN COMPLETE] Run {run_id} voltooid met {totals['total_tokens']} tokens, "
                f"kosten: ${totals['estimated_cost_usd']:.4f}"
            )
            return totals
    finally:
        conn.close()

def mark_analysis_run_failed(run_id: int, reason: str = None):
    """
    Markeert een run als 'failed', vult completed_at in, aggregeert tokens en logt de gebeurtenis.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            totals = get_token_totals_for_run(run_id)

            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET status = 'failed',
                    notes = %s,
                    completed_at = NOW()
                WHERE id = %s
            """, (reason, run_id))

            conn.commit()

            logging.warning(
                f"[RUN FAILED] Run {run_id} gemarkeerd als 'failed'. "
                f"Reden: {reason} — Tokens: {totals['total_tokens']}, Kosten: ${totals['estimated_cost_usd']:.4f}"
            )
    finally:
        conn.close()


def mark_analysis_run_aborted(run_id: int, reason: str = None):
    """
    Markeert een run als 'aborted', vult completed_at in, aggregeert tokens en logt de gebeurtenis.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            totals = get_token_totals_for_run(run_id)

            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET status = 'aborted',
                    notes = %s,
                    completed_at = NOW()
                WHERE id = %s
            """, (reason, run_id))

            conn.commit()

            logging.info(
                f"[RUN ABORTED] Run {run_id} gemarkeerd als 'aborted'. "
                f"Reden: {reason} — Tokens: {totals['total_tokens']}, Kosten: ${totals['estimated_cost_usd']:.4f}"
            )
    finally:
        conn.close()

def store_ai_table_analysis(run_id: int, table: dict, result: dict, analysis_type: str):
    """
    Slaat AI-analyse op inclusief table_id, column_id, schema_id en database_id.
    Bij column_classification wordt per kolom een regel opgeslagen met losse velden voor prompt/response.
    """
    now = datetime.now()
    conn = get_catalog_connection()

    try:
        with conn.cursor() as cur:

            if analysis_type == "column_classification" and "column_classification" in result:
                response_dict = result["column_classification"]
                prompt = result.get("prompt")

                for column_name, label in response_dict.items():
                    column_id = get_column_id(table_id=table.get("table_id"), column_name=column_name)

                    if column_id is None:
                        logging.warning(f"[PARSER] column_id niet gevonden voor {table['table_name']}.{column_name}. Wordt opgeslagen met column_id=None.")

                    response_json = json.dumps(label)

                    values = (
                        run_id,
                        table.get("database_id"),
                        table.get("schema_id"),
                        table.get("table_id"),
                        column_id, 
                        column_name,
                        table.get("server_name"),
                        table.get("database_name"),
                        table.get("schema_name"),
                        table.get("table_name"),
                        analysis_type,
                        prompt,
                        response_json,
                        None,  
                        "ok", 
                        None, 
                        None,  
                        result.get("tokens", {}).get("prompt"),
                        result.get("tokens", {}).get("completion"),
                        result.get("tokens", {}).get("total"),
                        result.get("tokens", {}).get("estimated_cost_usd"),
                        now,
                        False,
                        "pending"
                    )

                    cur.execute(""" 
                        INSERT INTO catalog.catalog_ai_analysis_results (
                            run_id,
                            database_id,
                            schema_id,
                            table_id,
                            column_id,
                            column_name,    
                            server_name,
                            database_name,
                            schema_name,
                            table_name,
                            analysis_type,
                            prompt,
                            response_json,
                            summary_json,
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
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s)
                    """, values)

                    logging.info(f"[STORE] Kolomanalyse opgeslagen voor {table.get('table_name')}[{column_name}]")

            else:
                # Tabel-analyse: 1 regel
                prompt = result.get("prompt")
                response_json = None

                if "result" in result:
                    response_json = json.dumps(result["result"])
                elif "response_json" in result:
                    response_json = json.dumps(result["response_json"])

                if not prompt:
                    logging.warning(f"[STORE] Lege prompt voor {table.get('table_name')} (analysis_type={analysis_type})")
                if not response_json:
                    logging.warning(f"[STORE] Geen response_json beschikbaar voor {table.get('table_name')} (analysis_type={analysis_type})")

                values = (
                    run_id,
                    table.get("database_id"),
                    table.get("schema_id"),
                    table.get("table_id"),
                    None,  # column_id
                    None,  # column_name
                    table.get("server_name"),
                    table.get("database_name"),
                    table.get("schema_name"),
                    table.get("table_name"),
                    result.get("analysis_type", analysis_type),
                    prompt,
                    response_json,
                    json.dumps(result.get("summary_json")) if result.get("summary_json") else None,
                    result.get("status", "ok"),
                    result.get("score"),
                    result.get("insights_summary") or result.get("summary"),
                    result.get("tokens", {}).get("prompt"),
                    result.get("tokens", {}).get("completion"),
                    result.get("tokens", {}).get("total"),
                    result.get("tokens", {}).get("estimated_cost_usd"),
                    now,
                    False,  # description_generated
                    "pending"
                )

                cur.execute("""
                    INSERT INTO catalog.catalog_ai_analysis_results (
                        run_id,
                        database_id,
                        schema_id,
                        table_id,
                        column_id,
                        column_name,
                        server_name,
                        database_name,
                        schema_name,
                        table_name,
                        analysis_type,
                        prompt,
                        response_json,
                        summary_json,
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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s)
                """, values)

                logging.info(f"[STORE] Tabelanalyse opgeslagen voor {table.get('table_name')} (analysis_type={analysis_type})")

        conn.commit()
    finally:
        conn.close()
