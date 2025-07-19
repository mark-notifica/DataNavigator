import logging
import json
from ai_analyzer.catalog_access.catalog_reader import get_metadata, get_tables_for_pattern, get_view_definition
from ai_analyzer.samples.sample_data_builder import get_sample_data
from ai_analyzer.postprocessor.output_writer import store_ai_schema_analysis, store_analysis_result_to_file
from ai_analyzer.utils.openai_client import analyze_with_openai
from ai_analyzer.prompts.prompt_builder import build_prompt_for_schema
from data_catalog.connection_handler import get_catalog_connection
from ai_analyzer.postprocessor.output_writer import finalize_run_with_token_totals
from data_catalog.ai_analyzer.utils.schema_validation import ensure_single_schema_across_tables

def run_schema_analysis(server, database, schema, author, dry_run: bool, run_id: int, analysis_type: str = "schema_context"):
    logging.info(f"[RUN] Schema-analyse gestart voor {server}.{database}.{schema} (run_id={run_id})")
    tables = get_tables_for_pattern(server, database, schema, "")
    logging.info(f"{len(tables)} tabellen gevonden.")

    table_analyses = []
    missing_analysis = []

    for table in tables:
        summary, is_ai_based = get_table_summary_with_fallback(
            table["server_name"],
            table["database_name"],
            table["schema_name"],
            table["table_name"]
        )
        table_info = {
            "table_name": table["table_name"],
            "summary": summary,
            "type": "UNKNOWN"  # eventueel in toekomst uit result_json halen
        }
        table_analyses.append(table_info)
        if not is_ai_based:
            missing_analysis.append(table["table_name"])

    prompt = build_prompt_for_schema(
        schema_metadata={"schema_name": schema},
        table_analyses=table_analyses,
        analysis_type=analysis_type
    )

    if dry_run:
        logging.info(f"[DRY RUN] Prompt voor schema {schema}:\n{prompt}")
        store_analysis_result_to_file(f"{schema}_schema_prompt", {
            "prompt": prompt,
            "missing_tables": missing_analysis,
            "analysis_type": analysis_type
        })
        return

    result = analyze_with_openai(prompt)
    result["missing_tables"] = missing_analysis
    result["analysis_type"] = analysis_type

    store_ai_schema_analysis(run_id, server, database, schema, result)

    totals = finalize_run_with_token_totals(run_id)
    logging.info(f"[RUN STATS] Totaal tokens: {totals['total_tokens']} | Kosten: ${totals['estimated_cost_usd']:.4f}")

    logging.info(f"[OK] Analyse opgeslagen voor schema {schema} ({analysis_type})")

    if missing_analysis:
        logging.info(f"[INFO] Geen AI-analyse beschikbaar voor {len(missing_analysis)} tabellen:")
        for t in missing_analysis:
            logging.info(f"  - {t}")


def get_table_summary_with_fallback(server, database, schema, table_name):
    """Retourneert (samenvatting, is_ai_based: bool)"""
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT result_json
                FROM catalog.catalog_ai_analysis_results
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name = %s
                  AND analysis_type = 'table_description'
                ORDER BY created_at DESC
                LIMIT 1
            """, (server, database, schema, table_name))
            row = cur.fetchone()
            if row:
                result = row[0]
                summary = result.get("summary") or ""
                suggested_keys = result.get("suggested_keys", [])
                dimension_type = result.get("type", "")
                keys_text = f"(sleutels: {', '.join(suggested_keys)})" if suggested_keys else ""
                return f"{summary} {dimension_type} {keys_text}".strip(), True
    except Exception as e:
        logging.warning(f"[WARN] Fout bij ophalen table analysis voor {schema}.{table_name}: {e}")
    finally:
        conn.close()

    return fallback_summary_from_metadata(server, database, schema, table_name), False


def fallback_summary_from_metadata(server, database, schema, table_name):
    """Geeft beknopte kolominformatie terug als geen AI-samenvatting beschikbaar is"""
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM catalog.catalog_column
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name = %s
                ORDER BY ordinal_position
            """, (server, database, schema, table_name))
            rows = cur.fetchall()
            if not rows:
                return "[geen kolominformatie beschikbaar]"

            columns = ", ".join([f"{r[0]} ({r[1]})" for r in rows[:6]])
            if len(rows) > 6:
                columns += ", ..."
            return f"[structure: {columns}]"
    except Exception as e:
        return f"[fout bij ophalen kolommen: {e}]"
    finally:
        conn.close()


