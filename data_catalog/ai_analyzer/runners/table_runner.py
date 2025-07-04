import logging
from collections import Counter
import os
import json
from dotenv import load_dotenv

from ai_analyzer.utils.catalog_reader import get_metadata_with_ids, get_view_definition_with_ids, get_filtered_tables_with_ids
from ai_analyzer.prompts.prompt_builder import build_prompt_for_table
from ai_analyzer.utils.openai_client import analyze_with_openai
from ai_analyzer.postprocessor.ai_analysis_writer import store_ai_table_analysis
from ai_analyzer.analysis.analysis_matrix import ANALYSIS_TYPES
from ai_analyzer.postprocessor.ai_analysis_writer import (
    create_analysis_run_entry,
    finalize_run_with_token_totals,
    mark_analysis_run_complete,
    mark_analysis_run_failed,
    mark_analysis_run_aborted
)
from connection_handler import (
    get_ai_config_by_id,
    get_specific_connection,
    connect_to_source_database
)
from ai_analyzer.utils.file_writer import store_analysis_result_to_file

load_dotenv()

try:
    MAX_ALLOWED_TABLES = int(os.getenv("AI_MAX_ALLOWED_TABLES", 500))
except ValueError:
    MAX_ALLOWED_TABLES = 500
    logging.warning("[WAARSCHUWING] AI_MAX_ALLOWED_TABLES bevat geen geldige integer — fallback naar 500")
    
ALLOW_UNFILTERED_SELECTION = os.getenv("AI_ALLOW_UNFILTERED_SELECTION", "false").lower() == "true"



def run_batch_tables_by_config(connection_id: int, ai_config_id: int, analysis_type: str, author: str, dry_run: bool):
    ai_config = get_ai_config_by_id(ai_config_id)
    if not ai_config:
        logging.error(f"[ABORT] Geen AI-config gevonden met id={ai_config_id}")
        return

    try:
        connection = get_specific_connection(ai_config["connection_id"])
    except Exception as e:
        logging.error(f"[ABORT] Kan geen verbinding ophalen: {e}")
        return

    schema = ai_config.get("ai_schema_filter") or ''
    prefix = ai_config.get("ai_table_filter") or ''

    run_id = create_analysis_run_entry(
        server=connection["host"],
        database=ai_config["ai_database_filter"],
        schema=schema,
        prefix=prefix,
        analysis_type=analysis_type,
        author=author,
        is_dry_run=dry_run,
        connection_id=connection["id"],
        ai_config_id=ai_config_id
    )

    try:
        if not schema and not prefix and not ALLOW_UNFILTERED_SELECTION:
            logging.warning("[ABORT] Geen schema of prefix opgegeven — onveilige selectie wordt geblokkeerd.")
            mark_analysis_run_aborted(run_id, "onveilige selectie geblokkeerd")
            return

        # ✅ Gebruik catalogus voor filtering met wildcards
        tables = get_filtered_tables_with_ids(
            server_name=connection["host"],
            database_name=ai_config["ai_database_filter"],
            schema_pattern=schema,
            table_pattern=prefix
        )

        if not tables:
            logging.warning("[WAARSCHUWING] Geen tabellen gevonden met opgegeven filters.")
            mark_analysis_run_aborted(run_id, "Geen tabellen gevonden in catalogus")
            return

        print(f"[DEBUG] MAX_ALLOWED_TABLES = {MAX_ALLOWED_TABLES}")

        if len(tables) > MAX_ALLOWED_TABLES:
            logging.warning(f"[ABORT] Te veel tabellen geselecteerd ({len(tables)}). Maximaal toegestaan: {MAX_ALLOWED_TABLES}.")
            mark_analysis_run_aborted(run_id, f"Te veel tabellen geselecteerd: {len(tables)}")
            return

        logging.info(f"[INFO] {len(tables)} tabellen geselecteerd uit catalogus.")
        issue_counts = Counter()

        for row in tables:
            table = {
                "server_name": connection["host"],
                "database_name": ai_config["ai_database_filter"],
                "schema_name": row["schema_name"],
                "table_name": row["table_name"],
                "connection_id": connection["id"],
                "table_type": "BASE TABLE"
            }
            try:
                run_single_table(table, analysis_type, author, dry_run, run_id)
            except Exception as e:
                issue_counts["exceptions"] += 1
                logging.exception(f"[ERROR] Fout bij analyse van {table['table_name']}: {e}")

        if issue_counts:
            total_issues = sum(issue_counts.values())
            logging.info(f"[SUMMARY] {total_issues} tabellen overgeslagen of met fouten:")
            for issue, count in issue_counts.items():
                logging.info(f"  - {issue}: {count}")

        finalize_run_with_token_totals(run_id)
        mark_analysis_run_complete(run_id)
        logging.info(f"[DONE] Batch-analyse {'gesimuleerd' if dry_run else 'voltooid'}")

    except Exception as e:
        logging.exception("[FAIL] Batch-analyse gefaald")
        mark_analysis_run_failed(run_id, str(e))
    finally:
        conn = connect_to_source_database(connection, ai_config["ai_database_filter"])  # ← conn moet beschikbaar zijn
        conn.close()

def run_single_table(table: dict, analysis_type: str, author: str, dry_run: bool, run_id: int):
    logging.info(f"[RUN] Analyse gestart voor {table['table_name']} (run_id={run_id})")
    is_view = table.get("table_type", "").upper() in ("V", "VIEW")

    try:
        if analysis_type == "view_definition_analysis" and is_view:
            view_def = get_view_definition_with_ids(table)
            if not view_def:
                logging.warning(f"[SKIP] Geen viewdefinitie voor {table['table_name']}")
                result = {"issues": ["no_view_definition"]}
                if dry_run:
                    store_analysis_result_to_file(table['table_name'], result)
                else:
                    store_ai_table_analysis(run_id, table, result, analysis_type)
                return

            prompt = build_prompt_for_table(table, {"definition": view_def}, None, analysis_type)
            if dry_run:
                store_analysis_result_to_file(table['table_name'], {"prompt": prompt})
                return

            result = analyze_with_openai(prompt)
            result["analysis_type"] = analysis_type
            store_ai_table_analysis(run_id, table, result, analysis_type)
            return

        metadata = get_metadata_with_ids(table)
        if not metadata:
            logging.warning(f"[SKIP] Geen kolommen voor {table['table_name']}")
            result = {"issues": ["no_columns"]}
            if dry_run:
                store_analysis_result_to_file(table['table_name'], result)
            else:
                store_ai_table_analysis(run_id, table, result, analysis_type)
            return

        sample_data_func = ANALYSIS_TYPES.get(analysis_type, {}).get("sample_data_function")
        sample = sample_data_func(table) if sample_data_func else None

        if sample is None or sample.empty:
            logging.warning(f"[SKIP] Geen data in {table['table_name']}")
            result = {"issues": ["no_sample_data"]}
            if dry_run:
                store_analysis_result_to_file(table['table_name'], result)
            else:
                store_ai_table_analysis(run_id, table, result, analysis_type)
            return

        prompt = build_prompt_for_table(table, metadata, sample, analysis_type)
        if dry_run:
            store_analysis_result_to_file(table['table_name'], {"prompt": prompt})
            return

        result = analyze_with_openai(prompt)
        result["analysis_type"] = analysis_type
        store_ai_table_analysis(run_id, table, result, analysis_type)
        logging.info(f"[OK] Analyse opgeslagen voor {table['table_name']}")

    except Exception as e:
        logging.exception(f"[FAIL] Analyse gefaald voor {table.get('table_name', '?')}: {e}")
        store_analysis_result_to_file(table.get("table_name", "onbekend"), {"error": str(e)})