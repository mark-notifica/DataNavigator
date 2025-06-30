import logging
from collections import Counter
from ai_analyzer.utils.catalog_reader import get_metadata, get_tables_for_pattern, get_view_definition
from ai_analyzer.utils.source_data_reader import get_sample_data
from ai_analyzer.prompts.prompt_builder import build_prompt_for_table
from ai_analyzer.utils.openai_client import analyze_with_openai
from ai_analyzer.postprocessor.output_writer import (
    store_ai_table_analysis,
    store_analysis_result_to_file,
    store_ai_column_descriptions,
    store_ai_table_description,
    finalize_run_with_token_totals
)


def run_single_table(table: dict, analysis_type: str, author: str, dry_run: bool, run_id: int):
    logging.info(f"[RUN] Analyse gestart voor {table['table_name']} (run_id={run_id})")
    is_view = table.get("table_type", "").upper() in ("V", "VIEW")

    if analysis_type == "view_definition_analysis" and is_view:
        view_def = get_view_definition(table)
        if not view_def:
            logging.warning(f"[SKIP] Geen viewdefinitie beschikbaar voor {table['table_name']}")
            result = {"issues": ["no_view_definition"]}
            if dry_run:
                store_analysis_result_to_file(table["table_name"], result)
            else:
                store_ai_table_analysis(run_id, table, result)
            return

        prompt = build_prompt_for_table(table, {"definition": view_def}, None, analysis_type)

        if dry_run:
            logging.info(f"[DRY RUN] Prompt voor {table['table_name']}:\n{prompt}")
            store_analysis_result_to_file(table["table_name"], {"prompt": prompt})
            return

        result = analyze_with_openai(prompt)
        result["analysis_type"] = analysis_type
        store_ai_table_analysis(run_id, table, result)
        store_ai_table_description(run_id, table, result, analysis_type, author)
        logging.info(f"[OK] View-analyse opgeslagen voor {table['table_name']}")
        return

    metadata = get_metadata(table)
    if not metadata:
        logging.warning(f"[SKIP] Geen kolommen gevonden in catalogus voor {table['table_name']}")
        result = {"issues": ["no_columns"]}
        if dry_run:
            store_analysis_result_to_file(table["table_name"], result)
        else:
            store_ai_table_analysis(run_id, table, result)
        return

    sample = get_sample_data(table)
    if sample.empty:
        logging.warning(f"[SKIP] Geen data in {table['table_name']}")
        result = {"issues": ["no_sample_data"]}
        if dry_run:
            store_analysis_result_to_file(table["table_name"], result)
        else:
            store_ai_table_analysis(run_id, table, result)
        return

    prompt = build_prompt_for_table(table, metadata, sample, analysis_type)

    if dry_run:
        logging.info(f"[DRY RUN] Prompt voor {table['table_name']}:\n{prompt}")
        store_analysis_result_to_file(table["table_name"], {"prompt": prompt})
        return

    result = analyze_with_openai(prompt)
    result["analysis_type"] = analysis_type
    store_ai_table_analysis(run_id, table, result)

    if analysis_type in ("column_classification", "all_in_one") and "column_classification" in result:
        store_ai_column_descriptions(run_id, table, result["column_classification"], author)

    if analysis_type in ("table_description", "view_definition_analysis", "all_in_one"):
        store_ai_table_description(run_id, table, result, analysis_type, author)

    logging.info(f"[OK] Analyse opgeslagen voor {table['table_name']}")


def run_batch_tables(server: str, database: str, schema: str, prefix: str, analysis_type: str, author: str, dry_run: bool, run_id: int):
    logging.info(f"[RUN] Batch-analyse gestart voor {server}.{database}.{schema}.{prefix}* (run_id={run_id})")
    tables = get_tables_for_pattern(server, database, schema, prefix)
    logging.info(f"{len(tables)} tabellen geselecteerd.")

    issue_counts = Counter()

    for table in tables:
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

    totals = finalize_run_with_token_totals(run_id)
    logging.info(f"[RUN STATS] Totaal tokens: {totals['total_tokens']} | Kosten: ${totals['estimated_cost_usd']:.4f}")
    logging.info(f"[DONE] Batch-analyse {'gesimuleerd' if dry_run else 'voltooid'}")