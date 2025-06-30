import logging
from ai_analyzer.utils.catalog_reader import get_tables_for_pattern, get_metadata
from ai_analyzer.utils.source_data_reader import get_sample_data
from ai_analyzer.prompts.prompt_builder import build_relationship_prompt
from ai_analyzer.utils.openai_client import analyze_with_openai
from ai_analyzer.postprocessor.output_writer import store_relationship_suggestion

def run_relationship_analysis(server, database, schema, prefix, run_id, author, dry_run=False):
    tables = get_tables_for_pattern(server, database, schema, prefix)
    logging.info(f"[REL] {len(tables)} tabellen geladen voor relatie-analyse")

    for i, t1 in enumerate(tables):
        for j, t2 in enumerate(tables):
            if i >= j:
                continue  # voorkom dubbele paren of zelfvergelijking

            meta1 = get_metadata(t1)
            meta2 = get_metadata(t2)
            if not meta1 or not meta2:
                continue

            sample1 = get_sample_data(t1)
            sample2 = get_sample_data(t2)

            context = {
                "table1": t1,
                "table2": t2,
                "metadata1": meta1,
                "metadata2": meta2,
                "sample1": sample1,
                "sample2": sample2
            }
            prompt = build_relationship_prompt(context)

            if dry_run:
                logging.info(f"[DRY RUN] Prompt tussen {t1['table_name']} en {t2['table_name']}\n{prompt}")
                continue

            result = analyze_with_openai(prompt)
            for rel in result.get("relationships", []):
                store_relationship_suggestion(
                    run_id,
                    server,
                    database,
                    schema,
                    source_table=t1["table_name"],
                    target_table=t2["table_name"],
                    source_column=rel.get("source_column"),
                    target_column=rel.get("target_column"),
                    relationship_type=rel.get("type", "unknown"),
                    confidence_score=rel.get("confidence"),
                    description=rel.get("description", ""),
                    source="AI",
                    author=author
                )
            logging.info(f"[OK] Relatieanalyse opgeslagen voor {t1['table_name']} -> {t2['table_name']}")
