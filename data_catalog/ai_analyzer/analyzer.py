from .data_loader import get_enk_tables, get_metadata, get_sample_data
from .prompt_builder import build_prompt
from .openai_client import analyze_with_openai
from .output_writer import store_analysis_result, store_table_analysis_result

def run_ai_analysis():
    tables = get_enk_tables()

    for table in tables:
        metadata = get_metadata(table)
        sample = get_sample_data(table, metadata)

        if not sample:
            print(f"[SKIP] Geen data in {table['table_name']}")
            continue

        prompt = build_prompt(table, metadata, sample)
        response = analyze_with_openai(prompt)
        store_analysis_result(table, response)
        store_table_analysis_result(table, response)

        print(f"[OK] Analyzed {table['table_name']}")