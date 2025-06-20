import json
import os

def store_analysis_result(table, result):
    output_dir = "/mnt/data/ai_analyzer/output"
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{table['table_name']}_analysis.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(result, f, indent=2)