import os
import json
import glob
from pathlib import Path
from dotenv import load_dotenv
from ai_analyzer.runners.table_runner import run_batch_tables_by_config
import logging

logging.basicConfig(level=logging.DEBUG)


# Laad .env zodat AI_ANALYZER_OUTPUT_DIR wordt ingesteld
load_dotenv(dotenv_path=".env", override=True)


def run_live_column_classification():
    print("LIVE RUN START")

    run_batch_tables_by_config(
        ai_config_id=1,
        analysis_type="column_classification",
        author="live_user",
        dry_run=False  # <-- let op: dit is nu een echte run
    )

    log_dir = os.getenv("AI_ANALYZER_OUTPUT_DIR", "data_catalog/logfiles/ai_analyzer")
    log_path_pattern = Path(log_dir) / "run_*_results.json"
    log_files = glob.glob(str(log_path_pattern))

    if not log_files:
        print(f"❌ Geen logfile gevonden in {log_dir}")
        return

    latest = max(log_files, key=os.path.getctime)
    with open(latest, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            valid_items = [
                item for item in data
                if isinstance(item, dict) and ("prompt" in item or "status" in item)
            ]
            print(f"✅ {len(valid_items)} geldige entries gevonden")
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            return

    print(f"[INFO] Laatste logfile: {latest}")
    print("LIVE RUN END")


if __name__ == "__main__":
    run_live_column_classification()
