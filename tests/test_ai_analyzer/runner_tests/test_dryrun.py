import glob
import builtins
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)

import os
print("[TEST DEBUG] AI_MAX_ALLOWED_TABLES =", os.getenv("AI_MAX_ALLOWED_TABLES"))

from ai_analyzer.runners.table_runner import run_batch_tables_by_config

# Zorg dat print niet door pytest wordt onderdrukt
print = builtins.print

def test_dryrun_real_config_column_classification():
    print("TEST START")

    # 🔁 Dry-run met echte config en connectie
    run_batch_tables_by_config(
        connection_id=6,
        ai_config_id=1,
        analysis_type="column_classification",
        author="pytest_user",
        dry_run=True
    )

    print("TEST END")

    # ✅ Controleer of er bestanden zijn weggeschreven
    files = glob.glob("./tests/output/*_analysis.json")
    assert len(files) > 0, "Geen output-bestanden gevonden"
    print(f"[TEST] Gevonden bestanden: {files}")
