import os
import pytest
import glob
import builtins
import json
from pathlib import Path
from dotenv import load_dotenv
from ai_analyzer.runners.table_runner import run_batch_tables_by_config

# Zorg dat print niet door pytest wordt onderdrukt
print = builtins.print

# Laad .env om correcte output dir te gebruiken
load_dotenv(dotenv_path=".env", override=True)

RUN_REAL_DB_TESTS = os.getenv("RUN_REAL_DB_TESTS") == "1"
NAV_DB_HOST = os.getenv("NAV_DB_HOST")
NAV_DB_NAME = os.getenv("NAV_DB_NAME")


@pytest.mark.skipif(
    not RUN_REAL_DB_TESTS or not NAV_DB_HOST or not NAV_DB_NAME,
    reason="Real DB tests uitgeschakeld of NAV_DB_* niet geconfigureerd"
)
def test_dryrun_real_config_column_classification():
    print("TEST START")

    # 🔁 Dry-run met echte config en connectie
    run_batch_tables_by_config(
        ai_config_id=1,
        analysis_type="column_classification",
        author="pytest_user",
        dry_run=True
    )

    # 🔍 Zoek logfile
    log_dir = os.getenv("AI_ANALYZER_OUTPUT_DIR", "data_catalog/logfiles/ai_analyzer")
    log_path_pattern = Path(log_dir) / "run_*_dryrun_results.json"
    log_files = glob.glob(str(log_path_pattern))

    assert log_files, f"❌ Geen logfile gevonden in {log_dir}"
    print(f"[TEST] Gevonden logfile(s): {log_files}")

    # ✅ Valideer inhoud van meest recente bestand
    latest = max(log_files, key=os.path.getctime)
    with open(latest, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            assert isinstance(data, list), "❌ JSON root is geen lijst"
        except json.JSONDecodeError as e:
            assert False, f"❌ JSON decode error: {e}"

    valid_items = [item for item in data if isinstance(item, dict) and ("prompt" in item or "status" in item)]

    if not valid_items:
        print("⚠️  Geen geldige prompt/status entries in resultaat (mogelijk geen bruikbare tabellen)")
    else:
        print(f"✅ {len(valid_items)} geldige prompt/status entries gevonden")

    print(f"[TEST] Laatste logfile geldig: {latest}")
    print("TEST END")
