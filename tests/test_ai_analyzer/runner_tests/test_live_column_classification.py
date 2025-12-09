import os
import pytest
import builtins
from dotenv import load_dotenv
from ai_analyzer.runners.table_runner import run_batch_tables_by_config
import logging

logging.basicConfig(level=logging.DEBUG)

# pytest-compatibel print
print = builtins.print

# .env laden voor instellingen (zoals output dirs of DB info)
load_dotenv(dotenv_path=".env", override=True)

RUN_REAL_DB_TESTS = os.getenv("RUN_REAL_DB_TESTS") == "1"
NAV_DB_HOST = os.getenv("NAV_DB_HOST")
NAV_DB_NAME = os.getenv("NAV_DB_NAME")


@pytest.mark.skipif(
    not RUN_REAL_DB_TESTS or not NAV_DB_HOST or not NAV_DB_NAME,
    reason="Real DB tests uitgeschakeld of NAV_DB_* niet geconfigureerd"
)
def test_live_column_classification():
    print("LIVE TEST START")

    # 🔁 Live-run (schrijft naar database)
    run_batch_tables_by_config(
        ai_config_id=1,
        analysis_type="column_classification",
        author="pytest_user",
        dry_run=False  # ← LIVE RUN
    )
    print("LIVE TEST END")


if __name__ == "__main__":
    test_live_column_classification()
