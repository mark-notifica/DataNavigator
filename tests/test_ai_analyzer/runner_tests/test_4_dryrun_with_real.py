import os
import pytest
from data_catalog.ai_analyzer.runners.table_runner import run_batch_tables_by_config

RUN_REAL_DB_TESTS = os.getenv("RUN_REAL_DB_TESTS") == "1"
NAV_DB_HOST = os.getenv("NAV_DB_HOST")
NAV_DB_NAME = os.getenv("NAV_DB_NAME")


@pytest.mark.skipif(
    not RUN_REAL_DB_TESTS or not NAV_DB_HOST or not NAV_DB_NAME,
    reason="Real DB tests uitgeschakeld of NAV_DB_* niet geconfigureerd"
)
def test_3_dryrun_with_real_data():
    run_batch_tables_by_config(
        connection_id=6,
        ai_config_id=1,
        analysis_type="column_classification",
        author="test_user",
        dry_run=True
    )

    # Deze test heeft geen asserts, want hij draait als validatie voor handmatige inspectie
    # of gebruik in CI als smoke test. Bij failure zal pytest de fout loggen.
