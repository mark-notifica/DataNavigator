import pytest
from ai_analyzer.runners.table_runner import run_batch_tables_by_config


def test_3_dryrun_with_real_data():
    """
    🧪 TESTTYPE: Dry run met echte catalogus + brondatabase
    🧪 DOEL: Verifieert dat echte metadata wordt opgehaald en prompts gegenereerd zonder OpenAI-call
    """
    run_batch_tables_by_config(
        connection_id=6,
        ai_config_id=1,
        analysis_type="column_classification",
        author="test_user",
        dry_run=True
    )

    # Deze test heeft geen asserts, want hij draait als validatie voor handmatige inspectie
    # of gebruik in CI als smoke test. Bij failure zal pytest de fout loggen.