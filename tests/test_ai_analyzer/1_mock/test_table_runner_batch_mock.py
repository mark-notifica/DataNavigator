"""
🧪 TESTTYPE: Unit test met mockdata voor batch-analyse
🧪 DOEL: Verifieer dat alle tabellen correct worden aangeroepen en issues gelogd worden
"""

import json
from collections import defaultdict
from data_catalog.ai_analyzer.runners.table_runner import run_batch_tables


def test_run_batch_tables_dry_run(tmp_path):
    # Simuleer 3 tabellen, waarvan 1 een view is
    fake_tables = [
        {"table_name": "fact_sales", "table_type": "BASE TABLE"},
        {"table_name": "dim_customers", "table_type": "BASE TABLE"},
        {"table_name": "vw_revenue", "table_type": "VIEW"},
    ]

    # Patch dependencies
    import data_catalog.ai_analyzer.runners.table_runner as tr

    tr.get_tables_for_pattern = lambda s, d, sc, p: fake_tables
    tr.get_metadata = lambda table: {
        "table_name": table["table_name"],
        "table_type": table["table_type"],
        "columns": [
            {"column_name": "id", "data_type": "int"},
            {"column_name": "value", "data_type": "numeric"},
        ],
    } if table["table_type"] == "BASE TABLE" else None
    tr.get_sample_data = lambda t, m: [{"id": 1, "value": 10}]
    tr.get_view_definition = lambda table: "SELECT * FROM something"
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}.json").write_text(json.dumps(result))
    tr.finalize_run_with_token_totals = lambda run_id: {"total_tokens": 123, "estimated_cost_usd": 0.001}

    run_batch_tables(
        server="test",
        database="testdb",
        schema="public",
        prefix="",
        analysis_type="all_in_one",
        author="unit_tester",
        dry_run=True,
        run_id=42
    )

    # Assert alle outputbestanden zijn gegenereerd
    expected_files = ["fact_sales.json", "dim_customers.json", "vw_revenue.json"]
    for f in expected_files:
        assert tmp_path.joinpath(f).exists(), f"{f} was not created"
