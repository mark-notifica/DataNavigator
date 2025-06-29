"""
✅ TESTTYPE: Unit tests met mock data
✅ DOEL: Test de analyse van losse tabellen en views zonder afhankelijkheid van echte catalogus
"""

import os
import json
import tempfile
from data_catalog.ai_analyzer.runners.table_runner import run_single_table


def test_run_single_table_regular_table(tmp_path):
    table = {
        "table_name": "fact_sales",
        "table_type": "BASE TABLE"
    }

    metadata = {
        "table_name": "fact_sales",
        "table_type": "BASE TABLE",
        "columns": [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "amount", "data_type": "numeric"},
            {"column_name": "order_date", "data_type": "date"},
        ]
    }

    sample_data = [
        {"id": 1, "amount": 100, "order_date": "2024-01-01"},
        {"id": 2, "amount": 200, "order_date": "2024-01-02"},
    ]

    # Patch dependencies
    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_metadata = lambda t: metadata
    tr.get_sample_data = lambda t, m: sample_data
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}.json").write_text(json.dumps(result))

    run_single_table(table=table, analysis_type="all_in_one", author="tester", dry_run=True, run_id=1)

    output_file = tmp_path.joinpath("fact_sales.json")
    assert output_file.exists()

    data = json.loads(output_file.read_text())
    assert "prompt" in data
    assert "PRIMARY_KEY" in data["prompt"] or "Classificeer" in data["prompt"]


def test_run_single_view_definition(tmp_path):
    table = {
        "table_name": "vw_active_customers",
        "table_type": "VIEW"
    }

    view_def = "SELECT * FROM customers WHERE active = true"

    # Patch dependencies
    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_view_definition = lambda t: view_def
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}.json").write_text(json.dumps(result))

    run_single_table(table=table, analysis_type="view_definition_analysis", author="tester", dry_run=True, run_id=99)

    output_file = tmp_path.joinpath("vw_active_customers.json")
    assert output_file.exists()
    data = json.loads(output_file.read_text())
    assert "prompt" in data
    assert "SELECT * FROM customers" in data["prompt"]


def test_run_single_table_no_metadata(tmp_path):
    table = {"table_name": "missing_table", "table_type": "BASE TABLE"}

    # Patch dependencies
    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_metadata = lambda t: None
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}.json").write_text(json.dumps(result))

    run_single_table(table=table, analysis_type="all_in_one", author="tester", dry_run=True, run_id=2)

    output_file = tmp_path.joinpath("missing_table.json")
    assert output_file.exists()
    data = json.loads(output_file.read_text())
    assert data.get("issues") == ["no_columns"]

def test_run_single_view_definition_empty(tmp_path):
    table = {"table_name": "vw_empty", "table_type": "VIEW"}

    # Patch: view zonder SQL-definitie
    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_view_definition = lambda t: ""
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}.json").write_text(json.dumps(result))

    run_single_table(table=table, analysis_type="view_definition_analysis", author="tester", dry_run=True, run_id=100)

    output_file = tmp_path.joinpath("vw_empty.json")
    assert output_file.exists()
    data = json.loads(output_file.read_text())
    assert data.get("issues") == ["no_view_definition"]

def test_run_single_table_missing_column_types(tmp_path):
    table = {"table_name": "broken_table", "table_type": "BASE TABLE"}

    metadata = {
        "table_name": "broken_table",
        "table_type": "BASE TABLE",
        "columns": [
            {"column_name": "id"},
            {"column_name": "value"}
        ]
    }

    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_metadata = lambda t: metadata
    tr.get_sample_data = lambda t, m: [{"id": 1, "value": 10}]
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}.json").write_text(json.dumps(result))

    run_single_table(table=table, analysis_type="column_classification", author="tester", dry_run=True, run_id=3)

    output_file = tmp_path.joinpath("broken_table.json")
    assert output_file.exists()
    data = json.loads(output_file.read_text())
    assert "prompt" in data
    assert "value" in data["prompt"]
