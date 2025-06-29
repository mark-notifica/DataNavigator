
"""
🧪 DRY-RUN TEST: Verifieert per analysis_type dat de juiste prompt wordt gegenereerd
"""

import json
import pytest
from data_catalog.ai_analyzer.runners.table_runner import run_single_table


# 🔸 Testdata voor gewone tabel
base_table = {
    "table_name": "test_orders",
    "table_type": "BASE TABLE"
}

metadata = {
    "table_name": "test_orders",
    "table_type": "BASE TABLE",
    "columns": [
        {"column_name": "order_id", "data_type": "integer"},
        {"column_name": "customer_id", "data_type": "integer"},
        {"column_name": "order_date", "data_type": "date"},
        {"column_name": "amount", "data_type": "numeric"}
    ]
}

sample_data = [
    {"order_id": 1, "customer_id": 42, "order_date": "2024-01-01", "amount": 100},
    {"order_id": 2, "customer_id": 43, "order_date": "2024-01-02", "amount": 200}
]

# 🔸 Testdata voor view
view_table = {
    "table_name": "vw_customer_summary",
    "table_type": "VIEW"
}

view_definition = "SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id"


@pytest.mark.parametrize("analysis_type", [
    "table_description",
    "column_classification",
    "data_quality_check",
    "all_in_one"
])
def test_base_table_analysis_types_generate_prompt(tmp_path, analysis_type):
    print(f"[DRYRUN OUTPUT DIR] {tmp_path}")

    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_metadata = lambda t: metadata
    tr.get_sample_data = lambda t, m: sample_data
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}_{analysis_type}.json").write_text(json.dumps(result))

    run_single_table(base_table, analysis_type, author="dryrun_tester", dry_run=True, run_id=2025)

    output_file = tmp_path / f"test_orders_{analysis_type}.json"
    assert output_file.exists()
    result = json.loads(output_file.read_text())
    assert "prompt" in result
    assert "order_date" in result["prompt"] or "Classificeer" in result["prompt"]


def test_view_definition_analysis_prompt(tmp_path):
    print(f"[DRYRUN OUTPUT DIR] {tmp_path}")

    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_view_definition = lambda t: view_definition
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath(f"{table_name}_view.json").write_text(json.dumps(result))

    run_single_table(view_table, analysis_type="view_definition_analysis", author="dryrun_tester", dry_run=True, run_id=2026)

    output_file = tmp_path / "vw_customer_summary_view.json"
    assert output_file.exists()
    result = json.loads(output_file.read_text())
    assert "prompt" in result
    assert "SELECT" in result["prompt"]
    assert "customer_id" in result["prompt"]


def test_column_classification_prompt_structure(tmp_path):
    print(f"[DRYRUN OUTPUT DIR] {tmp_path}")

    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_metadata = lambda t: metadata
    tr.get_sample_data = lambda t, m: sample_data
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath("structure_check.json").write_text(json.dumps(result))

    run_single_table(base_table, "column_classification", author="tester", dry_run=True, run_id=123)

    output_file = tmp_path / "structure_check.json"
    assert output_file.exists()

    result = json.loads(output_file.read_text())
    prompt = result.get("prompt", "")
    assert "Antwoord als JSON" in prompt, "Prompt mist JSON-outputinstructie"
    assert '"kolomnaam": "LABEL"' in prompt, "Voorbeeldstructuur ontbreekt in prompt"

    # Controleer of alle kolomnamen terugkomen
    for col in metadata["columns"]:
        assert col["column_name"] in prompt, f"Kolom '{col['column_name']}' ontbreekt in prompt"


def test_data_quality_prompt_keywords(tmp_path):
    print(f"[DRYRUN OUTPUT DIR] {tmp_path}")

    import data_catalog.ai_analyzer.runners.table_runner as tr
    tr.get_metadata = lambda t: metadata
    tr.get_sample_data = lambda t, m: sample_data
    tr.store_analysis_result_to_file = lambda table_name, result: tmp_path.joinpath("dq_keywords.json").write_text(json.dumps(result))

    run_single_table(base_table, "data_quality_check", author="tester", dry_run=True, run_id=321)

    output_file = tmp_path / "dq_keywords.json"
    assert output_file.exists()

    result = json.loads(output_file.read_text())
    prompt = result.get("prompt", "")
    assert "⚠️" in prompt or "datakwaliteit" in prompt or "nulls" in prompt
    assert "duplicaten" in prompt or "vreemde waarden" in prompt

