import json
from data_catalog.ai_analyzer.runners.table_runner import run_single_table
from data_catalog.ai_analyzer.output_writer import store_analysis_result_to_file

def test_real_table_analysis_ods_alias_facturen(tmp_path):
    print(f"[DRYRUN OUTPUT DIR] {tmp_path}")

    table = {
        "server_name": "10.3.152.2",
        "database_name": "ENK_DEV1",
        "schema_name": "stg",
        "table_name": "ods_alias_facturen",
        "table_type": "BASE TABLE"
    }

    # Override output writer
    def capture_to_tmp(table_name, result):
        path = tmp_path / f"{table_name}_dryrun.json"
        path.write_text(json.dumps(result, indent=2))
        print(f"[SAVED] Resultaat opgeslagen in {path}")

    import data_catalog.ai_analyzer.output_writer as ow
    ow.store_analysis_result_to_file = capture_to_tmp

    print("[RUN] run_single_table wordt gestart...")
    run_single_table(
        table=table,
        analysis_type="all_in_one",
        author="dryrun_real",
        dry_run=True,
        run_id=4001
    )

    # Controleer of resultaat er is
    output_file = tmp_path / "ods_alias_facturen_dryrun.json"
    assert output_file.exists()
    result = json.loads(output_file.read_text())

    print("\n=== GEGENEERDE PROMPT (eerste 1000 tekens) ===")
    print(result["prompt"][:1000])

    assert "factuur" in result["prompt"].lower() or "omschrijving" in result["prompt"].lower()
