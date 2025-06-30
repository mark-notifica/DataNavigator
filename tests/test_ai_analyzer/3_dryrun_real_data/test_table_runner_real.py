import json
import pytest
from data_catalog.ai_analyzer.runners.table_runner import run_single_table
from ai_analyzer.postprocessor.output_writer import store_analysis_result_to_file
from ai_analyzer.utils.catalog_reader import get_metadata
from ai_analyzer.utils.source_data_reader import get_sample_data

def test_real_table_analysis_ods_alias_facturen(tmp_path):
    print(f"[DRYRUN OUTPUT DIR] {tmp_path}")

    table = {
        "server_name": "10.3.152.2",
        "database_name": "ENK_DEV1",
        "schema_name": "stg",
        "table_name": "ods_alias_facturen",
        "table_type": "BASE TABLE"
    }

    # Extra precheck: metadata
    metadata = get_metadata(table)
    if not metadata:
        pytest.fail("❌ Geen metadata beschikbaar in catalogus voor deze tabel.")
    print("✅ Metadata aanwezig:")
    for col in metadata:
        print("   -", col["column_name"], col.get("data_type", ""))

    # Extra precheck: sample data
    sample = get_sample_data(table)
    if sample.empty:
        pytest.fail("❌ Sampledata is leeg voor deze tabel.")
    print("✅ Sample data aanwezig:")
    print(sample.head())

    # Override output writer
    def capture_to_tmp(table_name, result):
        path = tmp_path / f"{table_name}_dryrun.json"
        path.write_text(json.dumps(result, indent=2))
        print(f"[SAVED] Resultaat opgeslagen in {path}")
        print(f"[INHOUD] {json.dumps(result, indent=2)}")

    import ai_analyzer.postprocessor.output_writer as ow
    ow.store_analysis_result_to_file = capture_to_tmp

    print("[RUN] run_single_table wordt gestart...")
    run_single_table(
        table=table,
        analysis_type="all_in_one",
        author="dryrun_real",
        dry_run=True,
        run_id=4001
    )

    output_file = tmp_path / "ods_alias_facturen_dryrun.json"

    print("[DEBUG] Bestanden in tmp_path:")
    for p in tmp_path.glob("*"):
        print("  -", p.name)

    if not output_file.exists():
        pytest.fail("❌ Outputbestand niet aangemaakt. Controleer of run_single_table correct is uitgevoerd.")

    result = json.loads(output_file.read_text())

    if "prompt" not in result:
        pytest.fail(f"❌ Geen prompt gegenereerd. Inhoud resultaat: {result}")

    print("\n=== GEGENEERDE PROMPT (eerste 1000 tekens) ===")
    print(result["prompt"][:1000])

    assert "factuur" in result["prompt"].lower() or "omschrijving" in result["prompt"].lower()


