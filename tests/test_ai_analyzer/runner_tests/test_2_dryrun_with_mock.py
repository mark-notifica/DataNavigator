from unittest.mock import patch
from pathlib import Path
import pytest

from ai_analyzer.runners.table_runner import run_batch_tables_by_config

# Mockdata
mock_tables = [
    {"table_name": "fact_sales", "table_type": "BASE TABLE", "table_schema": "public"},
    {"table_name": "dim_customers", "table_type": "BASE TABLE", "table_schema": "public"},
    {"table_name": "vw_revenue", "table_type": "VIEW", "table_schema": "public"},
]
mock_metadata = [
    {"column_name": "id", "data_type": "int"},
    {"column_name": "value", "data_type": "numeric"},
]
mock_sample_data = [{"id": 1, "value": 10}]
mock_view_def = "SELECT * FROM sales"

@patch("ai_analyzer.runners.table_runner.store_analysis_result_to_file")
@patch("ai_analyzer.runners.table_runner.get_ai_config_by_id", return_value={
    "id": 1,
    "connection_id": 6,
    "ai_database_filter": "mockdb",
    "ai_schema_filter": "public",
    "ai_table_filter": None,
})
@patch("ai_analyzer.runners.table_runner.get_specific_connection", return_value={
    "id": 6,
    "name": "mock_conn",
    "connection_type": "PostgreSQL",
    "host": "localhost",
    "port": "5432",
    "username": "user",
    "password": "pass",
})
@patch("ai_analyzer.runners.table_runner.connect_to_source_database")
@patch("ai_analyzer.runners.table_runner.create_analysis_run_entry", return_value=99)
@patch("ai_analyzer.runners.table_runner.finalize_run_with_token_totals")
@patch("ai_analyzer.runners.table_runner.mark_analysis_run_complete")
@patch("ai_analyzer.utils.catalog_reader.get_view_definition_with_ids", return_value=mock_view_def)
@patch("ai_analyzer.utils.catalog_reader.get_metadata_with_ids", return_value=mock_metadata)
@patch("ai_analyzer.utils.catalog_reader.get_tables_for_pattern_with_ids", return_value=mock_tables)
def test_dry_run_with_mock_data(
    mock_tables_list,
    mock_meta,
    mock_view,
    mock_mark_complete,
    mock_finalize,
    mock_create,
    mock_connect,
    mock_get_conn,
    mock_get_conf,
    mock_store,
    tmp_path: Path
):
    from ai_analyzer.analysis import analysis_matrix

    # Sample functie tijdelijk overschrijven
    original_func = analysis_matrix.ANALYSIS_TYPES["column_classification"]["sample_data_function"]
    analysis_matrix.ANALYSIS_TYPES["column_classification"]["sample_data_function"] = lambda *args, **kwargs: mock_sample_data

    try:
        run_batch_tables_by_config(
            connection_id=6,
            ai_config_id=1,
            analysis_type="column_classification",
            author="pytest_user",
            dry_run=True  # belangrijk!
        )
    finally:
        analysis_matrix.ANALYSIS_TYPES["column_classification"]["sample_data_function"] = original_func

    # ✅ Verwachtingen controleren
    assert mock_store.call_count == 3, f"Expected 3 calls, got {mock_store.call_count}"
    mock_create.assert_called_once()
    mock_finalize.assert_called_once()
    mock_mark_complete.assert_called_once()
