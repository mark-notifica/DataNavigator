import pytest
from unittest.mock import patch, MagicMock
from ai_analyzer.analysis.analysis_matrix import ANALYSIS_TYPES

@pytest.mark.parametrize("analysis_type, expects_sample_data", [
    ("table_description", True),
    ("column_classification", True),
    ("data_quality_check", True),
    ("data_presence_analysis", True),
    ("view_definition_analysis", False),
])
@patch("ai_analyzer.runners.table_runner.connect_to_source_database")
@patch("connection_handler.get_ai_config_by_id", return_value={
    "id": 1,
    "connection_id": 6,
    "ai_database_filter": "mockdb",
    "ai_schema_filter": "public",
    "ai_table_filter": None,
})
@patch("connection_handler.get_specific_connection", return_value={
    "id": 6,
    "name": "mock_conn",
    "connection_type": "PostgreSQL",
    "host": "localhost",
    "port": "5432",
    "username": "user",
    "password": "pass"
})
@patch("ai_analyzer.runners.table_runner.create_analysis_run_entry", return_value=99)
@patch("ai_analyzer.runners.table_runner.finalize_run_with_token_totals")
@patch("ai_analyzer.runners.table_runner.mark_analysis_run_complete")
@patch("ai_analyzer.utils.catalog_reader.get_tables_for_pattern_with_ids", return_value=[
    {"table_name": "mock_table", "table_type": "BASE TABLE", "table_schema": "public"}
])
@patch("ai_analyzer.utils.catalog_reader.get_metadata_with_ids", return_value=[
    {"column_name": "id", "data_type": "int"}
])
@patch("ai_analyzer.utils.catalog_reader.get_view_definition_with_ids", return_value="SELECT * FROM something")
@patch("ai_analyzer.utils.file_writer.store_analysis_result_to_file")
def test_analysis_type_sample_function_usage(
    mock_store,
    mock_viewdef,
    mock_meta,
    mock_tables,
    mock_mark_complete,
    mock_finalize,
    mock_create,
    mock_get_conn,
    mock_get_conf,
    mock_connect,
    analysis_type,
    expects_sample_data
):
    from ai_analyzer.runners.table_runner import run_batch_tables_by_config
    import ai_analyzer.analysis.analysis_matrix as matrix

    # sample-functie patchen
    sample_func_path = matrix.ANALYSIS_TYPES[analysis_type].get("sample_data_function")
    if expects_sample_data and sample_func_path:
        patch_target = f"ai_analyzer.analysis.analysis_matrix.{sample_func_path.__name__}"
        with patch(patch_target, return_value=[{"id": 1}]) as mock_sample:
            run_batch_tables_by_config(
                connection_id=6,
                ai_config_id=1,
                analysis_type=analysis_type,
                author="pytest",
                dry_run=True
            )
            assert mock_sample.called
    else:
        # als er géén sample_data verwacht wordt
        run_batch_tables_by_config(
            connection_id=6,
            ai_config_id=1,
            analysis_type=analysis_type,
            author="pytest",
            dry_run=True
        )
        assert mock_store.called
