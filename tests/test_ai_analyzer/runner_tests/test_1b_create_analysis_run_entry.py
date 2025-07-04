import pytest
from unittest.mock import patch

mock_ai_config = {
    "id": 456,
    "connection_id": 123,
    "ai_database_filter": "mockdb",
    "ai_schema_filter": "public",
    "ai_table_filter": None,
    "config_name": "mock_ai_config",
}

mock_connection = {
    "id": 123,
    "name": "mock_connection",
    "connection_type": "PostgreSQL",
    "host": "mock_connection",
    "port": "5432",
    "username": "testuser",
    "password": "testpass",
}

@patch("ai_analyzer.runners.table_runner.get_ai_config_by_id", return_value=mock_ai_config)
@patch("ai_analyzer.runners.table_runner.get_specific_connection", return_value=mock_connection)
@patch("ai_analyzer.runners.table_runner.create_analysis_run_entry", return_value=101)
@patch("ai_analyzer.runners.table_runner.connect_to_source_database")
@patch("ai_analyzer.runners.table_runner.mark_analysis_run_aborted")  # voorkomt echte exit bij ontbrekend schema/prefix
def test_create_analysis_run_entry_called(
    mock_abort,
    mock_connect,
    mock_create,
    mock_get_conn,
    mock_get_conf
):
    from ai_analyzer.runners.table_runner import run_batch_tables_by_config

    run_batch_tables_by_config(
        connection_id=123,
        ai_config_id=456,
        analysis_type="column_classification",
        author="test_user",
        dry_run=True
    )

    mock_create.assert_called_once()
    _, kwargs = mock_create.call_args

    assert kwargs["server"] == "mock_connection"
    assert kwargs["database"] == "mockdb"
    assert kwargs["schema"] == "public"
    assert kwargs["prefix"] == ""
    assert kwargs["analysis_type"] == "column_classification"
    assert kwargs["author"] == "test_user"
    assert kwargs["is_dry_run"] is True
    assert kwargs["connection_id"] == 123
    assert kwargs["ai_config_id"] == 456
