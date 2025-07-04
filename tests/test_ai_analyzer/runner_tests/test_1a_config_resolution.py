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
    "host": "localhost",
    "port": "5432",
    "username": "testuser",
    "password": "testpass",
}

@patch("connection_handler.get_ai_config_by_id", return_value=mock_ai_config)
@patch("connection_handler.get_specific_connection", return_value=mock_connection)
def test_config_resolution(mock_conn, mock_ai):
    from ai_analyzer.runners.table_runner import run_batch_tables_by_config

    run_batch_tables_by_config(
        connection_id=123,
        ai_config_id=456,
        analysis_type="column_classification",
        author="test_user",
        dry_run=True
    )

    mock_ai.assert_called_once_with(456)
    mock_conn.assert_called_once_with(123)
