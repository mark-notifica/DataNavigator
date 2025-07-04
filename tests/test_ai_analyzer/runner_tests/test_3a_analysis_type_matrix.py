from unittest.mock import patch, MagicMock
import pytest

analysis_types = {
    "table_description": "get_sample_data_for_table_description",
    "column_classification": "get_sample_data_for_column_classification",
    "data_quality_check": "get_sample_data_for_data_quality",
    "data_presence_analysis": "get_sample_data_for_data_presence",
    "view_definition_analysis": None,  # gebruikt geen sample data
}

@pytest.mark.parametrize("analysis_type,sample_func_name", analysis_types.items())
def test_analysis_type_sample_function_mapping(analysis_type, sample_func_name):
    from ai_analyzer.runners.table_runner import run_single_table

    table = {
        "server_name": "mockserver",
        "database_name": "mockdb",
        "schema_name": "public",
        "table_name": "fact_sales",
        "connection_id": 6,
        "table_type": "BASE TABLE"
    }

    # Mocks
    prompt_builder_patch = patch("ai_analyzer.prompts.prompt_builder.build_prompt_for_table", return_value="PROMPT")
    ai_call_patch = patch("ai_analyzer.analysis.llm_model_wrapper.call_llm", return_value="LLM RESPONSE")
    file_writer_patch = patch("ai_analyzer.utils.file_writer.store_analysis_result_to_file")
    store_result_patch = patch("ai_analyzer.postprocessor.ai_analysis_writer.store_ai_table_analysis")

    with (
        prompt_builder_patch as mock_prompt,
        ai_call_patch as mock_llm,
        file_writer_patch as mock_store_file,
        store_result_patch as mock_store_result,
    ):
        if sample_func_name:
            with patch(f"ai_analyzer.samples.sample_data_builder.{sample_func_name}", return_value=[{"id": 1}]) as mock_sample_func:
                run_single_table(table, analysis_type, author="testuser", dry_run=True, run_id=42)
                mock_sample_func.assert_called_once()
        else:
            # Bijv. view_definition_analysis — geen sample functie
            run_single_table(table, analysis_type, author="testuser", dry_run=True, run_id=42)
