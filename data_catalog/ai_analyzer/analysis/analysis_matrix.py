from ai_analyzer.prompts.prompt_builder import build_prompt_for_table
from ai_analyzer.samples.sample_data_builder import (
    get_sample_data_for_table_description,
    get_sample_data_for_column_classification,
    get_sample_data_for_data_quality,
    get_sample_data_for_data_presence
)

# Matrix van ondersteunde table analysetypes
ANALYSIS_TYPES = {
    "table_description": {
        "label": "Tabelbeschrijving",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_table_description,
        "description_target": "table",
        "default_model": "gpt-3.5-turbo",
        "temperature": 0.4,
        "max_tokens": 700
    },
    "column_classification": {
        "label": "Kolomclassificatie",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_column_classification,
        "description_target": "column",
        "default_model": "gpt-3.5-turbo",
        "temperature": 0.3,
        "max_tokens": 600
    },
    "data_quality_check": {
        "label": "Datakwaliteit controleren",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_data_quality,
        "description_target": "table",
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 1000
    },
    "data_presence_analysis": {
        "label": "Databeschikbaarheid en actualiteit",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_data_presence,
        "description_target": "table",
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 1000
    },
    "view_definition_analysis": {
        "label": "Analyse van view-definitie",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": None,  # gebruikt geen sample data
        "description_target": "table",
        "default_model": "gpt-4o",
        "temperature": 0.5,
        "max_tokens": 1200
    }
}
