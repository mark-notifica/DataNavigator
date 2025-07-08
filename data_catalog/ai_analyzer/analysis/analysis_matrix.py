from ai_analyzer.prompts.prompt_builder import build_prompt_for_table,  build_prompt_for_schema
from ai_analyzer.samples.sample_data_builder import (
    get_sample_data_for_base_table_analysis,
    get_sample_data_for_column_classification,
    get_sample_data_for_data_quality,
    get_sample_data_for_data_presence
)

# Matrix van ondersteunde table analysetypes
ANALYSIS_TYPES = {
    "base_table_analysis": {
        "label": "Tabelbeschrijving",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_base_table_analysis,
        "description_target": "table",
        "default_model": "gpt-3.5-turbo",
        "temperature": 0.4,
        "max_tokens": 700,
        "allowed_table_types": ["BASE TABLE"]
    },
    "column_classification": {
        "label": "Kolomclassificatie",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_column_classification,
        "description_target": "column",
        "default_model": "gpt-3.5-turbo",
        "temperature": 0.3,
        "max_tokens": 600,
        "allowed_table_types": ["BASE TABLE"]
    },
    "data_quality_check": {
        "label": "Datakwaliteit controleren",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_data_quality,
        "description_target": "table",
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 1000,
        "allowed_table_types": ["BASE TABLE", "VIEW"]
    },
    "data_presence_analysis": {
        "label": "Databeschikbaarheid en actualiteit",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_data_presence,
        "description_target": "table",
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 1000,
        "allowed_table_types": ["BASE TABLE", "VIEW"]
    },
    "view_definition_analysis": {
        "label": "Analyse van view-definitie",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": None,
        "description_target": "table",
        "default_model": "gpt-4o",
        "temperature": 0.5,
        "max_tokens": 1200,
        "allowed_table_types": ["VIEW"]
    }
}

SCHEMA_ANALYSIS_TYPES = {
    "schema_context": {
        "label": "Overzicht van het schema",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 2000,
        "requires_graph": True,
        "requires_clusters": True,
        "requires_centrality": True,
        "description_target": "schema"
    },
    "schema_summary": {
        "label": "Samenvatting van structuur",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.4,
        "max_tokens": 1500,
        "requires_graph": False,
        "requires_clusters": False,
        "description_target": "schema"
    },
    "schema_table_overview": {
        "label": "Overzicht per tabel (met context)",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.4,
        "max_tokens": 2000,
        "requires_graph": True,
        "requires_clusters": True,
        "description_target": "schema"
    },
    "naming_convention": {
        "label": "Beoordeling naamgeving",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.3,
        "max_tokens": 1000,
        "requires_graph": False,
        "requires_clusters": False,
        "description_target": "schema"
    },
    "all_in_one": {
        "label": "Volledig oordeel over schema",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.6,
        "max_tokens": 2500,
        "requires_graph": True,
        "requires_clusters": True,
        "requires_centrality": True,
        "description_target": "schema"
    },
    "schema_recommendations": {
        "label": "Verbetersuggesties voor schema",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.7,
        "max_tokens": 1800,
        "requires_graph": True,
        "requires_clusters": True,
        "requires_centrality": True,
        "description_target": "schema"
    },
    "schema_cluster_mapping": {
        "label": "Thema's per cluster",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 2000,
        "requires_graph": True,
        "requires_clusters": True,
        "description_target": "schema"
    },
    "schema_evaluation": {
    "label": "Eindbeoordeling van het schema",
    "prompt_builder": build_prompt_for_schema,
    "default_model": "gpt-4",
    "temperature": 0.6,
    "max_tokens": 2500,
    "requires_graph": True,
    "requires_clusters": True,
    "requires_centrality": True,
    "description_target": "schema"
    }
}