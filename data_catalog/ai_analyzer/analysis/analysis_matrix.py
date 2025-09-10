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
    "view_definition_analysis": {
        "label": "Analyse van view-definitie",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": None,  # views analyseren op definitie, geen sample nodig
        "description_target": "table",
        "default_model": "gpt-4o",
        "temperature": 0.5,
        "max_tokens": 1200,
        "allowed_table_types": ["VIEW"]
    },
    "column_classification": {
        "label": "Kolomclassificatie",
        "prompt_builder": build_prompt_for_table,
        "sample_data_function": get_sample_data_for_column_classification,
        "description_target": "column",
        "default_model": "gpt-3.5-turbo",
        "temperature": 0.3,
        "max_tokens": 600,
        "allowed_table_types": ["BASE TABLE"]  # MVP: alleen echte tabellen
    },
    "column_description": {  # ✅ nieuw: beschrijving per kolom, los van classificatie
        "label": "Kolombeschrijving",
        "prompt_builder": build_prompt_for_table,
        # Heb je nog geen aparte sampler? Hergebruik die van base_table, of maak later get_sample_data_for_column_description
        "sample_data_function": get_sample_data_for_base_table_analysis,
        "description_target": "column",
        "default_model": "gpt-4",
        "temperature": 0.3,
        "max_tokens": 700,
        "allowed_table_types": ["BASE TABLE", "VIEW"]  # mag ook alleen BASE TABLE als je dat wilt
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
        "requires_centrality": False,
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
        "requires_centrality": False,
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
        "requires_centrality": False,
        "description_target": "schema"
    },
    "relationship_mapping": {
        "label": "Relatiebeschrijvingen (FK/joins) in natuurlijke taal",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.4,
        "max_tokens": 1800,
        "requires_graph": True,
        "requires_clusters": False,
        "requires_centrality": False,
        "description_target": "schema"
    },
    "cluster_labeling": {
        "label": "Thema/label per cluster",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 1200,
        "requires_graph": True,
        "requires_clusters": True,
        "requires_centrality": False,
        "description_target": "schema"
    },
    "central_table": {
        "label": "Centrale tabel per cluster",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.4,
        "max_tokens": 1200,
        "requires_graph": True,
        "requires_clusters": True,
        "requires_centrality": True,
        "description_target": "schema"
    },
    "schema_pattern_detection": {
        "label": "Patroondetectie (bijv. star/snowflake/hub-spoke)",
        "prompt_builder": build_prompt_for_schema,
        "default_model": "gpt-4",
        "temperature": 0.5,
        "max_tokens": 1800,
        "requires_graph": True,
        "requires_clusters": False,
        "requires_centrality": False,
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