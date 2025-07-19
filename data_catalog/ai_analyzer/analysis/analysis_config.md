# 📊 DataNavigator – Analyseconfiguratie

Overzicht van alle ingestelde analysetypes binnen `analysis_config.yaml`.

## 📋 Table Analysis

| Naam                    | Beschrijving                                           | Methode | Prompt                | Output Table                      | Status |
|-------------------------|--------------------------------------------------------|---------|------------------------|------------------------------------|--------|
| column_classification   | Classificeer kolommen op rol (PRIMARY_KEY, enz.)       | openai  | column_classification  | catalog_ai_column_description     | active |
| table_description       | Beschrijf wat de tabel representeert                   | openai  | table_description      | catalog_ai_table_description      | active |
| view_definition_analysis| Analyse van SQL-viewdefinitie                          | openai  | view_analysis          | catalog_ai_table_description      | active |
| data_quality_check      | Controleer datakwaliteit (nulls, types, duplicaten)    | openai  | data_quality_check     | catalog_ai_analysis_results       | active |
| all_in_one              | Combineert classificatie, beschrijving en datakwaliteit| openai  | mixed                  | multi                             | active |

## 🧠 Schema Analysis

| Naam                    | Methode    | Input                           | Output                          | Notes |
|-------------------------|------------|----------------------------------|----------------------------------|-------|
| schema_clustering       | algorithmic| foreign_key_graph                | cluster_id                      | Uses undirected graph |
| centrality_analysis     | algorithmic| foreign_key_graph                | in/out_degree, pagerank, ...    | Uses directed graph   |
| cluster_labeling        | openai     | table_descriptions               | cluster_label                   |        |
| central_table           | openai     | cluster_tables, centrality_scores| central_table_name              |        |
| schema_role_refinement  | openai     | cluster_context, existing_roles  | refined_roles                   |        |
| relationship_mapping    | openai     | tables, foreign_keys             | relationship_description        |        |
| schema_pattern_detection| openai     | schema_graph                     | detected_patterns               |        |

## 🔄 Graph Building

| Naam                    | Methode    | Input         | Output         | Gebruikt door                                  |
|-------------------------|------------|----------------|----------------|------------------------------------------------|
| foreign_key_graph_builder | algorithmic| foreign_keys   | graph_object   | centrality_analysis, schema_clustering, ai_context_generation |
