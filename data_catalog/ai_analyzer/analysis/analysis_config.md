# 📊 DataNavigator – Analyseconfiguratie

Overzicht van alle ingestelde analysetypes binnen `analysis_config.yaml`.

## 📋 Table Analysis

| Naam                    | Beschrijving                                                                                | Methode                | Prompt                | Output (laag 1 → laag 2)                                          | MVP Status |
|-------------------------|---------------------------------------------------------------------------------------------|------------------------|-----------------------|------------------------------------------------------------------|------------|
| column_profiler         | Verzamel statistieken per kolom (row_count, null_count, unique_count, uniqueness_ratio, …)  | sql (preprocessor)     | —                     | dw_column_profiles                                               | active     |
| column_occurrence       | Tel hoe vaak kolomnamen voorkomen (globaal, per scope)                                      | sql (preprocessor)     | —                     | dw_column_occurrence                                             | optional   |
| base_table_analysis     | Beschrijf wat de tabel representeert (alleen BASE TABLE)                                    | ai                     | table_description     | dw_ai_analysis_results → dw_table_description                    | active     |
| view_definition_analysis| Analyse van SQL-viewdefinitie (alleen VIEW)                                                 | ai                     | view_analysis         | dw_ai_analysis_results → dw_table_description                    | active     |
| column_classification   | Classificeer kolommen op rol (PRIMARY_KEY, enz.)                                            | ai                     | column_classification | dw_ai_analysis_results → dw_column_classifications               | active     |
| column_description      | Beschrijf de betekenis van kolommen in natuurlijke taal                                     | ai                     | column_description    | dw_ai_analysis_results → dw_column_descriptions                  | active     |
| data_quality_check      | Controleer datakwaliteit (nulls, types, duplicaten)                                         | ai                     | data_quality_check    | dw_ai_analysis_results → (nog geen doeltabel, alleen logging)    | optional   |
| data_presence_analysis  | Controleer databeschikbaarheid en actualiteit                                               | ai                     | data_presence         | dw_ai_analysis_results → (nog geen doeltabel, alleen logging)    | optional   |
| all_in_one              | Combineert classificatie, beschrijving en datakwaliteit                                     | ai                     | mixed                 | dw_ai_analysis_results → multi (nog niet uitgewerkt)             | optional   |


## 🧠 Schema Analysis (MVP-volgorde: preprocessors eerst)

| Naam                      | Methode            | Input                                   | Output Tabel                        | Notes                                   | MVP Status |
|---------------------------|--------------------|-----------------------------------------|-------------------------------------|------------------------------------------|-----------|
| foreign_key_graph_builder | sql (preprocessor) | foreign_keys                            | dw_schema_graph                     | Bouwt graaf (nodes=tabellen, edges=FK)   | active    |
| centrality_analysis       | sql (preprocessor) | dw_schema_graph                         | dw_schema_centrality                | in/out-degree, pagerank op de graaf      | active    |
| schema_clustering         | sql (preprocessor) | dw_schema_graph                         | dw_schema_clusters                  | Cluster-id’s op basis van graafstructuur | active    |
| cluster_labeling          | ai                 | dw_schema_clusters, dw_table_description| dw_schema_cluster_labels            | Thema/label per cluster                  | optional  |
| central_table             | ai                 | cluster_tables, dw_schema_centrality    | dw_schema_central_tables            | Benoemt “centrale” tabel per cluster     | optional  |
| schema_role_refinement    | ai                 | cluster_context, existing_roles         | dw_schema_roles_refined             | Verfijnt rollen (fact/dim/bridge/… )     | optional  |
| relationship_mapping      | ai                 | tables, foreign_keys                    | dw_schema_relationship_descriptions | Beschrijft relaties in NL/EN tekst       | optional  |
| schema_pattern_detection  | ai                 | dw_schema_graph                         | dw_schema_detected_patterns         | Vindt patronen (snowflake, hub/spoke)    | optional  |
