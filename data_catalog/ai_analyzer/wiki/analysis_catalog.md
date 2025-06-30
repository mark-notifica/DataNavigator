# 📊 DataNavigator – Analyse Catalogus

Overzicht van alle analysecomponenten. Zie `wiki_promptcatalogus.md` voor details over prompts.

---

## 📋 Table Analysis

| Naam                    | Beschrijving                                           | Methode | Prompt                | Output Table                      | Status |
|-------------------------|--------------------------------------------------------|---------|------------------------|------------------------------------|--------|
| column_classification   | Classificeer kolommen op rol (PRIMARY_KEY, enz.)       | OpenAI  | column_classification  | catalog_ai_column_description     | ✅     |
| table_description       | Beschrijf wat de tabel representeert                   | OpenAI  | table_description      | catalog_ai_table_description      | ✅     |
| view_definition_analysis| Analyse van SQL-viewdefinitie                          | OpenAI  | view_analysis          | catalog_ai_table_description      | ✅     |
| data_quality_check      | Controleer datakwaliteit (nulls, types, duplicaten)    | OpenAI  | data_quality_check     | catalog_ai_analysis_results       | ✅     |
| all_in_one              | Combineert classificatie, beschrijving en datakwaliteit| OpenAI  | mixed                  | meerdere tabellen (multi-output)  | ✅     |

---

## 🧠 Schema Analysis

| Naam                    | Beschrijving                              | Methode    | Input                           | Output                          | Notes |
|-------------------------|-------------------------------------------|------------|----------------------------------|----------------------------------|-------|
| schema_clustering       | Groepeer tabellen op structuur            | Algorithmic| foreign_key_graph                | cluster_id                      | Uses undirected graph |
| centrality_analysis     | Bepaal centrale tabellen via metrics      | Algorithmic| foreign_key_graph                | in/out_degree, pagerank, ...    | Uses directed graph   |
| cluster_labeling        | Label clusters op betekenis               | OpenAI     | table_descriptions               | cluster_label                   |        |
| central_table           | Detecteer centrale tabel binnen cluster   | OpenAI     | cluster_tables, centrality_scores| central_table_name              |        |
| schema_role_refinement  | Verfijn rollen (FACT/DIM/BRIDGE)          | OpenAI     | cluster_context, existing_roles  | refined_roles                   |        |
| relationship_mapping    | Omschrijf relatiestructuur en joins       | OpenAI     | tables, foreign_keys             | relationship_description        |        |
| schema_pattern_detection| Herken patronen zoals sterren, bridge-tabellen | OpenAI | schema_graph                    | detected_patterns               |        |

---

## 🔄 Graph Building

| Naam                    | Beschrijving                                      | Methode    | Input       | Output       | Gebruikt door                                  |
|-------------------------|---------------------------------------------------|------------|-------------|--------------|------------------------------------------------|
| foreign_key_graph_builder | Bouw graaf van FK-relaties                     | Algorithmic| foreign_keys| graph_object | centrality_analysis, schema_clustering, ai_context_generation |
