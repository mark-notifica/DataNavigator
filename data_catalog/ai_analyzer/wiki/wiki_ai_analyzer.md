# AI Analyzer Architectuur & Verwerkingslogica

Deze pagina documenteert de architectuur en logica van de AI-analyzer binnen het DataNavigator-project. De analyzer voert verschillende analyses uit op basis van metadata uit de catalogus, genereert prompts, voert AI-calls uit, en verwerkt de resultaten gestructureerd door.

---

## Inhoudsopgave
1. Overzicht van het proces
2. Datasourcing uit de catalogus
3. Promptgeneratie
4. AI-analyse via runners
5. Vastlegging van runs en resultaten
6. Opsplitsing naar descriptions
7. Relatie met promptcatalogus.md

---

## 1. Overzicht van het proces

```
[Catalogusdata] ─┬─> [Prompt Builder] ──> [OpenAI] ──> [AI Resultaat JSON]
                 │
                 └──> [table_runner / schema_runner]

                                          ↓
                            [catalog_ai_analysis_results]  (volledige output per run)
                                          ↓
                            [catalog_*_descriptions]       (structurele extractie / versiebeheer)
```

Het proces bestaat uit:
- Het ophalen van metadata
- Het bouwen van prompts afhankelijk van `analysis_type`
- Het uitvoeren van de AI-analyse
- Het opslaan van resultaten (volledig)
- Het extraheren van betekenisvolle onderdelen naar semantische `descriptions`

---

## 2. Datasourcing uit de catalogus

De volgende catalogustabellen worden gebruikt:
- `catalog_tables` — lijst van tabellen en views
- `catalog_columns` — kolommetadata
- `catalog_views` — view-definities (SQL)

Helperfuncties:
- `get_tables_for_pattern(...)`
- `get_metadata(table)`
- `get_view_definition(table)`

---

## 3. Promptgeneratie

Op basis van `analysis_type` wordt een prompt gegenereerd met behulp van:

- `build_prompt_for_table(...)`
- `build_prompt_for_schema(...)`
- `build_prompt_for_view(...)`

Ondersteunde `analysis_type`-waarden:
- `table_description`
- `column_classification`
- `data_quality_check`
- `schema_context`
- `naming_convention`
- `view_definition_analysis`
- `all_in_one` (combineert meerdere doelen)

Deze mapping is inhoudelijk beschreven in `promptcatalogus.md`.

---

## 4. AI-analyse via runners

Er zijn verschillende runners voor verschillende scopes:
- `table_runner.py` — voert analyses uit op individuele tabellen of views
- `schema_runner.py` — voert analyses uit op hele schema's

Elke runner voert de volgende stappen uit:
1. Ophalen van metadata
2. Bouwen van de prompt
3. Verzenden naar OpenAI via `analyze_with_openai()`
4. Opslaan van het resultaat in `catalog_ai_analysis_results`
5. (optioneel) Doorvertaling naar `descriptions` via een `*_auto(...)` functie

---

## 5. Vastlegging van runs en resultaten

Elke AI-analysesessie:
- wordt geregistreerd in `catalog_ai_analysis_runs`
- krijgt een unieke `run_id`
- bevat tokeninformatie en status

Resultaten worden opgeslagen in:
- `catalog_ai_analysis_results`
  - JSON-resultaat
  - Prompt + tokens + type + status

Belangrijke functies:
- `store_ai_table_analysis(...)`
- `store_ai_schema_analysis(...)`
- `finalize_run_with_token_totals(...)`

---

## 6. Opsplitsing naar descriptions

Structurele kerninformatie wordt opgeslagen in aparte tabellen voor downstream gebruik, documentatie of visualisatie.

| Doel                      | Opslagtabel                        | Extractie-functie                   |
|---------------------------|------------------------------------|-------------------------------------|
| Kolomlabels               | `catalog_column_descriptions`      | `store_ai_column_descriptions()`    |
| Tabelbeschrijving         | `catalog_table_descriptions`       | `store_ai_table_description()`      |
| Viewbeschrijving          | `catalog_table_descriptions`       | `store_ai_view_description()`       |
| Schemabeschrijving        | `catalog_schema_descriptions`      | `store_ai_schema_description()`     |
| Meervoudige extractie     | meerdere                           | `store_ai_table_descriptions_auto()` / `store_ai_schema_descriptions_auto()` |

Beschrijvingstabellen gebruiken `is_current = TRUE` voor de meest recente versie. Bij elke nieuwe opslag wordt de oude versie automatisch gedeactiveerd (`is_current = FALSE`).

---

## 7. Relatie met promptcatalogus.md

De `promptcatalogus.md` beschrijft de inhoudelijke kant van de prompts:
- Doelstelling per `analysis_type`
- Outputstructuur (JSON, samenvatting, tekst)
- Stijl en instructies aan de AI

Deze `.md` documentatiepagina beschrijft juist de technische **verwerking en architectuur**.

## 1. Overzicht van het proces

