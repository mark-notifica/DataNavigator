import pandas as pd 

def build_prompt_for_table(table: dict, table_metadata, sample_data: pd.DataFrame, analysis_type: str) -> str:
    table_name = table.get("table_name", "[UNKNOWN TABLE]")
    table_type = table.get("table_type", "BASE TABLE").upper()
    prompt_parts = []

    # Speciale prompt voor views
    if table_type == "VIEW" and analysis_type == "view_definition_analysis":
        # Verwacht dat table_metadata een dict is met "definition"
        definition = table_metadata.get("definition", "").strip()
        return f"""
🧠 Doel: Analyseer de onderstaande SQL-viewdefinitie.
- Wat is het doel van deze view?
- Welke onderliggende tabellen en relaties worden gebruikt?
- Welke businesslogica valt op?
- Zijn er suggesties voor optimalisatie, hernoemen of documentatie?

Viewdefinitie van `{table_name}`:
--- SQL START ---
{definition}
--- SQL END ---
        """.strip()

    # Standaard prompts voor tabellen en all-in-one
    if analysis_type in ("table_description", "all_in_one"):
        prompt_parts.append(f"""
🔍 Doel: Beschrijf de functie van de tabel `{table_name}`.
- Wat representeert deze tabel?
- Is het een feitentabel, dimensietabel of iets anders?
- Zijn er primaire of foreign keys?
        """.strip())

    if analysis_type in ("column_classification", "all_in_one"):
        prompt_parts.append("""
🧬 Doel: Classificeer de kolommen van de tabel.
Gebruik één van de volgende labels per kolom:
PRIMARY_KEY, FOREIGN_KEY, DATE, MEASURE, DIMENSION, CONTEXT, FLAG, IDENTIFIER, ENUM, ORDERING_FIELD, TIMESTAMP, DURATION, SENSITIVE

Kies het meest passende label per kolom. Als meerdere labels gelden, kies degene die het meest bepalend is voor gebruik in rapportages of analyses.

Antwoord als JSON: { "kolomnaam": "LABEL" }
        """.strip())

    if analysis_type in ("data_quality_check", "all_in_one"):
        prompt_parts.append("""
⚠️ Doel: Onderzoek de datakwaliteit.
- Zijn er kolommen met veel nulls of vreemde waarden?
- Zijn er duplicaten?
- Komen datatypes overeen met hun inhoud?
        """.strip())

    if isinstance(sample_data, pd.DataFrame) and not sample_data.empty:
        prompt_parts.append("Voorbeelddata:")
        formatted_rows = [str(row) for row in sample_data[:20].to_dict(orient="records")]
        prompt_parts.extend(formatted_rows)

    return "\n\n".join(prompt_parts)



def build_prompt_for_schema(schema_metadata: dict, table_analyses: list, analysis_type: str) -> str:
    schema_name = schema_metadata.get("schema_name", "[ONBEKEND SCHEMA]")
    table_lines = [
        f"- `{t['table_name']}`: {t.get('type', 'UNKNOWN')} — {t.get('summary', '')}"
        for t in table_analyses
    ]

    if analysis_type == "schema_context":
        return f"""
📘 Doel: Beschrijf het schema `{schema_name}`.
- Wat is het thema van dit schema?
- Hoe hangen de tabellen logisch samen?

Bekende tabellen:
{chr(10).join(table_lines)}
        """.strip()

    elif analysis_type == "table_overview":
        return f"""
📊 Doel: Geef per tabel een korte beschrijving.
- Is het een feit- of dimensietabel?
- Wat zijn sleutelrelaties?

Bekende tabellen:
{chr(10).join(table_lines)}
        """.strip()

    elif analysis_type == "naming_convention":
        return f"""
🧹 Doel: Evalueer de naamgeving in schema `{schema_name}`.
- Zijn tabellen en kolommen consistent en logisch benoemd?
- Zijn er afwijkingen?

Bekende tabellen:
{chr(10).join(table_lines)}
        """.strip()

    elif analysis_type == "all_in_one":
        return f"""
Je bent een data-architect. Hieronder zie je tabellen van schema `{schema_name}`:

{chr(10).join(table_lines)}

Beantwoord:
1. Welke tabellen horen logisch bij elkaar?
2. Wat valt op in het datamodel?
3. Hoe is de naamgeving?
4. Wat zijn verbetersuggesties?
        """.strip()

    else:
        raise ValueError(f"Onbekend analysis_type: {analysis_type}")
