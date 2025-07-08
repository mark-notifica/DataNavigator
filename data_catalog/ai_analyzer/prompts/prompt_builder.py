import pandas as pd 

def build_prompt_for_table(table: dict, table_metadata, sample_data: pd.DataFrame, analysis_type: str) -> str:
    """
    Genereert een prompt voor een bepaalde analyse op een tabel of view.
    """
    table_name = table.get("table_name", "[UNKNOWN TABLE]")
    table_type = table.get("table_type", "BASE TABLE").upper()
    prompt_parts = []

    # -- Speciale prompt: Viewdefinitie-analyse --
    if table_type == "VIEW" and analysis_type == "view_definition_analysis":
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

    # -- Tabelbeschrijving --
    if analysis_type == "base_table_analysis":
        prompt_parts.append(f"""
        Doel: Beschrijf de functie van de tabel `{table_name}`.
        - Wat representeert deze tabel in businesscontext?
        - Is het een feitentabel, dimensietabel of iets anders?

        Negeer sleutelinformatie zoals primaire of foreign keys. Deze worden later apart geanalyseerd.
        Sluit af zonder generieke opmerkingen zoals 'Laat het me weten als je vragen hebt'. Beperk je tot de analyse.
        """.strip())

    # -- Kolomclassificatie --
    if analysis_type == "column_classification":
        prompt_parts.append("""
        Doel: Classificeer de kolommen van de tabel.
        Gebruik één van de volgende labels per kolom:
        PRIMARY_KEY, FOREIGN_KEY, DATE, MEASURE, DIMENSION, CONTEXT, FLAG, IDENTIFIER, ENUM, ORDERING_FIELD, TIMESTAMP, DURATION, SENSITIVE

        Kies het meest passende label per kolom. Als meerdere labels gelden, kies degene die het meest bepalend is voor gebruik in rapportages of analyses.

        Antwoord als JSON: { "kolomnaam": "LABEL" }
                """.strip())

    # -- Datakwaliteit --
    if analysis_type == "data_quality_check":
        prompt_parts.append("""
⚠️ Doel: Onderzoek de datakwaliteit.
- Zijn er kolommen met veel nulls of vreemde waarden?
- Zijn er duplicaten?
- Komen datatypes overeen met hun inhoud?
        """.strip())

    # -- Aanwezigheid van data (presence) --
    if analysis_type == "data_presence_analysis":
        prompt_parts.append(f"""
📊 Doel: Beoordeel of de tabel `{table_name}` nog in gebruik is.
- Is er voldoende actuele data aanwezig?
- Zijn er tijdstempels of kolommen met wijzigingsdata?
- Is er spreiding in de datums, of lijkt alles oud of statisch?
        """.strip())

    # -- Sampledata toevoegen indien aanwezig --
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
    joined_tables = chr(10).join(table_lines)

    if analysis_type == "schema_context":
        return f"""
📘 Doel: Beschrijf het schema `{schema_name}`.
- Wat is het thema van dit schema?
- Hoe hangen de tabellen logisch samen?

Bekende tabellen:
{joined_tables}
        """.strip()

    elif analysis_type == "schema_summary":
        return f"""
📎 Doel: Vat samen wat dit schema `{schema_name}` doet.
- Welke soorten tabellen zijn er?
- Wat is de rol van dit schema binnen de database?
- Hoe logisch en compleet is het model?

Bekende tabellen:
{joined_tables}
        """.strip()

    elif analysis_type == "schema_table_overview":
        return f"""
📊 Doel: Geef per tabel een contextuele beschrijving binnen schema `{schema_name}`.
- Welke rol speelt de tabel binnen het geheel?
- Is het een feit-, dimensie- of hulpset?
- Wat zijn sleutelrelaties?

Bekende tabellen:
{joined_tables}
        """.strip()

    elif analysis_type == "naming_convention":
        return f"""
🧹 Doel: Evalueer de naamgeving binnen schema `{schema_name}`.
- Zijn tabellen en kolommen logisch en consistent benoemd?
- Zijn er inconsistenties, afkortingen of ongebruikelijke termen?

Bekende tabellen:
{joined_tables}
        """.strip()



    elif analysis_type == "schema_recommendations":
        return f"""
🛠️ Doel: Geef verbetersuggesties voor schema `{schema_name}`.
- Welke tabellen kunnen beter hernoemd, gesplitst of samengevoegd worden?
- Waar ontbreken sleutelrelaties of documentatie?
- Wat zou je aanpassen om de structuur begrijpelijker of efficiënter te maken?

Bekende tabellen:
{joined_tables}
        """.strip()

    elif analysis_type == "schema_cluster_mapping":
        return f"""
🔗 Doel: Koppel tabellen aan thematische domeinen binnen schema `{schema_name}`.
- Welke groepjes tabellen horen logisch bij elkaar?
- Welke thema's (bijv. 'klantbeheer', 'facturatie') herken je?

Bekende tabellen:
{joined_tables}
        """.strip()
    
    elif analysis_type == "schema_evaluation":
        return f"""
    🧠 Doel: Geef een expert-oordeel over schema `{schema_name}`.
    - Beoordeel de samenhang, structuur, naamgeving en modelkwaliteit.
    - Benoem sterke punten én verbeterpunten.
    - Waar nodig mag je tabellen of structuren hergroeperen.

    Bekende tabellen:
    {joined_tables}
        """.strip()

    else:
        raise ValueError(f"Onbekend analysis_type: {analysis_type}")
