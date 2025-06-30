import os
import json
from datetime import datetime
from data_catalog.connection_handler import get_catalog_connection


def store_ai_table_analysis(run_id, table: dict, result_json: dict):
    """
    Slaat analyse per tabel op in catalog.catalog_ai_analysis_results,
    inclusief tokenverbruik indien aanwezig.
    """
    tokens = result_json.get("tokens", {})

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_ai_analysis_results (
                    run_id,
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    analysis_type,
                    result_json,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    estimated_cost_usd,
                    status,
                    score,
                    insights_summary,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ok', NULL, NULL, %s)
            """, (
                run_id,
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"],
                result_json.get("analysis_type", "table_description"),
                json.dumps(result_json),
                tokens.get("prompt"),
                tokens.get("completion"),
                tokens.get("total"),
                tokens.get("estimated_cost_usd"),
                datetime.utcnow()
            ))
            conn.commit()
    finally:
        conn.close()


def store_ai_schema_analysis(run_id, server, database, schema, result_json):
    """
    Slaat analyse van een schema op in catalog.catalog_ai_analysis_results
    (zonder specifieke table_name), inclusief tokenverbruik indien aanwezig.
    """
    tokens = result_json.get("tokens", {})

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_ai_analysis_results (
                    run_id,
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    analysis_type,
                    result_json,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    estimated_cost_usd,
                    status,
                    score,
                    insights_summary,
                    created_at
                )
                VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, 'ok', NULL, NULL, %s)
            """, (
                run_id,
                server,
                database,
                schema,
                result_json.get("analysis_type", "schema_structure"),
                json.dumps(result_json),
                tokens.get("prompt"),
                tokens.get("completion"),
                tokens.get("total"),
                tokens.get("estimated_cost_usd"),
                datetime.utcnow()
            ))
            conn.commit()
    finally:
        conn.close()


def store_analysis_result_to_file(name: str, result_json: dict, output_dir="/mnt/data/ai_analyzer/output"):
    """
    Slaat analyse op als lokaal JSON-bestand (voor debugging of dry-run)
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{name}_analysis.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(result_json, f, indent=2)


def finalize_run_with_token_totals(run_id: int):
    """
    Aggregeert tokengebruik uit catalog_ai_analysis_results
    en schrijft totaalresultaten weg in catalog_ai_analysis_runs.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(prompt_tokens), 0),
                    COALESCE(SUM(completion_tokens), 0),
                    COALESCE(SUM(total_tokens), 0),
                    COALESCE(SUM(estimated_cost_usd), 0)
                FROM catalog.catalog_ai_analysis_results
                WHERE run_id = %s
            """, (run_id,))
            prompt_tokens, completion_tokens, total_tokens, cost = cur.fetchone()

            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET prompt_tokens = %s,
                    completion_tokens = %s,
                    total_tokens = %s,
                    estimated_cost_usd = %s
                WHERE id = %s
            """, (prompt_tokens, completion_tokens, total_tokens, cost, run_id))

            conn.commit()
            return {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "estimated_cost_usd": float(cost)
            }
    finally:
        conn.close()

def store_ai_column_descriptions(run_id, table: dict, column_classification: dict, author="ai_analyzer"):
    """
    Slaat classificatie van kolommen op in catalog.catalog_column_descriptions,
    alleen als deze classificatie nog niet bestaat voor deze kolom + run.
    """
    conn = get_catalog_connection()
    inserted_count = 0
    skipped_count = 0
    now = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            for column_name, info in column_classification.items():
                # Check of classificatie al bestaat voor deze kolom en run
                cur.execute("""
                    SELECT 1 FROM catalog.catalog_column_descriptions
                    WHERE server_name = %s
                      AND database_name = %s
                      AND schema_name = %s
                      AND table_name = %s
                      AND column_name = %s
                      AND analysis_run_id = %s
                """, (
                    table["server_name"],
                    table["database_name"],
                    table["schema_name"],
                    table["table_name"],
                    column_name,
                    run_id
                ))
                if cur.fetchone():
                    skipped_count += 1
                    continue

                cur.execute("""
                    INSERT INTO catalog.catalog_column_descriptions (
                        server_name,
                        database_name,
                        schema_name,
                        table_name,
                        column_name,
                        analysis_run_id,
                        classification,
                        confidence,
                        notes,
                        author,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    table["server_name"],
                    table["database_name"],
                    table["schema_name"],
                    table["table_name"],
                    column_name,
                    run_id,
                    info.get("classification"),
                    info.get("confidence"),
                    info.get("notes"),
                    author,
                    now
                ))
                inserted_count += 1

            conn.commit()
    finally:
        conn.close()

    # Simpele stdout-log — je kunt hier desgewenst `logging` toevoegen
    print(f"🔍 Kolomclassificaties opgeslagen voor {table['table_name']}: {inserted_count} toegevoegd, {skipped_count} overgeslagen.")

def store_ai_view_description(run_id, table: dict, result_json: dict, author="ai_analyzer"):
    """
    Slaat viewbeschrijving op in catalog.catalog_table_descriptions
    met description_type = 'view_definition'.

    Deactiveert eerdere beschrijvingen voor dezelfde tabel (zelfde server/schema/table + type).
    """
    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.utcnow()

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            # 1. Deactiveer eerdere view_definitions
            cur.execute("""
                UPDATE catalog.catalog_table_descriptions
                SET is_current = FALSE,
                    date_updated = %s,
                    author_updated = %s
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND table_name = %s
                  AND description_type = 'view_definition'
                  AND is_current = TRUE
            """, (
                now,
                author,
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"]
            ))

            # 2. Voeg nieuwe beschrijving toe
            cur.execute("""
                INSERT INTO catalog.catalog_table_descriptions (
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    description,
                    description_type,
                    source,
                    is_current,
                    date_created,
                    date_updated,
                    author_created,
                    ai_table_type,
                    ai_classified_at
                )
                VALUES (%s, %s, %s, %s, %s, 'view_definition', 'AI', TRUE, %s, %s, %s, %s, %s)
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"],
                summary.strip(),
                now,
                now,
                author,
                table.get("table_type", "VIEW"),
                now
            ))

            conn.commit()
    finally:
        conn.close()


def store_ai_table_description(run_id, table: dict, result_json: dict, author="ai_analyzer", description_type="short_summary"):
    """
    Slaat beschrijving van een BASE TABLE of VIEW op in catalog_table_descriptions.
    - Gebruik description_type om het type tekst te onderscheiden, bv:
        - 'short_summary'
        - 'view_definition'
        - 'dwh_layer_explanation'
    """
    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.utcnow()

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_table_descriptions (
                    server_name,
                    database_name,
                    schema_name,
                    table_name,
                    description,
                    description_type,
                    source,
                    is_current,
                    date_created,
                    date_updated,
                    author_created,
                    ai_table_type,
                    ai_classified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'AI', TRUE, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uq_table_description_unique_current DO NOTHING
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"],
                summary.strip(),
                description_type,
                now,
                now,
                author,
                table.get("table_type"),
                now
            ))
            conn.commit()
    finally:
        conn.close()

def store_ai_table_descriptions_auto(run_id: int, table: dict, result_json: dict, author="ai_analyzer"):
    """
    Slaat beschrijvingen en kolomclassificaties automatisch op
    vanuit een gecombineerd AI-resultaat (bv. 'all_in_one').
    """
    from ai_analyzer.postprocessor.output_writer import (
        store_ai_table_description,
        store_ai_column_descriptions
    )

    # Tabelomschrijving (beschrijving)
    if result_json.get("summary") or result_json.get("insights_summary"):
        store_ai_table_description(
            run_id=run_id,
            table=table,
            result_json=result_json,
            author=author,
            description_type="short_summary"
        )

    # Kolomclassificaties (optioneel)
    if "column_classification" in result_json:
        store_ai_column_descriptions(
            run_id=run_id,
            table=table,
            column_classification=result_json["column_classification"],
            author=author
        )

    # Data quality? → voorlopig alleen in result_json als JSON
    # In de toekomst zou je dit structureel kunnen bewaren in bv. `catalog_column_quality_flags`



def store_ai_schema_description(run_id: int, schema: dict, result_json: dict, author="ai_analyzer", description_type="schema_context"):
    """
    Slaat schema-analyse op in catalog_schema_descriptions met versiebeheer.

    Deactiveert eerdere beschrijvingen van dit type voor dezelfde server/db/schema.
    """
    summary = result_json.get("summary") or result_json.get("insights_summary") or ""
    now = datetime.utcnow()

    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            # 1. Zet eerdere beschrijvingen van dit type op inactive
            cur.execute("""
                UPDATE catalog.catalog_schema_descriptions
                SET is_current = FALSE,
                    date_updated = %s,
                    author_updated = %s
                WHERE server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
                  AND description_type = %s
                  AND is_current = TRUE
            """, (
                now,
                author,
                schema["server_name"],
                schema["database_name"],
                schema["schema_name"],
                description_type
            ))

            # 2. Voeg nieuwe regel toe
            cur.execute("""
                INSERT INTO catalog.catalog_schema_descriptions (
                    server_name,
                    database_name,
                    schema_name,
                    analysis_run_id,
                    description,
                    description_type,
                    source,
                    is_current,
                    date_created,
                    date_updated,
                    author_created
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'AI', TRUE, %s, %s, %s)
            """, (
                schema["server_name"],
                schema["database_name"],
                schema["schema_name"],
                run_id,
                summary.strip(),
                description_type,
                now,
                now,
                author
            ))

            conn.commit()
    finally:
        conn.close()

def store_ai_schema_descriptions_auto(run_id: int, schema: dict, result_json: dict, author="ai_analyzer"):
    """
    Detecteert en slaat meerdere beschrijvingstypes op uit een enkel AI-resultaat.
    Bijv. vanuit een 'all_in_one' analyse.
    """

    def extract_section(text: str, marker: str) -> str:
        """
        Simpele marker-based extractie voor opgesplitste delen (optioneel).
        Bijvoorbeeld: marker = 'NAAMGEVING:', enz.
        """
        lines = text.splitlines()
        section = []
        capture = False
        for line in lines:
            if marker in line.upper():
                capture = True
                continue
            elif line.strip() == "" and capture:
                break
            elif capture:
                section.append(line)
        return "\n".join(section).strip()

    # Basis extractie (alles in één summary of insights_summary)
    full_summary = result_json.get("summary") or result_json.get("insights_summary") or ""

    if not full_summary:
        return  # niets te doen

    # 1. Schema-context opslaan
    store_ai_schema_description(
        run_id=run_id,
        schema=schema,
        result_json={"summary": full_summary},
        author=author,
        description_type="schema_context"
    )

    # 2. Probeer optioneel naamgevingsdeel te extraheren (indien van toepassing)
    naming_part = extract_section(full_summary, marker="NAAMGEVING")
    if naming_part and len(naming_part.split()) > 3:  # minimaal iets van inhoud
        store_ai_schema_description(
            run_id=run_id,
            schema=schema,
            result_json={"summary": naming_part},
            author=author,
            description_type="naming_convention"
        )


def store_relationship_suggestion(
    run_id: int,
    server: str,
    database: str,
    schema: str,
    source_table: str,
    target_table: str,
    source_column: str = None,
    target_column: str = None,
    relationship_type: str = "unknown",
    confidence_score: float = None,
    description: str = "",
    source: str = "AI",
    author: str = "system"
):
    """
    Slaat een relatievoorstel op tussen twee tabellen (optioneel ook kolommen).
    Wordt gebruikt door relationship_runner.py
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_relationship_suggestions (
                    run_id,
                    server_address,
                    database_name,
                    schema_name,
                    source_table,
                    source_column,
                    target_table,
                    target_column,
                    relationship_type,
                    confidence_score,
                    description,
                    is_current,
                    source,
                    date_created,
                    date_updated,
                    author_created,
                    author_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id,
                server,
                database,
                schema,
                source_table,
                source_column,
                target_table,
                target_column,
                relationship_type,
                confidence_score,
                description,
                True,
                source,
                datetime.utcnow(),
                datetime.utcnow(),
                author,
                author
            ))
            conn.commit()
    finally:
        conn.close()