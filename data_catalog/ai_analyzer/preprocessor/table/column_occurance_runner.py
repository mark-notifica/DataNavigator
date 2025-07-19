# Column Occurrence Analyzer
# --------------------------
# Deze preprocessor telt hoe vaak een kolomnaam voorkomt binnen een database/schema.
# Berekent ook in hoeveel tabellen een kolom voorkomt (table_count).
#
# ➕ Potentiële toegevoegde waarde:
# 1. AI Column Analyzer:
#    - Frequent voorkomende kolommen (zoals 'id', 'user_id', 'project_id') kunnen wijzen op foreign keys.
#    - Samen met uniqueness-profielen vormt dit input voor key-classificatie.
#
# 2. Graph Builder:
#    - Veelvoorkomende kolommen kunnen als verbindende 'anchors' gebruikt worden in de graph.
#    - Bijv. 'project_id' in 20 tabellen → potentiële koppelkans.
#
# 3. Standaardisatie-inzicht:
#    - Laat zien hoe kolomnamen consistent (of inconsistent) zijn toegepast binnen een datawarehouse.
#
# ⚠️ Beperkingen & risico’s:
# - Kolomnamen zijn geen garantie voor semantische gelijkheid!
#   'id' in de ene tabel kan iets totaal anders betekenen dan in een andere.
# - Cross-database/schemagebruik kan verwarrend of zelfs misleidend zijn.
# - Naming-inconsistentie ('project_id' vs. 'proj_id') beperkt effectiviteit van deze analyse.
# - Kolommen kunnen vaak voorkomen zonder unieke waarden → geen sleutel!
#
# 🎯 Aanbevolen gebruik:
# - Combineer occurrence-analyse altijd met:
#   • Uniqueness-ratio uit column_profile
#   • AI-classificatie op basis van context + voorbeelden
#   • Filters per schema/databron om betekenis te waarborgen
#
# ❗ Gebruik dit dus *niet* als beslissende factor voor key-herkenning,
# maar als verkennende indicator of ondersteunend signaal.
#
# 📌 Tip:
# Aanroepen van de databasefunctie kan met:
# SELECT *
# FROM catalog.fn_filtered_column_occurrences(
#     p_database_name => 'ENK_DEV1',
#     p_schema_name   => 'stg',
#     p_table_like_pattern => 'ods_alias_%'
# );

import logging
from data_catalog.connection_handler import get_catalog_connection
from ai_analyzer.catalog_access.dw_config_reader import get_ai_config_by_id
from data_catalog.ai_analyzer.preprocessor.preprocessor_runs import (
    start_preprocessor_run,
    complete_preprocessor_run,
    mark_preprocessor_run_failed
)

logger = logging.getLogger(__name__)


def write_occurrence_profile_to_db(conn, run_id, profile: dict):
    with conn.cursor() as cur:
        # Zet eerdere profielen op is_current = FALSE
        cur.execute("""
            UPDATE catalog.catalog_column_occurrence_profiles
            SET is_current = FALSE
            WHERE server_name = %(server_name)s
              AND database_name = %(database_name)s
              AND schema_name IS NOT DISTINCT FROM %(schema_name)s
              AND column_name = %(column_name)s
        """, {
            "server_name": profile["server_name"],
            "database_name": profile["database_name"],
            "schema_name": profile["schema_name"],
            "column_name": profile["column_name"]
        })

        # Insert nieuw profiel inclusief schema_ids
        cur.execute("""
            INSERT INTO catalog.catalog_column_occurrence_profiles (
                preprocessor_run_id, server_name, database_name, schema_name, column_name,
                occurrence_count, table_count, data_types, tables,
                column_ids, table_ids, schema_ids, is_current
            ) VALUES (
                %(run_id)s, %(server_name)s, %(database_name)s, %(schema_name)s, %(column_name)s,
                %(occurrence_count)s, %(table_count)s, %(data_types)s, %(tables)s,
                %(column_ids)s, %(table_ids)s, %(schema_ids)s, TRUE
            )
        """, {
            "run_id": run_id,
            **profile
        })


def run_column_occurrence_runner(ai_config_id: int):
    """
    Voert column name occurrence clustering uit op basis van ai_config_id.
    Resultaten worden opgeslagen in catalog.catalog_column_occurrence_profiles.
    """
    catalog_conn = get_catalog_connection()
    run_id = None

    try:
        # Ophalen van AI-configuratie
        ai_config = get_ai_config_by_id(catalog_conn, ai_config_id)
        database_name = ai_config["database_name"]
        schema_filter = ai_config.get("schema_filter")
        table_like = ai_config.get("table_filter")  # Verwacht LIKE-patroon of None

        # Start run logging
        run_id = start_preprocessor_run(
            conn=catalog_conn,
            run_type="column_occurrence",
            ai_config_id=ai_config_id
        )
        logger.info(f"[START] Occurrence run voor {database_name} gestart (run_id={run_id})")

        # Ophalen occurrence clusters via functie
        with catalog_conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM catalog.fn_filtered_column_occurrences(%s, %s, %s)
            """, (database_name, schema_filter, table_like))
            rows = cur.fetchall()
            colnames = [desc.name for desc in cur.description]

        count = 0
        for row in rows:
            profile = dict(zip(colnames, row))
            profile["run_id"] = run_id
            write_occurrence_profile_to_db(catalog_conn, run_id, profile)
            count += 1

        catalog_conn.commit()
        complete_preprocessor_run(catalog_conn, run_id, success=True, note=f"{count} profielen")
        logger.info(f"[DONE] {count} occurrence-profielen opgeslagen voor {database_name} (run_id={run_id})")

    except Exception as e:
        logger.exception(f"[ERROR] Column occurrence run gefaald: {e}")
        if catalog_conn and run_id:
            catalog_conn.rollback()
            mark_preprocessor_run_failed(catalog_conn, run_id, note=str(e))

    finally:
        catalog_conn.close()

