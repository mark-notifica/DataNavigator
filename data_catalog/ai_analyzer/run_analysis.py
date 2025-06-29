import argparse
import logging
import os
from datetime import datetime

from data_catalog.database_server_cataloger import get_catalog_connection


def setup_logger(run_id: int) -> str:
    """
    Initialiseert logging met run_id in de bestandsnaam.
    Retourneert het relatieve pad van het logbestand.
    """
    if logging.getLogger().hasHandlers():
        return ""  # Logger is al actief

    log_dir = os.path.join(os.path.dirname(__file__), "..", "logfiles", "ai_analyzer")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"log_{timestamp}_run{run_id}.txt"
    log_path = os.path.join(log_dir, filename)

    logging.basicConfig(
        filename=log_path,
        filemode="w",
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO
    )

    logging.info(f"Log gestart voor run_id={run_id}")
    return os.path.relpath(log_path, os.path.dirname(__file__))


def create_ai_analysis_run(server, database, schema, prefix_or_table, analysis_type, author="system", is_dry_run=False, scope="table") -> int:
    """
    Registreert een nieuwe analyse-run in de catalogus.
    `prefix_or_table` kan een tabelnaam, prefix of '*' zijn.
    """
    if scope == "schema":
        run_name = f"{analysis_type} | {schema} (schema)"
        description = f"Schema-analyse van structuur en relaties binnen schema `{schema}` in database `{database}`."
    elif scope == "table":
        if prefix_or_table == "*":
            run_name = f"{analysis_type} | {schema}.*"
            description = f"Table-analyse van alle tabellen in schema `{schema}` in database `{database}`."
        elif prefix_or_table.endswith("_"):
            run_name = f"{analysis_type} | {schema}.{prefix_or_table}*"
            description = f"Batch table-analyse voor tabellen met prefix `{prefix_or_table}` in schema `{schema}`."
        else:
            run_name = f"{analysis_type} | {schema}.{prefix_or_table}"
            description = f"Analyse van tabel `{prefix_or_table}` in schema `{schema}`."

    with get_catalog_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO catalog.catalog_ai_analysis_runs (
                    run_name,
                    analysis_type,
                    description,
                    filter_server_name,
                    filter_database_name,
                    filter_schema_name,
                    filter_table_name_prefix,
                    author,
                    started_at,
                    status,
                    is_dry_run
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), 'started', %s)
                RETURNING id
            """, (
                run_name,
                analysis_type,
                description,
                server, database, schema, prefix_or_table,
                author,
                is_dry_run
            ))
            run_id = cur.fetchone()[0]
            conn.commit()
    return run_id, description

def store_log_path_in_run(run_id: int, relative_log_path: str):
    """Slaat het pad van het logbestand op in de run-registratie"""
    with get_catalog_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE catalog.catalog_ai_analysis_runs
                SET log_path = %s
                WHERE id = %s
            """, (relative_log_path, run_id))
            conn.commit()
    logging.info(f"Logpad opgeslagen voor run_id={run_id}: {relative_log_path}")


def main():
    from data_catalog.ai_analyzer.runners import (
        run_single_table,
        run_batch_tables,
        run_schema_analysis,
    )

    parser = argparse.ArgumentParser(description="Voer AI-analyse uit op tabellen of schema’s uit de catalogus")
    parser.add_argument("--server", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", help="Specifieke tabelnaam (voor single analysis)")
    parser.add_argument("--prefix", help="Prefix om meerdere tabellen te selecteren (voor batch analysis)")
    parser.add_argument("--analysis_type", default="table_description")
    parser.add_argument("--author", default="system")
    parser.add_argument("--dry-run", action="store_true", help="Geen opslag of AI-call, toon alleen prompt")
    parser.add_argument("--analysis-scope", choices=["table", "schema"], default="table")
    args = parser.parse_args()

    try:
        # Bepaal identifier voor registratie
        identifier = args.table or args.prefix or "*"

        # Stap 1: run registreren
        run_id, description = create_ai_analysis_run(
            args.server, args.database, args.schema,
            identifier, args.analysis_type, args.author,
            is_dry_run=args.dry_run,
            scope=args.analysis_scope
        )


        relative_log_path = setup_logger(run_id)
        store_log_path_in_run(run_id, relative_log_path)
        logging.info(f"Beschrijving van run: {description}")
        
        # Stap 2: logging starten met run_id
        relative_log_path = setup_logger(run_id)

        # Stap 3: logpad opslaan in DB
        store_log_path_in_run(run_id, relative_log_path)

        logging.info("AI-analyse CLI gestart")

        # Stap 4: routeren naar juiste runner
        if args.analysis_scope == "table":
            if args.table:
                run_single_table(
                    args.server, args.database, args.schema,
                    args.table, args.analysis_type, args.author,
                    args.dry_run, run_id
                )
            elif args.prefix:
                run_batch_tables(
                    args.server, args.database, args.schema,
                    args.prefix, args.analysis_type, args.author,
                    args.dry_run, run_id
                )
            else:
                logging.error("Geen --table of --prefix opgegeven bij analysis-scope=table.")
                print("⚠️  Geef --table of --prefix op bij table-analyse.")
        elif args.analysis_scope == "schema":
            run_schema_analysis(
                args.server, args.database, args.schema,
                args.author, args.dry_run, run_id
            )

    except Exception as e:
        logging.exception(f"Fout tijdens analyse: {e}")
        print(f"❌ Analyse mislukt: {e}")


if __name__ == "__main__":
    main()
