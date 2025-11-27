import logging
from collections import Counter
import os
import json
from dotenv import load_dotenv
import logging
import sys

from ai_analyzer.catalog_access.catalog_reader import get_metadata_with_ids, get_view_definition_with_ids, get_filtered_tables_with_ids
from ai_analyzer.prompts.prompt_builder import build_prompt_for_table
from ai_analyzer.analysis.llm_model_wrapper import call_llm
from ai_analyzer.postprocessor.ai_analysis_writer import store_ai_table_analysis
from ai_analyzer.analysis.analysis_matrix import ANALYSIS_TYPES
from ai_analyzer.config.analysis_config_loader import load_analysis_config, merge_analysis_configs
from ai_analyzer.model_logic.model_config import get_model_config
from ai_analyzer.postprocessor.ai_analysis_writer import (
    create_analysis_run_entry,
    finalize_and_complete_run,
    mark_analysis_run_failed,
    mark_analysis_run_aborted,
    update_log_path_for_run,
)


# Compat: tests patch finalize_run_with_token_totals en mark_analysis_run_complete
def finalize_run_with_token_totals(run_id: int):  # pragma: no cover (thin wrapper)
    finalize_and_complete_run(run_id)


def mark_analysis_run_complete(run_id: int):  # pragma: no cover (thin wrapper)
    finalize_and_complete_run(run_id)


from data_catalog.connection_handler import (
    get_specific_connection,
    connect_to_source_database
)
from ai_analyzer.catalog_access.dw_config_reader import get_ai_config_by_id
from ai_analyzer.model_logic.llm_clients.openai_parsing import parse_column_classification_response
from ai_analyzer.utils.file_writer import store_analysis_result_to_file as _compat_store_file


# Compatibility shim so tests can patch ai_analyzer.runners.table_runner.store_analysis_result_to_file
def store_analysis_result_to_file(
    name: str,
    result_json: dict,
    output_dir: str | None = None,
) -> str:
    return _compat_store_file(name, result_json, output_dir)


load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s %(asctime)s [%(filename)s:%(lineno)d] %(message)s"
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter("%(levelname)s %(asctime)s [%(filename)s:%(lineno)d] %(message)s"))
logger.addHandler(handler)

try:
    MAX_ALLOWED_TABLES = int(os.getenv("AI_MAX_ALLOWED_TABLES", 500))
except ValueError:
    MAX_ALLOWED_TABLES = 500
    logging.warning("[WAARSCHUWING] AI_MAX_ALLOWED_TABLES bevat geen geldige integer — fallback naar 500")
    
ALLOW_UNFILTERED_SELECTION = os.getenv("AI_ALLOW_UNFILTERED_SELECTION", "false").lower() == "true"


def get_enabled_table_analysis_types() -> dict:
    """
    Haalt alle 'active' table analysetypes op uit YAML en koppelt ze aan hun matrixdefinitie.
    :return: dict van analysis_type → config met runtime logica
    """
    config = load_analysis_config()
    active_items = [item for item in config.get("table_analysis", []) if item.get("status") == "active"]

    matrix = ANALYSIS_TYPES
    yaml_config = {}

    for item in active_items:
        name = item["name"]
        if name not in matrix:
            raise ValueError(f"Analysis type '{name}' uit YAML bestaat niet in ANALYSIS_TYPES.")
        yaml_config[name] = item

    combined = merge_analysis_configs(yaml_config, matrix)
    return combined

 
def run_batch_tables_by_config(
    ai_config_id: int,
    analysis_type: str,
    author: str,
    dry_run: bool,
    connection_id: int | None = None,
):
    print("[TEST] run_batch_tables_by_config aangeroepen")
    logging.info("[TEST] LOGGING: run_batch_tables_by_config aangeroepen")
    ai_config = get_ai_config_by_id(ai_config_id)
    if not ai_config:
        logging.error(f"[ABORT] Geen AI-config gevonden met id={ai_config_id}")
        return

    try:
        # Connection ID kan worden overschreven via argument; anders uit ai_config
        effective_conn_id = connection_id if connection_id is not None else ai_config["connection_id"]
        connection = get_specific_connection(effective_conn_id)
    except Exception as e:
        logging.error(f"[ABORT] Kan geen verbinding ophalen: {e}")
        return

    schema = ai_config.get("ai_schema_filter") or ''
    prefix = ai_config.get("ai_table_filter") or ''
    model_used, temperature, max_tokens, model_config_source = get_model_config(analysis_type, ai_config)
    logging.info(
        f"[CONFIG] model={model_used}, temp={temperature}, max_tokens={max_tokens} "
        f"voor analysis_type={analysis_type} (bron: {model_config_source})"
    )

    # Haal ingeschakelde analyses op en valideer analysis_type voordat een run wordt aangemaakt
    enabled_analyses = get_enabled_table_analysis_types()
    if analysis_type not in enabled_analyses:
        logging.error(f"[ABORT] Analyse '{analysis_type}' is niet ingeschakeld in de YAML-configuratie.")
        # Leg alsnog een run vast zodat er een auditspoor is
        run_id = create_analysis_run_entry(
            server=connection["host"],
            database=ai_config["ai_database_filter"],
            schema=schema,
            prefix=prefix,
            analysis_type=analysis_type,
            author=author,
            is_dry_run=dry_run,
            connection_id=connection["id"],
            ai_config_id=ai_config_id,
            model_used=model_used,
            temperature=temperature,
            max_tokens=max_tokens,
            model_config_source=model_config_source
        )
        mark_analysis_run_aborted(run_id, f"Analyse '{analysis_type}' niet geactiveerd")
        return

    analysis_config = enabled_analyses[analysis_type]
    # Overschrijf model parameters indien gespecificeerd in analysis_config
    model_used = analysis_config.get("default_model", model_used)
    temperature = analysis_config.get("temperature", temperature)
    max_tokens = analysis_config.get("max_tokens", max_tokens)

    run_id = create_analysis_run_entry(
        server=connection["host"],
        database=ai_config["ai_database_filter"],
        schema=schema,
        prefix=prefix,
        analysis_type=analysis_type,
        author=author,
        is_dry_run=dry_run,
        connection_id=connection["id"],
        ai_config_id=ai_config_id,
        model_used=model_used,
        temperature=temperature,
        max_tokens=max_tokens,
        model_config_source=model_config_source
    )

    try:
        if not schema and not prefix and not ALLOW_UNFILTERED_SELECTION:
            logging.warning("[ABORT] Geen schema of prefix opgegeven — onveilige selectie wordt geblokkeerd.")
            mark_analysis_run_aborted(run_id, "onveilige selectie geblokkeerd")
            return

        logging.debug(f"[DEBUG] Python-filter op schema: {schema}, table: {prefix}")
        # ✅ Gebruik catalogus voor filtering met wildcards
        tables = get_filtered_tables_with_ids(
            server_name=connection["host"],
            database_name=ai_config["ai_database_filter"],
            schema_pattern=schema,
            table_pattern=prefix
        )
        
        if not tables:
            logging.warning("[WAARSCHUWING] Geen tabellen gevonden met opgegeven filters.")
            mark_analysis_run_aborted(run_id, "Geen tabellen gevonden in catalogus")
            return

        print(f"[DEBUG] MAX_ALLOWED_TABLES = {MAX_ALLOWED_TABLES}")

        if len(tables) > MAX_ALLOWED_TABLES:
            logging.warning(
                f"[ABORT] Te veel tabellen geselecteerd ({len(tables)}). "
                f"Maximaal toegestaan: {MAX_ALLOWED_TABLES}."
            )
            mark_analysis_run_aborted(run_id, f"Te veel tabellen geselecteerd: {len(tables)}")
            return

        logging.info(f"[INFO] {len(tables)} tabellen geselecteerd uit catalogus.")
        issue_counts = Counter()

        batch_results = []
        log_filename = f"run_{run_id}_{'dryrun' if dry_run else 'live'}_results.json"

        # relatieve en absolute paden scheiden
        rel_log_path = os.path.join("data_catalog", "logfiles", "ai_analyzer", log_filename)
        abs_log_path = os.path.abspath(rel_log_path)

        # zorg dat de directory bestaat
        os.makedirs(os.path.dirname(abs_log_path), exist_ok=True)

        logging.info(
            f"[CONFIG] model={model_used}, temp={temperature}, max_tokens={max_tokens} "
            f"via {model_config_source}"
        )
        logging.info(f"[RUN] Start batch-analyse voor {len(tables)} tabellen (run_id={run_id})")

        # analysis_config is reeds opgehaald vóór het aanmaken van de run
        allowed_types = analysis_config.get("allowed_table_types")
        if allowed_types:
            before_count = len(tables)
            tables = [t for t in tables if t["table_type"].upper() in allowed_types]
            after_count = len(tables)
            skipped = before_count - after_count
            logging.info(f"[FILTER] {after_count} tabellen toegestaan (type ∈ {allowed_types}), {skipped} overgeslagen")

        
        for row in tables:
            logging.debug(f"[DEBUG] Tabeltype voor {row['table_name']}: {row.get('table_type')}")
            assert row.get("table_type") in ("VIEW", "BASE TABLE", "V", "T"), (
                f"Onbekend table_type: {row.get('table_type')}"
            )

            table = {
                "server_name": connection["host"],
                "database_name": ai_config["ai_database_filter"],
                "schema_name": row["schema_name"],
                "table_name": row["table_name"],
                "database_id": row["database_id"],
                "schema_id": row["schema_id"],
                "table_id": row["table_id"],
                "connection_id": connection["id"],
                "main_connector_id": connection["id"],
                "ai_config_id": ai_config_id,
                "table_type": row.get("table_type", "BASE TABLE")
            }
            try:
                result = run_single_table(
                    table,
                    analysis_type,
                    author,
                    dry_run,
                    run_id,
                    model_used,
                    temperature,
                    max_tokens,
                    analysis_config=analysis_config
                )
                batch_results.append(result)
            except Exception as e:
                issue_counts["exceptions"] += 1
                logging.exception(f"[ERROR] Fout bij analyse van {table['table_name']}: {e}")

        if issue_counts:
            total_issues = sum(issue_counts.values())
            logging.info(f"[SUMMARY] {total_issues} tabellen overgeslagen of met fouten:")
            for issue, count in issue_counts.items():
                logging.info(f"  - {issue}: {count}")

        with open(abs_log_path, "w", encoding="utf-8") as f:
            json.dump(batch_results, f, indent=2, default=str)

        logging.info(f"[DEBUG] rel_log_path = {rel_log_path}")
        logging.info(f"[DEBUG] update_log_path_for_run({run_id}, {rel_log_path}) wordt aangeroepen")
        update_log_path_for_run(run_id, rel_log_path)
        logging.info(f"[LOG] Resultaatbestand opgeslagen in {abs_log_path}")
        logging.info(f"[RUN] Batch-analyse voltooid voor {len(tables)} tabellen (run_id={run_id})")
        # Gebruik compat wrappers zodat patches in tests werken
        finalize_run_with_token_totals(run_id)
        mark_analysis_run_complete(run_id)
        logging.info(f"[DONE] Batch-analyse {'gesimuleerd' if dry_run else 'voltooid'}")

    except Exception as e:
        logging.exception("[FAIL] Batch-analyse gefaald")
        mark_analysis_run_failed(run_id, str(e))
    finally:
        # Alleen verbinding openen/sluiten als het geen dry-run is
        try:
            if not dry_run:
                # open en sluit bronverbinding alleen bij echte run
                conn = connect_to_source_database(
                    connection,
                    ai_config["ai_database_filter"],
                )
                conn.close()
        except Exception:
            logging.debug("[FINALIZE] Bronverbinding niet gesloten (dry-run of fout) — doorgaan")

 
def run_single_table(
    table: dict,
    analysis_type: str,
    author: str,
    dry_run: bool,
    run_id: int,
    model_used: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    analysis_config: dict | None = None,
):
    logging.info(f"[RUN] Analyse gestart voor {table['table_name']} (run_id={run_id})")
    is_view = table.get("table_type", "").upper() in ("V", "VIEW")

    try:
        # --- VIEW ANALYSE ---
        if analysis_type == "view_definition_analysis" and is_view:
            view_def = get_view_definition_with_ids(table)

            if not view_def:
                logging.warning(f"[SKIP] Geen viewdefinitie voor {table['table_name']}")
                result = {"issues": ["no_view_definition"]}
                if not dry_run:
                    store_ai_table_analysis(run_id, table, result, analysis_type)
                return {
                    "schema": table["schema_name"],
                    "table": table["table_name"],
                    "type": "view",
                    "status": "skipped",
                    "reason": "no_view_definition",
                    "prompt": None
                }

            prompt = build_prompt_for_table(table, {"definition": view_def}, None, analysis_type)

            if dry_run:
                # Compat: write prompt to file in dry-run mode (used by tests)
                try:
                    store_analysis_result_to_file(
                        f"{table['schema_name']}.{table['table_name']}",
                        {"prompt": prompt, "analysis_type": analysis_type},
                    )
                except Exception:
                    logging.debug("[DRYRUN] store_analysis_result_to_file failed; continuing")
                return {
                    "schema": table["schema_name"],
                    "table": table["table_name"],
                    "type": "view",
                    "status": "ok",
                    "prompt": prompt
                }

            # Zorg voor defaults indien niet meegegeven (legacy test-call)
            if analysis_config is None:
                enabled_analyses = get_enabled_table_analysis_types()
                analysis_config = enabled_analyses.get(analysis_type, {})
            if model_used is None or temperature is None or max_tokens is None:
                # model_config kan ontbreken in tests; kies uit matrix of veilige defaults
                from ai_analyzer.model_logic.model_config import get_model_config
                mc_model, mc_temp, mc_max, _source = get_model_config(analysis_type, {})
                model_used = model_used or analysis_config.get("default_model", mc_model)
                temperature = temperature if temperature is not None else analysis_config.get("temperature", mc_temp)
                max_tokens = max_tokens if max_tokens is not None else analysis_config.get("max_tokens", mc_max)

            result = call_llm(prompt, model=model_used, temperature=temperature, max_tokens=max_tokens)
            result.update({
                "analysis_type": analysis_type,
                "prompt": prompt,
                "model_used": model_used,
                "temperature": temperature,
                "max_tokens": max_tokens
            })
            store_ai_table_analysis(run_id, table, result, analysis_type)
            logging.info(f"[OK] Analyse opgeslagen voor {table['table_name']}")

            return {
                "schema": table["schema_name"],
                "table": table["table_name"],
                "type": "view",
                "status": "ok",
                "prompt": prompt
            }

        # --- METADATA CHECK ---
        metadata = get_metadata_with_ids(table)

        if not metadata:
            logging.warning(f"[SKIP] Geen kolommen voor {table['table_name']}")
            result = {"issues": ["no_columns"]}
            if not dry_run:
                store_ai_table_analysis(run_id, table, result, analysis_type)
            return {
                "schema": table["schema_name"],
                "table": table["table_name"],
                "type": "table",
                "status": "skipped",
                "reason": "no_columns",
                "prompt": None
            }

        # --- SAMPLEDATA CHECK ---
        sample_data_func = analysis_config.get("sample_data_function")
        sample = sample_data_func(table) if sample_data_func else None

        if sample is None or sample.empty:
            logging.warning(f"[SKIP] Geen data in {table['table_name']}")
            result = {"issues": ["no_sample_data"]}
            if not dry_run:
                store_ai_table_analysis(run_id, table, result, analysis_type)
            return {
                "schema": table["schema_name"],
                "table": table["table_name"],
                "type": "table",
                "status": "skipped",
                "reason": "no_sample_data",
                "prompt": None
            }

        # --- PROMPT + AI ---
        prompt = build_prompt_for_table(table, metadata, sample, analysis_type)

        if dry_run:
            # Compat: write prompt and inputs to file in dry-run mode (used by tests)
            try:
                store_analysis_result_to_file(
                    f"{table['schema_name']}.{table['table_name']}",
                    {
                        "prompt": prompt,
                        "analysis_type": analysis_type,
                        "metadata": metadata,
                        "sample": sample.to_dict(orient="records")
                    },
                )
            except Exception:
                logging.debug("[DRYRUN] store_analysis_result_to_file failed; continuing")
            return {
                "schema": table["schema_name"],
                "table": table["table_name"],
                "type": "table",
                "status": "ok",
                "prompt": prompt,
                "metadata": metadata,
                "sample": sample.to_dict(orient="records")
            }

        # Zorg voor defaults indien niet meegegeven
        if analysis_config is None:
            enabled_analyses = get_enabled_table_analysis_types()
            analysis_config = enabled_analyses.get(analysis_type, {})
        if model_used is None or temperature is None or max_tokens is None:
            from ai_analyzer.model_logic.model_config import get_model_config
            mc_model, mc_temp, mc_max, _source = get_model_config(analysis_type, {})
            model_used = model_used or analysis_config.get("default_model", mc_model)
            temperature = temperature if temperature is not None else analysis_config.get("temperature", mc_temp)
            max_tokens = max_tokens if max_tokens is not None else analysis_config.get("max_tokens", mc_max)

        result = call_llm(prompt, model=model_used, temperature=temperature, max_tokens=max_tokens)
        result.update({
            "analysis_type": analysis_type,
            "prompt": prompt,
            "model_used": model_used,
            "temperature": temperature,
            "max_tokens": max_tokens
        })

        if analysis_type == "column_classification":
            raw_response = result.get("result", "")
            logging.debug(f"[DEBUG] Ruwe AI-response:\n{raw_response}")
            parsed = parse_column_classification_response(raw_response)
            if parsed is not None:
                result["column_classification"] = parsed
            else:
                logging.warning("[PARSER] Kon AI-resultaat niet parseren tot JSON.")

        logging.debug(json.dumps(result, indent=2))

        store_ai_table_analysis(run_id, table, result, analysis_type)
        logging.info(f"[OK] Analyse opgeslagen voor {table['table_name']}")

        return {
            "schema": table["schema_name"],
            "table": table["table_name"],
            "type": "table",
            "status": "ok",
            "prompt": prompt,
            "metadata": metadata,
            "sample": sample.to_dict(orient="records")
        }

    except Exception as e:
        logging.exception(f"[FAIL] Analyse gefaald voor {table.get('table_name', '?')}: {e}")
        return {
            "schema": table.get("schema_name"),
            "table": table.get("table_name"),
            "status": "error",
            "message": str(e)
        }

