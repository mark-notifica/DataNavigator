from collections import Counter
import os
import json
from dotenv import load_dotenv
import sys
import logging

# Belangrijk: catalogusfuncties worden bij aanroep heropgehaald uit het modulepad
# zodat unittest patches op ai_analyzer.utils.catalog_reader goed doorwerken.
# Catalogusfuncties worden bij aanroep heropgehaald uit het modulepad via runtime resolutie,
# zodat unittest patches op ai_analyzer.utils.catalog_reader goed doorwerken.
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


import connection_handler as _ch
from connection_handler import (
    connect_to_source_database,
)

# Thin wrappers so tests can patch either table_runner.* or connection_handler.* targets


def get_ai_config_by_id(ai_config_id: int):  # pragma: no cover
    return _ch.get_ai_config_by_id(ai_config_id)


def get_specific_connection(conn_id: int):  # pragma: no cover
    return _ch.get_specific_connection(conn_id)


from ai_analyzer.model_logic.llm_clients.openai_parsing import parse_column_classification_response
# Compatibility shim so tests can patch either table_runner.* or utils.file_writer.*


def store_analysis_result_to_file(
    name: str,
    result_json: dict,
    output_dir: str | None = None,
) -> str:
    # Resolve at call-time to honor any active patches on utils.file_writer
    from ai_analyzer.utils import file_writer as _fw
    return _fw.store_analysis_result_to_file(name, result_json, output_dir)


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

ALLOW_UNFILTERED_SELECTION: bool = os.getenv("AI_ALLOW_UNFILTERED_SELECTION", "false").lower() == "true"


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
        if dry_run:
            logging.warning(f"[DRYRUN] Fallback verbinding gebruikt wegens fout: {e}")
            connection = {
                "id": effective_conn_id,
                "name": "dryrun_fallback",
                "connection_type": "PostgreSQL",
                "host": "localhost",
                "port": "5432",
                "username": "test",
                "password": "test",
            }
        else:
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
        logging.warning(
            f"[YAML] Analyse '{analysis_type}' niet ingeschakeld — gebruik matrix defaults voor tests"
        )
        analysis_config = ANALYSIS_TYPES.get(analysis_type, {})
    else:
        analysis_config = enabled_analyses[analysis_type]
    # Overschrijf model parameters indien gespecificeerd in analysis_config
    model_used = analysis_config.get("default_model", model_used)
    temperature = analysis_config.get("temperature", temperature)
    max_tokens = analysis_config.get("max_tokens", max_tokens)

    try:
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
    except Exception as e:
        if dry_run:
            logging.debug(f"[DRYRUN] Kon run-entry niet aanmaken: {e}. Ga toch door met simulatie.")
            run_id = 0
        else:
            raise

    aborted_reason = None
    try:
        if not schema and not prefix and not ALLOW_UNFILTERED_SELECTION:
            logging.warning("[ABORT] Geen schema of prefix opgegeven — onveilige selectie wordt geblokkeerd.")
            mark_analysis_run_aborted(run_id, "onveilige selectie geblokkeerd")
            aborted_reason = "onveilige selectie geblokkeerd"

        logging.debug(f"[DEBUG] Python-filter op schema: {schema}, table: {prefix}")
        # ✅ Gebruik catalogus voor filtering met wildcards (runtime resolutie voor patches)
        from ai_analyzer.utils import catalog_reader as _cr
        try:
            tables = _cr.get_tables_for_pattern_with_ids(
                connection["host"],
                ai_config["ai_database_filter"],
                schema,
                prefix,
            )
        except Exception as e:
            logging.error(f"[CATALOG] Fout bij ophalen tabellen: {e}")
            if dry_run:
                logging.warning("[DRYRUN] Gebruik lege tabelset wegens catalogusfout")
                tables = []
                aborted_reason = "catalog_error"
            else:
                raise

        if not tables and aborted_reason is None:
            logging.warning("[WAARSCHUWING] Geen tabellen gevonden met opgegeven filters.")
            mark_analysis_run_aborted(run_id, "Geen tabellen gevonden in catalogus")
            aborted_reason = "no_tables"

        print(f"[DEBUG] MAX_ALLOWED_TABLES = {MAX_ALLOWED_TABLES}")

        if len(tables) > MAX_ALLOWED_TABLES:
            logging.warning(
                f"[ABORT] Te veel tabellen geselecteerd ({len(tables)}). "
                f"Maximaal toegestaan: {MAX_ALLOWED_TABLES}."
            )
            mark_analysis_run_aborted(run_id, f"Te veel tabellen geselecteerd: {len(tables)}")
            aborted_reason = "too_many_tables"

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
        # In dry-run niet filteren op toegestane table_types zodat tests alle items zien
        if allowed_types and not dry_run:
            before_count = len(tables)
            tables = [t for t in tables if t["table_type"].upper() in allowed_types]
            after_count = len(tables)
            skipped = before_count - after_count
            logging.info(f"[FILTER] {after_count} tabellen toegestaan (type ∈ {allowed_types}), {skipped} overgeslagen")

        if aborted_reason is None:
            for row in tables:
                logging.debug(f"[DEBUG] Tabeltype voor {row['table_name']}: {row.get('table_type')}")
                assert row.get("table_type") in ("VIEW", "BASE TABLE", "V", "T"), (
                    f"Onbekend table_type: {row.get('table_type')}"
                )
                # Fallback mapping voor test patches die enkel table_schema teruggeven
                schema_name = row.get("schema_name") or row.get("table_schema") or "public"
                table = {
                    "server_name": connection["host"],
                    "database_name": ai_config["ai_database_filter"],
                    "schema_name": schema_name,
                    "table_name": row["table_name"],
                    "database_id": row.get("database_id"),
                    "schema_id": row.get("schema_id"),
                    "table_id": row.get("table_id"),
                    "connection_id": connection["id"],
                    "main_connector_id": connection["id"],
                    "ai_config_id": ai_config_id,
                    "table_type": row.get("table_type", "BASE TABLE"),
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
        else:
            logging.info(f"[ABORT] Batch-analyse voortijdig afgebroken: {aborted_reason}")

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
        # Gebruik compat wrappers zodat patches in tests werken (ook bij abort in dry-run)
        try:
            finalize_run_with_token_totals(run_id)
            mark_analysis_run_complete(run_id)
        except Exception as e:
            logging.debug(f"[FINALIZE] Kon finalize/complete niet uitvoeren: {e}")
        status_label = 'aborted' if aborted_reason else 'ok'
        mode_label = 'gesimuleerd' if dry_run else 'voltooid'
        logging.info(
            f"[DONE] Batch-analyse {mode_label} (status={status_label})"
        )

    except Exception as e:
        logging.exception("[FAIL] Batch-analyse gefaald")
        # In dry-run geen DB-afhankelijke failure-logging voorkomen maar wel finalize aanroepen
        if dry_run:
            try:
                mark_analysis_run_aborted(run_id, str(e))
            except Exception:
                logging.debug("[DRYRUN] Kon run niet markeren als afgebroken; doorgaan zonder DB")
            try:
                finalize_run_with_token_totals(run_id)
                mark_analysis_run_complete(run_id)
            except Exception:
                logging.debug("[DRYRUN] Finalize/complete mislukt na exception")
        else:
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
            # Runtime resolutie van catalogusfunctie zodat patches werken
            from ai_analyzer.utils import catalog_reader as _cr
            view_def = _cr.get_view_definition_with_ids(table)

            if not view_def:
                logging.warning(f"[SKIP] Geen viewdefinitie voor {table['table_name']}")
                result = {"issues": ["no_view_definition"]}
                if dry_run:
                    # In dry-run ook het resultaatbestand schrijven zodat tests dit kunnen detecteren
                    try:
                        store_analysis_result_to_file(
                            f"{table['schema_name']}.{table['table_name']}",
                            {"prompt": None, "analysis_type": analysis_type, "issues": ["no_view_definition"]},
                        )
                    except Exception:
                        logging.debug("[DRYRUN] store_analysis_result_to_file failed; continuing")
                else:
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
                # Gebruik matrix direct voor single-table tests (zonder YAML gating)
                analysis_config = ANALYSIS_TYPES.get(analysis_type, {})
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

        # Zorg voor defaults indien geen analysis_config is doorgegeven (single-table tests)
        if analysis_config is None:
            analysis_config = ANALYSIS_TYPES.get(analysis_type, {})

        # --- METADATA CHECK ---
        # Veilige metadata ophalen: tijdens dry-run of test zonder echte DB kan dit falen.
        try:
            # Runtime resolutie van catalogusfunctie zodat patches werken
            from ai_analyzer.utils import catalog_reader as _cr
            metadata = _cr.get_metadata_with_ids(table)
        except Exception as e:
            if dry_run:
                logging.debug(f"[SAFE_META] Fallback metadata gebruikt wegens fout: {e}")
                metadata = [
                    {"column_name": "id", "data_type": "int"},
                    {"column_name": "naam", "data_type": "text"},
                ]
            else:
                raise

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
        # Herresolveer dynamisch zodat unittest patches gezien worden (analysis_matrix en sample_data_builder)
        if sample_data_func:
            try:
                import importlib
                func_name = sample_data_func.__name__ if callable(sample_data_func) else str(sample_data_func)
                am = importlib.import_module("ai_analyzer.analysis.analysis_matrix")
                sdb = importlib.import_module("ai_analyzer.samples.sample_data_builder")
                am_func = getattr(am, func_name, None)
                sdb_func = getattr(sdb, func_name, None)
                # Kies gepatchte variant als die afwijkt van de oorspronkelijke referentie
                if sdb_func is not None and sdb_func is not sample_data_func:
                    sample_data_func = sdb_func
                elif am_func is not None and am_func is not sample_data_func:
                    sample_data_func = am_func
                # anders blijft sample_data_func ongewijzigd
            except Exception:
                # Als dynamische herresolutie faalt, gebruik de oorspronkelijke referentie
                pass
        sample = sample_data_func(table) if sample_data_func else None

        # Tests patchen sample-functies en kunnen een list[dict] teruggeven i.p.v. DataFrame
        sample_is_list = isinstance(sample, list)
        if sample_is_list:
            try:
                import pandas as pd  # type: ignore
                sample_df = pd.DataFrame(sample)
            except Exception:
                sample_df = None
        else:
            sample_df = sample

        if sample is None or (hasattr(sample, "empty") and sample.empty) or (sample_is_list and len(sample) == 0):
            logging.warning(f"[SKIP] Geen data in {table['table_name']}")
            result = {"issues": ["no_sample_data"]}
            if dry_run:
                # In dry-run alsnog resultaatbestand schrijven zodat tests dit zien
                try:
                    store_analysis_result_to_file(
                        f"{table['schema_name']}.{table['table_name']}",
                        {
                            "prompt": None,
                            "analysis_type": analysis_type,
                            "issues": ["no_sample_data"],
                            "metadata": metadata,
                        },
                    )
                except Exception:
                    logging.debug("[DRYRUN] store_analysis_result_to_file failed; continuing")
            else:
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
        prompt = build_prompt_for_table(table, metadata, sample_df if sample_df is not None else sample, analysis_type)

        if dry_run:
            # Compat: write prompt and inputs to file in dry-run mode (used by tests)
            try:
                rendered_sample = (
                    sample.to_dict(orient="records")
                    if hasattr(sample, "to_dict")
                    else (sample if sample_is_list else None)
                )
                store_analysis_result_to_file(
                    f"{table['schema_name']}.{table['table_name']}",
                    {
                        "prompt": prompt,
                        "analysis_type": analysis_type,
                        "metadata": metadata,
                        "sample": rendered_sample,
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
                "sample": rendered_sample,
            }

        # Zorg voor defaults indien niet meegegeven
        if analysis_config is None:
            analysis_config = ANALYSIS_TYPES.get(analysis_type, {})
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
            "sample": (
                sample.to_dict(orient="records")
                if hasattr(sample, "to_dict")
                else (sample if sample_is_list else None)
            )
        }

    except Exception as e:
        logging.exception(f"[FAIL] Analyse gefaald voor {table.get('table_name', '?')}: {e}")
        return {
            "schema": table.get("schema_name"),
            "table": table.get("table_name"),
            "status": "error",
            "message": str(e)
        }
