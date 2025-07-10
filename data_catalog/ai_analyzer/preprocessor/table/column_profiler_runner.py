# Column Profiler
# ---------------
# Deze preprocessor verzamelt statistieken per kolom zoals null_count, unique_count,
# row_count en uniqueness_ratio. Deze informatie ondersteunt:
#
# 1. AI Column Analyzer:
#    - Betere classificatie van kolommen als PRIMARY_KEY, FOREIGN_KEY, DIMENSION, enz.
#    - Uniqueness_ratio ≈ 1 wijst op kandidaat keys.
#    - Combinatie met column_occurrence vergroot interpretatiekracht van AI.
#
# 2. Graph Builder:
#    - Gebruikt uniqueness- en distributie-info voor het leggen van relaties.
#    - Unieke kolommen kunnen als knooppunt fungeren in graph clustering.
#
# ⚠️ Let op: samples uit meerdere databases kunnen leiden tot verkeerde inschatting
# (bijv. 'gestapelde' kolommen lijken unieker dan ze zijn).
#
# 🔜 Mogelijke optimalisaties:
#    - Paralleliseren van kolomprofilering per tabel of batch.
#    - Intelligente sampling per database of datadomein.
#    - ➕ Meegeven van expliciete filters (bijv. schema = 'verheggen') zodat
#      de gebruiker controle heeft over welk datadomein geanalyseerd wordt en
#      gestapelde data kan worden voorkomen.


import logging
from datetime import datetime

import pandas as pd
import numpy as np

from data_catalog.connection_handler import (
    get_catalog_connection,
    connect_to_source_database,
    get_specific_connection
)
from data_catalog.ai_analyzer.utils.config_reader import get_ai_config_by_id
from data_catalog.ai_analyzer.utils.catalog_reader import (
    get_filtered_tables_with_ids,
    get_metadata_with_ids
)
from data_catalog.ai_analyzer.preprocessor.preprocessor_runs import (
    start_preprocessor_run,
    complete_preprocessor_run,
    mark_preprocessor_run_failed,
    mark_preprocessor_run_aborted
)


logger = logging.getLogger(__name__)


def read_table_as_dataframe(conn, schema_name: str, table_name: str, limit: int = None) -> pd.DataFrame:
    """
    Leest een volledige tabel in als Pandas DataFrame.
    Let op: pas LIMIT toe voor grote tabellen.
    """
    try:
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f'SELECT * FROM "{schema_name}"."{table_name}" {limit_clause}'
        df = pd.read_sql(query, conn)
        logger.debug(f"📥 {len(df)} rijen opgehaald uit {schema_name}.{table_name}")
        return df
    except Exception as e:
        logger.warning(f"Fout bij ophalen {schema_name}.{table_name}: {e}")
        return pd.DataFrame()
    
def analyze_column(series: pd.Series) -> dict:
    s = series.dropna()
    row_count = len(series)
    unique_count = s.nunique(dropna=True)

    return {
        "data_type": str(s.dtype),
        "null_count": int(series.isnull().sum()),
        "non_null_count": int(s.count()),
        "unique_count": int(unique_count),
        "row_count": int(row_count),
        "uniqueness_ratio": float(unique_count) / row_count if row_count > 0 else None,
    }


def write_profile_to_db(conn, run_id, server, db, schema, table, column, table_id, column_id, profile: dict):
    with conn.cursor() as cur:
        cur.execute("""
            insert into catalog.catalog_column_profiles (
                preprocessor_run_id, server_name, database_name, schema_name, table_name,
                table_id, column_name, column_id,
                data_type, null_count, non_null_count, unique_count, row_count, uniqueness_ratio
            ) values (
                %(run_id)s, %(server)s, %(db)s, %(schema)s, %(table)s,
                %(table_id)s, %(column)s, %(column_id)s,
                %(data_type)s, %(null_count)s, %(non_null_count)s, %(unique_count)s, %(row_count)s, %(uniqueness_ratio)s
            )
        """, {
            "run_id": run_id,
            "server": server,
            "db": db,
            "schema": schema,
            "table": table,
            "table_id": table_id,
            "column": column,
            "column_id": column_id,
            "data_type": profile.get("data_type"),
            "null_count": profile.get("null_count"),
            "non_null_count": profile.get("non_null_count"),
            "unique_count": profile.get("unique_count"),
            "row_count": profile.get("row_count"),
            "uniqueness_ratio": profile.get("uniqueness_ratio"),
        })
    conn.commit()

def profile_table(
    source_conn,
    catalog_conn,
    table: dict,
    preprocessor_run_id: int
) -> int:
    """
    Profileert één tabel:
    - Leest data uit bron
    - Analyseert alle kolommen
    - Schrijft profielen weg naar catalogus
    Retourneert: aantal geprofileerde kolommen (int)
    """
    try:
        df = read_table_as_dataframe(source_conn, table["schema_name"], table["table_name"])
        if df.empty:
            logger.warning(f"[SKIP] Geen rijen in {table['schema_name']}.{table['table_name']}")
            return 0

        column_metadata = get_metadata_with_ids(table)
        if not column_metadata:
            logger.warning(f"[SKIP] Geen kolommen gevonden voor {table['schema_name']}.{table['table_name']}")
            return 0

        column_ids = {col["name"]: col["column_id"] for col in column_metadata}
        profiled_count = 0

        for colname in df.columns:
            column_id = column_ids.get(colname)
            if column_id is None:
                logger.warning(f"[COLUMN MISSING] '{colname}' niet gevonden in metadata van {table['table_name']}")
                continue

            try:
                profile = analyze_column(df[colname])
                write_profile_to_db(
                    conn=catalog_conn,
                    run_id=preprocessor_run_id,
                    server=table["server_name"],
                    db=table["database_name"],
                    schema=table["schema_name"],
                    table=table["table_name"],
                    column=colname,
                    table_id=table["table_id"],
                    column_id=column_id,
                    profile=profile
                )
                profiled_count += 1
                logger.info(f"[PROFILED] {profiled_count} kolommen geprofiled in {table['table_name']}")
            except Exception as col_err:
                logger.warning(f"[COLUMN ERROR] Fout bij kolom '{colname}' in {table['table_name']}: {col_err}")

        return profiled_count

    except Exception as e:
        logger.error(f"[TABLE ERROR] Kan tabel {table['table_name']} niet profileren: {e}")
        return 0
    
def run_column_profiler_batch_by_config(ai_config: dict, author: str = None, log_path: str = None):
    """
    Voert een batch kolomprofilering uit op basis van een AI-config object.
    """
    logger.info(f"[START] Column profiler batch gestart via AI-config {ai_config.get('id')}")
    conn_info = get_specific_connection(ai_config["connection_id"])
    source_conn = connect_to_source_database(conn_info, ai_config["ai_database_filter"])
    catalog_conn = get_catalog_connection()

    try:
        run_id = start_preprocessor_run(
            run_name = f"Column profile – {ai_config['ai_database_filter']}.{ai_config['ai_schema_filter']}",
            description = "Batchrun kolomprofilering via AI-config",
            filter_server_name = conn_info["host"],
            filter_database_name = ai_config["ai_database_filter"],
            filter_schema_name = ai_config["ai_schema_filter"],
            filter_table_name_prefix = ai_config.get("ai_table_filter"),
            author = author,
            config_source = "run_column_profiler_batch_by_config",
            log_path = log_path,
            preprocessor_type = "column_profile"
        )

        tables = get_filtered_tables_with_ids(
            server_name = conn_info["host"],
            database_name = ai_config["ai_database_filter"],
            schema_pattern = ai_config.get("ai_schema_filter"),
            table_pattern = ai_config.get("ai_table_filter")
        )

        total_tables = 0
        total_columns = 0

        if not tables:
            logger.warning("[ABORT] Geen tabellen gevonden voor profiling.")
            mark_preprocessor_run_aborted(
                run_id = run_id,
                notes = "Geen tabellen gevonden voor profiling."
            )
            return
        
        for table in tables:
            logger.info(f"[TABLE] Profiler: {table['table_name']}")
            n_cols = profile_table(
                source_conn = source_conn,
                catalog_conn = catalog_conn,
                table = table,
                preprocessor_run_id = run_id
            )
            if n_cols > 0:
                total_tables += 1
                total_columns += n_cols

        complete_preprocessor_run(
            # conn = catalog_conn,
            run_id = run_id,
            status = "completed",
            tables_processed = total_tables,
            columns_profiled = total_columns,
            notes = f"{total_tables} tabellen en {total_columns} kolommen geprofiled"
        )

        logger.info(f"[COMPLETE] Column profiling afgerond: {total_tables} tabellen, {total_columns} kolommen.")
    except Exception as e:
        logger.error(f"[ERROR] Profiler gefaald: {e}")
        mark_preprocessor_run_failed(run_id, notes=f"Profiler error: {str(e)}")
        return
    
    finally:
        catalog_conn.close()
        source_conn.close()


def run_column_profile_by_ai_config(ai_config_id: int, author: str = None):
    """
    Wrapper: haalt AI-config op op basis van ID en voert profiling uit.
    """
    logger.info(f"[WRAPPER] Start column profiler via ai_config_id={ai_config_id}")
    ai_config = get_ai_config_by_id(ai_config_id)
    if not ai_config:
        logger.error(f"[ABORT] Geen AI-config gevonden met id={ai_config_id}")
        return
    run_column_profiler_batch_by_config(ai_config, author=author)

if __name__ == "__main__":
    import sys
    ai_config_id = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if ai_config_id:
        run_column_profile_by_ai_config(ai_config_id=ai_config_id, author="test_user")
    else:
        print("❌ Geef een AI-config ID mee als argument.")