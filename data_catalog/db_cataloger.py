import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Dict, Optional

import psycopg2
from dotenv import load_dotenv

from data_catalog.connection_handler import (
    get_catalog_connection,
    fetch_dw_details,
    connect_to_source_database,
    get_databases_on_server,
)

load_dotenv()
logger = logging.getLogger(__name__)

# Helpers -------------------------------------------------------------

def setup_logging_with_run_id(run_id: Optional[int]) -> str:
    script_dir = Path(__file__).parent
    log_dir = script_dir / 'logfiles' / 'database_server'
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"db_catalog_{ts}{('_run_'+str(run_id)) if run_id else ''}.log"

    from logging.handlers import RotatingFileHandler
    handler = RotatingFileHandler(str(log_file), maxBytes=5*1024*1024, backupCount=5)
    console = logging.StreamHandler()
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s',
                        handlers=[handler, console],
                        force=True)

    # return path relative to project root
    project_root = script_dir.parent
    return str(log_file.relative_to(project_root))


def _build_conn_info_from_config(connection_id: int) -> Dict:
    """Build a source connection dict using config.dw_connection_details + secret."""
    d = fetch_dw_details(connection_id, with_secret=True)
    if not d:
        raise ValueError(f"No DW connection details for connection_id={connection_id}")
    engine = d.get('engine_type')
    if engine not in ("PostgreSQL", "Azure SQL Server"):
        raise ValueError(f"Unsupported engine_type: {engine}")

    return {
        'id': connection_id,
        'connection_type': engine,
        'host': d.get('host'),
        'port': d.get('port'),
        'username': d.get('username'),
        'password': d.get('secret_value'),
        'database_name': d.get('default_database') or ("postgres" if engine == "PostgreSQL" else "master"),
    }


# Catalog run lifecycle (catalog.catalog_runs) ------------------------

def start_catalog_run(catalog_conn, connection_id: int, context: Dict) -> int:
    with catalog_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO catalog.catalog_runs
                (run_type, connection_id, source_label, status, mode, context)
            VALUES
                ('DB_CATALOG', %s, %s, 'running', 'manual', %s)
            RETURNING id
            """,
            (connection_id, context.get('source_label'), json.dumps(context))
        )
        run_id = cur.fetchone()[0]
        return run_id


def complete_catalog_run(catalog_conn, run_id: int, nodes_created: int, nodes_updated: int, objects_total: int):
    with catalog_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE catalog.catalog_runs
               SET completed_at = NOW()
                 , status       = 'completed'
                 , nodes_created = %s
                 , nodes_updated = %s
                 , objects_total = %s
             WHERE id = %s
            """,
            (nodes_created, nodes_updated, objects_total, run_id)
        )


def fail_catalog_run(catalog_conn, run_id: int, message: str):
    try:
        with catalog_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE catalog.catalog_runs
                   SET completed_at = NOW()
                     , status = 'failed'
                     , error_message = %s
                 WHERE id = %s
                """,
                (message, run_id)
            )
    except Exception:
        pass


# Node upserts --------------------------------------------------------

def upsert_node(cur, node_type: str, name: str, qualified_name: str, run_id: int, props: Optional[Dict] = None) -> int:
    cur.execute(
        """
        INSERT INTO catalog.nodes
            (node_type
            , name
            , qualified_name
            , props
            , created_in_run_id
            , last_seen_run_id
            )
        VALUES
            (%s
            , %s
            , %s
            , %s
            , %s
            , %s
            )
        ON CONFLICT (node_type, qualified_name) DO UPDATE
           SET name             = EXCLUDED.name
             , props            = COALESCE(EXCLUDED.props, catalog.nodes.props)
             , last_seen_run_id = EXCLUDED.last_seen_run_id
             , updated_at       = NOW()
             , deleted_in_run_id = NULL
             , deleted_at        = NULL
        RETURNING node_id
        """,
        (node_type, name, qualified_name, json.dumps(props or {}), run_id, run_id)
    )
    return cur.fetchone()[0]



def upsert_database(cur, host: str, database: str, run_id: int) -> int:
    qn = f"{host}/{database}"
    node_id = upsert_node(cur, 'DB_DATABASE', database, qn, run_id)
    cur.execute(
        """
        INSERT INTO catalog.node_database (node_id, server_name, database_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (node_id) DO UPDATE
           SET server_name = EXCLUDED.server_name
             , database_name = EXCLUDED.database_name
        """,
        (node_id, host, database)
    )
    return node_id


def upsert_schema(cur, host: str, database_node_id: int, database: str, schema: str, run_id: int) -> int:
    qn = f"{host}/{database}.{schema}"
    node_id = upsert_node(cur, 'DB_SCHEMA', schema, qn, run_id)
    cur.execute(
        """
        INSERT INTO catalog.node_schema (node_id, database_node_id, schema_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (node_id) DO UPDATE
           SET database_node_id = EXCLUDED.database_node_id
             , schema_name = EXCLUDED.schema_name
        """,
        (node_id, database_node_id, schema)
    )
    return node_id


def upsert_table(cur, host: str, database: str, schema_node_id: int, schema: str, table: str, table_type: str, run_id: int) -> int:
    qn = f"{host}/{database}.{schema}.{table}"
    node_id = upsert_node(cur, 'DB_TABLE' if table_type == 'TABLE' else 'DB_VIEW', table, qn, run_id)
    cur.execute(
        """
        INSERT INTO catalog.node_table (node_id, schema_node_id, table_name, table_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (node_id) DO UPDATE
           SET schema_node_id = EXCLUDED.schema_node_id
             , table_name = EXCLUDED.table_name
             , table_type = EXCLUDED.table_type
        """,
        (node_id, schema_node_id, table, 'TABLE' if table_type == 'TABLE' else 'VIEW')
    )
    return node_id


def upsert_column(cur, host: str, database: str, schema: str, table_node_id: int, table: str, column: Dict, run_id: int) -> int:
    col_name = column['column_name']
    qn = f"{host}/{database}.{schema}.{table}.{col_name}"
    node_id = upsert_node(cur, 'DB_COLUMN', col_name, qn, run_id)
    cur.execute(
        """
        INSERT INTO catalog.node_column (node_id, table_node_id, column_name, data_type, is_nullable)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (node_id) DO UPDATE
           SET table_node_id = EXCLUDED.table_node_id
             , column_name = EXCLUDED.column_name
             , data_type = EXCLUDED.data_type
             , is_nullable = EXCLUDED.is_nullable
        """,
        (node_id, table_node_id, col_name, column.get('data_type'), bool(column.get('is_nullable')))
    )
    return node_id


# Source enumeration ---------------------------------------------------

def get_schemas_in_database(src_conn, connection_type: str) -> List[str]:
    if connection_type == 'PostgreSQL':
        with src_conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog','information_schema')
                ORDER BY schema_name
                """
            )
            return [r[0] for r in cur.fetchall()]
    else:
        with src_conn.cursor() as cur:
            cur.execute(
                """
                SELECT name
                FROM sys.schemas
                WHERE name NOT IN ('sys','INFORMATION_SCHEMA')
                ORDER BY name
                """
            )
            return [r[0] for r in cur.fetchall()]


def get_tables_in_schema(src_conn, connection_type: str, schema: str, include_views: bool) -> List[Dict]:
    if connection_type == 'PostgreSQL':
        with src_conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type IN (%s, %s)
                ORDER BY table_name
                """,
                (schema, 'BASE TABLE', 'VIEW' if include_views else 'BASE TABLE')
            )
            rows = cur.fetchall()
            out = []
            for name, ttype in rows:
                if ttype == 'VIEW' and not include_views:
                    continue
                out.append({'table_name': name, 'table_type': 'VIEW' if ttype == 'VIEW' else 'TABLE'})
            return out
    else:
        with src_conn.cursor() as cur:
            if include_views:
                cur.execute(
                    """
                    SELECT t.name AS table_name, 'TABLE' AS table_type
                    FROM sys.tables t
                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE s.name = ?
                    UNION ALL
                    SELECT v.name AS table_name, 'VIEW' AS table_type
                    FROM sys.views v
                    JOIN sys.schemas s ON s.schema_id = v.schema_id
                    WHERE s.name = ?
                    ORDER BY table_name
                    """,
                    (schema, schema)
                )
            else:
                cur.execute(
                    """
                    SELECT t.name AS table_name, 'TABLE' AS table_type
                    FROM sys.tables t
                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE s.name = ?
                    ORDER BY table_name
                    """,
                    (schema,)
                )
            return [{'table_name': r[0], 'table_type': r[1]} for r in cur.fetchall()]


def get_columns_for_table(src_conn, connection_type: str, schema: str, table: str) -> List[Dict]:
    if connection_type == 'PostgreSQL':
        with src_conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table)
            )
            return [
                {
                    'column_name': r[0],
                    'data_type': r[1],
                    'is_nullable': (r[2] == 'YES'),
                    'column_default': r[3],
                    'ordinal_position': r[4],
                }
                for r in cur.fetchall()
            ]
    else:
        with src_conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.name AS column_name,
                       t.name AS data_type,
                       CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
                       c.column_id AS ordinal_position
                FROM sys.columns c
                JOIN sys.types t ON t.user_type_id = c.user_type_id
                JOIN sys.tables tb ON tb.object_id = c.object_id
                JOIN sys.schemas s ON s.schema_id = tb.schema_id
                WHERE s.name = ? AND tb.name = ?
                ORDER BY c.column_id
                """,
                (schema, table)
            )
            return [
                {
                    'column_name': r[0],
                    'data_type': r[1],
                    'is_nullable': (r[2] == 'YES'),
                    'column_default': None,
                    'ordinal_position': r[3],
                }
                for r in cur.fetchall()
            ]


# Orchestration --------------------------------------------------------

def _split_filter(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    return [p.strip() for p in s.split(',') if p.strip()]


def run_catalog(connection_id: int,
                db_filter: Optional[str] = None,
                schema_filter: Optional[str] = None,
                table_filter: Optional[str] = None,
                include_views: bool = False) -> int:
    catalog_conn = get_catalog_connection()
    src_info = _build_conn_info_from_config(connection_id)

    context = {
        'source_label': f"{src_info['host']}:{src_info['port']}",
        'db_filter': db_filter,
        'schema_filter': schema_filter,
        'table_filter': table_filter,
        'include_views': include_views,
    }

    run_id = start_catalog_run(catalog_conn, connection_id, context)
    catalog_conn.commit()

    # setup logging and store relative filename
    rel_log = setup_logging_with_run_id(run_id)
    with catalog_conn.cursor() as cur:
        cur.execute("UPDATE catalog.catalog_runs SET log_filename = %s WHERE id = %s", (rel_log, run_id))
    catalog_conn.commit()

    created = 0
    updated = 0
    total_objects = 0

    try:
        # Discover databases to process
        dbs: List[str]
        dbs = get_databases_on_server(src_info)
        dbs_f = _split_filter(db_filter)
        if dbs_f:
            dbs = [d for d in dbs if d in dbs_f]

        for db_name in dbs:
            logger.info(f"Cataloging database: {db_name}")
            # connect to this database
            db_info = dict(src_info)
            db_info['database_name'] = db_name
            src_conn = connect_to_source_database(db_info, db_name)
            if not src_conn:
                logger.warning(f"Skip database (cannot connect): {db_name}")
                continue

            try:
                with catalog_conn.cursor() as cur:
                    db_node_id = upsert_database(cur, src_info['host'], db_name, run_id)
                catalog_conn.commit()
                total_objects += 1

                # Schemas
                schemas = get_schemas_in_database(src_conn, src_info['connection_type'])
                sch_f = _split_filter(schema_filter)
                if sch_f:
                    schemas = [s for s in schemas if s in sch_f]

                for schema in schemas:
                    with catalog_conn.cursor() as cur:
                        schema_node_id = upsert_schema(cur, src_info['host'], db_node_id, db_name, schema, run_id)
                    catalog_conn.commit()
                    total_objects += 1

                    # Tables/views
                    tables = get_tables_in_schema(src_conn, src_info['connection_type'], schema, include_views)
                    tbl_f = _split_filter(table_filter)
                    if tbl_f:
                        tables = [t for t in tables if t['table_name'] in tbl_f]

                    for t in tables:
                        table_name = t['table_name']
                        table_type = t['table_type']
                        with catalog_conn.cursor() as cur:
                            table_node_id = upsert_table(cur, src_info['host'], db_name, schema_node_id, schema, table_name, table_type, run_id)
                        catalog_conn.commit()
                        total_objects += 1

                        # Columns
                        columns = get_columns_for_table(src_conn, src_info['connection_type'], schema, table_name)
                        for col in columns:
                            with catalog_conn.cursor() as cur:
                                upsert_column(cur, src_info['host'], db_name, schema, table_node_id, table_name, col, run_id)
                            catalog_conn.commit()
                            total_objects += 1

            finally:
                try:
                    src_conn.close()
                except Exception:
                    pass

        complete_catalog_run(catalog_conn, run_id, created, updated, total_objects)
        catalog_conn.commit()
        logger.info(f"Catalog run {run_id} completed. Objects processed: {total_objects}")
        return run_id

    except Exception as e:
        logger.exception("Cataloging failed")
        try:
            fail_catalog_run(catalog_conn, run_id, str(e))
            catalog_conn.commit()
        finally:
            pass
        raise
    finally:
        try:
            catalog_conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Database cataloger (nodes-based)')
    p.add_argument('--connection-id', type=int, required=True, help='config.connections.id to use')
    p.add_argument('--db-filter', type=str, help='Comma-separated database names')
    p.add_argument('--schema-filter', type=str, help='Comma-separated schema names')
    p.add_argument('--table-filter', type=str, help='Comma-separated table names')
    p.add_argument('--include-views', action='store_true', help='Include views')
    args = p.parse_args()

    run_catalog(args.connection_id, args.db_filter, args.schema_filter, args.table_filter, args.include_views)
