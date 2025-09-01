from dotenv import load_dotenv
import sqlalchemy as sa
import os
from pathlib import Path
from data_catalog.db import q_all, exec_tx 

# ---------------- DB connectie ----------------
# .env naast repo-root of pas pad aan
load_dotenv(dotenv_path=os.path.join(Path(__file__).resolve().parent.parent, ".env"))

db_url = sa.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("NAV_DB_USER"),
    password=os.getenv("NAV_DB_PASSWORD"),
    host=os.getenv("NAV_DB_HOST"),
    port=os.getenv("NAV_DB_PORT"),
    database=os.getenv("NAV_DB_NAME"),
)
engine = sa.create_engine(db_url, future=True)

# -----------------------------------------
# DW Catalog Config Handlers --------------
# -----------------------------------------
def deactivate_dw_catalog_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dw_catalog_config
           SET is_active  = false
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

def reactivate_dw_catalog_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dw_catalog_config
           SET is_active  = true
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

def fetch_dw_catalog_configs(conn_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.config_name
             , cfg.database_filter
             , cfg.schema_filter
             , cfg.table_filter
             , cfg.include_views
             , cfg.include_system_objects
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
             , cfg.last_test_status
             , cfg.last_tested_at
             , cfg.last_test_notes
        FROM   config.dw_catalog_config cfg
        WHERE  cfg.connection_id = :cid
        ORDER  BY cfg.is_active DESC
             ,   cfg.updated_at DESC NULLS LAST
    """, {"cid": conn_id})
    return [dict(r._mapping) for r in rows]

def insert_dw_catalog_config(conn_id: int
                           , config_name: str
                           , database_filter: str | None
                           , schema_filter: str | None
                           , table_filter: str | None
                           , include_views: bool
                           , include_system_objects: bool
                           , notes: str | None
                           , is_active: bool) -> None:
    exec_tx("""
        INSERT INTO config.dw_catalog_config
        ( connection_id
        , config_name
        , database_filter
        , schema_filter
        , table_filter
        , include_views
        , include_system_objects
        , notes
        , is_active
        )
        VALUES
        ( :cid
        , :n
        , :dbf
        , :scf
        , :tbf
        , :iv
        , :iso
        , :notes
        , :active
        )
    """, {"cid": conn_id, "n": config_name.strip(),
          "dbf": (database_filter or "").strip() or None,
          "scf": (schema_filter or "").strip() or None,
          "tbf": (table_filter or "").strip() or None,
          "iv": bool(include_views),
          "iso": bool(include_system_objects),
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})

def update_dw_catalog_config(cfg_id: int
                           , config_name: str
                           , database_filter: str | None
                           , schema_filter: str | None
                           , table_filter: str | None
                           , include_views: bool
                           , include_system_objects: bool
                           , notes: str | None
                           , is_active: bool) -> None:
    exec_tx("""
        UPDATE config.dw_catalog_config
           SET config_name            = :n
             , database_filter        = :dbf
             , schema_filter          = :scf
             , table_filter           = :tbf
             , include_views          = :iv
             , include_system_objects = :iso
             , notes                  = :notes
             , is_active              = :active
             , updated_at             = NOW()
         WHERE id = :id
    """, {"id": cfg_id, "n": config_name.strip(),
          "dbf": (database_filter or "").strip() or None,
          "scf": (schema_filter or "").strip() or None,
          "tbf": (table_filter or "").strip() or None,
          "iv": bool(include_views),
          "iso": bool(include_system_objects),
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})
    
def fetch_dw_catalog_config_by_id(cfg_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.config_name
             , cfg.database_filter
             , cfg.schema_filter
             , cfg.table_filter
             , cfg.include_views
             , cfg.include_system_objects
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
             , cfg.last_test_status
             , cfg.last_tested_at
             , cfg.last_test_notes
        FROM   config.dw_catalog_config cfg
        WHERE  cfg.id = :id
    """, {"id": cfg_id})
    return dict(rows[0]._mapping) if rows else None

def set_dw_catalog_last_test_result(cfg_id: int
                                  , status: str | None
                                  , notes: str | None) -> None:
    exec_tx("""
        UPDATE config.dw_catalog_config
           SET last_test_status = :status
             , last_tested_at   = NOW()
             , last_test_notes  = :notes
             , updated_at       = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "status": (status or "").strip() or None,
          "notes": (notes or "").strip() or None})


def clear_dw_catalog_last_test_result(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dw_catalog_config
           SET last_test_status = NULL
             , last_tested_at   = NULL
             , last_test_notes  = NULL
             , updated_at       = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

# -----------------------------------------    
# DW AI Config Handlers -------------------
# -----------------------------------------

def deactivate_dw_ai_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dw_ai_config
           SET is_active  = false
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

def reactivate_dw_ai_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dw_ai_config
           SET is_active  = true
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

def fetch_dw_ai_configs(conn_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.config_name
             , cfg.analysis_type
             , cfg.model_provider
             , cfg.model_name
             , cfg.model_version
             , cfg.temperature
             , cfg.max_tokens
             , cfg.top_p
             , cfg.frequency_penalty
             , cfg.presence_penalty
             , cfg.runner_concurrency
             , cfg.propagation_mode
             , cfg.overwrite_policy
             , cfg.confidence_threshold
             , cfg.respect_human_locks
             , cfg.model_profile
             , cfg.prompt_pack
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
        FROM   config.dw_ai_config cfg
        WHERE  cfg.connection_id = :cid
        ORDER  BY cfg.is_active DESC
             ,   cfg.analysis_type
             ,   cfg.updated_at DESC NULLS LAST
    """, {"cid": conn_id})
    return [dict(r._mapping) for r in rows]

def insert_dw_ai_config(conn_id: int
                      , config_name: str
                      , analysis_type: str
                      , model_provider: str | None
                      , model_name: str | None
                      , model_version: str | None
                      , temperature: float | None
                      , max_tokens: int | None
                      , top_p: float | None
                      , frequency_penalty: float | None
                      , presence_penalty: float | None
                      , runner_concurrency: int | None
                      , propagation_mode: str | None
                      , overwrite_policy: str | None
                      , confidence_threshold: float | None
                      , respect_human_locks: bool
                      , model_profile: str | None
                      , prompt_pack: str | None
                      , notes: str | None
                      , is_active: bool) -> None:
    exec_tx("""
        INSERT INTO config.dw_ai_config
        ( connection_id
        , config_name
        , analysis_type
        , model_provider
        , model_name
        , model_version
        , temperature
        , max_tokens
        , top_p
        , frequency_penalty
        , presence_penalty
        , runner_concurrency
        , propagation_mode
        , overwrite_policy
        , confidence_threshold
        , respect_human_locks
        , model_profile
        , prompt_pack
        , notes
        , is_active
        )
        VALUES
        ( :cid
        , :n
        , :atype
        , :mp
        , :mn
        , :mv
        , :temp
        , :maxtok
        , :topp
        , :fpen
        , :ppen
        , :conc
        , :prop
        , :op
        , :conf
        , :locks
        , :profile
        , :ppack
        , :notes
        , :active
        )
    """, {"cid": conn_id,
          "n": config_name.strip(),
          "atype": analysis_type.strip(),
          "mp": (model_provider or "").strip() or None,
          "mn": (model_name or "").strip() or None,
          "mv": (model_version or "").strip() or None,
          "temp": temperature,
          "maxtok": max_tokens,
          "topp": top_p,
          "fpen": frequency_penalty,
          "ppen": presence_penalty,
          "conc": runner_concurrency,
          "prop": (propagation_mode or "").strip() or None,
          "op": (overwrite_policy or "").strip() or None,
          "conf": confidence_threshold,
          "locks": bool(respect_human_locks),
          "profile": (model_profile or "").strip() or None,
          "ppack": (prompt_pack or "").strip() or None,
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})


def update_dw_ai_config(cfg_id: int
                      , config_name: str
                      , analysis_type: str
                      , model_provider: str | None
                      , model_name: str | None
                      , model_version: str | None
                      , temperature: float | None
                      , max_tokens: int | None
                      , top_p: float | None
                      , frequency_penalty: float | None
                      , presence_penalty: float | None
                      , runner_concurrency: int | None
                      , propagation_mode: str | None
                      , overwrite_policy: str | None
                      , confidence_threshold: float | None
                      , respect_human_locks: bool
                      , model_profile: str | None
                      , prompt_pack: str | None
                      , notes: str | None
                      , is_active: bool) -> None:
    exec_tx("""
        UPDATE config.dw_ai_config
           SET config_name         = :n
             , analysis_type       = :atype
             , model_provider      = :mp
             , model_name          = :mn
             , model_version       = :mv
             , temperature         = :temp
             , max_tokens          = :maxtok
             , top_p               = :topp
             , frequency_penalty   = :fpen
             , presence_penalty    = :ppen
             , runner_concurrency  = :conc
             , propagation_mode    = :prop
             , overwrite_policy    = :op
             , confidence_threshold= :conf
             , respect_human_locks = :locks
             , model_profile       = :profile
             , prompt_pack         = :ppack
             , notes               = :notes
             , is_active           = :active
             , updated_at          = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "n": config_name.strip(),
          "atype": analysis_type.strip(),
          "mp": (model_provider or "").strip() or None,
          "mn": (model_name or "").strip() or None,
          "mv": (model_version or "").strip() or None,
          "temp": temperature,
          "maxtok": max_tokens,
          "topp": top_p,
          "fpen": frequency_penalty,
          "ppen": presence_penalty,
          "conc": runner_concurrency,
          "prop": (propagation_mode or "").strip() or None,
          "op": (overwrite_policy or "").strip() or None,
          "conf": confidence_threshold,
          "locks": bool(respect_human_locks),
          "profile": (model_profile or "").strip() or None,
          "ppack": (prompt_pack or "").strip() or None,
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})

def fetch_dw_ai_config_by_id(cfg_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.config_name
             , cfg.analysis_type
             , cfg.model_provider
             , cfg.model_name
             , cfg.model_version
             , cfg.temperature
             , cfg.max_tokens
             , cfg.top_p
             , cfg.frequency_penalty
             , cfg.presence_penalty
             , cfg.runner_concurrency
             , cfg.propagation_mode
             , cfg.overwrite_policy
             , cfg.confidence_threshold
             , cfg.respect_human_locks
             , cfg.model_profile
             , cfg.prompt_pack
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
        FROM   config.dw_ai_config cfg
        WHERE  cfg.id = :id
    """, {"id": cfg_id})
    return dict(rows[0]._mapping) if rows else None

# -----------------------------
# PBI CATALOG CONFIG HELPERS
# -----------------------------

def fetch_pbi_catalog_configs(conn_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.workspace_filter
             , cfg.model_filter
             , cfg.table_filter
             , cfg.include_tmdl
             , cfg.include_model_bim
             , cfg.respect_perspectives
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
             , cfg.last_test_status
             , cfg.last_tested_at
             , cfg.last_test_notes
        FROM   config.pbi_catalog_config cfg
        WHERE  cfg.connection_id = :cid
        ORDER  BY cfg.is_active DESC
             ,   cfg.updated_at DESC NULLS LAST
    """, {"cid": conn_id})
    return [dict(r._mapping) for r in rows]


def fetch_pbi_catalog_config_by_id(cfg_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.workspace_filter
             , cfg.model_filter
             , cfg.table_filter
             , cfg.include_tmdl
             , cfg.include_model_bim
             , cfg.respect_perspectives
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
             , cfg.last_test_status
             , cfg.last_tested_at
             , cfg.last_test_notes
        FROM   config.pbi_catalog_config cfg
        WHERE  cfg.id = :id
    """, {"id": cfg_id})
    return dict(rows[0]._mapping) if rows else None


def insert_pbi_catalog_config(conn_id: int
                            , config_name: str
                            , workspace_filter: str | None
                            , model_filter: str | None
                            , table_filter: str | None
                            , include_tmdl: bool | None = None
                            , include_model_bim: bool | None = None
                            , respect_perspectives: bool | None = None
                            , notes: str | None = None
                            , is_active: bool | None = None) -> None:
    exec_tx("""
        INSERT INTO config.pbi_catalog_config
        ( connection_id
        , config_name
        , workspace_filter
        , model_filter
        , table_filter
        , include_tmdl
        , include_model_bim
        , respect_perspectives
        , notes
        , is_active
        )
        VALUES
        ( :cid
        , :n
        , :wsf
        , :mf
        , :tf
        , :tmdl
        , :bim
        , :rsp
        , :notes
        , :active
        )
    """, {"cid": conn_id,
          "n": config_name.strip(),
          "wsf": (workspace_filter or "").strip() or None,
          "mf": (model_filter or "").strip() or None,
          "tf": (table_filter or "").strip() or None,
          # defaults conform DDL (NOT NULL + DEFAULT):
          "tmdl": True if include_tmdl is None else bool(include_tmdl),
          "bim": False if include_model_bim is None else bool(include_model_bim),
          "rsp": True if respect_perspectives is None else bool(respect_perspectives),
          "notes": (notes or "").strip() or None,
          "active": True if is_active is None else bool(is_active)})


def update_pbi_catalog_config(cfg_id: int
                            , config_name: str
                            , workspace_filter: str | None
                            , model_filter: str | None
                            , table_filter: str | None
                            , include_tmdl: bool
                            , include_model_bim: bool
                            , respect_perspectives: bool
                            , notes: str | None
                            , is_active: bool) -> None:
    exec_tx("""
        UPDATE config.pbi_catalog_config
           SET config_name          = :n
             , workspace_filter     = :wsf
             , model_filter         = :mf
             , table_filter         = :tf
             , include_tmdl         = :tmdl
             , include_model_bim    = :bim
             , respect_perspectives = :rsp
             , notes                = :notes
             , is_active            = :active
             , updated_at           = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "n": config_name.strip(),
          "wsf": (workspace_filter or "").strip() or None,
          "mf": (model_filter or "").strip() or None,
          "tf": (table_filter or "").strip() or None,
          "tmdl": bool(include_tmdl),
          "bim": bool(include_model_bim),
          "rsp": bool(respect_perspectives),
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})


def deactivate_pbi_catalog_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.pbi_catalog_config
           SET is_active  = false
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})


def reactivate_pbi_catalog_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.pbi_catalog_config
           SET is_active  = true
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

def set_pbi_catalog_last_test_result(cfg_id: int
                                   , status: str | None
                                   , notes: str | None) -> None:
    exec_tx("""
        UPDATE config.pbi_catalog_config
           SET last_test_status = :status
             , last_tested_at   = NOW()
             , last_test_notes  = :notes
             , updated_at       = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "status": (status or "").strip() or None,
          "notes": (notes or "").strip() or None})


def clear_pbi_catalog_last_test_result(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.pbi_catalog_config
           SET last_test_status = NULL
             , last_tested_at   = NULL
             , last_test_notes  = NULL
             , updated_at       = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

# -----------------------------
# PBI AI CONFIG HELPERS
# -----------------------------

def fetch_pbi_ai_configs(conn_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.analysis_type
             , cfg.model_provider
             , cfg.model_name
             , cfg.model_version
             , cfg.temperature
             , cfg.max_tokens
             , cfg.top_p
             , cfg.frequency_penalty
             , cfg.presence_penalty
             , cfg.workspace_filter
             , cfg.model_filter
             , cfg.table_filter
             , cfg.include_tmdl
             , cfg.include_model_bim
             , cfg.respect_perspectives
             , cfg.runner_concurrency
             , cfg.propagation_mode
             , cfg.overwrite_policy
             , cfg.confidence_threshold
             , cfg.respect_human_locks
             , cfg.model_profile
             , cfg.prompt_pack
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
        FROM   config.pbi_ai_config cfg
        WHERE  cfg.connection_id = :cid
        ORDER  BY cfg.is_active DESC
             ,   cfg.analysis_type
             ,   cfg.updated_at DESC NULLS LAST
    """, {"cid": conn_id})
    return [dict(r._mapping) for r in rows]


def fetch_pbi_ai_config_by_id(cfg_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.analysis_type
             , cfg.model_provider
             , cfg.model_name
             , cfg.model_version
             , cfg.temperature
             , cfg.max_tokens
             , cfg.top_p
             , cfg.frequency_penalty
             , cfg.presence_penalty
             , cfg.workspace_filter
             , cfg.model_filter
             , cfg.table_filter
             , cfg.include_tmdl
             , cfg.include_model_bim
             , cfg.respect_perspectives
             , cfg.runner_concurrency
             , cfg.propagation_mode
             , cfg.overwrite_policy
             , cfg.confidence_threshold
             , cfg.respect_human_locks
             , cfg.model_profile
             , cfg.prompt_pack
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
        FROM   config.pbi_ai_config cfg
        WHERE  cfg.id = :id
    """, {"id": cfg_id})
    return dict(rows[0]._mapping) if rows else None


def insert_pbi_ai_config(conn_id: int
                       , config_name: str
                       , analysis_type: str
                       , model_provider: str | None
                       , model_name: str
                       , model_version: str | None
                       , temperature: float | None = None
                       , max_tokens: int | None = None
                       , top_p: float | None = None
                       , frequency_penalty: float | None = None
                       , presence_penalty: float | None = None
                       , workspace_filter: str | None = None
                       , model_filter: str | None = None
                       , table_filter: str | None = None
                       , include_tmdl: bool | None = None
                       , include_model_bim: bool | None = None
                       , respect_perspectives: bool | None = None
                       , runner_concurrency: int | None = None
                       , propagation_mode: str | None = None
                       , overwrite_policy: str | None = None
                       , confidence_threshold: float | None = None
                       , respect_human_locks: bool | None = None
                       , model_profile: str | None = None
                       , prompt_pack: str | None = None
                       , notes: str | None = None
                       , is_active: bool | None = None) -> None:
    exec_tx("""
        INSERT INTO config.pbi_ai_config
        ( connection_id
        , config_name
        , analysis_type
        , model_provider
        , model_name
        , model_version
        , temperature
        , max_tokens
        , top_p
        , frequency_penalty
        , presence_penalty
        , workspace_filter
        , model_filter
        , table_filter
        , include_tmdl
        , include_model_bim
        , respect_perspectives
        , runner_concurrency
        , propagation_mode
        , overwrite_policy
        , confidence_threshold
        , respect_human_locks
        , model_profile
        , prompt_pack
        , notes
        , is_active
        )
        VALUES
        ( :cid
        , :n
        , :atype
        , :mp
        , :mn
        , :mv
        , :temp
        , :maxtok
        , :topp
        , :fpen
        , :ppen
        , :wsf
        , :mf
        , :tf
        , :tmdl
        , :bim
        , :rsp
        , :conc
        , :prop
        , :op
        , :conf
        , :locks
        , :profile
        , :ppack
        , :notes
        , :active
        )
    """, {"cid": conn_id,
          "n": config_name.strip(),
          "atype": analysis_type.strip(),
          "mp": (model_provider or "openai").strip(),
          "mn": model_name.strip(),
          "mv": (model_version or "").strip() or None,
          # numeriek (laat None toe waar DDL NULL toestaat; anders defaulten):
          "temp": 0.0 if temperature is None else float(temperature),
          "maxtok": 2048 if max_tokens is None else int(max_tokens),
          "topp": 1.0 if top_p is None else float(top_p),
          "fpen": 0.0 if frequency_penalty is None else float(frequency_penalty),
          "ppen": 0.0 if presence_penalty is None else float(presence_penalty),
          "wsf": (workspace_filter or "").strip() or None,
          "mf": (model_filter or "").strip() or None,
          "tf": (table_filter or "").strip() or None,
          "tmdl": True if include_tmdl is None else bool(include_tmdl),
          "bim": False if include_model_bim is None else bool(include_model_bim),
          "rsp": True if respect_perspectives is None else bool(respect_perspectives),
          "conc": 2 if runner_concurrency is None else int(runner_concurrency),
          "prop": (propagation_mode or "auto").strip(),
          "op": (overwrite_policy or "fill_empty").strip(),
          # NOT NULL in DDL -> altijd defaulten als None:
          "conf": 0.700 if confidence_threshold is None else float(confidence_threshold),
          "locks": True if respect_human_locks is None else bool(respect_human_locks),
          "profile": (model_profile or "").strip() or None,
          "ppack": (prompt_pack or "").strip() or None,
          "notes": (notes or "").strip() or None,
          "active": True if is_active is None else bool(is_active)})


def update_pbi_ai_config(cfg_id: int
                       , config_name: str
                       , analysis_type: str
                       , model_provider: str
                       , model_name: str
                       , model_version: str | None
                       , temperature: float | None
                       , max_tokens: int | None
                       , top_p: float | None
                       , frequency_penalty: float | None
                       , presence_penalty: float | None
                       , workspace_filter: str | None
                       , model_filter: str | None
                       , table_filter: str | None
                       , include_tmdl: bool
                       , include_model_bim: bool
                       , respect_perspectives: bool
                       , runner_concurrency: int
                       , propagation_mode: str
                       , overwrite_policy: str
                       , confidence_threshold: float
                       , respect_human_locks: bool
                       , model_profile: str | None
                       , prompt_pack: str | None
                       , notes: str | None
                       , is_active: bool) -> None:
    exec_tx("""
        UPDATE config.pbi_ai_config
           SET config_name         = :n
             , analysis_type       = :atype
             , model_provider      = :mp
             , model_name          = :mn
             , model_version       = :mv
             , temperature         = :temp
             , max_tokens          = :maxtok
             , top_p               = :topp
             , frequency_penalty   = :fpen
             , presence_penalty    = :ppen
             , workspace_filter    = :wsf
             , model_filter        = :mf
             , table_filter        = :tf
             , include_tmdl        = :tmdl
             , include_model_bim   = :bim
             , respect_perspectives= :rsp
             , runner_concurrency  = :conc
             , propagation_mode    = :prop
             , overwrite_policy    = :op
             , confidence_threshold= :conf
             , respect_human_locks = :locks
             , model_profile       = :profile
             , prompt_pack         = :ppack
             , notes               = :notes
             , is_active           = :active
             , updated_at          = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "n": config_name.strip(),
          "atype": analysis_type.strip(),
          "mp": model_provider.strip(),
          "mn": model_name.strip(),
          "mv": (model_version or "").strip() or None,
          "temp": temperature if temperature is None else float(temperature),
          "maxtok": max_tokens if max_tokens is None else int(max_tokens),
          "topp": top_p if top_p is None else float(top_p),
          "fpen": frequency_penalty if frequency_penalty is None else float(frequency_penalty),
          "ppen": presence_penalty if presence_penalty is None else float(presence_penalty),
          "wsf": (workspace_filter or "").strip() or None,
          "mf": (model_filter or "").strip() or None,
          "tf": (table_filter or "").strip() or None,
          "tmdl": bool(include_tmdl),
          "bim": bool(include_model_bim),
          "rsp": bool(respect_perspectives),
          "conc": int(runner_concurrency),
          "prop": propagation_mode.strip(),
          "op": overwrite_policy.strip(),
          "conf": float(confidence_threshold),
          "locks": bool(respect_human_locks),
          "profile": (model_profile or "").strip() or None,
          "ppack": (prompt_pack or "").strip() or None,
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})


def deactivate_pbi_ai_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.pbi_ai_config
           SET is_active  = false
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})


def reactivate_pbi_ai_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.pbi_ai_config
           SET is_active  = true
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})

# -----------------------------
# DL CATALOG CONFIG HELPERS
# -----------------------------

def fetch_dl_catalog_configs(conn_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.path_filter
             , cfg.format_whitelist
             , cfg.partition_filter
             , cfg.include_hidden_files
             , cfg.infer_schema
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
             , cfg.last_test_status
             , cfg.last_tested_at
             , cfg.last_test_notes
        FROM   config.dl_catalog_config cfg
        WHERE  cfg.connection_id = :cid
        ORDER  BY cfg.is_active DESC
             ,   cfg.updated_at DESC NULLS LAST
    """, {"cid": conn_id})
    return [dict(r._mapping) for r in rows]


def fetch_dl_catalog_config_by_id(cfg_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.path_filter
             , cfg.format_whitelist
             , cfg.partition_filter
             , cfg.include_hidden_files
             , cfg.infer_schema
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
             , cfg.last_test_status
             , cfg.last_tested_at
             , cfg.last_test_notes
        FROM   config.dl_catalog_config cfg
        WHERE  cfg.id = :id
    """, {"id": cfg_id})
    return dict(rows[0]._mapping) if rows else None


def insert_dl_catalog_config(conn_id: int
                           , config_name: str
                           , path_filter: str | None
                           , format_whitelist: str | None
                           , partition_filter: str | None
                           , include_hidden_files: bool | None = None
                           , infer_schema: bool | None = None
                           , notes: str | None = None
                           , is_active: bool | None = None) -> None:
    exec_tx("""
        INSERT INTO config.dl_catalog_config
        ( connection_id
        , config_name
        , path_filter
        , format_whitelist
        , partition_filter
        , include_hidden_files
        , infer_schema
        , notes
        , is_active
        )
        VALUES
        ( :cid
        , :n
        , :pf
        , :fw
        , :partf
        , :ihf
        , :infer
        , :notes
        , :active
        )
    """, {"cid": conn_id,
          "n": config_name.strip(),
          "pf": (path_filter or "").strip() or None,
          "fw": (format_whitelist or "").strip() or None,
          "partf": (partition_filter or "").strip() or None,
          "ihf": False if include_hidden_files is None else bool(include_hidden_files),
          "infer": True if infer_schema is None else bool(infer_schema),
          "notes": (notes or "").strip() or None,
          "active": True if is_active is None else bool(is_active)})


def update_dl_catalog_config(cfg_id: int
                           , config_name: str
                           , path_filter: str | None
                           , format_whitelist: str | None
                           , partition_filter: str | None
                           , include_hidden_files: bool
                           , infer_schema: bool
                           , notes: str | None
                           , is_active: bool) -> None:
    exec_tx("""
        UPDATE config.dl_catalog_config
           SET config_name         = :n
             , path_filter         = :pf
             , format_whitelist    = :fw
             , partition_filter    = :partf
             , include_hidden_files= :ihf
             , infer_schema        = :infer
             , notes               = :notes
             , is_active           = :active
             , updated_at          = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "n": config_name.strip(),
          "pf": (path_filter or "").strip() or None,
          "fw": (format_whitelist or "").strip() or None,
          "partf": (partition_filter or "").strip() or None,
          "ihf": bool(include_hidden_files),
          "infer": bool(infer_schema),
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})


def deactivate_dl_catalog_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dl_catalog_config
           SET is_active  = false
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})


def reactivate_dl_catalog_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dl_catalog_config
           SET is_active  = true
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})


def set_dl_catalog_last_test_result(cfg_id: int
                                  , status: str | None
                                  , notes: str | None) -> None:
    exec_tx("""
        UPDATE config.dl_catalog_config
           SET last_test_status = :status
             , last_tested_at   = NOW()
             , last_test_notes  = :notes
             , updated_at       = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "status": (status or "").strip() or None,
          "notes": (notes or "").strip() or None})


def clear_dl_catalog_last_test_result(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dl_catalog_config
           SET last_test_status = NULL
             , last_tested_at   = NULL
             , last_test_notes  = NULL
             , updated_at       = NOW()
         WHERE id = :id
    """, {"id": cfg_id})


# -----------------------------
# DL AI CONFIG HELPERS
# -----------------------------

def fetch_dl_ai_configs(conn_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.analysis_type
             , cfg.model_provider
             , cfg.model_name
             , cfg.model_version
             , cfg.temperature
             , cfg.max_tokens
             , cfg.top_p
             , cfg.frequency_penalty
             , cfg.presence_penalty
             , cfg.path_filter
             , cfg.format_whitelist
             , cfg.partition_filter
             , cfg.include_hidden_files
             , cfg.infer_schema
             , cfg.runner_concurrency
             , cfg.propagation_mode
             , cfg.overwrite_policy
             , cfg.confidence_threshold
             , cfg.respect_human_locks
             , cfg.model_profile
             , cfg.prompt_pack
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
        FROM   config.dl_ai_config cfg
        WHERE  cfg.connection_id = :cid
        ORDER  BY cfg.is_active DESC
             ,   cfg.analysis_type
             ,   cfg.updated_at DESC NULLS LAST
    """, {"cid": conn_id})
    return [dict(r._mapping) for r in rows]


def fetch_dl_ai_config_by_id(cfg_id: int):
    rows = q_all("""
        SELECT cfg.id
             , cfg.connection_id
             , cfg.config_name
             , cfg.analysis_type
             , cfg.model_provider
             , cfg.model_name
             , cfg.model_version
             , cfg.temperature
             , cfg.max_tokens
             , cfg.top_p
             , cfg.frequency_penalty
             , cfg.presence_penalty
             , cfg.path_filter
             , cfg.format_whitelist
             , cfg.partition_filter
             , cfg.include_hidden_files
             , cfg.infer_schema
             , cfg.runner_concurrency
             , cfg.propagation_mode
             , cfg.overwrite_policy
             , cfg.confidence_threshold
             , cfg.respect_human_locks
             , cfg.model_profile
             , cfg.prompt_pack
             , cfg.notes
             , cfg.is_active
             , cfg.updated_at
        FROM   config.dl_ai_config cfg
        WHERE  cfg.id = :id
    """, {"id": cfg_id})
    return dict(rows[0]._mapping) if rows else None


def insert_dl_ai_config(conn_id: int
                      , config_name: str
                      , analysis_type: str
                      , model_provider: str | None
                      , model_name: str
                      , model_version: str | None
                      , temperature: float | None = None
                      , max_tokens: int | None = None
                      , top_p: float | None = None
                      , frequency_penalty: float | None = None
                      , presence_penalty: float | None = None
                      , path_filter: str | None = None
                      , format_whitelist: str | None = None
                      , partition_filter: str | None = None
                      , include_hidden_files: bool | None = None
                      , infer_schema: bool | None = None
                      , runner_concurrency: int | None = None
                      , propagation_mode: str | None = None
                      , overwrite_policy: str | None = None
                      , confidence_threshold: float | None = None
                      , respect_human_locks: bool | None = None
                      , model_profile: str | None = None
                      , prompt_pack: str | None = None
                      , notes: str | None = None
                      , is_active: bool | None = None) -> None:
    exec_tx("""
        INSERT INTO config.dl_ai_config
        ( connection_id
        , config_name
        , analysis_type
        , model_provider
        , model_name
        , model_version
        , temperature
        , max_tokens
        , top_p
        , frequency_penalty
        , presence_penalty
        , path_filter
        , format_whitelist
        , partition_filter
        , include_hidden_files
        , infer_schema
        , runner_concurrency
        , propagation_mode
        , overwrite_policy
        , confidence_threshold
        , respect_human_locks
        , model_profile
        , prompt_pack
        , notes
        , is_active
        )
        VALUES
        ( :cid
        , :n
        , :atype
        , :mp
        , :mn
        , :mv
        , :temp
        , :maxtok
        , :topp
        , :fpen
        , :ppen
        , :pf
        , :fw
        , :partf
        , :ihf
        , :infer
        , :conc
        , :prop
        , :op
        , :conf
        , :locks
        , :profile
        , :ppack
        , :notes
        , :active
        )
    """, {"cid": conn_id,
          "n": config_name.strip(),
          "atype": analysis_type.strip(),
          "mp": (model_provider or "openai").strip(),
          "mn": model_name.strip(),
          "mv": (model_version or "").strip() or None,
          "temp": 0.0 if temperature is None else float(temperature),
          "maxtok": 2048 if max_tokens is None else int(max_tokens),
          "topp": 1.0 if top_p is None else float(top_p),
          "fpen": 0.0 if frequency_penalty is None else float(frequency_penalty),
          "ppen": 0.0 if presence_penalty is None else float(presence_penalty),
          "pf": (path_filter or "").strip() or None,
          "fw": (format_whitelist or "").strip() or None,
          "partf": (partition_filter or "").strip() or None,
          "ihf": False if include_hidden_files is None else bool(include_hidden_files),
          "infer": True if infer_schema is None else bool(infer_schema),
          "conc": 2 if runner_concurrency is None else int(runner_concurrency),
          "prop": (propagation_mode or "auto").strip(),
          "op": (overwrite_policy or "fill_empty").strip(),
          "conf": 0.700 if confidence_threshold is None else float(confidence_threshold),
          "locks": True if respect_human_locks is None else bool(respect_human_locks),
          "profile": (model_profile or "").strip() or None,
          "ppack": (prompt_pack or "").strip() or None,
          "notes": (notes or "").strip() or None,
          "active": True if is_active is None else bool(is_active)})


def update_dl_ai_config(cfg_id: int
                      , config_name: str
                      , analysis_type: str
                      , model_provider: str
                      , model_name: str
                      , model_version: str | None
                      , temperature: float | None
                      , max_tokens: int | None
                      , top_p: float | None
                      , frequency_penalty: float | None
                      , presence_penalty: float | None
                      , path_filter: str | None
                      , format_whitelist: str | None
                      , partition_filter: str | None
                      , include_hidden_files: bool
                      , infer_schema: bool
                      , runner_concurrency: int
                      , propagation_mode: str
                      , overwrite_policy: str
                      , confidence_threshold: float
                      , respect_human_locks: bool
                      , model_profile: str | None
                      , prompt_pack: str | None
                      , notes: str | None
                      , is_active: bool) -> None:
    exec_tx("""
        UPDATE config.dl_ai_config
           SET config_name         = :n
             , analysis_type       = :atype
             , model_provider      = :mp
             , model_name          = :mn
             , model_version       = :mv
             , temperature         = :temp
             , max_tokens          = :maxtok
             , top_p               = :topp
             , frequency_penalty   = :fpen
             , presence_penalty    = :ppen
             , path_filter         = :pf
             , format_whitelist    = :fw
             , partition_filter    = :partf
             , include_hidden_files= :ihf
             , infer_schema        = :infer
             , runner_concurrency  = :conc
             , propagation_mode    = :prop
             , overwrite_policy    = :op
             , confidence_threshold= :conf
             , respect_human_locks = :locks
             , model_profile       = :profile
             , prompt_pack         = :ppack
             , notes               = :notes
             , is_active           = :active
             , updated_at          = NOW()
         WHERE id = :id
    """, {"id": cfg_id,
          "n": config_name.strip(),
          "atype": analysis_type.strip(),
          "mp": model_provider.strip(),
          "mn": model_name.strip(),
          "mv": (model_version or "").strip() or None,
          "temp": temperature if temperature is None else float(temperature),
          "maxtok": max_tokens if max_tokens is None else int(max_tokens),
          "topp": top_p if top_p is None else float(top_p),
          "fpen": frequency_penalty if frequency_penalty is None else float(frequency_penalty),
          "ppen": presence_penalty if presence_penalty is None else float(presence_penalty),
          "pf": (path_filter or "").strip() or None,
          "fw": (format_whitelist or "").strip() or None,
          "partf": (partition_filter or "").strip() or None,
          "ihf": bool(include_hidden_files),
          "infer": bool(infer_schema),
          "conc": int(runner_concurrency),
          "prop": propagation_mode.strip(),
          "op": overwrite_policy.strip(),
          "conf": float(confidence_threshold),
          "locks": bool(respect_human_locks),
          "profile": (model_profile or "").strip() or None,
          "ppack": (prompt_pack or "").strip() or None,
          "notes": (notes or "").strip() or None,
          "active": bool(is_active)})


def deactivate_dl_ai_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dl_ai_config
           SET is_active  = false
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})


def reactivate_dl_ai_config(cfg_id: int) -> None:
    exec_tx("""
        UPDATE config.dl_ai_config
           SET is_active  = true
             , updated_at = NOW()
         WHERE id = :id
    """, {"id": cfg_id})
