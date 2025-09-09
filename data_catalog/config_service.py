from typing import Callable, Any, Dict, List, Optional, Tuple,Literal
import streamlit as st
from data_catalog.connection_handler import (
    list_connections_df
)
from data_catalog.db import q_all, exec_tx
from inspect import signature

from data_catalog.config_crud import (
    # DW configs
    fetch_dw_catalog_configs,
    fetch_dw_catalog_config_by_id,
    insert_dw_catalog_config,
    update_dw_catalog_config,
    deactivate_dw_catalog_config,
    reactivate_dw_catalog_config,
    set_dw_catalog_last_test_result,
    clear_dw_catalog_last_test_result,

    # PBI configs
    fetch_pbi_catalog_configs,
    fetch_pbi_catalog_config_by_id,
    insert_pbi_catalog_config,
    update_pbi_catalog_config,
    deactivate_pbi_catalog_config,
    reactivate_pbi_catalog_config,
    set_pbi_catalog_last_test_result,
    clear_pbi_catalog_last_test_result,

    # DL configs
    fetch_dl_catalog_configs,
    fetch_dl_catalog_config_by_id,
    insert_dl_catalog_config,
    update_dl_catalog_config,
    deactivate_dl_catalog_config,
    reactivate_dl_catalog_config,
    set_dl_catalog_last_test_result,
    clear_dl_catalog_last_test_result,

    # AI-configs (DW/PBI/DL) – geen last_test_* kolommen
    fetch_dw_ai_configs,
    fetch_pbi_ai_configs,
    fetch_dl_ai_configs,
    fetch_dw_ai_config_by_id,
    fetch_pbi_ai_config_by_id,
    fetch_dl_ai_config_by_id,
    insert_pbi_ai_config,
    update_pbi_ai_config,
    insert_dl_ai_config,
    update_dl_ai_config,
    insert_dw_ai_config,
    update_dw_ai_config,
)

Family = Literal["catalog", "ai"]

def _table_for(family: Family, sc: str) -> str:
    sc = (sc or "").strip().lower()
    if family == "catalog":
        if sc == "dw":  return "config.dw_catalog_config"
        if sc == "pbi": return "config.pbi_catalog_config"
        if sc == "dl":  return "config.dl_catalog_config"
    elif family == "ai":
        if sc == "dw":  return "config.dw_ai_config"
        if sc == "pbi": return "config.pbi_ai_config"
        if sc == "dl":  return "config.dl_ai_config"
    raise ValueError(f"Unknown family/sc: {family}/{sc}")

def list_deactivated_configs(conn_id: int, family: Family, sc: str):
    table = _table_for(family, sc)
    sql = f"""
        SELECT id
             , config_name
             , is_active
             , updated_at
             , deleted_at
             , deleted_by
             , delete_reason
        FROM   {table}
        WHERE  connection_id = :cid
        AND    deleted_at IS NULL
        AND    is_active  = false
        ORDER  BY id
    """
    return q_all(sql, {"cid": int(conn_id)})

def soft_delete_config(cfg_id: int, family: Family, sc: str, *, user_email: str, reason: str):
    table = _table_for(family, sc)
    sql = f"""
        UPDATE {table}
        SET deleted_at    = CURRENT_TIMESTAMP
          , deleted_by    = :user
          , delete_reason = :reason
          , updated_at    = CURRENT_TIMESTAMP
        WHERE id         = :id
        AND   deleted_at IS NULL
        AND   is_active  = false
    """
    return exec_tx(sql, {"id": int(cfg_id), "user": user_email, "reason": reason})

CREATE_NEW_SENTINEL_ID = "__create_new_catalog_config__"
EDIT_SENTINEL_PREFIX = "__edit_catalog_config__:"


def render_active_connection_picker_stable(key_prefix: str = "catalog"):
    df = list_connections_df()  # bevat alleen deleted_at IS NULL
    df = df[df["is_active"] == True]  # noqa: E712
    if df.empty:
        st.info("No active connections available. Create and activate a connection first.")
        return None, None

    # 1) Stabiele opties = IDs
    df = df.sort_values(["connection_name", "id"], kind="mergesort")  # stabiele sort
    ids = df["id"].astype(int).tolist()

    by_id = {int(row["id"]): row for row in df.to_dict(orient="records")}

    def fmt_conn(conn_id: int) -> str:
        r = by_id.get(conn_id, {})
        sc = r.get("short_code") or ""
        ctype = r.get("connection_type") or ""
        return f"#{conn_id} — {r.get('connection_name', '')}  [{ctype}/{sc}] · 🟢 Active"

    # 2) Vorige keuze ophalen
    state_key = f"{key_prefix}_conn_id"
    sel_id = st.session_state.get(state_key, None)

    # 3) Index bepalen (fallback naar eerste)
    if sel_id in ids:
        index = ids.index(sel_id)
    else:
        index = 0
        sel_id = ids[0]

    # 4) Selectbox met opties=IDs
    chosen_id = st.selectbox(
        "Select an active main connection.",
        options=ids,
        index=index,
        format_func=fmt_conn,
        key=f"{key_prefix}_conn_selectbox",  # unieke UI-key
    )

    # 5) Session bijwerken + caption
    st.session_state[state_key] = chosen_id
    row = by_id[chosen_id]
    st.caption(f"**Selected:** {chosen_id} · {row['connection_name']}")
    st.divider()
    return chosen_id, row

def render_catalog_config_picker_stable(
    conn_id: int,
    short_code: str,
    key_prefix: str = "catalog",
):
    # Type router
    from config_crud import (
        fetch_dw_catalog_configs, fetch_pbi_catalog_configs, fetch_dl_catalog_configs,
    )
    sc = (short_code or "").strip().lower()
    fetch_all: Callable[[int], object]
    label = {"dw": "Data Warehouse", "dl": "Data Lake", "pbi": "Power BI"}.get(sc, sc.upper())
    if sc == "dw":
        fetch_all = fetch_dw_catalog_configs
    elif sc == "dl":
        fetch_all = fetch_dl_catalog_configs
    elif sc == "pbi":
        fetch_all = fetch_pbi_catalog_configs
    else:
        st.error(f"Unknown connection type (short_code): '{short_code}'. Expected: dw, dl of pbi.")
        return None, None

    st.subheader(f"Catalog configuration · {label}")

    # Ophalen en normaliseren
    def _to_records(data) -> list[dict]:
        try:
            if hasattr(data, "to_dict"):
                return data.to_dict(orient="records")  # pandas DF
        except Exception:
            pass
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data
        return []

    records = _to_records(fetch_all(conn_id))
    # Extra safety: filter lokaal op conn_id (als kolom bestaat)
    if records and "connection_id" in records[0]:
        records = [r for r in records if int(r.get("connection_id") or -1) == int(conn_id)]

    # Toggle actief (default AAN)
    active_key = f"{key_prefix}_cfg_active_only_{sc}"
    if st.toggle("Alleen actieve catalog-configs tonen", value=True, key=active_key):
        records = [r for r in records if bool(r.get("is_active"))]

    if not records:
        st.info("No (active) catalog-configurations for this connections available.")
        return None, None

    # Stabiele opties = IDs
    # Zorg dat 'id' integer is en sorteer stabiel
    for r in records:
        r["id"] = int(r["id"])
    records.sort(key=lambda r: (str(r.get("config_name") or r.get("name") or ""), r["id"]))
    ids = [r["id"] for r in records]
    by_id = {r["id"]: r for r in records}

    def fmt_cfg(cfg_id: int) -> str:
        r = by_id.get(cfg_id, {})
        status = "🟢" if r.get("is_active") else "🔴"
        name = r.get("config_name") or r.get("name") or f"Config {cfg_id}"
        sf = r.get("schema_filter") or "*"
        tf = r.get("table_filter") or "*"
        return f"{status} #{cfg_id} — {name} · filters: {sf}/{tf}"

    # Vorige keuze ophalen (per type eigen state key)
    state_key = f"{key_prefix}_cfg_id_{sc}"
    sel_id = st.session_state.get(state_key)

    # Index bepalen (fallback naar eerste of behoud als beschikbaar)
    if sel_id in ids:
        index = ids.index(sel_id)
    else:
        index = 0
        sel_id = ids[0]

    # Selectbox met opties = IDs
    chosen_id = st.selectbox(
        "Select a catalog-configuration",
        options=ids,
        index=index,
        format_func=fmt_cfg,
        key=f"{key_prefix}_cfg_selectbox_{sc}",  # unieke UI-key
    )
    st.session_state[state_key] = chosen_id
    chosen_cfg = by_id[chosen_id]

    st.caption(f"**Selected catalog-config:** {chosen_id} · {(chosen_cfg.get('config_name') or chosen_cfg.get('name') or '')}")

    with st.expander("Details of this catalog-config"):
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Status**:", "🟢 Active" if chosen_cfg.get("is_active") else "🔴 Inactive")
            st.write("**Schema filter**:", chosen_cfg.get("schema_filter", "—"))
            st.write("**Table filter**:", chosen_cfg.get("table_filter", "—"))
            st.write("**Include views**:", "Ja" if chosen_cfg.get("include_views") else "Nee")
            st.write("**Remarks**:", chosen_cfg.get("notes", "—"))
        with col2:
            st.write("**Target catalog DB**:", chosen_cfg.get("target_catalog_db", "—"))
            st.write("**Server name**:", chosen_cfg.get("server_name", "—"))
            st.write("**Database name**:", chosen_cfg.get("database_name", "—"))
            st.write("**Laatst getest**:", chosen_cfg.get("last_test_at", "—"))
            st.write("**Laatste testresultaat**:", chosen_cfg.get("last_test_result", "—"))
    st.divider()
    return chosen_id, chosen_cfg

def select_or_create_catalog_config(
    main_connection_id: str,
    *,
    fetch_configs: Callable[[str], List[Dict[str, Any]]],
    create_config: Callable[[str, str, Dict[str, Any]], Dict[str, Any]],
    render_picker: Callable[[List[Tuple[str, str]], Optional[str], Optional[str]], str],
    prompt_new_config: Callable[[str], Tuple[str, Dict[str, Any]]],
    preselected_config_id: Optional[str] = None,
    title: str = "Selecteer Catalog Config",
) -> Dict[str, Any]:
    """
    Toon een picker met bestaande catalog configs voor de meegegeven main connection.
    Bied tevens 'Nieuwe config...' aan. Bij aanmaken: doorloop prompt flow en maak config aan.
    Returns de gekozen of aangemaakte config (dict).

    Parameters:
      - main_connection_id: geselecteerde main connection ID
      - fetch_configs: fn(main_connection_id) -> list[dict] met minimaal 'id' en 'name'
      - create_config: fn(main_connection_id, name, settings) -> dict (aangemaakte config)
      - render_picker: fn(options, preselected_id, title) -> gekozen optie-id
          options: list[(id, label)]
      - prompt_new_config: fn(main_connection_id) -> (name, settings)
      - preselected_config_id: optioneel vooraf geselecteerde config-id
      - title: picker titel
    """
    # 1) Ophalen bestaande configs voor deze main connection
    configs = fetch_configs(main_connection_id) or []

    # 2) Optie-lijst opbouwen
    options: List[Tuple[str, str]] = [(c["id"], c.get("name") or c["id"]) for c in configs]
    options.append((CREATE_NEW_SENTINEL_ID, "➕ Nieuwe catalog config…"))

    # 3) Render picker
    chosen_id = render_picker(options, preselected_config_id, title)

    # 4) Create-flow indien gekozen
    if chosen_id == CREATE_NEW_SENTINEL_ID:
        name, settings = prompt_new_config(main_connection_id)
        new_cfg = create_config(main_connection_id, name, settings or {})
        return new_cfg

    # 5) Return de geselecteerde bestaande config
    by_id = {c["id"]: c for c in configs}
    if chosen_id not in by_id:
        raise ValueError(f"Onbekende catalog config id gekozen: {chosen_id}")
    return by_id[chosen_id]


# Optionele thin-wrapper die je huidige renderer hergebruikt
def render_catalog_config_picker_with_create(
    main_connection_id: str,
    *,
    fetch_configs: Callable[[str], List[Dict[str, Any]]],
    create_config: Callable[[str, str, Dict[str, Any]], Dict[str, Any]],
    prompt_new_config: Callable[[str], Tuple[str, Dict[str, Any]]],
    preselected_config_id: Optional[str] = None,
    title: str = "Selecteer Catalog Config",
):
    """
    Wrapper die bestaande render_catalog_config_picker_stable gebruikt als picker.
    """
    # Verwacht signatuur: render_catalog_config_picker_stable(options, preselected_id, title) -> selected_id
    def _picker(options, preselected_id, picker_title):
        return render_catalog_config_picker_stable(options, preselected_id, picker_title)

    return select_or_create_catalog_config(
        main_connection_id,
        fetch_configs=fetch_configs,
        create_config=create_config,
        render_picker=_picker,
        prompt_new_config=prompt_new_config,
        preselected_config_id=preselected_config_id,
        title=title,
    )

def select_create_or_edit_catalog_config(
    main_connection_id: str,
    short_code: str,
    *,
    fetch_configs: Callable[[str, str], List[Dict[str, Any]]],
    create_config: Callable[[str, str, str, Dict[str, Any]], Dict[str, Any]],
    update_config: Callable[[str, str, str, Dict[str, Any]], Dict[str, Any]],
    render_picker: Callable[[List[Tuple[str, str]], Optional[str], Optional[str]], str],
    prompt_new_config: Callable[[str, str], Tuple[str, Dict[str, Any]]],
    prompt_edit_config: Callable[[Dict[str, Any]], Tuple[Optional[str], Optional[Dict[str, Any]]]],
    preselected_config_id: Optional[str] = None,
    title: str = "Selecteer of bewerk Catalog Config",
) -> Dict[str, Any]:
    """
    - Haalt catalog-configs op per main_connection_id + short_code (dw/pbi/dl)
    - Picker met: bestaande configs, voor elk ook 'Bewerk …', plus 'Nieuwe …'
    - Retourneert geselecteerde, aangemaakte of bijgewerkte config
    """
    # 1) Ophalen
    configs = fetch_configs(main_connection_id, short_code) or []
    by_id = {c["id"]: c for c in configs}

    # 2) Opties
    options: List[Tuple[str, str]] = []
    for c in configs:
        name = c.get("name") or c["id"]
        options.append((c["id"], f"🗂️ {name}"))
        options.append((f"{EDIT_SENTINEL_PREFIX}{c['id']}", f"✏️ Bewerk ‘{name}’…"))
    options.append((CREATE_NEW_SENTINEL_ID, "➕ Nieuwe catalog config…"))

    chosen_id = render_picker(options, preselected_config_id, title)

    # 3) Aanmaken
    if chosen_id == CREATE_NEW_SENTINEL_ID:
        name, settings = prompt_new_config(main_connection_id, short_code)
        # >>> Guard: geannuleerd of geen submit
        if name is None and settings is None:
            # Niets doen; return bijv. eerste config of None
            # Kies wat jij wilt; hier kiezen we: gewoon opnieuw tonen (geen actie)
            return {}
        # >>> Guard: lege naam niet doorgeven
        if not name or not str(name).strip():
            # Laat create-adapter hier NIET meer crashen
            name = "Naamloze config"
        settings = settings or {}
        return create_config(main_connection_id, short_code, name, settings)

    # 4) Bewerken
    if chosen_id.startswith(EDIT_SENTINEL_PREFIX):
        target_id = chosen_id[len(EDIT_SENTINEL_PREFIX) :]
        if target_id not in by_id:
            raise ValueError(f"Onbekende catalog config id voor bewerken: {target_id}")
        cfg = by_id[target_id]
        new_name, new_settings = prompt_edit_config(cfg)
        if new_name is None and new_settings is None:
            return cfg  # niets gewijzigd
        patch: Dict[str, Any] = {}
        if new_name is not None:
            new_name = new_name.strip()
            if new_name:              # voorkom lege string in DB
                patch["name"] = new_name
        # settings mag None zijn (geen wijziging); alleen toevoegen als niet None
        if new_settings is not None:
            patch.update(new_settings)
        if not patch:
            return cfg
        return update_config(main_connection_id, short_code, target_id, patch)

    # 5) Selectie van bestaande
    if chosen_id not in by_id:
        raise ValueError(f"Onbekende catalog config id gekozen: {chosen_id}")
    return by_id[chosen_id]


# Wrapper: gebruikt bestaande stable picker + CRUD uit config_crud
def render_catalog_config_picker_with_create_or_edit(
    main_connection_id: str,
    short_code: str,
    *,
    fetch_configs: Callable[[str, str], List[Dict[str, Any]]],
    create_config: Callable[[str, str, str, Dict[str, Any]], Dict[str, Any]],
    update_config: Callable[[str, str, str, Dict[str, Any]], Dict[str, Any]],
    prompt_new_config: Callable[[str, str], Tuple[str, Dict[str, Any]]],
    prompt_edit_config: Callable[[Dict[str, Any]], Tuple[Optional[str], Optional[Dict[str, Any]]]],
    preselected_config_id: Optional[str] = None,
    title: str = "Selecteer of bewerk Catalog Config",
) -> Dict[str, Any]:
    def _picker(options, preselected_id, picker_title):
        return render_catalog_config_picker_stable(options, preselected_id, picker_title)

    return select_create_or_edit_catalog_config(
        main_connection_id,
        short_code,
        fetch_configs=fetch_configs,
        create_config=create_config,
        update_config=update_config,
        render_picker=_picker,
        prompt_new_config=prompt_new_config,
        prompt_edit_config=prompt_edit_config,
        preselected_config_id=preselected_config_id,
        title=title,
    )


# Optionele adapter naar config_crud op basis van short_code
# Verwachte functies in config_crud (pas aan indien namen afwijken):
# - list_<short>_catalog_configs(conn_id)
# - create_<short>_catalog_config(conn_id, name, settings)
# - update_<short>_catalog_config(conn_id, config_id, patch)
def make_crud_adapters_for_short_code(config_crud_module):
    def fetch_configs(conn_id: str, short_code: str):
        fn = getattr(config_crud_module, f"list_{short_code}_catalog_configs")
        return fn(conn_id)

    def create_config(conn_id: str, short_code: str, name: str, settings: Dict[str, Any]):
        fn = getattr(config_crud_module, f"create_{short_code}_catalog_config")
        return fn(conn_id, name, settings)

    def update_config(conn_id: str, short_code: str, config_id: str, patch: Dict[str, Any]):
        fn = getattr(config_crud_module, f"update_{short_code}_catalog_config")
        return fn(conn_id, config_id, patch)

    return fetch_configs, create_config, update_config

# --- Picker die precies doet wat select_* verwacht ---
def simple_options_picker(options: List[Tuple[str, str]],
                          preselected_id: Optional[str],
                          title: Optional[str]) -> str:
    # options: [(id, label), ...]
    ids = [opt_id for opt_id, _ in options]
    labels_by_id = {opt_id: label for opt_id, label in options}

    # bepaal index
    if preselected_id in ids:
        index = ids.index(preselected_id)
    else:
        index = 0

    # toon selectbox
    chosen_label = st.selectbox(
        title or "Kies een optie",
        options=[labels_by_id[i] for i in ids],
        index=index,
        key=f"catalog_cfg_picker_{title or ''}",
    )
    # map label -> id
    inv = {v: k for k, v in labels_by_id.items()}
    return inv[chosen_label]


# --- Adapters die aansluiten op je échte config_crud-functies ---

def _none_if_blank(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None

def _coerce_bool(val):
    # None blijft None; alleen als er iets is doorgegeven coercen we naar bool
    if val is None:
        return None
    return bool(val)

# Toegestane velden per type
_ALLOWED_KEYS = {
    "dw": {
        "name",
        "database_filter", "schema_filter", "table_filter",
        "include_views", "include_system_objects",
        "notes",
        "last_test_status", "last_test_notes",  # voor status updates
    },
    "pbi": {
        "name",
        "workspace_filter", "model_filter", "table_filter",
        "include_tmdl", "include_model_bim", "respect_perspectives",
        "notes",
        "last_test_status", "last_test_notes",
    },
    "dl": {
        "name",
        "path_filter", "format_whitelist", "partition_filter",
        "include_hidden_files", "infer_schema",
        "notes",
        "last_test_status", "last_test_notes",
    },
}

# Normaliseer alleen keys die in de patch zitten (geen impliciete defaults bij update)
def normalize_patch_for_type(short_code: str, patch: dict) -> dict:
    sc = (short_code or "").strip().lower()
    if sc not in _ALLOWED_KEYS:
        raise ValueError(f"Unknown short_code: {short_code}")

    allowed = _ALLOWED_KEYS[sc]
    norm: dict = {}

    for k, v in (patch or {}).items():
        if k not in allowed:
            # onbekende keys negeren we (of loggen naar wens)
            continue

        if k in ("name", "notes", "database_filter", "schema_filter", "table_filter",
                 "workspace_filter", "model_filter", "path_filter", "format_whitelist",
                 "partition_filter", "last_test_status", "last_test_notes"):
            norm[k] = _none_if_blank(v)

        elif k in ("include_views", "include_system_objects",
                   "include_tmdl", "include_model_bim", "respect_perspectives",
                   "include_hidden_files", "infer_schema"):
            bv = _coerce_bool(v)
            # Alleen toevoegen als expliciet in patch aanwezig (None betekent: niet wijzigen)
            if bv is not None:
                norm[k] = bv

        else:
            # fallback; zou niet mogen voorkomen door allowed-sets
            norm[k] = v

    return norm

from inspect import signature

def make_crud_adapters_matching_imports():
    # --- FETCH ---
    def fetch_configs(conn_id: int, short_code: str):
        sc = (short_code or "").strip().lower()
        if sc == "dw":
            return normalize_configs(sc, fetch_dw_catalog_configs(conn_id))
        if sc == "pbi":
            return normalize_configs(sc, fetch_pbi_catalog_configs(conn_id))
        if sc == "dl":
            return normalize_configs(sc, fetch_dl_catalog_configs(conn_id))
        raise ValueError(f"Unknown short_code: {short_code}")

    # --- CREATE ---
    def create_config(conn_id: int, short_code: str, name: str, settings: dict):
        sc = (short_code or "").strip().lower()
        if not name or not str(name).strip():
            name = "Naamloze config"  # fallback

        if sc == "dw":
            # Defaults & normalisatie
            database_filter    = settings.get("database_filter") or None
            schema_filter      = settings.get("schema_filter") or None
            table_filter       = settings.get("table_filter") or None
            include_views      = bool(settings.get("include_views", False))
            include_system_obj = bool(settings.get("include_system_objects", False))
            notes              = settings.get("notes") or None
            is_active          = bool(settings.get("is_active", True))

            # Fallback op lege naam
            if not name or not str(name).strip():
                name = "Naamloze config"

            res = None
            try:
                sig = signature(insert_dw_catalog_config)
                kw = {}
                # keyword-args op basis van param-namen
                if "conn_id" in sig.parameters:               kw["conn_id"] = conn_id
                if "connection_id" in sig.parameters:         kw["connection_id"] = conn_id
                if "name" in sig.parameters:                  kw["name"] = name
                if "config_name" in sig.parameters:           kw["config_name"] = name
                if "database_filter" in sig.parameters:       kw["database_filter"] = database_filter
                if "schema_filter" in sig.parameters:         kw["schema_filter"] = schema_filter
                if "table_filter" in sig.parameters:          kw["table_filter"] = table_filter
                if "include_views" in sig.parameters:         kw["include_views"] = include_views
                if "include_system_objects" in sig.parameters:kw["include_system_objects"] = include_system_obj
                if "notes" in sig.parameters:                 kw["notes"] = notes
                if "is_active" in sig.parameters:             kw["is_active"] = is_active

                if kw:
                    res = insert_dw_catalog_config(**kw)
                else:
                    # Positievolgorde fallback (meest gebruikelijk)
                    res = insert_dw_catalog_config(
                        conn_id,
                        name,
                        database_filter,
                        schema_filter,
                        table_filter,
                        include_views,
                        include_system_obj,
                        notes,
                        is_active,
                    )
            except TypeError:
                # Laatste fallback: dict-variant
                res = insert_dw_catalog_config(conn_id, name, {
                    "database_filter": database_filter,
                    "schema_filter": schema_filter,
                    "table_filter": table_filter,
                    "include_views": include_views,
                    "include_system_objects": include_system_obj,
                    "notes": notes,
                    "is_active": is_active,
                })

            # >>> Altijd via één pad teruggeven, met gegarandeerde 'id'
            return ensure_config_has_id_or_refetch(sc, conn_id, res, fetch_configs, prefer_name=name)


        if sc == "pbi":
            workspace_filter     = settings.get("workspace_filter")
            model_filter         = settings.get("model_filter")
            table_filter         = settings.get("table_filter")
            include_tmdl         = bool(settings.get("include_tmdl", True))
            include_model_bim    = bool(settings.get("include_model_bim", False))
            respect_perspectives = bool(settings.get("respect_perspectives", True))
            notes                = settings.get("notes")
            try:
                sig = signature(insert_pbi_catalog_config)
                if len(sig.parameters) >= 9:
                    res = insert_pbi_catalog_config(conn_id, name, workspace_filter, model_filter,
                                                    table_filter, include_tmdl, include_model_bim,
                                                    respect_perspectives, notes)
                else:
                    res = insert_pbi_catalog_config(conn_id, name, {
                        "workspace_filter": workspace_filter,
                        "model_filter": model_filter,
                        "table_filter": table_filter,
                        "include_tmdl": include_tmdl,
                        "include_model_bim": include_model_bim,
                        "respect_perspectives": respect_perspectives,
                        "notes": notes,
                    })
            except Exception:
                res = insert_pbi_catalog_config(conn_id, name, {
                    "workspace_filter": workspace_filter,
                    "model_filter": model_filter,
                    "table_filter": table_filter,
                    "include_tmdl": include_tmdl,
                    "include_model_bim": include_model_bim,
                    "respect_perspectives": respect_perspectives,
                    "notes": notes,
                })
            return ensure_config_has_id_or_refetch(sc, conn_id, res, fetch_configs, prefer_name=name)

        if sc == "dl":
            path_filter       = settings.get("path_filter")
            format_whitelist  = settings.get("format_whitelist")
            partition_filter  = settings.get("partition_filter")
            include_hidden    = bool(settings.get("include_hidden_files", False))
            infer_schema      = bool(settings.get("infer_schema", True))
            notes             = settings.get("notes")
            try:
                sig = signature(insert_dl_catalog_config)
                if len(sig.parameters) >= 8:
                    res = insert_dl_catalog_config(conn_id, name, path_filter, format_whitelist,
                                                   partition_filter, include_hidden, infer_schema, notes)
                else:
                    res = insert_dl_catalog_config(conn_id, name, {
                        "path_filter": path_filter,
                        "format_whitelist": format_whitelist,
                        "partition_filter": partition_filter,
                        "include_hidden_files": include_hidden,
                        "infer_schema": infer_schema,
                        "notes": notes,
                    })
            except Exception:
                res = insert_dl_catalog_config(conn_id, name, {
                    "path_filter": path_filter,
                    "format_whitelist": format_whitelist,
                    "partition_filter": partition_filter,
                    "include_hidden_files": include_hidden,
                    "infer_schema": infer_schema,
                    "notes": notes,
                })
            return ensure_config_has_id_or_refetch(sc, conn_id, res, fetch_configs, prefer_name=name)

        raise ValueError(f"Unknown short_code: {short_code}")

    # --- UPDATE ---
    def update_config(conn_id: int, short_code: str, config_id: int, patch: dict):
        sc = (short_code or "").strip().lower()
        # normaliseer PATCH (ui-vrij), zie eerdere normalize_patch_for_type(...)
        norm = normalize_patch_for_type(sc, patch)

        if sc == "dw":
            res = update_dw_catalog_config(conn_id, config_id, norm)
        elif sc == "pbi":
            res = update_pbi_catalog_config(conn_id, config_id, norm)
        elif sc == "dl":
            res = update_dl_catalog_config(conn_id, config_id, norm)
        else:
            raise ValueError(f"Unknown short_code: {short_code}")

        # return altijd de actuele, volledige config (met id)
        return ensure_config_has_id_or_refetch(sc, conn_id, res, fetch_configs, prefer_id=config_id)

    return fetch_configs, create_config, update_config


# --- Gebruik deze wrapper i.p.v. de huidige die stable picker doorgeeft ---
def render_catalog_config_picker_with_create_or_edit(
    main_connection_id: str,
    short_code: str,
    *,
    fetch_configs: Callable[[str, str], List[Dict[str, Any]]],
    create_config: Callable[[str, str, str, Dict[str, Any]], Dict[str, Any]],
    update_config: Callable[[str, str, str, Dict[str, Any]], Dict[str, Any]],
    prompt_new_config: Callable[[str, str], Tuple[str, Dict[str, Any]]],
    prompt_edit_config: Callable[[Dict[str, Any]], Tuple[Optional[str], Optional[Dict[str, Any]]]],
    preselected_config_id: Optional[str] = None,
    title: str = "Selecteer of bewerk Catalog Config",
) -> Dict[str, Any]:

    return select_create_or_edit_catalog_config(
        main_connection_id,
        short_code,
        fetch_configs=fetch_configs,
        create_config=create_config,
        update_config=update_config,
        render_picker=simple_options_picker,  # << hier de juiste picker!
        prompt_new_config=prompt_new_config,
        prompt_edit_config=prompt_edit_config,
        preselected_config_id=preselected_config_id,
        title=title,
    )

def validate_catalog_config_inputs(short_code: str, name: str | None, settings: dict) -> list[str]:
    errors: list[str] = []
    sc = (short_code or "").strip().lower()

    # Altijd verplicht
    if not name or not str(name).strip():
        errors.append("Naam is verplicht.")

    # Per type: booleans met NOT NULL moeten gedefinieerd zijn (als je posities doorgeeft)
    if sc == "pbi":
        for k in ("include_tmdl", "include_model_bim", "respect_perspectives"):
            if settings.get(k) is None:
                errors.append(f"{k} (bool) ontbreekt.")
    if sc == "dl":
        for k in ("include_hidden_files", "infer_schema"):
            if settings.get(k) is None:
                errors.append(f"{k} (bool) ontbreekt.")

    return errors

def _to_records(obj) -> list[dict]:
    # pandas DF -> records; dict -> [dict]; list -> list; anders []
    try:
        if hasattr(obj, "to_dict"):
            return obj.to_dict(orient="records")
    except Exception:
        pass
    if isinstance(obj, dict): return [obj]
    if isinstance(obj, list): return obj
    return []

def _norm_name(r: dict) -> str:
    # harmoniseer naamveld
    return (
        r.get("name")
        or r.get("config_name")
        or r.get("Naam")
        or r.get("ConfigName")
        or ""
    )

def _norm_id(r: dict) -> int | None:
    # harmoniseer id-veld
    for k in ("id", "config_id", "ID", "Id"):
        if k in r and r[k] is not None:
            try:
                return int(r[k])
            except Exception:
                return None
    return None

def _with_short_code(r: dict, sc: str) -> dict:
    if "short_code" not in r:
        r = {**r, "short_code": sc}
    return r

def normalize_configs(sc: str, data) -> list[dict]:
    recs = _to_records(data)
    out = []
    for r in recs:
        rid = _norm_id(r)
        name = _norm_name(r)
        nr = {**r, "id": rid, "name": name}
        nr = _with_short_code(nr, sc)
        out.append(nr)
    # filter records zonder id
    out = [r for r in out if r["id"] is not None]
    return out

def ensure_config_has_id_or_refetch(
    sc: str,
    conn_id: int,
    maybe_cfg: dict | list | None,
    refetch_fn,                # callable(conn_id, sc) -> list[dict]/DF
    prefer_id: int | None = None,
    prefer_name: str | None = None,
) -> dict:
    """
    Zorgt dat we een config dict met 'id' teruggeven.
    - Als maybe_cfg al een dict met id is: return die.
    - Als insert/update niets bruikbaars geeft: refetch en kies op id of name of 'laatst bijgewerkt'.
    """
    if isinstance(maybe_cfg, dict) and _norm_id(maybe_cfg) is not None:
        cfg = {**maybe_cfg, "id": _norm_id(maybe_cfg), "name": _norm_name(maybe_cfg)}
        return _with_short_code(cfg, sc)

    # Refetch
    lst = normalize_configs(sc, refetch_fn(conn_id, sc))
    if not lst:
        return {}

    # 1) prefer exact id
    if prefer_id is not None:
        for r in lst:
            if r["id"] == prefer_id:
                return r
    # 2) prefer by (normalized) name (laatste met die naam)
    if prefer_name:
        for r in reversed(lst):
            if (r.get("name") or "").strip() == (prefer_name or "").strip():
                return r
    # 3) fallback: pak de laatst (gesorteerd op id) of eerste
    lst.sort(key=lambda r: (r["name"] or "", r["id"]))
    return lst[-1]

def select_or_edit_catalog_config(
    main_connection_id: int,
    short_code: str,
    *,
    fetch_configs: Callable[[int, str], List[Dict[str, Any]]],
    create_config: Callable[[int, str, str, Dict[str, Any]], Dict[str, Any]],
    update_config: Callable[[int, str, int, Dict[str, Any]], Dict[str, Any]],
    render_picker: Callable[[List[Tuple[int, str]], Optional[int], str], int],
    prompt_new_config: Callable[[int, str], Tuple[str, Dict[str, Any]]],
    prompt_edit_config: Callable[[Dict[str, Any]], Tuple[Optional[str], Optional[Dict[str, Any]]]],
    preselected_config_id: Optional[int] = None,
    title: str = "Selecteer of bewerk Catalog Config",
) -> Dict[str, Any]:
    """
    - Toon lijst met bestaande configs (geen aparte 'Bewerk ...' entries).
    - Kies je een bestaande: direct edit-form.
    - Kies je 'Nieuwe ...': prompt voor nieuw + daarna direct edit-form.
    """

    # 1) Ophalen
    configs = fetch_configs(main_connection_id, short_code) or []
    by_id = {c["id"]: c for c in configs if "id" in c}

    # 2) Opties: alleen configs zelf + create (labels in dezelfde stijl als connections)
    options: List[Tuple[int | str, str]] = []
    for c in configs:
        options.append((c["id"], format_catalog_cfg_label(c, short_code)))
    options.append((CREATE_NEW_SENTINEL_ID, "➕ Nieuwe catalog config…"))

    chosen_id = render_picker(options, preselected_config_id, title)

    # 3) Nieuwe config
    if chosen_id == CREATE_NEW_SENTINEL_ID:
        name, settings = prompt_new_config(main_connection_id, short_code)
        if name is None and settings is None:
            return {}  # geannuleerd
        if not name.strip():
            name = "Naamloze config"
        cfg = create_config(main_connection_id, short_code, name, settings or {})
        # direct in edit
        new_name, new_settings = prompt_edit_config(cfg)
        patch: Dict[str, Any] = {}
        if new_name and new_name.strip():
            patch["name"] = new_name.strip()
        if new_settings is not None:
            patch.update(new_settings)
        if patch:
            return update_config(main_connection_id, short_code, cfg["id"], patch)
        return cfg

    # 4) Bestaande config
    if chosen_id not in by_id:
        raise ValueError(f"Onbekende catalog config id gekozen: {chosen_id}")
    cfg = by_id[chosen_id]

    # direct edit
    new_name, new_settings = prompt_edit_config(cfg)
    patch: Dict[str, Any] = {}
    if new_name and new_name.strip():
        patch["name"] = new_name.strip()
    if new_settings is not None:
        patch.update(new_settings)
    if patch:
        return update_config(main_connection_id, short_code, cfg["id"], patch)
    return cfg

def select_catalog_config(
    main_connection_id: int,
    short_code: str,
    *,
    fetch_configs: Callable[[int, str], List[Dict[str, Any]]],
    render_picker: Callable[[List[Tuple[int, str]], Optional[int], str], int],
    preselected_config_id: Optional[int] = None,
    title: str = "Selecteer Catalog Config",
) -> Dict[str, Any]:
    """
    Simpele picker: lijst met configs, keuze teruggeven.
    Geen create/edit functionaliteit.
    """
    configs = fetch_configs(main_connection_id, short_code) or []
    by_id = {c["id"]: c for c in configs if "id" in c}

    if not configs:
        st.info("Geen catalog-configs beschikbaar voor deze connection.")
        return {}

    options: List[Tuple[int | str, str]] = []
    for c in configs:
        options.append((c["id"], format_catalog_cfg_label(c, short_code)))

    chosen_id = render_picker(options, preselected_config_id, title)

    if chosen_id not in by_id:
        st.error(f"Onbekende catalog config id gekozen: {chosen_id}")
        st.stop()

    chosen_cfg = by_id[chosen_id]
    st.caption(f"Gekozen catalog-config: #{chosen_cfg['id']} · {chosen_cfg.get('name') or ''}")

    return chosen_cfg

def render_catalog_config_picker_with_edit(
    main_connection_id: int,
    short_code: str,
    *,
    fetch_configs,
    create_config,
    update_config,
    prompt_new_config,
    prompt_edit_config,
    preselected_config_id: int | None = None,
    title: str = "Selecteer of bewerk Catalog Config",
):
    """
    Wrapper voor beheerpagina's: kies bestaande config (direct edit) of maak nieuw.
    """
    # Bouw opties en labels hier, zodat format_func geen by_id nodig heeft
    configs = fetch_configs(main_connection_id, short_code) or []
    options = []
    labels = {}
    for c in configs:
        label = format_catalog_cfg_label(c, short_code)
        options.append((c["id"], label))
        labels[c["id"]] = label
    options.append((CREATE_NEW_SENTINEL_ID, "➕ Nieuwe catalog config…"))
    labels[CREATE_NEW_SENTINEL_ID] = "➕ Nieuwe catalog config…"

    def _picker(opts, preselected_id, picker_title):
        id_list = [oid for (oid, _lbl) in opts]
        idx = id_list.index(preselected_id) if (preselected_id in id_list) else 0
        return st.selectbox(
            picker_title,
            options=id_list,
            index=idx,
            format_func=lambda oid: labels.get(oid, f"#{oid}"),
            key=f"cfg_select_{short_code}",
        )

    # Let op: we geven hier de "voorgesmede" options door aan je orchestrator
    return select_or_edit_catalog_config(
        main_connection_id,
        short_code,
        fetch_configs=lambda cid, sc: configs,  # al opgehaald
        create_config=create_config,
        update_config=update_config,
        render_picker=lambda _ignored_opts, pre_id, ttl: _picker(options, pre_id, title),
        prompt_new_config=prompt_new_config,
        prompt_edit_config=prompt_edit_config,
        preselected_config_id=preselected_config_id,
        title=title,
    )


def render_catalog_config_picker_readonly(
    main_connection_id: int,
    short_code: str,
    *,
    fetch_configs,
    preselected_config_id: int | None = None,
    title: str = "Selecteer Catalog Config",
):
    """
    Wrapper voor uitvoerpagina's: alleen selecteren/tonen (geen edit).
    """
    configs = fetch_configs(main_connection_id, short_code) or []
    if not configs:
        st.info("Geen catalog-configs beschikbaar voor deze connection.")
        return {}

    options = []
    labels = {}
    for c in configs:
        label = format_catalog_cfg_label(c, short_code)
        options.append((c["id"], label))
        labels[c["id"]] = label

    def _picker(opts, preselected_id, picker_title):
        id_list = [oid for (oid, _lbl) in opts]
        idx = id_list.index(preselected_id) if (preselected_id in id_list) else 0
        return st.selectbox(
            picker_title,
            options=id_list,
            index=idx,
            format_func=lambda oid: labels.get(oid, f"#{oid}"),
            key=f"cfg_select_{short_code}_readonly",
        )

    return select_catalog_config(
        main_connection_id,
        short_code,
        fetch_configs=lambda cid, sc: configs,  # al opgehaald
        render_picker=lambda _ignored_opts, pre_id, ttl: _picker(options, pre_id, title),
        preselected_config_id=preselected_config_id,
        title=title,
    )


def format_catalog_cfg_label(cfg: dict, short_code: str | None = None) -> str:
    """
    Zelfde stijl als je main connections:
    #<id> — <name> [<short>] · 🟢/🔴
    """
    cfg_id = cfg.get("id", "?")
    name = (cfg.get("name") or cfg.get("config_name") or "Naamloos").strip()
    sc = (short_code or cfg.get("short_code") or "").strip().lower()
    status = "🟢 Active" if cfg.get("is_active") else "🔴 Inactive"
    return f"#{cfg_id} — {name} [{sc}] · {status}"

def format_ai_cfg_label(cfg: dict, short_code: str | None = None) -> str:
    """
    Label in dezelfde stijl als connections:
    #<id> — <name> [ai/<short>] · 🟢/🔴
    """
    cfg_id = cfg.get("id", "?")
    name = (cfg.get("name") or cfg.get("config_name") or "Naamloos").strip()
    sc = (short_code or cfg.get("short_code") or "").strip().lower()
    status = "🟢 Active" if cfg.get("is_active") else "🔴 Inactive"
    return f"#{cfg_id} — {name} [ai/{sc}] · {status}"

def make_catalog_crud_adapters(config_crud_module):
    """
    Adapters voor CATALOG-configs (dw/pbi/dl).
    UI-signaturen:
      fetch_configs(conn_id, short_code) -> list[dict]
      create_config(conn_id, short_code, name, settings) -> dict
      update_config(conn_id, short_code, config_id, patch) -> dict|None
    """
    def fetch_configs(conn_id: int, short_code: str):
        sc = (short_code or "").strip().lower()
        fn = {
            "dw": _resolve_fn(config_crud_module, [
                "list_dw_catalog_configs", "fetch_dw_catalog_configs", "get_dw_catalog_configs"
            ]),
            "pbi": _resolve_fn(config_crud_module, [
                "list_pbi_catalog_configs", "fetch_pbi_catalog_configs", "get_pbi_catalog_configs"
            ]),
            "dl": _resolve_fn(config_crud_module, [
                "list_dl_catalog_configs", "fetch_dl_catalog_configs", "get_dl_catalog_configs"
            ]),
        }[sc]
        return fn(conn_id)

    def create_config(conn_id: int, short_code: str, name: str, settings: dict):
        sc = (short_code or "").strip().lower()
        fn = {
            "dw": _resolve_fn(config_crud_module, [
                "create_dw_catalog_config", "insert_dw_catalog_config"
            ]),
            "pbi": _resolve_fn(config_crud_module, [
                "create_pbi_catalog_config", "insert_pbi_catalog_config"
            ]),
            "dl": _resolve_fn(config_crud_module, [
                "create_dl_catalog_config", "insert_dl_catalog_config"
            ]),
        }[sc]
        return fn(conn_id, name, settings or {})

    def update_config(conn_id: int, short_code: str, config_id: int, patch: dict):
        sc = (short_code or "").strip().lower()
        fn = {
            "dw": _resolve_fn(config_crud_module, ["update_dw_catalog_config"]),
            "pbi": _resolve_fn(config_crud_module, ["update_pbi_catalog_config"]),
            "dl": _resolve_fn(config_crud_module, ["update_dl_catalog_config"]),
        }[sc]
        # Probeer (config_id, patch) eerst; zo niet, val terug op (conn_id, config_id, patch)
        try:
            return fn(config_id, patch or {})
        except TypeError:
            return fn(conn_id, config_id, patch or {})

    return fetch_configs, create_config, update_config


def make_ai_crud_adapters(config_crud_module):
    """
    Adapters voor AI-configs (dw/pbi/dl).
    UI-signaturen identiek aan catalog.
    """
    def fetch_configs(conn_id: int, short_code: str):
        sc = (short_code or "").strip().lower()
        fn = {
            "dw": _resolve_fn(config_crud_module, [
                "list_dw_ai_configs", "fetch_dw_ai_configs", "get_dw_ai_configs"
            ]),
            "pbi": _resolve_fn(config_crud_module, [
                "list_pbi_ai_configs", "fetch_pbi_ai_configs", "get_pbi_ai_configs"
            ]),
            "dl": _resolve_fn(config_crud_module, [
                "list_dl_ai_configs", "fetch_dl_ai_configs", "get_dl_ai_configs"
            ]),
        }[sc]
        return fn(conn_id)

    def create_config(conn_id: int, short_code: str, name: str, settings: dict):
        sc = (short_code or "").strip().lower()
        fn = {
            "dw": _resolve_fn(config_crud_module, [
                "create_dw_ai_config", "insert_dw_ai_config"
            ]),
            "pbi": _resolve_fn(config_crud_module, [
                "create_pbi_ai_config", "insert_pbi_ai_config"
            ]),
            "dl": _resolve_fn(config_crud_module, [
                "create_dl_ai_config", "insert_dl_ai_config"
            ]),
        }[sc]
        return fn(conn_id, name, settings or {})

    def update_config(conn_id: int, short_code: str, config_id: int, patch: dict):
        sc = (short_code or "").strip().lower()
        fn = {
            "dw": _resolve_fn(config_crud_module, ["update_dw_ai_config"]),
            "pbi": _resolve_fn(config_crud_module, ["update_pbi_ai_config"]),
            "dl": _resolve_fn(config_crud_module, ["update_dl_ai_config"]),
        }[sc]
        try:
            return fn(config_id, patch or {})
        except TypeError:
            return fn(conn_id, config_id, patch or {})

    return fetch_configs, create_config, update_config


def _resolve_fn(mod, candidates: list[str]):
    for name in candidates:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn
    # duidelijke foutmelding met alle kandidaten
    raise AttributeError(f"None of these functions exist in {mod.__name__}: {', '.join(candidates)}")

def _build_ai_patch(patch: dict) -> dict:
    """Allow only safe AI fields; normalize blanks → None; coerce booleans."""
    if not isinstance(patch, dict):
        return {}
    allowed = {"name", "config_name", "settings", "notes", "is_active"}
    clean = {}
    for k, v in patch.items():
        if k not in allowed:
            continue
        if k in {"name", "config_name", "notes"}:
            v = (v or "").strip()
            clean[k] = v if v else None
        elif k == "settings":
            # must be a dict (your CRUD handles JSONB/JSON)
            clean[k] = v if isinstance(v, dict) else {}
        elif k == "is_active":
            clean[k] = bool(v)
    return clean

CREATE_NEW_AI_SENTINEL = "__create_new_ai_config__"
EDIT_AI_SENTINEL_PREFIX = "__edit_ai_config__:"

def select_or_edit_ai_config(
    main_connection_id: int,
    short_code: str,
    *,
    fetch_configs,
    create_config,
    update_config,
    render_picker,             # (options, preselected_id, title) -> chosen
    prompt_new_config,         # (conn_id, sc) -> dict velden (ruw)
    prompt_edit_config,        # (cfg, sc) -> dict patch (ruw) of None
    preselected_config_id: int | None = None,
    title: str = "Selecteer of bewerk AI-config",
) -> dict:
    sc = (short_code or "").strip().lower()
    from data_catalog.ui_prompts import prompt_new_ai_config, prompt_edit_ai_config  # if needed

    configs = fetch_configs(main_connection_id, sc) or []
    by_id = {c["id"]: c for c in configs if "id" in c}

    options = []
    for c in configs:
        name = (c.get("name") or c.get("config_name") or f"#{c.get('id')}").strip()
        options.append((c["id"], f"🧠 {name}"))
        options.append((f"{EDIT_AI_SENTINEL_PREFIX}{c['id']}", f"✏️ Bewerk ‘{name}’…"))
    options.append((CREATE_NEW_AI_SENTINEL, "➕ Nieuwe AI-config…"))

    chosen = render_picker(options, preselected_config_id, title)

    if chosen == CREATE_NEW_AI_SENTINEL:
        raw = prompt_new_config(main_connection_id, sc)  # ruwe dict uit UI
        settings = build_ai_settings_for_type(sc, raw)
        # create: (conn_id, sc, name, settings)
        created = create_config(main_connection_id, sc, settings["config_name"], settings)
        return created

    if isinstance(chosen, str) and chosen.startswith(EDIT_AI_SENTINEL_PREFIX):
        target_id = int(chosen[len(EDIT_AI_SENTINEL_PREFIX):])
        if target_id not in by_id:
            raise ValueError(f"Onbekende AI-config id: {target_id}")
        cfg = by_id[target_id]
        raw_patch = prompt_edit_config(cfg, sc)
        if not raw_patch:
            return cfg
        patch = build_ai_settings_for_type(sc, raw_patch)
        # name/config_name consistent
        if "config_name" not in patch and "name" in raw_patch:
            patch["config_name"] = raw_patch["name"]
        update_config(main_connection_id, sc, target_id, patch)
        cfg.update(patch)
        return cfg

    if chosen not in by_id:
        raise ValueError(f"Onbekende AI-config id gekozen: {chosen}")
    return by_id[chosen]

def render_ai_config_picker_with_edit(
    main_connection_id: int,
    short_code: str,
    *,
    fetch_configs,
    create_config,
    update_config,
    prompt_new_config,
    prompt_edit_config,
    preselected_config_id: int | None = None,
    title: str = "Selecteer of bewerk AI-config",
):
    sc = (short_code or "").strip().lower()
    configs = fetch_configs(main_connection_id, sc) or []

    # labels
    id_to_label = {}
    options = []
    for c in configs:
        name = (c.get("name") or c.get("config_name") or f"#{c.get('id')}").strip()
        label = f"#{c['id']} — {name} [ai/{sc}] · " + ("🟢 Active" if c.get("is_active") else "🔴 Inactive")
        id_to_label[c["id"]] = label
        options.append((c["id"], label))
        edit_id = f"{EDIT_AI_SENTINEL_PREFIX}{c['id']}"
        options.append((edit_id, f"✏️ Bewerk ‘{name}’…"))
        id_to_label[edit_id] = f"✏️ Bewerk ‘{name}’…"
    options.append((CREATE_NEW_AI_SENTINEL, "➕ Nieuwe AI-config…"))
    id_to_label[CREATE_NEW_AI_SENTINEL] = "➕ Nieuwe AI-config…"

    def _picker(_opts, pre_id, picker_title):
        ids = [i for (i, _lbl) in options]
        idx = ids.index(pre_id) if pre_id in ids else 0
        return st.selectbox(
            picker_title,
            options=ids,
            index=idx,
            format_func=lambda oid: id_to_label.get(oid, f"#{oid}"),
            key=f"ai_cfg_picker_{sc}_{main_connection_id}",
        )

    return select_or_edit_ai_config(
        main_connection_id,
        sc,
        fetch_configs=fetch_configs,
        create_config=create_config,
        update_config=update_config,
        render_picker=_picker,
        prompt_new_config=prompt_new_config,
        prompt_edit_config=prompt_edit_config,
        preselected_config_id=preselected_config_id,
        title=title,
    )

# ------------------ AI config: normalisatie & validatie ------------------

def _none_if_blank(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def _num_or_none(x, cast=float):
    if x is None or x == "":
        return None
    try:
        return cast(x)
    except Exception:
        return None

def _clamp_or_none(x, lo, hi):
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if v < lo: v = lo
    if v > hi: v = hi
    return v

# geldige keuzen uit je CHECK-constraints
_VALID_PROPAGATION = {"auto", "suggest_only", "off"}
_VALID_OVERWRITE   = {"fill_empty", "overwrite_if_confident", "never"}

def build_ai_settings_for_type(short_code: str, data: dict) -> dict:
    """
    Normaliseert velden voor AI-config-create/update o.b.v. short_code.
    Houdt rekening met NOT NULL + defaults (zoals in je tabellen).
    Returned dict met keys die je INSERT/UPDATE wil schrijven.
    """
    sc = (short_code or "").strip().lower()
    d  = data or {}

    # --- velden die voor alle typen gelden ---
    out = {
        "config_name": _none_if_blank(d.get("name") or d.get("config_name")) or "Naamloze AI-config",
        "analysis_type": _none_if_blank(d.get("analysis_type")),  # NOT NULL
        "model_provider": _none_if_blank(d.get("model_provider")) or "openai",  # NOT NULL (default)
        "model_name": _none_if_blank(d.get("model_name")),        # NOT NULL
        "model_version": _none_if_blank(d.get("model_version")),
        "temperature": _clamp_or_none(_num_or_none(d.get("temperature")), 0.0, 2.0),   # CHECK
        "max_tokens": _num_or_none(d.get("max_tokens"), int),                         # int, mag None
        "top_p": _clamp_or_none(_num_or_none(d.get("top_p")), 1e-9, 1.0),             # (0,1]
        "frequency_penalty": _clamp_or_none(_num_or_none(d.get("frequency_penalty")), -2.0, 2.0),
        "presence_penalty":  _clamp_or_none(_num_or_none(d.get("presence_penalty")),  -2.0, 2.0),
        "runner_concurrency": int(d.get("runner_concurrency") or 2),                  # NOT NULL default 2
        "propagation_mode": (d.get("propagation_mode") or "auto"),
        "overwrite_policy": (d.get("overwrite_policy") or "fill_empty"),
        "confidence_threshold": float(d.get("confidence_threshold") or 0.700),        # NOT NULL default
        "respect_human_locks": bool(d.get("respect_human_locks", True)),              # NOT NULL default
        "model_profile": _none_if_blank(d.get("model_profile")),
        "prompt_pack": _none_if_blank(d.get("prompt_pack")),
        "notes": _none_if_blank(d.get("notes")),
        "is_active": bool(d.get("is_active", True)),
    }

    # validatie enums
    if out["propagation_mode"] not in _VALID_PROPAGATION:
        out["propagation_mode"] = "auto"
    if out["overwrite_policy"] not in _VALID_OVERWRITE:
        out["overwrite_policy"] = "fill_empty"

    # --- type-specifieke filters/flags ---
    if sc == "dw":
        out.update({
            "database_filter": _none_if_blank(d.get("database_filter")),
            "schema_filter":   _none_if_blank(d.get("schema_filter")),
            "table_filter":    _none_if_blank(d.get("table_filter")),
            # dw heeft geen include_* extra flags in jouw AI-schema
        })
    elif sc == "pbi":
        out.update({
            "workspace_filter":    _none_if_blank(d.get("workspace_filter")),
            "model_filter":        _none_if_blank(d.get("model_filter")),
            "table_filter":        _none_if_blank(d.get("table_filter")),
            "include_tmdl":        bool(d.get("include_tmdl", True)),
            "include_model_bim":   bool(d.get("include_model_bim", False)),
            "respect_perspectives": bool(d.get("respect_perspectives", True)),
        })
    elif sc == "dl":
        out.update({
            "path_filter":        _none_if_blank(d.get("path_filter")),
            "format_whitelist":   _none_if_blank(d.get("format_whitelist")),
            "partition_filter":   _none_if_blank(d.get("partition_filter")),
            "include_hidden_files": bool(d.get("include_hidden_files", False)),
            "infer_schema":         bool(d.get("infer_schema", True)),
        })
    else:
        raise ValueError(f"Unknown short_code for AI-config: {short_code}")

    # verplichte kernvelden checken (raise met heldere fout)
    if not out["analysis_type"]:
        raise ValueError("analysis_type is verplicht.")
    if not out["model_name"]:
        raise ValueError("model_name is verplicht.")

    return out