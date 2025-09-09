import streamlit as st
import pandas as pd
import json
from typing import Callable, Optional
import uuid

from data_catalog.config_crud import (
    update_dw_catalog_config,
    update_pbi_catalog_config,
    update_dl_catalog_config)

from data_catalog.config_service import (
    list_deactivated_configs,
    soft_delete_config,
    format_catalog_cfg_label,
    list_deactivated_configs, 
    soft_delete_config, 
    format_ai_cfg_label,
)

from data_catalog.connection_handler import fetch_connection_type_registry

def _none_if_blank(val: str | None) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def build_settings_for_type(short_code: str, form_values: dict) -> dict:
    sc = (short_code or "").strip().lower()
    if sc == "dw":
        return {
            "database_filter": _none_if_blank(form_values.get("database_filter")),
            "schema_filter": _none_if_blank(form_values.get("schema_filter")),
            "table_filter": _none_if_blank(form_values.get("table_filter")),
            # DW booleans zijn in schema NIET NOT NULL; we sturen netjes bools of laten DB-default werken.
            "include_views": bool(form_values.get("include_views", False)),
            "include_system_objects": bool(form_values.get("include_system_objects", False)),
            "notes": _none_if_blank(form_values.get("notes")),
        }

    if sc == "pbi":
        return {
            "workspace_filter": _none_if_blank(form_values.get("workspace_filter")),
            "model_filter": _none_if_blank(form_values.get("model_filter")),
            "table_filter": _none_if_blank(form_values.get("table_filter")),
            # PBI booleans zijn NOT NULL → altijd bools aanleveren
            "include_tmdl": bool(form_values.get("include_tmdl", True)),
            "include_model_bim": bool(form_values.get("include_model_bim", False)),
            "respect_perspectives": bool(form_values.get("respect_perspectives", True)),
            "notes": _none_if_blank(form_values.get("notes")),
        }

    if sc == "dl":
        return {
            "path_filter": _none_if_blank(form_values.get("path_filter")),
            "format_whitelist": _none_if_blank(form_values.get("format_whitelist")),
            "partition_filter": _none_if_blank(form_values.get("partition_filter")),
            # DL booleans zijn NOT NULL → altijd bools aanleveren
            "include_hidden_files": bool(form_values.get("include_hidden_files", False)),
            "infer_schema": bool(form_values.get("infer_schema", True)),
            "notes": _none_if_blank(form_values.get("notes")),
        }

    raise ValueError(f"Unknown short_code: {short_code}")

def prompt_new_catalog_config(main_connection_id: int, short_code: str):
    sc = (short_code or "").strip().lower()
    with st.form(f"new_cfg_form_{main_connection_id}_{sc}"):
        st.subheader(f"Nieuwe catalog-config ({sc.upper()})")
        render_catalog_config_field_help(sc)
        name = st.text_input("Naam", placeholder="Bijv. 'Standaard scan'")

        if sc == "dw":
            database_filter = st.text_input("Database filter", value="")
            schema_filter   = st.text_input("Schema filter",   value="")
            table_filter    = st.text_input("Tabel filter",    value="")
            include_views   = st.checkbox("Include views", value=False)
            include_sysobj  = st.checkbox("Include system objects", value=False)
            notes           = st.text_area("Opmerkingen", value="", height=80)

        elif sc == "pbi":
            workspace_filter     = st.text_input("Workspace filter", value="")
            model_filter         = st.text_input("Model filter",     value="")
            table_filter         = st.text_input("Tabel filter",     value="")
            include_tmdl         = st.checkbox("Include TMDL", value=True)
            include_model_bim    = st.checkbox("Include model.bim", value=False)
            respect_perspectives = st.checkbox("Respect perspectives", value=True)
            notes                = st.text_area("Opmerkingen", value="", height=80)

        elif sc == "dl":
            path_filter        = st.text_input("Path filter", value="")
            format_whitelist   = st.text_input("Format whitelist (csv;parquet;json)", value="")
            partition_filter   = st.text_input("Partition filter", value="")
            include_hidden     = st.checkbox("Include hidden files", value=False)
            infer_schema       = st.checkbox("Infer schema", value=True)
            notes              = st.text_area("Opmerkingen", value="", height=80)
        else:
            st.error(f"Onbekende short_code: {short_code}")
            return None, None

        submitted = st.form_submit_button("Aanmaken")
        
        if not submitted:
            return None, None

        if not name or not name.strip():
            st.error("Naam is verplicht.")
            return None, None  # laat orchestrator het als 'geannuleerd' behandelen

    # normaliseer en bouw settings per type
    form_values = locals()
    settings = build_settings_for_type(sc, {
        k: form_values.get(k) for k in form_values
    })
    return name.strip(), settings


def prompt_edit_catalog_config(cfg: dict):
    sc = (cfg.get("short_code") or cfg.get("type") or "").strip().lower()
    # Als short_code niet in cfg zit, laat de caller die meegeven via session of context
    if sc not in ("dw", "pbi", "dl"):
        # heuristiek: presence van kolommen
        if "database_filter" in cfg or "schema_filter" in cfg:
            sc = "dw"
        elif "workspace_filter" in cfg or "model_filter" in cfg:
            sc = "pbi"
        elif "path_filter" in cfg or "format_whitelist" in cfg:
            sc = "dl"

    with st.form(f"edit_cfg_form_{cfg.get('id')}"):
        st.subheader(f"Bewerk catalog-config #{cfg.get('id')} ({sc.upper()})")
        render_catalog_config_field_help(sc)
        name = st.text_input("Naam", value=cfg.get("name") or cfg.get("config_name") or "")

        if sc == "dw":
            database_filter = st.text_input("Database filter", value=cfg.get("database_filter") or "*")
            schema_filter = st.text_input("Schema filter", value=cfg.get("schema_filter") or "*")
            table_filter = st.text_input("Tabel filter", value=cfg.get("table_filter") or "*")
            include_views = st.checkbox("Include views", value=bool(cfg.get("include_views")))
            include_system_objects = st.checkbox("Include system objects", value=bool(cfg.get("include_system_objects")))
            notes = st.text_area("Opmerkingen", value=cfg.get("notes") or "", height=80)

            submitted = st.form_submit_button("Wijzigingen opslaan")
            if not submitted: return None, None
            new_settings = build_settings_for_type("dw", {
                "database_filter": database_filter,
                "schema_filter": schema_filter,
                "table_filter": table_filter,
                "include_views": include_views,
                "include_system_objects": include_system_objects,
                "notes": notes,
            })

        elif sc == "pbi":
            workspace_filter = st.text_input("Workspace filter", value=cfg.get("workspace_filter") or "*")
            model_filter = st.text_input("Model filter", value=cfg.get("model_filter") or "*")
            table_filter = st.text_input("Tabel filter", value=cfg.get("table_filter") or "*")
            include_tmdl = st.checkbox("Include TMDL", value=bool(cfg.get("include_tmdl", True)))
            include_model_bim = st.checkbox("Include model.bim", value=bool(cfg.get("include_model_bim", False)))
            respect_perspectives = st.checkbox("Respect perspectives", value=bool(cfg.get("respect_perspectives", True)))
            notes = st.text_area("Opmerkingen", value=cfg.get("notes") or "", height=80)

            submitted = st.form_submit_button("Wijzigingen opslaan")
            if not submitted: return None, None
            new_settings = build_settings_for_type("pbi", {
                "workspace_filter": workspace_filter,
                "model_filter": model_filter,
                "table_filter": table_filter,
                "include_tmdl": include_tmdl,
                "include_model_bim": include_model_bim,
                "respect_perspectives": respect_perspectives,
                "notes": notes,
            })

        elif sc == "dl":
            path_filter = st.text_input("Path filter", value=cfg.get("path_filter") or "*")
            format_whitelist = st.text_input("Format whitelist (csv;parquet;json)", value=cfg.get("format_whitelist") or "")
            partition_filter = st.text_input("Partition filter", value=cfg.get("partition_filter") or "")
            include_hidden_files = st.checkbox("Include hidden files", value=bool(cfg.get("include_hidden_files", False)))
            infer_schema = st.checkbox("Infer schema", value=bool(cfg.get("infer_schema", True)))
            notes = st.text_area("Opmerkingen", value=cfg.get("notes") or "", height=80)

            submitted = st.form_submit_button("Wijzigingen opslaan")
            if not submitted: return None, None
            new_settings = build_settings_for_type("dl", {
                "path_filter": path_filter,
                "format_whitelist": format_whitelist,
                "partition_filter": partition_filter,
                "include_hidden_files": include_hidden_files,
                "infer_schema": infer_schema,
                "notes": notes,
            })

        else:
            st.error(f"Onbekende short_code: {sc}")
            return None, None

    return (name or None), new_settings


def save_test_status(short_code: str, conn_id: int, cfg_id: int, status: str, notes: str = ""):
    patch = {
        "last_test_status": (status or None),
        "last_test_notes": (notes or None),
        # 'last_tested_at' kun je in DB op trigger NOW() zetten; zo niet, voeg hier datetime.now() toe
    }
    if short_code == "dw":
        update_dw_catalog_config(conn_id, cfg_id, patch)
    elif short_code == "pbi":
        update_pbi_catalog_config(conn_id, cfg_id, patch)
    else:
        update_dl_catalog_config(conn_id, cfg_id, patch)

def clear_test_status(short_code: str, conn_id: int, cfg_id: int):
    patch = {"last_test_status": None, "last_test_notes": None}
    if short_code == "dw":
        update_dw_catalog_config(conn_id, cfg_id, patch)
    elif short_code == "pbi":
        update_pbi_catalog_config(conn_id, cfg_id, patch)
    else:
        update_dl_catalog_config(conn_id, cfg_id, patch)

def render_catalog_config_details(cfg: dict, sc: str):
    """Toont details van een gekozen catalog-config, afhankelijk van short_code."""
    with st.expander("Details"):
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Status:**", "🟢 Active" if cfg.get("is_active") else "🔴 Inactive")
            st.write("**Opmerkingen:**", cfg.get("notes", "—"))
        with col2:
            st.write("**Laatste teststatus:**", cfg.get("last_test_status", "—"))
            st.write("**Laatst getest op:**", cfg.get("last_tested_at", "—"))
            st.write("**Testnotities:**", cfg.get("last_test_notes", "—"))

        st.markdown("---")

        if sc == "dw":
            st.write("**Database filter:**", cfg.get("database_filter", "—"))
            st.write("**Schema filter:**", cfg.get("schema_filter", "—"))
            st.write("**Tabel filter:**", cfg.get("table_filter", "—"))
            st.write("**Include views:**", "Ja" if cfg.get("include_views") else "Nee")
            st.write("**Include system objects:**", "Ja" if cfg.get("include_system_objects") else "Nee")

        elif sc == "pbi":
            st.write("**Workspace filter:**", cfg.get("workspace_filter", "—"))
            st.write("**Model filter:**", cfg.get("model_filter", "—"))
            st.write("**Tabel filter:**", cfg.get("table_filter", "—"))
            st.write("**Include TMDL:**", "Ja" if cfg.get("include_tmdl") else "Nee")
            st.write("**Include model.bim:**", "Ja" if cfg.get("include_model_bim") else "Nee")
            st.write("**Respect perspectives:**", "Ja" if cfg.get("respect_perspectives") else "Nee")

        elif sc == "dl":
            st.write("**Path filter:**", cfg.get("path_filter", "—"))
            st.write("**Format whitelist:**", cfg.get("format_whitelist", "—"))
            st.write("**Partition filter:**", cfg.get("partition_filter", "—"))
            st.write("**Include hidden files:**", "Ja" if cfg.get("include_hidden_files") else "Nee")
            st.write("**Infer schema:**", "Ja" if cfg.get("infer_schema") else "Nee")

# def format_catalog_cfg_label(cfg: dict) -> str:
#     """
#     Format label voor catalog configs, vergelijkbaar met main connections:
#     #<id> — <config_name> [<short_code>] · status
#     """
#     cfg_id = cfg.get("id", "?")
#     name = cfg.get("name") or cfg.get("config_name") or "Naamloos"
#     short_code = (cfg.get("short_code") or "").lower()
#     status = "🟢 Active" if cfg.get("is_active") else "🔴 Inactive"
#     return f"#{cfg_id} — {name} [{short_code}] · {status}"


def render_catalog_configs_overview(
    *,
    main_connection_id: int,
    short_code: str,
    fetch_configs,                 # Callable[[int, str], list[dict]]
    title: str = "Catalog Configs · Overview",
    include_download: bool = False,
    table_height: int | None = None,
    include_label_column: bool = True,
):
    sc = (short_code or "").strip().lower()
    st.markdown(f"### {title}")

    configs = fetch_configs(main_connection_id, sc) or []
    if not configs:
        st.info("No catalog-configs available for this connection.")
        return

    df = pd.DataFrame(configs)

    # Normaliseer name-kolom
    if "name" not in df.columns and "config_name" in df.columns:
        df["name"] = df["config_name"]

    # Label-kolom met formatter
    if include_label_column:
        df["_label"] = [
            format_catalog_cfg_label(row, sc) for row in df.to_dict(orient="records")
        ]

    # Kolommen per type (alleen tonen als ze bestaan)
    if sc == "dw":
        pref = [
            "id", "name", "database_filter", "schema_filter", "table_filter",
            "is_active", "updated_at", "last_test_status", "last_tested_at", "notes"
        ]
    elif sc == "pbi":
        pref = [
            "id", "name", "workspace_filter", "model_filter", "table_filter",
            "include_tmdl", "include_model_bim", "respect_perspectives",
            "is_active", "updated_at", "last_test_status", "last_tested_at", "notes"
        ]
    else:  # dl
        pref = [
            "id", "name", "path_filter", "format_whitelist", "partition_filter",
            "include_hidden_files", "infer_schema",
            "is_active", "updated_at", "last_test_status", "last_tested_at", "notes"
        ]

    cols = ["_label"] + [c for c in pref if c in df.columns] if include_label_column else [c for c in pref if c in df.columns]
    # Fallback als bijna niets matcht
    if not cols:
        cols = (["_label"] if include_label_column else []) + sorted(df.columns.tolist())

    st.dataframe(
        df[cols],
        use_container_width=True,
        hide_index=True,
        height=table_height,
    )

    if include_download:
        csv = df[cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download overview (CSV)",
            data=csv,
            file_name=f"catalog_configs_{sc}_conn{main_connection_id}.csv",
            mime="text/csv",
        )


def render_catalog_config_actions(
    *,
    main_connection_id: int,
    short_code: str,
    fetch_configs,          # Callable[[int, str], list[dict]]
    test_fn=None,           # Optional[Callable[[int, str, int], list[str] | None]]
    deactivate_fn=None,     # Optional[Callable[[int, str, int], None]]
    activate_fn=None,       # Optional[Callable[[int, str, int], None]]
    title: str = "Actions",
    key_prefix: str = "catalog_actions",
):
    """
    Toont een actiesectie:
    - dropdown met bestaande catalog-configs (zelfde labelstijl als main connections)
    - knoppen: Test, Deactivate (alleen als actief), Activate (alleen als inactief)

    test_fn(conn_id, sc, cfg_id) -> Optional[list[str]]  (bijv. log/regels om te tonen)
    deactivate_fn(conn_id, sc, cfg_id) -> None
    activate_fn(conn_id, sc, cfg_id) -> None
    """
    sc = (short_code or "").strip().lower()
    st.markdown(f"### {title}")

    configs = fetch_configs(main_connection_id, sc) or []
    if not configs:
        st.info("No catalog-configs to test or manage.")
        return

    # Selectbox met nette labels
    def _fmt_cfg(cfg: dict) -> str:
        return format_catalog_cfg_label(cfg, sc)

    selected_cfg = st.selectbox(
        "Select a catalog config",
        options=configs,
        format_func=_fmt_cfg,
        key=f"{key_prefix}_{sc}_select",
    )

    cfg_id = int(selected_cfg["id"])
    is_active_now = bool(selected_cfg.get("is_active"))

    col1, col2, col3 = st.columns([1, 1, 1])

    # ---------- Test ----------
    with col1:
        if st.button("🔍 Test", use_container_width=True, key=f"{key_prefix}_{sc}_btn_test"):
            messages = None
            if callable(test_fn):
                try:
                    messages = test_fn(main_connection_id, sc, cfg_id)
                except Exception as e:
                    st.error(f"Fout tijdens testen: {e}")
            else:
                st.info("Geen test-functie geconfigureerd voor catalog-configs.")

            if messages:
                for msg in messages:
                    if isinstance(msg, str) and msg.startswith(("✅", "🟢")):
                        st.success(msg)
                    elif isinstance(msg, str) and msg.startswith(("❌", "🔴")):
                        st.error(msg)
                    else:
                        st.info(msg)

    # ---------- Deactivate ----------
    with col2:
        if st.button(
            "🗑️ Deactivate",
            use_container_width=True,
            disabled=not is_active_now,
            key=f"{key_prefix}_{sc}_btn_deactivate",
        ):
            if callable(deactivate_fn):
                try:
                    deactivate_fn(main_connection_id, sc, cfg_id)
                    st.success(f"Catalog-config #{cfg_id} gedeactiveerd.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Deactiveren mislukt: {e}")
            else:
                st.info("Geen deactivate-functie geconfigureerd.")

    # ---------- Activate ----------
    with col3:
        if st.button(
            "✅ Activate",
            use_container_width=True,
            disabled=is_active_now,
            key=f"{key_prefix}_{sc}_btn_activate",
        ):
            if callable(activate_fn):
                try:
                    activate_fn(main_connection_id, sc, cfg_id)
                    st.success(f"Catalog-config #{cfg_id} geactiveerd.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Activeren mislukt: {e}")
            else:
                st.info("Geen activate-functie geconfigureerd.")

def render_catalog_config_actions_minimal(
    *,
    main_connection_id: int,
    short_code: str,
    fetch_configs,     # Callable[[int, str], list[dict]]
    deactivate_fn,     # Callable[[int, str, int], None]
    activate_fn,       # Callable[[int, str, int], None]
    title: str = "Actions",
    key_prefix: str = "catalog_actions_min",
):
    sc = (short_code or "").strip().lower()
    st.markdown(f"### {title}")

    configs = fetch_configs(main_connection_id, sc) or []
    if not configs:
        st.info("No catalog-configs to manage.")
        return

    def _fmt_cfg(cfg: dict) -> str:
        return format_catalog_cfg_label(cfg, sc)

    selected_cfg = st.selectbox(
        "Select a catalog config",
        options=configs,
        format_func=_fmt_cfg,
        key=f"{key_prefix}_{sc}_select",
    )

    cfg_id = int(selected_cfg["id"])
    is_active_now = bool(selected_cfg.get("is_active"))

    col2, col3 = st.columns([1, 1])

    with col2:
        if st.button(
            "🗑️ Deactivate",
            use_container_width=True,
            disabled=not is_active_now,
            key=f"{key_prefix}_{sc}_btn_deactivate",
        ):
            try:
                deactivate_fn(main_connection_id, sc, cfg_id)
                st.success(f"Catalog-config #{cfg_id} gedeactiveerd.")
                st.rerun()
            except Exception as e:
                st.error(f"Deactiveren mislukt: {e}")

    with col3:
        if st.button(
            "✅ Activate",
            use_container_width=True,
            disabled=is_active_now,
            key=f"{key_prefix}_{sc}_btn_activate",
        ):
            try:
                activate_fn(main_connection_id, sc, cfg_id)
                st.success(f"Catalog-config #{cfg_id} geactiveerd.")
                st.rerun()
            except Exception as e:
                st.error(f"Activeren mislukt: {e}")


def render_ai_configs_overview(
    *,
    main_connection_id: int,
    short_code: str,
    fetch_configs,                 # Callable[[int, str], list[dict]]
    title: str = "AI Configs · Overview",
    include_download: bool = False,
    table_height: int | None = None,
    include_label_column: bool = True,
):
    sc = (short_code or "").strip().lower()
    st.markdown(f"### {title}")

    configs = fetch_configs(main_connection_id, sc) or []
    if not configs:
        st.info("No AI-configs available for this connection.")
        return

    df = pd.DataFrame(configs)

    # Normaliseer name-kolom
    if "name" not in df.columns and "config_name" in df.columns:
        df["name"] = df["config_name"]

    # Label-kolom met formatter
    if include_label_column:
        df["_label"] = [
            format_ai_cfg_label(row, sc) for row in df.to_dict(orient="records")
        ]

    # AI-kolommen die vaak nuttig zijn (bestaan afhankelijk van type)
    common = [
        "id", "name",
        "analysis_type", "model_provider", "model_name", "model_version",
        "temperature", "max_tokens", "top_p", "frequency_penalty", "presence_penalty",
        "runner_concurrency", "propagation_mode", "overwrite_policy",
        "confidence_threshold", "respect_human_locks",
        "is_active", "updated_at", "notes",
        "model_profile", "prompt_pack",
    ]

    # Type-specifieke filters
    if sc == "dw":
        type_cols = ["database_filter", "schema_filter", "table_filter"]
    elif sc == "pbi":
        type_cols = ["workspace_filter", "model_filter", "table_filter", "include_tmdl", "include_model_bim", "respect_perspectives"]
    else:
        type_cols = ["path_filter", "format_whitelist", "partition_filter", "include_hidden_files", "infer_schema"]

    pref = common + type_cols
    cols = ["_label"] + [c for c in pref if c in df.columns] if include_label_column else [c for c in pref if c in df.columns]
    if not cols:
        cols = (["_label"] if include_label_column else []) + sorted(df.columns.tolist())

    st.dataframe(
        df[cols],
        use_container_width=True,
        hide_index=True,
        height=table_height,
    )

    if include_download:
        csv = df[cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download overview (CSV)",
            data=csv,
            file_name=f"ai_configs_{sc}_conn{main_connection_id}.csv",
            mime="text/csv",
        )

def render_ai_config_actions_minimal(
    *,
    main_connection_id: int,
    short_code: str,
    fetch_configs,     # Callable[[int, str], list[dict]]
    deactivate_fn,     # Callable[[int, str, int], None]  (conn_id, sc, cfg_id) -> None
    activate_fn,       # Callable[[int, str, int], None]
    title: str = "AI Configs · Actions",
    key_prefix: str = "ai_cfg_actions_min",
):
    sc = (short_code or "").strip().lower()
    st.markdown(f"### {title}")

    configs = fetch_configs(main_connection_id, sc) or []
    if not configs:
        st.info("No AI-configs to manage.")
        return

    def _fmt_cfg(cfg: dict) -> str:
        return format_ai_cfg_label(cfg, sc)

    selected_cfg = st.selectbox(
        "Select an AI config",
        options=configs,
        format_func=_fmt_cfg,
        key=f"{key_prefix}_{sc}_select",
    )

    cfg_id = int(selected_cfg["id"])
    is_active_now = bool(selected_cfg.get("is_active"))

    col2, col3 = st.columns([1, 1])

    # Deactivate
    with col2:
        if st.button(
            "🗑️ Deactivate",
            use_container_width=True,
            disabled=not is_active_now,
            key=f"{key_prefix}_{sc}_btn_deactivate",
        ):
            try:
                deactivate_fn(main_connection_id, sc, cfg_id)
                st.success(f"AI-config #{cfg_id} gedeactiveerd.")
                st.rerun()
            except Exception as e:
                st.error(f"Deactiveren mislukt: {e}")

    # Activate
    with col3:
        if st.button(
            "✅ Activate",
            use_container_width=True,
            disabled=is_active_now,
            key=f"{key_prefix}_{sc}_btn_activate",
        ):
            try:
                activate_fn(main_connection_id, sc, cfg_id)
                st.success(f"AI-config #{cfg_id} geactiveerd.")
                st.rerun()
            except Exception as e:
                st.error(f"Activeren mislukt: {e}")



def _json_text_to_dict(raw: str) -> dict:
    try:
        return json.loads(raw) if raw.strip() else {}
    except Exception as e:
        st.error(f"Settings is geen geldige JSON: {e}")
        st.stop()

# ------------------ AI create/edit prompts ------------------

def prompt_new_ai_config(main_connection_id: int, short_code: str):
    sc = (short_code or "").strip().lower()
    with st.form(f"new_ai_cfg_form_{main_connection_id}_{sc}"):
        st.subheader(f"New AI-config ({sc.upper()})")
        
 
        # Kern (NOT NULL of belangrijk)
        name            = st.text_input("Naam", placeholder="Bijv. 'Default AI profile'")
        analysis_type   = st.text_input("Analysis type", placeholder="Bijv. 'lineage', 'profiling', 'summary'")
        model_provider  = st.text_input("Model provider", value="openai")
        model_name      = st.text_input("Model name", placeholder="Bijv. 'gpt-4o-mini'")
        model_version   = st.text_input("Model version (optioneel)", value="")

        # GenAI parameters
        cols = st.columns(3)
        with cols[0]:
            temperature = st.number_input("temperature (0–2)", min_value=0.0, max_value=2.0, value=0.0, step=0.1)
            top_p       = st.number_input("top_p (0–1]", min_value=0.0, max_value=1.0, value=1.0, step=0.05)
        with cols[1]:
            max_tokens  = st.number_input("max_tokens", min_value=1, value=2048, step=64)
            freq_pen    = st.number_input("frequency_penalty (-2..2)", min_value=-2.0, max_value=2.0, value=0.0, step=0.1)
        with cols[2]:
            pres_pen    = st.number_input("presence_penalty (-2..2)", min_value=-2.0, max_value=2.0, value=0.0, step=0.1)

        # Orchestratie
        colx = st.columns(3)
        with colx[0]:
            runner_concurrency = st.number_input("runner_concurrency", min_value=1, value=2, step=1)
        with colx[1]:
            propagation_mode = st.selectbox("propagation_mode", ["auto", "manual"], index=0)
        with colx[2]:
            overwrite_policy = st.selectbox("overwrite_policy", ["fill_empty", "overwrite_if_confident", "never"], index=0)

        coly = st.columns(3)
        with coly[0]:
            confidence_threshold = st.number_input("confidence_threshold (0–1)", min_value=0.0, max_value=1.0, value=0.700, step=0.01)
        with coly[1]:
            respect_human_locks = st.checkbox("respect_human_locks", value=True)
        with coly[2]:
            is_active = st.checkbox("Actief", value=True)

        # Type-specifieke filters
        st.markdown("---")
        if sc == "dw":
            database_filter = st.text_input("Database filter", value="")
            schema_filter   = st.text_input("Schema filter", value="")
            table_filter    = st.text_input("Tabel filter", value="")
        elif sc == "pbi":
            workspace_filter   = st.text_input("Workspace filter", value="")
            model_filter       = st.text_input("Model filter", value="")
            table_filter       = st.text_input("Tabel filter", value="")
            include_tmdl       = st.checkbox("Include TMDL", value=True)

            include_model_bim  = st.checkbox("Include model.bim", value=False)
            respect_persp      = st.checkbox("Respect perspectives", value=True)
        else:  # dl
            path_filter       = st.text_input("Path filter", value="")
            format_whitelist  = st.text_input("Format whitelist", value="")
            partition_filter  = st.text_input("Partition filter", value="")
            include_hidden    = st.checkbox("Include hidden files", value=False)
            infer_schema      = st.checkbox("Infer schema", value=True)

        st.markdown("---")
        model_profile = st.text_input("Model profile (optioneel)", value="")
        prompt_pack   = st.text_input("Prompt pack (optioneel)", value="")
        notes         = st.text_area("Notities (optioneel)", value="", height=80)

        submitted = st.form_submit_button("Aanmaken")

    if not submitted:
        st.stop()

    # minimale validaties
    if not (name or "").strip():
        st.error("Naam is verplicht.")
        st.stop()
    if not (analysis_type or "").strip():
        st.error("analysis_type is verplicht.")
        st.stop()
    if not (model_name or "").strip():
        st.error("model_name is verplicht.")
        st.stop()

    base = {
        "name": name,
        "analysis_type": analysis_type,
        "model_provider": model_provider,
        "model_name": model_name,
        "model_version": model_version,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "top_p": top_p,
        "frequency_penalty": freq_pen,
        "presence_penalty": pres_pen,
        "runner_concurrency": runner_concurrency,
        "propagation_mode": propagation_mode,
        "overwrite_policy": overwrite_policy,
        "confidence_threshold": confidence_threshold,
        "respect_human_locks": respect_human_locks,
        "model_profile": model_profile,
        "prompt_pack": prompt_pack,
        "notes": notes,
        "is_active": is_active,
    }

    if sc == "dw":
        base.update({
            "database_filter": database_filter,
            "schema_filter": schema_filter,
            "table_filter": table_filter,
        })
    elif sc == "pbi":
        base.update({
            "workspace_filter": workspace_filter,
            "model_filter": model_filter,
            "table_filter": table_filter,
            "include_tmdl": include_tmdl,
            "include_model_bim": include_model_bim,
            "respect_perspectives": respect_persp,
        })
    else:
        base.update({
            "path_filter": path_filter,
            "format_whitelist": format_whitelist,
            "partition_filter": partition_filter,
            "include_hidden_files": include_hidden,
            "infer_schema": infer_schema,
        })

    return base  # deze dict voer je door build_ai_settings_for_type


def prompt_edit_ai_config(cfg: dict, short_code: str):
    sc = (short_code or "").strip().lower()
    # Voorinvulling uit cfg
    def _g(k, default=None): return cfg.get(k, default)

    with st.form(f"edit_ai_cfg_form_{cfg.get('id')}"):
        st.subheader(f"Bewerk AI-config #{cfg.get('id')} ({sc.upper()})")
        

        name           = st.text_input("Naam", value=_g("name") or _g("config_name") or "")
        analysis_type  = st.text_input("Analysis type", value=_g("analysis_type") or "")
        model_provider = st.text_input("Model provider", value=_g("model_provider") or "openai")
        model_name     = st.text_input("Model name", value=_g("model_name") or "")
        model_version  = st.text_input("Model version (optioneel)", value=_g("model_version") or "")

        cols = st.columns(3)
        with cols[0]:
            temperature = st.number_input("temperature (0–2)", min_value=0.0, max_value=2.0, value=float(_g("temperature") or 0.0), step=0.1)
            top_p       = st.number_input("top_p (0–1]", min_value=0.0, max_value=1.0, value=float(_g("top_p") or 1.0), step=0.05)
        with cols[1]:
            max_tokens  = st.number_input("max_tokens", min_value=1, value=int(_g("max_tokens") or 2048), step=64)
            freq_pen    = st.number_input("frequency_penalty (-2..2)", min_value=-2.0, max_value=2.0, value=float(_g("frequency_penalty") or 0.0), step=0.1)
        with cols[2]:
            pres_pen    = st.number_input("presence_penalty (-2..2)", min_value=-2.0, max_value=2.0, value=float(_g("presence_penalty") or 0.0), step=0.1)

        colx = st.columns(3)
        with colx[0]:
            runner_concurrency = st.number_input("runner_concurrency", min_value=1, value=int(_g("runner_concurrency") or 2), step=1)
        with colx[1]:
            propagation_mode = st.selectbox("propagation_mode", ["auto","manual"], index=["auto","manual"].index(_g("propagation_mode") or "auto"))
        with colx[2]:
            overwrite_policy = st.selectbox("overwrite_policy", ["fill_empty", "overwrite_if_confident", "never"], index=["fill_empty","overwrite_if_confident","never"].index(_g("overwrite_policy") or "fill_empty"))

        coly = st.columns(3)
        with coly[0]:
            confidence_threshold = st.number_input("confidence_threshold (0–1)", min_value=0.0, max_value=1.0, value=float(_g("confidence_threshold") or 0.700), step=0.01)
        with coly[1]:
            respect_human_locks = st.checkbox("respect_human_locks", value=bool(_g("respect_human_locks", True)))
        with coly[2]:
            is_active = st.checkbox("Actief", value=bool(_g("is_active", True)))

        st.markdown("---")
        if sc == "dw":
            database_filter = st.text_input("Database filter", value=_g("database_filter") or "")
            schema_filter   = st.text_input("Schema filter", value=_g("schema_filter") or "")
            table_filter    = st.text_input("Tabel filter", value=_g("table_filter") or "")
        elif sc == "pbi":
            workspace_filter   = st.text_input("Workspace filter", value=_g("workspace_filter") or "")
            model_filter       = st.text_input("Model filter", value=_g("model_filter") or "")
            table_filter       = st.text_input("Tabel filter", value=_g("table_filter") or "")
            include_tmdl       = st.checkbox("Include TMDL", value=bool(_g("include_tmdl", True)))
            include_model_bim  = st.checkbox("Include model.bim", value=bool(_g("include_model_bim", False)))
            respect_persp      = st.checkbox("Respect perspectives", value=bool(_g("respect_perspectives", True)))
        else:
            path_filter       = st.text_input("Path filter", value=_g("path_filter") or "")
            format_whitelist  = st.text_input("Format whitelist", value=_g("format_whitelist") or "")
            partition_filter  = st.text_input("Partition filter", value=_g("partition_filter") or "")
            include_hidden    = st.checkbox("Include hidden files", value=bool(_g("include_hidden_files", False)))
            infer_schema      = st.checkbox("Infer schema", value=bool(_g("infer_schema", True)))

        st.markdown("---")
        model_profile = st.text_input("Model profile (optioneel)", value=_g("model_profile") or "")
        prompt_pack   = st.text_input("Prompt pack (optioneel)", value=_g("prompt_pack") or "")
        notes         = st.text_area("Notities (optioneel)", value=_g("notes") or "", height=80)

        submitted = st.form_submit_button("Wijzigingen opslaan")

    if not submitted:
        return None  # caller interpreteert als "geen wijzigingen"

    patch = {
        "name": name,
        "analysis_type": analysis_type,
        "model_provider": model_provider,
        "model_name": model_name,
        "model_version": model_version,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "top_p": top_p,
        "frequency_penalty": freq_pen,
        "presence_penalty": pres_pen,
        "runner_concurrency": runner_concurrency,
        "propagation_mode": propagation_mode,
        "overwrite_policy": overwrite_policy,
        "confidence_threshold": confidence_threshold,
        "respect_human_locks": respect_human_locks,
        "model_profile": model_profile,
        "prompt_pack": prompt_pack,
        "notes": notes,
        "is_active": is_active,
    }
    if sc == "dw":
        patch.update({
            "database_filter": database_filter,
            "schema_filter": schema_filter,
            "table_filter": table_filter,
        })
    elif sc == "pbi":
        patch.update({
            "workspace_filter": workspace_filter,
            "model_filter": model_filter,
            "table_filter": table_filter,
            "include_tmdl": include_tmdl,
            "include_model_bim": include_model_bim,
            "respect_perspectives": respect_persp,
        })
    else:
        patch.update({
            "path_filter": path_filter,
            "format_whitelist": format_whitelist,
            "partition_filter": partition_filter,
            "include_hidden_files": include_hidden,
            "infer_schema": infer_schema,
        })

    return patch

def render_ai_config_picker_readonly(
    *,
    main_connection_id: int,
    short_code: str,
    fetch_configs,                       # Callable[[int, str], list[dict]]
    preselected_config_id: int | None = None,
    title: str = "Selecteer AI-config",
    active_only: bool = False,           # toon alleen actieve configs
    key_prefix: str = "ai_cfg_ro",
):
    """
    Read-only selectbox voor AI-configs (DW/PBI/DL). Retourneert de gekozen config (dict) of None.
    - Géén create/edit opties
    - Optioneel filtert op alleen actieve configs
    """
    sc = (short_code or "").strip().lower()
    st.subheader(title)

    configs = fetch_configs(main_connection_id, sc) or []
    if active_only:
        configs = [c for c in configs if bool(c.get("is_active"))]

    if not configs:
        st.info("Geen AI-configs beschikbaar voor deze connection.")
        return None

    # Sorteer: actief bovenaan, daarna op naam, dan ID
    def _sort_key(c):
        is_active = 1 if c.get("is_active") else 0
        name = (c.get("name") or c.get("config_name") or "").strip().lower()
        return (-is_active, name, int(c.get("id") or 0))

    configs.sort(key=_sort_key)

    by_id = {int(c["id"]): c for c in configs}
    ids = list(by_id.keys())

    # Bepaal index
    if preselected_config_id in by_id:
        index = ids.index(preselected_config_id)
    else:
        index = 0

    chosen_id = st.selectbox(
        "AI-config",
        options=ids,
        index=index,
        format_func=lambda oid: format_ai_cfg_label(by_id[oid], sc),
        key=f"{key_prefix}_{sc}_{main_connection_id}",
    )

    chosen = by_id[chosen_id]
    # Kleine bevestiging onder de dropdown
    st.caption(f"Geselecteerd: {format_ai_cfg_label(chosen, sc)}")

    return chosen

def render_deactivated_catalog_configs(
    *,
    main_connection_id: int,
    short_code: str,
    key_prefix: str = "catalog_deact",
    user_email: str | None = None,
):
    sc = (short_code or "").strip().lower()
    st.markdown("---")
    st.subheader("🗂️ Deactivated Catalog Configs")

    rows = list_deactivated_configs(main_connection_id, "catalog", sc)
    if not rows:
        st.caption("No deactivated catalog-configs.")
        return

    for r in rows:
        d = dict(r._mapping)
        header = f"#{d['id']} — {(d.get('config_name') or '').strip()} [{sc}]"
        with st.expander(header):
            st.markdown(f"**Updated:** {d.get('updated_at') or '—'}")
            st.markdown(f"**Soft-deleted?:** {'Ja' if d.get('deleted_at') else 'Nee'}")

            reason = st.text_input(
                f"Reason for soft delete #{d['id']}",
                key=f"{key_prefix}_reason_{sc}_{d['id']}"
            )
            if st.button(f"🧨 delete #{d['id']}", key=f"{key_prefix}_btn_{sc}_{d['id']}"):
                if not reason.strip():
                    st.error("Provide reason for delete.")
                else:
                    soft_delete_config(
                        d["id"], "catalog", sc,
                        user_email=user_email or st.session_state.get("user_email", "webapp"),
                        reason=reason.strip(),
                    )
                    st.success(f"Catalog-config #{d['id']} soft-deleted.")
                    st.rerun()

def render_deactivated_ai_configs(
    *,
    main_connection_id: int,
    short_code: str,
    key_prefix: str = "ai_deact",
    user_email: str | None = None,
):
    sc = (short_code or "").strip().lower()
    st.markdown("---")
    st.subheader("🧠 Deactivated AI Configs")

    rows = list_deactivated_configs(main_connection_id, "ai", sc)
    if not rows:
        st.caption("No deactivated AI-configs.")
        return

    for r in rows:
        d = dict(r._mapping)
        header = f"#{d['id']} — {(d.get('config_name') or '').strip()} [ai/{sc}]"
        with st.expander(header):
            st.markdown(f"**Updated:** {d.get('updated_at') or '—'}")
            st.markdown(f"**Soft-deleted?:** {'Ja' if d.get('deleted_at') else 'Nee'}")

            reason = st.text_input(
                f"Reason for soft delete #{d['id']}",
                key=f"{key_prefix}_reason_{sc}_{d['id']}"
            )
            if st.button(f"🧨 delete #{d['id']}", key=f"{key_prefix}_btn_{sc}_{d['id']}"):
                if not reason.strip():
                    st.error("Provide reason for delete.")
                else:
                    soft_delete_config(
                        d["id"], "ai", sc,
                        user_email=user_email or st.session_state.get("user_email", "webapp"),
                        reason=reason.strip(),
                    )
                    st.success(f"AI-config #{d['id']} soft-deleted.")
                    st.rerun()


# def render_catalog_config_help(conn_row: dict | None = None, short_code: str | None = None):
#     """
#     Shows concise guidance below the catalog config picker.
#     Pass the selected main connection row if available to tailor examples by category.
#     """
#     st.caption("You need **at least one** active catalog configuration to build your catalog.")

#     with st.popover("ℹ️ What is a catalog configuration?"):
#         st.markdown(
#             """
# A catalog configuration defines a **broad scope** for discovery (what exists in the source).
# An AI configuration builds **on top of the catalog** and uses narrower filters for targeted analyses.
#             """
#         )

#         # Category-aware examples
#         dcat = (conn_row or {}).get("data_source_category", "").upper() if conn_row else ""
#         sc   = (short_code or (conn_row or {}).get("short_code") or "").lower()

#         if dcat == "DATABASE_DATAWAREHOUSE" or sc == "dw":
#             st.markdown(
#                 """
# **Typical filters (Database/Data Warehouse)**  
# - `database_filter` — e.g., `sales_*`  
# - `schema_filter` — e.g., `*`  
# - `table_filter` — e.g., `fact_*`
#                 """
#             )
#         elif dcat == "POWERBI" or sc == "pbi":
#             st.markdown(
#                 """
# **Typical filters (Power BI)**  
# - `workspace_filter` — target one or more workspaces  
# - `model_filter` — select specific models  
# - `table_filter` — narrow to targeted tables
#                 """
#             )
#         elif dcat == "DATA_LAKE" or sc == "dl":
#             st.markdown(
#                 """
# **Typical filters (Data Lake)**  
# - `path_filter` — e.g., `/bronze/...`  
# - `format_whitelist` — e.g., `parquet,csv`  
# - `partition_filter` — include/exclude partitions
#                 """
#             )

#     with st.expander("Strategy"):
#         st.markdown(
#             """
# 1) Start broad with your catalog (wide filters) to discover structure.  
# 2) Validate coverage (schemas, models, paths).  
# 3) Add **AI configs** with **narrow** filters for each analysis use case.
#             """
#         )

# def render_ai_config_help(conn_row: dict | None = None, short_code: str | None = None):
#     st.caption("An **AI configuration** runs targeted analyses **on top of** your catalog. Keep the scope narrow to control compute.")

#     with st.popover("ℹ️ What is an AI configuration?"):
#         st.markdown(
#             """
# AI configurations define a **narrow scope** for analysis and may be compute-intensive.
# They rely on the catalog to understand what’s available and then focus on a subset.
#             """
#         )

#         dcat = (conn_row or {}).get("data_source_category", "").upper() if conn_row else ""
#         sc   = (short_code or (conn_row or {}).get("short_code") or "").lower()

#         if dcat == "DATABASE_DATAWAREHOUSE" or sc == "dw":
#             st.markdown(
#                 """
# **Examples (Database/Data Warehouse)**  
# - Analyze only schemas like `finance_*`  
# - Focus on tables with a specific prefix  
# - Limit to a single database for cost control
#                 """
#             )
#         elif dcat == "POWERBI" or sc == "pbi":
#             st.markdown(
#                 """
# **Examples (Power BI)**  
# - Analyze a single workspace  
# - Target one model (e.g., `Sales`)  
# - Limit to 2–3 model tables for a quick run
#                 """
#             )
#         elif dcat == "DATA_LAKE" or sc == "dl":
#             st.markdown(
#                 """
# **Examples (Data Lake)**  
# - Scope to a single path (e.g., `/bronze/customers/`)  
# - Restrict to Parquet only  
# - Analyze one partition first
#                 """
#             )

#     with st.expander("Strategy"):
#         st.markdown(
#             """
# - Use the catalog for **breadth**, and AI for **depth**.  
# - Start small (one schema/path/model), then expand.  
# - Narrow scopes keep runs fast and cost-effective.
#             """
#         )


def render_catalog_config_help(conn_or_sc=None, short_code: str | None = None):
    """
    One compact popover with everything users need.
    Accepts either (conn_row, sc) or just sc ('dw'|'pbi'|'dl').
    """
    # Normalize inputs
    if isinstance(conn_or_sc, dict):
        conn_row = conn_or_sc
        sc = (short_code or conn_row.get("short_code") or "").strip().lower()
        dcat = (conn_row.get("data_source_category") or "").upper()
    else:
        conn_row = None
        sc = (conn_or_sc or short_code or "").strip().lower()
        dcat = ""

    with st.popover("ℹ️ What is a Catalog configuration?"):
        st.markdown(
            """
A **Catalog configuration** defines a **broad discovery scope** (what exists in the source).
You typically start **broad** here, then add **AI configurations** with a **narrow** scope for targeted analysis.
            """
        )

        st.markdown(
            """
**Strategy**
1. Create a **broad** catalog config to discover structure.
2. Validate coverage (schemas, models, paths).
3. Add **AI configs** with **narrow** filters per use case.
            """
        )

        st.caption("You need **at least one** active catalog config to build a catalog.")

def render_ai_config_help(conn_or_sc=None, short_code: str | None = None):
    """
    One compact popover with all AI guidance (+ model parameters).
    Accepts either (conn_row, sc) or just sc ('dw'|'pbi'|'dl').
    """
    # Normalize inputs
    if isinstance(conn_or_sc, dict):
        conn_row = conn_or_sc
        sc = (short_code or conn_row.get("short_code") or "").strip().lower()
        dcat = (conn_row.get("data_source_category") or "").upper()
    else:
        conn_row = None
        sc = (conn_or_sc or short_code or "").strip().lower()
        dcat = ""

    with st.popover("ℹ️ What is an AI configuration?"):
        st.markdown(
            """
An **AI configuration** runs **targeted** analyses **on top of** your catalog.
AI can be compute-intensive — keep scopes **small and specific**.
            """
        )

        # Type-aware examples
        if dcat == "DATABASE_DATAWAREHOUSE" or sc == "dw":
            st.markdown(
                """
**Examples (Database/Data Warehouse)**
- Analyze only schemas like `finance_*`
- Focus on a handful of tables (e.g., `fact_*`)
- Limit to a single database for cost control
                """
            )
        elif dcat == "POWERBI" or sc == "pbi":
            st.markdown(
                """
**Examples (Power BI)**
- Analyze a single workspace
- Target one model (e.g., `Sales`)
- Limit to 2–3 tables for a quick run
                """
            )
        elif dcat == "DATA_LAKE" or sc == "dl":
            st.markdown(
                """
**Examples (Data Lake)**
- Scope to one directory (e.g., `/bronze/customers/`)
- Restrict to Parquet only
- Analyze one partition first
                """
            )

        st.markdown(
            """
**Strategy**
- Use the **catalog** for breadth; **AI** for depth.
- Start with a tiny scope, then expand.
- Narrow scopes are faster and cheaper.
            """
        )

#         st.markdown("---")
#         st.markdown("#### Model parameters (quick reference)")
#         st.markdown(
#             """
# - **Temperature** — randomness. `0.0` = deterministic; `0.7` = more variety.  
# - **Max tokens** — output length cap (more = longer, costlier).  
# - **Top-p** — “nucleus” sampling; lower narrows choices.  
# - **Frequency penalty** — reduces repetition (−2.0..2.0).  
# - **Presence penalty** — encourages new topics (−2.0..2.0).  
# - **Provider / model** — choose the model family/version suited to your task.
#             """
#         )
#         st.caption("Tip: use **low temperature** (0.0–0.3) for analysis; raise it for creative descriptions.")


# def render_main_connection_help():
#     st.caption("A **main connection** is the base data source. Catalog and AI configurations are attached to it.")

#     with st.popover("ℹ️ What’s a main connection?"):
#         st.markdown(
#             """
# A main connection defines **which system** we connect to and **how**.
# You first pick the main connection, then create catalog and/or AI configurations on top.

# **Data source categories & examples**
# - **DATABASE/DATAWAREHOUSE** — relational engines (e.g., Azure SQL Server (T-SQL), PostgreSQL).
# - **POWERBI** — Power BI artifacts (Local PBIP/TMDL, Service workspaces).
# - **DATA_LAKE (IN DEVELOPMENT)** — file/object storage (e.g., ADLS, S3, GCS).

# **Why this order?**
# 1) Choose a main connection (the system).  
# 2) Create a **catalog configuration** (broad scope) to discover what exists.  
# 3) Add **AI configurations** (narrow scope) for specific analyses that are more compute-intensive.
#             """
#         )

#     with st.expander("Good practices"):
#         st.markdown(
#             """
# - Create **one main connection per source** (e.g., one PostgreSQL database, one S3 bucket).
# - Keep connection metadata tidy (display name, owner, notes).
# - Prefer clear naming so downstream configs are easy to find.
#             """
#         )



def render_main_connection_help():
    with st.popover("ℹ️ What is a main connection?"):
        st.markdown(
            """
A **main connection** is the base data source. Catalog and AI configurations are attached to it.

**What it defines**
- **Which system** we connect to (engine / platform)
- **How** we connect (credentials, endpoint, folder, workspace)

**Data source categories & examples**
- **DATABASE_DATAWAREHOUSE** — relational engines (e.g., Azure SQL Server (T-SQL), PostgreSQL)
- **POWERBI** — Power BI artifacts (Local PBIP/TMDL, Service workspaces)
- **DATA_LAKE** *(in development)* — file/object storage (e.g., ADLS, S3, GCS)

**Why this order?**
1. Choose a **main connection** (the system)  
2. Create a **catalog configuration** (broad scope) to discover what exists  
3. Add **AI configurations** (narrow scope) for targeted, compute-heavier analyses

**Good practices**
- Create **one main connection per source** (e.g., one PostgreSQL database, one S3 bucket)
- Keep metadata tidy (display name, owner, notes)
- Use clear names so downstream configs are easy to find
            """
        )

def describe_connection(conn_row: dict) -> str:
    """
    Returns: 'Display Name [connection_type / data_source_category] · Active/Inactive'
    """
    display = conn_row.get("display_name") or conn_row.get("connection_name") or "Connection"
    ctype = conn_row.get("connection_type", "")
    dcat  = conn_row.get("data_source_category", "")
    status = "🟢 Active" if conn_row.get("is_active") else "🔴 Inactive"
    return f"{display} [{ctype}/{dcat}] · {status}"


_CATEGORY_ORDER = {
    "DATABASE_DATAWAREHOUSE": 0,
    "POWERBI": 1,
    "DATA_LAKE": 2,
}

def render_connection_type_legend(
    *,
    title: str = "Currently supported connection types",
    active_only: bool = True,
    enable_filters: bool = True,
    height: Optional[int] = None,
    show_download: bool = False,
):
    st.markdown(f"### {title}")

    rows = fetch_connection_type_registry(active_only=active_only)
    if not rows:
        st.info("No connection types found in registry.")
        return

    df = pd.DataFrame(rows)

    # Stable sort: category then display_name
    df["_cat_order"] = df["data_source_category"].map(_CATEGORY_ORDER).fillna(999)
    df = df.sort_values(["_cat_order", "display_name"], kind="mergesort")

    # Optional on-screen filters
    if enable_filters:
        cols = st.columns(3)
        with cols[0]:
            cat_choices = sorted(df["data_source_category"].unique().tolist(), key=lambda c: _CATEGORY_ORDER.get(c, 999))
            selected_cats = st.multiselect("Filter by category", options=cat_choices, default=cat_choices, key="ctr_filter_cat")
        with cols[1]:
            active_filter = st.selectbox("Active filter", options=["Active only", "All"], index=0 if active_only else 1, key="ctr_filter_active")
        with cols[2]:
            search = st.text_input("Search display name", key="ctr_filter_search", placeholder="Type to filter…")

        # Apply filters
        if selected_cats:
            df = df[df["data_source_category"].isin(selected_cats)]
        if active_filter == "Active only":
            df = df[df["is_active"] == True]  # noqa: E712
        if search.strip():
            s = search.strip().lower()
            df = df[df["display_name"].str.lower().str.contains(s)]

    # Final column order
    show_cols = [
        "display_name",
        "connection_type",
        "data_source_category",
        "short_code",
        "is_active",
        "created_at",
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    st.dataframe(
        df[show_cols],
        use_container_width=True,
        hide_index=True,
        height=height,
    )

    if show_download:
        csv = df[show_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download types (CSV)",
            data=csv,
            file_name="connection_type_registry.csv",
            mime="text/csv",
        )

def render_catalog_config_field_help(sc: str):
    """
    Single popover with all catalog-config guidance, tailored per type (dw/pbi/dl).
    Call inside your create/edit form, just under the title.
    """
    sc = (sc or "").strip().lower()
    with st.popover("ℹ️ Catalog settings explained"):

        if sc == "dw":
            st.markdown(
                """
**Database / Data Warehouse**
- `database_filter` — e.g., `sales_*`  
- `schema_filter` — e.g., `*`  
- `table_filter` — e.g., `fact_*`

> Tip: Use `*` initially and refine later.
                """
            )
        elif sc == "pbi":
            st.markdown(
                """
**Power BI**
- `workspace_filter` — one or more workspaces  
- `model_filter` — specific PBIP/TMDL/BIM models  
- `table_filter` — targeted tables within a model

> Tip: Start wide to inventory all available content.
                """
            )
        elif sc == "dl":
            st.markdown(
                """
**Data Lake**
- `path_filter` — e.g., `/bronze/customers/`  
- `format_whitelist` — e.g., `parquet,csv`  
- `partition_filter` — include/exclude partitions

> Tip: Begin broad; refine if scans become too large.
                """
            )


import streamlit as st
import pandas as pd

def render_ai_config_field_help(sc: str, *, state_prefix: str | None = None):
    """
    Per-parameter guidance for AI config fields.
    Wrapped in an expander; user can open/close on demand.
    Supports propagation modes: auto | manual
    """

    # Defaults (shown in detail text)
    defaults = {
        "temperature": 0.0,
        "max_tokens": 2048,
        "top_p": 1.0,
        "frequency_penalty": 0.0,
        "presence_penalty": 0.0,
        "propagation_mode": "auto",  # recommended default
    }

    # Parameters (easy to extend later)
    options = [
        "Temperature",
        "Max tokens",
        "Top-p",
        "Frequency penalty",
        "Presence penalty",
        "Propagation mode",
        "Overwrite policy",
    ]

    # Stable key (persists selection across reruns)
    sel_key = f"{state_prefix or 'ai_cfg'}_param_selector"
    if sel_key not in st.session_state:
        st.session_state[sel_key] = options[0]

    with st.expander("ℹ️ AI configuration field help", expanded=False):
        choice = st.selectbox(
            "Select a parameter",
            options=options,
            key=sel_key,
            help="Pick a field to see meaning, guidelines, and defaults.",
        )

        # ---- Renderers ----
        if choice == "Temperature":
            st.info(
                f"""**Temperature** controls randomness.

- 0.0–0.3 → predictable, analytical  
- 0.4–0.8 → more variety for descriptions/summaries  
- >1.0 → very random, avoid for analytics  

**Default:** **{defaults['temperature']}**"""
            )

        elif choice == "Max tokens":
            st.info(
                f"""**Max tokens** caps output length (≈ 1 token ~ 3–4 chars).

- 512–1024 → short and fast  
- 2048 → longer analysis/descriptions  
- >4000 → only if necessary (slower & costlier)  

**Default:** **{defaults['max_tokens']}**"""
            )

        elif choice == "Top-p":
            st.info(
                f"""**Top-p** (“nucleus sampling”) narrows choices to the most likely tokens.

- 1.0 → safe default  
- 0.8–0.9 → more focused outputs  
- Avoid combining high top-p with high temperature  

**Default:** **{defaults['top_p']}**"""
            )

        elif choice == "Frequency penalty":
            st.info(
                f"""**Frequency penalty** discourages repeating words/phrases. Range: −2.0 … 2.0.

- 0.0 → default  
- 0.5–1.0 → if outputs repeat themselves  

**Default:** **{defaults['frequency_penalty']}**"""
            )

        elif choice == "Presence penalty":
            st.info(
                f"""**Presence penalty** encourages new topics. Range: −2.0 … 2.0.

- 0.0 → default  
- 0.5–1.0 → if outputs get stuck on one theme  
- Too high may drift off-topic  

**Default:** **{defaults['presence_penalty']}**"""
            )

        elif choice == "Propagation mode":
            st.info(
                f"""**Propagation mode** controls if results are written to descriptions immediately.

- **auto** *(default)* → results are **written directly** to descriptions (obeying **Overwrite policy**). Each new/overwritten value is flagged as **unreviewed**; users can later mark as **reviewed** or **edited**.
- **manual** → results are **stored only** in the analysis store. No automatic write. You can later **export → review → apply** (see *Manual propagate*).

**Recommended default:** **{defaults['propagation_mode']}**"""
            )

            st.caption("Propagation × Overwrite matrix (what actually gets written):")
            matrix = pd.DataFrame(
                [
                    ["auto", "fill_empty", "Writes only into empty fields; existing text untouched (status = unreviewed)."],
                    ["auto", "overwrite_if_confident", "Overwrites when confidence ≥ threshold (status = unreviewed); otherwise leaves as-is."],
                    ["auto", "never", "Never overwrites; only empty fields can be filled (status = unreviewed)."],
                    ["manual", "any", "Writes nothing now; results remain in analysis store until you export/apply."],
                ],
                columns=["Propagation mode", "Overwrite policy", "Effect"],
            )
            st.dataframe(matrix, hide_index=True, use_container_width=True)

        elif choice == "Overwrite policy":
            st.info(
                """**Overwrite policy** (applies when `propagation_mode = auto`):

- **fill_empty** *(safe)* → write only if the target field is empty; existing text remains untouched.
- **overwrite_if_confident** → replace existing text **only** if model confidence ≥ threshold (e.g., 0.8).
- **never** → never replace existing text; only empty fields can be filled.

**Note:** All values written in **auto** mode are flagged as **unreviewed**; a user can later mark them as reviewed/edited."""
            )
