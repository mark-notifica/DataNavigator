# pages/01_Connection_manager_v2.py
import app_boot  # zet ROOT en ROOT/webapp op sys.path

import os
from pathlib import Path
from typing import Optional,Callable

import streamlit as st
import sqlalchemy as sa
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from data_catalog.db import q_all,q_one,exec_tx


# Helpers/UX uit jouw project
from shared_utils import (
    apply_compact_styling,
    test_main_connection,
    get_main_connection_test_status
)

from data_catalog.connection_handler import (
    # connections (main)
    load_mapping_df,
    list_connections_df,
    upsert_connection_row,
    set_connection_last_test_result,
    clear_connection_last_test_result,
    deactivate_connection,
    reactivate_connection,
    soft_delete_connection,

    # details (voor inflate/legacy)
    fetch_secret,
    fetch_dw_details,
    fetch_pbi_local_details,
    # upserts als je die pagina ook gebruikt om details te bewaren:
    upsert_dw_details,
    upsert_pbi_local_details,
    upsert_pbi_service_details,
    upsert_dl_details,
)

from data_catalog.config_handler import (
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




# ---------------- Page config & styling ----------------
st.set_page_config(
    page_title="Connection Manager",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_compact_styling()

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


# # ---------------- Inflate: maak “oude” velden voor testers ----------------


def to_legacy_type(type_code: str) -> str:
    mapping = {
        "POSTGRESQL": "PostgreSQL",
        "AZURE_SQL_SERVER": "Azure SQL Server",
        "POWERBI_LOCAL": "Power BI Semantic Model",
        "POWERBI_SERVICE": "Power BI Semantic Model",
    }
    return mapping.get(type_code, type_code)

def inflate_connection_row(base_row: dict) -> dict:
    conn = dict(base_row)
    conn.update({"host": None, "port": None, "username": None, "password": None, "folder_path": None})

    # sc = conn.get("short_code")
    # ctype = conn.get("connection_type")

    # if sc == "dw":
    #     d = fetch_dw_details(conn["id"])
    #     if d:
    #         conn["host"] = d.get("host")
    #         conn["port"] = d.get("port")
    #         conn["username"] = d.get("username")
    #         secret_ref = d.get("secret_ref")
    #         conn["password"] = fetch_secret(secret_ref) if secret_ref else None

    # elif sc == "pbi" and ctype == "POWERBI_LOCAL":
    #     d = fetch_pbi_local_details(conn["id"])
    #     if d:
    #         conn["folder_path"] = d.get("folder_path")

    # conn["name"] = conn.get("connection_name")
    # conn["connection_type_label"] = to_legacy_type(ctype or "")
    # return conn

    sc = conn.get("short_code")         # bv. "dw" | "pbi" | "dl"
    ctype = conn.get("connection_type") # bv. "POWERBI_LOCAL" | "POWERBI_SERVICE"

    if sc == "dw":
        d = fetch_dw_details(conn["id"])
        if d:
            conn["host"] = d.get("host")
            conn["port"] = d.get("port")
            conn["username"] = d.get("username")
            # password via secret vault
            secret_ref = d.get("secret_ref")
            conn["password"] = fetch_secret(secret_ref) if secret_ref else None

    elif sc == "pbi":
        if ctype == "POWERBI_LOCAL":
            d = fetch_pbi_local_details(conn["id"])
            if d:
                conn["folder_path"] = d.get("folder_path")
        elif ctype == "POWERBI_SERVICE":
            # testers kunnen hier eventueel tenant/auth tonen; geen host/port nodig
            pass

    elif sc == "dl":
        # testers verwachten hier geen host/port; niets toe te voegen
        pass

    # compat met oude testers: "name" key
    conn["name"] = conn.get("connection_name")

    # label-variant voor UX
    conn["connection_type_label"] = to_legacy_type(ctype or "")

    return conn

# ---------------- Shared state
if "selected_conn_id" not in st.session_state:
    st.session_state.selected_conn_id = None

#-----------------------
# HELPERS  
# ----------------------

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
        "Select an active connection.",
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
    from data_catalog.config_handler import (
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

#-----------------------
# UI -------------------   
# ----------------------


st.title("🔌 Connection Manager")


# 0) Mapping laden (registry)
mapping_df = load_mapping_df()
if mapping_df.empty:
    st.error("The mapping-table `config.connection_type_registry` is empty or not reachable.")
    st.stop()

tab_mc, tab_cc, tab_ac = st.tabs(["Main connection", "Catalog configuration", "AI configuration"])

# ======================================================================================
# TAB 1: MAIN CONNECTION
# ======================================================================================
with tab_mc:

    # ---------------- Connection selectie ----------------
    # 1) Ophalen van niet-soft-deleted connections
    df = list_connections_df()  # bevat al alleen c.deleted_at IS NULL

    # 2) Filteroptie: alleen actieve (optioneel)
    show_active_only = st.toggle("Only show active connections", value=False, key="conn_filter_active_only")
    if show_active_only and not df.empty:
        df = df[df["is_active"] == True]

    if df.empty:
        st.info("No (valid) connections to manage. Create a new connection first.")
        selected_row = None
        conn_id = None
        short_code = None
    else:
        # 3) Picker op de pagina (incl. optie 'Nieuwe verbinding')
        NEW_SENTINEL = {"id": None, "connection_name": "➕ New connection"}
        options = [NEW_SENTINEL] + df.to_dict(orient="records")

        def _fmt_conn(r: dict) -> str:
            if r.get("id") is None:
                return r["connection_name"]
            status = "🟢 Active" if r.get("is_active") else "🔴 Inactive"
            sc = r.get("short_code", "")
            ctype = r.get("connection_type", "")
            return f"#{r['id']} — {r['connection_name']}  [{ctype}/{sc}] · {status}"

        selected_row = st.selectbox(
            "Select connection of create new",
            options=options,
            format_func=_fmt_conn,
            key="cm_conn_select",
            index=0  # default: Nieuwe verbinding
        )

        # 4) Afleidingen voor vervolgsecties
        if selected_row.get("id") is not None:
            conn_id    = int(selected_row["id"])
            short_code = selected_row.get("short_code")
            st.caption(
                f"**Gekozen:** {conn_id} · {selected_row['connection_name']}"
                # f"{'🟢 Active' if selected_row.get('is_active') else '🔴 Inactive'}"
            )
        else:
            conn_id = None
            short_code = None
            st.caption("Create new connection…")

    st.divider()


    # ---------------- Basis (buiten de form, zodat type-wissel direct rerunt) ----------------
    st.markdown("## Main connection")

    # Opties uit mapping; we tonen alleen display_name
    mapping_opts = mapping_df.to_dict(orient="records")

    def _default_idx_for_selected():
        if selected_row and selected_row.get("id") is not None:
            for i, r in enumerate(mapping_opts):
                if r["connection_type"] == selected_row["connection_type"]:
                    return i
        return 0

    # Naam (blijft bewerkbaar)
    st.text_input(
        "Connection name *",
        value=(selected_row["connection_name"] if (selected_row and selected_row.get("id") is not None) else ""),
        key="conn_name",
    )

    # Status (read-only)
    # def _status_badge(active: bool) -> str:
    #     return "🟢 Active" if active else "🔴 Inactive"

    # if selected_row and selected_row.get("id") is not None:
    #     st.markdown(f"**Status:** {_status_badge(bool(selected_row['is_active']))}")
    # else:
    #     st.markdown("**Status:** (new) wordt standaard **Active** aangemaakt.")

    # Type-selectie (alleen display_name tonen)
    choice_row = st.selectbox(
        "Connection type *",
        options=mapping_opts,
        index=_default_idx_for_selected(),
        format_func=lambda r: r["display_name"],
        key="conn_type_row",
    )

    connection_type      = st.session_state["conn_type_row"]["connection_type"]
    short_code           = st.session_state["conn_type_row"]["short_code"]            # 'dw' | 'pbi' | 'dl'
    data_source_category = st.session_state["conn_type_row"]["data_source_category"]
    name                 = (st.session_state.get("conn_name") or "").strip()

    st.divider()

    # ---------------- Details + Save (binnen de form) ----------------
    with st.form("conn_form", clear_on_submit=False):
        st.markdown(f"### Details — **{short_code.upper()}**  ·  _{connection_type}_")

        # Prefill alleen als het gekozen type gelijk is aan het bestaande type
        pre = {}
        if selected_row and selected_row.get("id") is not None and selected_row["connection_type"] == connection_type:
            pre = inflate_connection_row(selected_row)

        dw_vals = {}
        pbi_local_vals = {}
        pbi_service_vals = {}
        dl_vals = {}

        if short_code == "dw":
            col1, col2 = st.columns(2)
            dw_vals["host"] = col1.text_input("Host *", value=pre.get("host") or "")
            dw_vals["port"] = col2.text_input("Port", value=str(pre.get("port") or ""))

            dw_vals["default_database"] = st.text_input("Default database (optioneel)", value=pre.get("default_database") or "")

            colu, cols = st.columns(2)
            dw_vals["username"] = colu.text_input("Username", value=pre.get("username") or "")
            dw_vals["ssl_mode"] = cols.selectbox(
                "SSL mode",
                ["", "require", "disable"],
                index=(["", "require", "disable"].index(pre.get("ssl_mode")) if pre.get("ssl_mode") in ["", "require", "disable"] else 1)
            )
            dw_vals["password_plain"] = st.text_input(
                "Password (saved as secret – keep empty not to change.)",
                type="password",
                value=""
            )

        elif short_code == "pbi":
            if connection_type == "POWERBI_LOCAL":
                pbi_local_vals["folder_path"] = st.text_input(
                    "Project root (PBIP/TMDL folder)",
                    value=pre.get("folder_path") or ""
                )
            elif connection_type == "POWERBI_SERVICE":
                col1, col2 = st.columns(2)
                pbi_service_vals["tenant_id"] = col1.text_input("Tenant ID", value=pre.get("tenant_id") or "")
                pbi_service_vals["auth_method"] = col2.selectbox(
                    "Auth method", ["DEVICE_CODE", "CLIENT_SECRET", "MANAGED_ID"],
                    index=(["DEVICE_CODE", "CLIENT_SECRET", "MANAGED_ID"].index(pre.get("auth_method")) if pre.get("auth_method") in ["DEVICE_CODE", "CLIENT_SECRET", "MANAGED_ID"] else 0)
                )
                col3, col4 = st.columns(2)
                pbi_service_vals["client_id"] = col3.text_input("Client ID", value=pre.get("client_id") or "")
                pbi_service_vals["secret_value"] = col4.text_input(
                    "Client Secret (als secret opgeslagen – leeg laten om niet te wijzigen)",
                    type="password",
                    value=""
                )
                pbi_service_vals["default_workspace_id"] = st.text_input("Default Workspace ID (optioneel)", value=pre.get("default_workspace_id") or "")
                pbi_service_vals["default_workspace_name"] = st.text_input("Default Workspace Name (optioneel)", value=pre.get("default_workspace_name") or "")
            else:
                st.info("Voor dit PBI-type is nog geen detailformulier gedefinieerd.")

        elif short_code == "dl":
            col1, col2 = st.columns(2)
            dl_vals["storage_type"] = col1.selectbox(
                "Storage type", ["ADLS", "S3", "GCS"],
                index=(["ADLS", "S3", "GCS"].index(pre.get("storage_type")) if pre.get("storage_type") in ["ADLS", "S3", "GCS"] else 0)
            )
            dl_vals["auth_method"] = col2.selectbox(
                "Auth method", ["ACCESS_KEY", "SAS", "MSI", "SERVICE_PRINCIPAL"],
                index=(["ACCESS_KEY", "SAS", "MSI", "SERVICE_PRINCIPAL"].index(pre.get("auth_method")) if pre.get("auth_method") in ["ACCESS_KEY", "SAS", "MSI", "SERVICE_PRINCIPAL"] else 0)
            )
            dl_vals["endpoint_url"] = st.text_input("Endpoint URL (optioneel)", value=pre.get("endpoint_url") or "")
            col3, col4 = st.columns(2)
            dl_vals["bucket_or_container"] = col3.text_input("Bucket/Container (optioneel)", value=pre.get("bucket_or_container") or "")
            dl_vals["base_path"] = col4.text_input("Base path (optioneel)", value=pre.get("base_path") or "")
            dl_vals["access_key_or_secret"] = st.text_input(
                "Access key / Secret (als secret opgeslagen – leeg laten om niet te wijzigen)",
                type="password",
                value=""
            )

        submitted = st.form_submit_button("💾 Save", type="primary")

        if submitted:
            if not name:
                st.error("Connection name is manadatory.")
                st.stop()

            # 1) Upsert connection (trigger zet data_source_category + short_code)
            new_id = upsert_connection_row(
                conn_id=(int(selected_row["id"]) if (selected_row and selected_row.get("id") is not None) else None),
                connection_name=name,
                connection_type=connection_type
            )

            # 2) Details per short_code (met kleine normalisaties/validatie)
            if short_code == "dw":
                # Normaliseer port -> int/None
                port_val = None
                if dw_vals["port"].strip():
                    try:
                        port_val = int(dw_vals["port"])
                    except ValueError:
                        st.error("Port must be a number.")
                        st.stop()

                if not dw_vals["host"].strip():
                    st.error("Host is mandatory for database/datawarehouse.")
                    st.stop()

                upsert_dw_details(
                    connection_id=new_id,
                    engine_type=connection_type,
                    host=dw_vals["host"].strip(),
                    port=port_val,
                    default_database=(dw_vals["default_database"] or "").strip(),
                    username=(dw_vals["username"] or "").strip(),
                    ssl_mode=(dw_vals["ssl_mode"] or "").strip(),
                    password_plain=(dw_vals["password_plain"] or "").strip(),
                )

            elif short_code == "pbi":
                if connection_type == "POWERBI_LOCAL":
                    upsert_pbi_local_details(
                        connection_id=new_id,
                        folder_path=(pbi_local_vals["folder_path"] or "").strip(),
                    )
                elif connection_type == "POWERBI_SERVICE":
                    upsert_pbi_service_details(
                        connection_id=new_id,
                        tenant_id=(pbi_service_vals["tenant_id"] or "").strip(),
                        client_id=(pbi_service_vals["client_id"] or "").strip(),
                        auth_method=pbi_service_vals["auth_method"],
                        secret_value=(pbi_service_vals["secret_value"] or "").strip(),
                        default_workspace_id=(pbi_service_vals["default_workspace_id"] or "").strip(),
                        default_workspace_name=(pbi_service_vals["default_workspace_name"] or "").strip(),
                    )

            elif short_code == "dl":
                upsert_dl_details(
                    connection_id=new_id,
                    storage_type=dl_vals["storage_type"],
                    endpoint_url=(dl_vals["endpoint_url"] or "").strip(),
                    bucket_or_container=(dl_vals["bucket_or_container"] or "").strip(),
                    base_path=(dl_vals["base_path"] or "").strip(),
                    auth_method=dl_vals["auth_method"],
                    access_key_or_secret=(dl_vals["access_key_or_secret"] or "").strip(),
                )

            st.success(f"✅ Connection #{new_id} saved ({short_code}, {connection_type})")
            st.rerun()



    # ---------------- Overzicht & acties ----------------
    st.markdown("### Overview")
    df = list_connections_df()
    if not df.empty:
        st.dataframe(
            df[["id", "connection_name", "connection_type", "data_source_category", "short_code", "is_active", "created_at", "updated_at"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No avilable connections.")


    # ---------------- Acties ----------------
    st.markdown("### Actions")

    df = list_connections_df()
    if df.empty:
        st.info("No connections to test or manage.")
    else:
        # Opties als records; format_func toont status in het label
        action_options = df.to_dict(orient="records")

        def _fmt_conn(r: dict) -> str:
            status = "🟢 Active" if r.get("is_active") else "🔴 Inactive"
            sc = r.get("short_code", "")
            ctype = r.get("connection_type", "")
            return f"#{r['id']} — {r['connection_name']}  [{ctype}/{sc}] · {status}"

        selected_row = st.selectbox(
            "Select a connection",
            options=action_options,
            format_func=_fmt_conn,
            key="actions_conn_select",
        )

        # States
        is_active_now  = bool(selected_row.get("is_active"))
        is_deleted_now = bool(selected_row.get("deleted_at"))  # handig als je soft delete ooit gebruikt/tonen wilt

        col1, col2, col3 = st.columns([1, 1, 1])

        # ---------- Test ----------
        with col1:
            if st.button("🔍 Test", use_container_width=True, key="btn_action_test"):
                inflated = inflate_connection_row(selected_row)
                test_payload = dict(inflated)
                # tester verwacht leesbare type-waarde:
                test_payload["connection_type"] = inflated.get("connection_type_label", selected_row["connection_type"])

                results = test_main_connection(test_payload, engine)
                for msg in results:
                    if msg.startswith("✅"):
                        st.success(msg)
                    elif msg.startswith("❌"):
                        st.error(msg)
                    else:
                        st.info(msg)

                status_info = get_main_connection_test_status(engine, int(selected_row["id"]))
                if status_info.get("status") == "success":
                    st.markdown("Status: 🟢 Connection OK")
                elif status_info.get("status") == "failed":
                    st.markdown("Status: 🔴 Connection Failed")
                else:
                    st.markdown("Status: ⚪️ Niet getest")

        # ---------- Deactivate (alleen als actief en niet soft-deleted) ----------
        with col2:
            if st.button(
                "🗑️ Deactivate",
                use_container_width=True,
                disabled=not is_active_now or is_deleted_now,
                key="btn_action_deactivate",
            ):
                exec_tx("""
                    UPDATE config.connections
                    SET is_active  = false
                        , updated_at = CURRENT_TIMESTAMP
                    WHERE id         = :id
                    AND   deleted_at IS NULL
                """, {"id": int(selected_row["id"])})
                st.success(f"Connection #{int(selected_row['id'])} gedeactiveerd.")
                st.rerun()

        # ---------- Activate (alleen als inactief en niet soft-deleted) ----------
        with col3:
            if st.button(
                "✅ Activate",
                use_container_width=True,
                disabled=is_active_now or is_deleted_now,
                key="btn_action_activate",
            ):
                exec_tx("""
                    UPDATE config.connections
                    SET is_active  = true
                        , updated_at = CURRENT_TIMESTAMP
                    WHERE id         = :id
                    AND   deleted_at IS NULL
                """, {"id": int(selected_row["id"])})
                st.success(f"Connection #{int(selected_row['id'])} geactiveerd.")
                st.rerun()


    # ---------------- Deactivated ----------------
    st.markdown("---")
    st.subheader("🗂️ Deactivated Connections")
    del_rows = q_all("""
        SELECT id
            , connection_name
            , connection_type
            , data_source_category
            , short_code
            , is_active
            , deleted_at
        FROM   config.connections
        WHERE  deleted_at IS NULL
        AND    is_active  = false
        ORDER  BY id
    """)

    if del_rows:
        for r in del_rows:
            d = dict(r._mapping)
            with st.expander(f"#{d['id']} — {d['connection_name']}  ({d['connection_type']}, {d['short_code']})"):
                # toon beknopt detail
                inflated = inflate_connection_row(d)
                cc = []
                if inflated.get("host"): cc.append(f"**Host:** {inflated['host']}")
                if inflated.get("port"): cc.append(f"**Port:** {inflated['port']}")
                if inflated.get("username"): cc.append(f"**User:** {inflated['username']}")
                if d["connection_type"] == "POWERBI_LOCAL" and inflated.get("folder_path"):
                    cc.append(f"**Folder:** {inflated['folder_path']}")
                st.markdown(" · ".join(cc) or "_(geen detail)_")

                # Soft delete: alleen als inactief (hier altijd zo)
                reason = st.text_input(
                    f"Reason for soft delete #{d['id']}",
                    key=f"ti_softdelete_reason_{d['id']}"
                )

                if st.button(f"🧨 delete #{d['id']}", key=f"softdelete_{d['id']}"):
                    if not reason.strip():
                        st.error("Provide reason for delete.")
                    else:
                        exec_tx("""
                            UPDATE config.connections
                            SET deleted_at    = CURRENT_TIMESTAMP
                                , deleted_by    = :user
                                , delete_reason = :reason
                                , updated_at    = CURRENT_TIMESTAMP
                            WHERE id         = :id
                            AND   deleted_at IS NULL
                            AND   is_active  = false
                        """, {
                            "id": d["id"],
                            "user": st.session_state.get("user_email", "webapp"),
                            "reason": reason.strip()
                        })
                        st.success(f"Connection #{d['id']} soft-deleted.")
                        st.rerun()
    else:
        st.caption("No deactivated connections.")

    # ---------------- Soft deleted ----------------
    # st.markdown("---")
    # st.subheader("🧺 Soft-deleted Connections")
    # sd_rows = q_all("""
    #     SELECT id
    #          , connection_name
    #          , connection_type
    #          , data_source_category
    #          , short_code
    #          , is_active
    #          , deleted_at
    #          , deleted_by
    #          , delete_reason
    #     FROM   config.connections
    #     WHERE  deleted_at IS NOT NULL
    #     ORDER  BY id DESC
    # """)

    # if sd_rows:
    #     for r in sd_rows:
    #         d = dict(r._mapping)
    #         with st.expander(f"#{d['id']} — {d['connection_name']}  ({d['connection_type']}, {d['short_code']})  — deleted @ {d['deleted_at']}"):
    #             st.caption(f"by {d.get('deleted_by') or '-'} · reason: {d.get('delete_reason') or '-'}")

    #             if st.button(f"Restore #{d['id']}", key=f"restore_sd_{d['id']}"):
    #                 exec_tx("""
    #                     UPDATE config.connections
    #                        SET deleted_at    = NULL
    #                          , deleted_by    = NULL
    #                          , delete_reason = NULL
    #                          , updated_at    = CURRENT_TIMESTAMP
    #                     WHERE id = :id
    #                 """, {"id": d["id"]})
    #                 st.success(f"Connection #{d['id']} hersteld (nog inactief).")
    #                 st.rerun()
    # else:
    #     st.caption("Geen soft-deleted verbindingen.")

# ======================================================================================
# TAB 2: CATALOG CONFIGURATION (DW/PBI/DL) per geselecteerde connection
# ======================================================================================
with tab_cc:
    # 1) Connection kiezen (stabiel)
    conn_id, conn_row = render_active_connection_picker_stable("catalog")
    if conn_id is None:
        st.stop()

    # 2) Catalog-config kiezen obv type
    cfg_id, cfg_row = render_catalog_config_picker_stable(
        conn_id=conn_id,
        short_code=conn_row.get("short_code"),
        key_prefix="catalog",
    )
# ======================================================================================
# TAB 3: AI CONFIGURATION (DW/PBI/DL) per geselecteerde connection
# ======================================================================================
with tab_ac:
    cid = st.session_state.selected_conn_id
    if not cid:
        st.info("Selecteer eerst een connection in de tab *Main connection*.")
    else:
        df_all = list_connections_df()
        base = df_all[df_all["id"] == cid].iloc[0].to_dict()
        short_code = base["short_code"]

        if short_code == "dw":
            st.caption("DW AI configs")
            cfgs = fetch_dw_ai_configs(cid)
            st.dataframe(pd.DataFrame(cfgs) if cfgs else pd.DataFrame())

        elif short_code == "pbi":
            st.caption("PBI AI configs")
            cfgs = fetch_pbi_ai_configs(cid)
            st.dataframe(pd.DataFrame(cfgs) if cfgs else pd.DataFrame())

        elif short_code == "dl":
            st.caption("DL AI configs")
            cfgs = fetch_dl_ai_configs(cid)
            st.dataframe(pd.DataFrame(cfgs) if cfgs else pd.DataFrame())