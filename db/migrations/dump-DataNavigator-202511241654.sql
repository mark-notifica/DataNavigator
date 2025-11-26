--
-- PostgreSQL database dump
--

-- Dumped from database version 16.8
-- Dumped by pg_dump version 16.8

-- Started on 2025-11-24 16:54:14

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 16 (class 2615 OID 49004)
-- Name: catalog; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA catalog;


ALTER SCHEMA catalog OWNER TO postgres;

--
-- TOC entry 6 (class 2615 OID 17617)
-- Name: config; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA config;


ALTER SCHEMA config OWNER TO postgres;

--
-- TOC entry 5 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: pg_database_owner
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO pg_database_owner;

--
-- TOC entry 5450 (class 0 OID 0)
-- Dependencies: 5
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: pg_database_owner
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- TOC entry 15 (class 2615 OID 49003)
-- Name: rel; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA rel;


ALTER SCHEMA rel OWNER TO postgres;

--
-- TOC entry 13 (class 2615 OID 21720)
-- Name: security; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA security;


ALTER SCHEMA security OWNER TO postgres;

--
-- TOC entry 1095 (class 1247 OID 20446)
-- Name: connection_type_enum; Type: TYPE; Schema: config; Owner: postgres
--

CREATE TYPE config.connection_type_enum AS ENUM (
    'POSTGRESQL',
    'AZURE_SQL_SERVER',
    'POWERBI_LOCAL',
    'POWERBI_CLOUD'
);


ALTER TYPE config.connection_type_enum OWNER TO postgres;

--
-- TOC entry 343 (class 1255 OID 21925)
-- Name: fn_connections_prevent_activate_when_deleted(); Type: FUNCTION; Schema: config; Owner: postgres
--

CREATE FUNCTION config.fn_connections_prevent_activate_when_deleted() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.deleted_at IS NOT NULL AND NEW.is_active = true THEN
    RAISE EXCEPTION 'Kan niet activeren: connection % is soft-deleted', NEW.id;
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION config.fn_connections_prevent_activate_when_deleted() OWNER TO postgres;

--
-- TOC entry 357 (class 1255 OID 21919)
-- Name: fn_connections_sync_from_registry(); Type: FUNCTION; Schema: config; Owner: postgres
--

CREATE FUNCTION config.fn_connections_sync_from_registry() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_category   text;
  v_short_code varchar(8);
BEGIN
  IF NEW.connection_type IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT r.data_source_category
       , r.short_code
    INTO v_category
       , v_short_code
  FROM config.connection_type_registry r
WHERE r.connection_type = NEW.connection_type;

  IF v_category IS NULL THEN
    RAISE EXCEPTION 'Onbekende connection_type: %', NEW.connection_type;
  END IF;

  -- Als gebruiker probeert deze kolommen zelf te zetten en het wijkt af: fout
  IF NEW.data_source_category IS NOT NULL
     AND NEW.data_source_category IS DISTINCT FROM v_category THEN
    RAISE EXCEPTION 'data_source_category (%) komt niet overeen met registry (%)
                     voor connection_type (%)'
      , NEW.data_source_category, v_category, NEW.connection_type;
  END IF;

  IF NEW.short_code IS NOT NULL
     AND NEW.short_code IS DISTINCT FROM v_short_code THEN
    RAISE EXCEPTION 'short_code (%) komt niet overeen met registry (%)
                     voor connection_type (%)'
      , NEW.short_code, v_short_code, NEW.connection_type;
  END IF;

  -- Zet ze (of bevestig) op de registry-waarden
  NEW.data_source_category := v_category;
  NEW.short_code           := v_short_code;

  RETURN NEW;
END;
$$;


ALTER FUNCTION config.fn_connections_sync_from_registry() OWNER TO postgres;

--
-- TOC entry 344 (class 1255 OID 21927)
-- Name: fn_prevent_activate_deleted(); Type: FUNCTION; Schema: config; Owner: postgres
--

CREATE FUNCTION config.fn_prevent_activate_deleted() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF OLD.deleted_at IS NOT NULL AND NEW.is_active = true THEN
    RAISE EXCEPTION 'Kan niet activeren: connection % is soft-deleted', OLD.id;
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION config.fn_prevent_activate_deleted() OWNER TO postgres;

--
-- TOC entry 345 (class 1255 OID 21741)
-- Name: fn_set_category_from_type(); Type: FUNCTION; Schema: config; Owner: postgres
--

CREATE FUNCTION config.fn_set_category_from_type() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_cat  text;
  v_code text;
BEGIN
  SELECT m.data_source_category
       , m.short_code
    INTO v_cat
       , v_code
  FROM config.connection_type_category_map m
  WHERE m.connection_type = NEW.connection_type
    AND m.is_active = true
  ;

  IF v_cat IS NULL THEN
     RAISE EXCEPTION 'Onbekend connection_type=%, voeg mapping toe in config.connection_type_category_map', NEW.connection_type;
  END IF;

  NEW.data_source_category := v_cat;
  NEW.short_code           := v_code;

  RETURN NEW;
END;
$$;


ALTER FUNCTION config.fn_set_category_from_type() OWNER TO postgres;

--
-- TOC entry 342 (class 1255 OID 18461)
-- Name: set_date_updated(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.set_date_updated() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.date_updated := now();
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.set_date_updated() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 341 (class 1259 OID 49376)
-- Name: catalog_runs; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.catalog_runs (
    id bigint NOT NULL,
    run_type text NOT NULL,
    connection_id bigint NOT NULL,
    source_label text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    mode text DEFAULT 'manual'::text NOT NULL,
    triggered_by text,
    objects_total integer,
    nodes_created integer,
    nodes_updated integer,
    nodes_deleted integer,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    log_filename text
);


ALTER TABLE catalog.catalog_runs OWNER TO postgres;

--
-- TOC entry 340 (class 1259 OID 49375)
-- Name: catalog_runs_id_seq; Type: SEQUENCE; Schema: catalog; Owner: postgres
--

CREATE SEQUENCE catalog.catalog_runs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE catalog.catalog_runs_id_seq OWNER TO postgres;

--
-- TOC entry 5453 (class 0 OID 0)
-- Dependencies: 340
-- Name: catalog_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: catalog; Owner: postgres
--

ALTER SEQUENCE catalog.catalog_runs_id_seq OWNED BY catalog.catalog_runs.id;


--
-- TOC entry 323 (class 1259 OID 49087)
-- Name: node_column; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_column (
    node_id bigint NOT NULL,
    table_node_id bigint NOT NULL,
    column_name text NOT NULL,
    data_type text NOT NULL,
    is_nullable boolean
);


ALTER TABLE catalog.node_column OWNER TO postgres;

--
-- TOC entry 320 (class 1259 OID 49034)
-- Name: node_database; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_database (
    node_id bigint NOT NULL,
    server_name text NOT NULL,
    database_name text NOT NULL
);


ALTER TABLE catalog.node_database OWNER TO postgres;

--
-- TOC entry 332 (class 1259 OID 49201)
-- Name: node_descriptions; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_descriptions (
    id bigint NOT NULL,
    node_id bigint NOT NULL,
    description_type text NOT NULL,
    description text,
    source text,
    status text,
    ai_result_id bigint,
    propagate_method text,
    is_current boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    author_created text,
    author_updated text
);


ALTER TABLE catalog.node_descriptions OWNER TO postgres;

--
-- TOC entry 331 (class 1259 OID 49200)
-- Name: node_descriptions_id_seq; Type: SEQUENCE; Schema: catalog; Owner: postgres
--

CREATE SEQUENCE catalog.node_descriptions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE catalog.node_descriptions_id_seq OWNER TO postgres;

--
-- TOC entry 5454 (class 0 OID 0)
-- Dependencies: 331
-- Name: node_descriptions_id_seq; Type: SEQUENCE OWNED BY; Schema: catalog; Owner: postgres
--

ALTER SEQUENCE catalog.node_descriptions_id_seq OWNED BY catalog.node_descriptions.id;


--
-- TOC entry 336 (class 1259 OID 49266)
-- Name: node_dl_container; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_dl_container (
    node_id bigint NOT NULL,
    account_name text,
    container_name text NOT NULL,
    props jsonb
);


ALTER TABLE catalog.node_dl_container OWNER TO postgres;

--
-- TOC entry 338 (class 1259 OID 49299)
-- Name: node_dl_file; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_dl_file (
    node_id bigint NOT NULL,
    folder_node_id bigint NOT NULL,
    file_name text NOT NULL,
    file_format text,
    size_bytes bigint,
    props jsonb
);


ALTER TABLE catalog.node_dl_file OWNER TO postgres;

--
-- TOC entry 337 (class 1259 OID 49280)
-- Name: node_dl_folder; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_dl_folder (
    node_id bigint NOT NULL,
    container_node_id bigint NOT NULL,
    folder_path text NOT NULL,
    props jsonb
);


ALTER TABLE catalog.node_dl_folder OWNER TO postgres;

--
-- TOC entry 330 (class 1259 OID 49188)
-- Name: node_etl_job; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_etl_job (
    node_id bigint NOT NULL,
    engine text,
    job_name text NOT NULL,
    schedule_cron text,
    props jsonb
);


ALTER TABLE catalog.node_etl_job OWNER TO postgres;

--
-- TOC entry 329 (class 1259 OID 49176)
-- Name: node_etl_notebook; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_etl_notebook (
    node_id bigint NOT NULL,
    workspace_name text,
    notebook_path text NOT NULL,
    props jsonb
);


ALTER TABLE catalog.node_etl_notebook OWNER TO postgres;

--
-- TOC entry 326 (class 1259 OID 49135)
-- Name: node_etl_pipeline; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_etl_pipeline (
    node_id bigint NOT NULL,
    engine text NOT NULL,
    pipeline_name text NOT NULL,
    location text,
    props jsonb
);


ALTER TABLE catalog.node_etl_pipeline OWNER TO postgres;

--
-- TOC entry 328 (class 1259 OID 49164)
-- Name: node_etl_script; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_etl_script (
    node_id bigint NOT NULL,
    language text NOT NULL,
    file_path text,
    props jsonb,
    content text
);


ALTER TABLE catalog.node_etl_script OWNER TO postgres;

--
-- TOC entry 327 (class 1259 OID 49147)
-- Name: node_etl_step; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_etl_step (
    node_id bigint NOT NULL,
    pipeline_node_id bigint NOT NULL,
    step_name text NOT NULL,
    step_type text,
    step_order integer,
    props jsonb
);


ALTER TABLE catalog.node_etl_step OWNER TO postgres;

--
-- TOC entry 325 (class 1259 OID 49118)
-- Name: node_pbi_measure; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_pbi_measure (
    node_id bigint NOT NULL,
    table_node_id bigint NOT NULL,
    measure_name text NOT NULL,
    data_type text NOT NULL,
    dax_expression text NOT NULL,
    format_string text,
    is_hidden boolean
);


ALTER TABLE catalog.node_pbi_measure OWNER TO postgres;

--
-- TOC entry 324 (class 1259 OID 49106)
-- Name: node_pbi_model; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_pbi_model (
    node_id bigint NOT NULL,
    workspace_name text,
    dataset_name text,
    props jsonb
);


ALTER TABLE catalog.node_pbi_model OWNER TO postgres;

--
-- TOC entry 339 (class 1259 OID 49334)
-- Name: node_pbi_query; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_pbi_query (
    node_id bigint NOT NULL,
    model_node_id bigint,
    table_node_id bigint,
    query_name text NOT NULL,
    source_kind text,
    source_path text,
    m_code text,
    props jsonb
);


ALTER TABLE catalog.node_pbi_query OWNER TO postgres;

--
-- TOC entry 321 (class 1259 OID 49048)
-- Name: node_schema; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_schema (
    node_id bigint NOT NULL,
    database_node_id bigint NOT NULL,
    schema_name text NOT NULL
);


ALTER TABLE catalog.node_schema OWNER TO postgres;

--
-- TOC entry 322 (class 1259 OID 49067)
-- Name: node_table; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.node_table (
    node_id bigint NOT NULL,
    schema_node_id bigint NOT NULL,
    table_name text NOT NULL,
    table_type text NOT NULL,
    CONSTRAINT node_table_table_type_check CHECK ((table_type = ANY (ARRAY['TABLE'::text, 'VIEW'::text, 'PBI_TABLE'::text, 'DL_TABLE'::text])))
);


ALTER TABLE catalog.node_table OWNER TO postgres;

--
-- TOC entry 319 (class 1259 OID 49013)
-- Name: nodes; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.nodes (
    node_id bigint NOT NULL,
    node_type text NOT NULL,
    name text NOT NULL,
    qualified_name text NOT NULL,
    description_short text,
    description_long text,
    description_status text DEFAULT 'pending'::text,
    props jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    is_current boolean DEFAULT true NOT NULL,
    created_in_run_id bigint,
    last_seen_run_id bigint,
    deleted_in_run_id bigint,
    deleted_at timestamp with time zone
);


ALTER TABLE catalog.nodes OWNER TO postgres;

--
-- TOC entry 318 (class 1259 OID 49012)
-- Name: nodes_node_id_seq; Type: SEQUENCE; Schema: catalog; Owner: postgres
--

CREATE SEQUENCE catalog.nodes_node_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE catalog.nodes_node_id_seq OWNER TO postgres;

--
-- TOC entry 5455 (class 0 OID 0)
-- Dependencies: 318
-- Name: nodes_node_id_seq; Type: SEQUENCE OWNED BY; Schema: catalog; Owner: postgres
--

ALTER SEQUENCE catalog.nodes_node_id_seq OWNED BY catalog.nodes.node_id;


--
-- TOC entry 317 (class 1259 OID 49005)
-- Name: object_type; Type: TABLE; Schema: catalog; Owner: postgres
--

CREATE TABLE catalog.object_type (
    object_type_code text NOT NULL,
    name text NOT NULL,
    description text,
    level_hint integer,
    is_physical boolean NOT NULL,
    is_tabular boolean NOT NULL,
    supports_embedding boolean NOT NULL,
    default_props jsonb
);


ALTER TABLE catalog.object_type OWNER TO postgres;

--
-- TOC entry 246 (class 1259 OID 18555)
-- Name: ai_analyzer_connection_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.ai_analyzer_connection_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    use_for_ai boolean DEFAULT false NOT NULL,
    ai_database_filter text,
    ai_schema_filter text,
    ai_table_filter text,
    ai_model_version character varying(50),
    notes text,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean DEFAULT true NOT NULL,
    config_name character varying(100) DEFAULT 'Naamloze config'::character varying NOT NULL,
    last_test_status character varying(20),
    last_tested_at timestamp without time zone,
    last_test_notes text
);


ALTER TABLE config.ai_analyzer_connection_config OWNER TO postgres;

--
-- TOC entry 245 (class 1259 OID 18554)
-- Name: ai_analyzer_connection_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

CREATE SEQUENCE config.ai_analyzer_connection_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE config.ai_analyzer_connection_config_id_seq OWNER TO postgres;

--
-- TOC entry 5457 (class 0 OID 0)
-- Dependencies: 245
-- Name: ai_analyzer_connection_config_id_seq; Type: SEQUENCE OWNED BY; Schema: config; Owner: postgres
--

ALTER SEQUENCE config.ai_analyzer_connection_config_id_seq OWNED BY config.ai_analyzer_connection_config.id;


--
-- TOC entry 248 (class 1259 OID 18572)
-- Name: catalog_connection_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.catalog_connection_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    use_for_catalog boolean DEFAULT true NOT NULL,
    catalog_database_filter text,
    catalog_schema_filter text,
    catalog_table_filter text,
    include_views boolean DEFAULT false,
    include_system_objects boolean DEFAULT false,
    notes text,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean DEFAULT true NOT NULL,
    config_name character varying(100) DEFAULT 'Naamloze config'::character varying NOT NULL,
    last_test_status character varying(20),
    last_tested_at timestamp without time zone,
    last_test_notes text
);


ALTER TABLE config.catalog_connection_config OWNER TO postgres;

--
-- TOC entry 247 (class 1259 OID 18571)
-- Name: catalog_connection_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

CREATE SEQUENCE config.catalog_connection_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE config.catalog_connection_config_id_seq OWNER TO postgres;

--
-- TOC entry 5460 (class 0 OID 0)
-- Dependencies: 247
-- Name: catalog_connection_config_id_seq; Type: SEQUENCE OWNED BY; Schema: config; Owner: postgres
--

ALTER SEQUENCE config.catalog_connection_config_id_seq OWNED BY config.catalog_connection_config.id;


--
-- TOC entry 314 (class 1259 OID 21797)
-- Name: connection_type_registry; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.connection_type_registry (
    connection_type text NOT NULL,
    data_source_category text NOT NULL,
    display_name text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    short_code character varying(8) GENERATED ALWAYS AS (
CASE data_source_category
    WHEN 'DATABASE_DATAWAREHOUSE'::text THEN 'dw'::text
    WHEN 'POWERBI'::text THEN 'pbi'::text
    WHEN 'DATA_LAKE'::text THEN 'dl'::text
    ELSE 'other'::text
END) STORED NOT NULL,
    CONSTRAINT ck_map_category_known CHECK ((data_source_category = ANY (ARRAY['DATABASE_DATAWAREHOUSE'::text, 'POWERBI'::text, 'DATA_LAKE'::text])))
);


ALTER TABLE config.connection_type_registry OWNER TO postgres;

--
-- TOC entry 316 (class 1259 OID 21815)
-- Name: connections; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.connections (
    id integer NOT NULL,
    connection_name character varying(255) NOT NULL,
    data_source_category text,
    connection_type text,
    short_code character varying(8),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean DEFAULT true,
    execution_mode character varying(20) DEFAULT 'manual'::character varying,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_test_status character varying(20),
    last_tested_at timestamp without time zone,
    last_test_notes text,
    type_specific_config jsonb,
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text,
    CONSTRAINT ck_connections_not_active_when_deleted CHECK (((deleted_at IS NULL) OR (is_active = false)))
);


ALTER TABLE config.connections OWNER TO postgres;

--
-- TOC entry 315 (class 1259 OID 21814)
-- Name: connections_new_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

CREATE SEQUENCE config.connections_new_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE config.connections_new_id_seq OWNER TO postgres;

--
-- TOC entry 5464 (class 0 OID 0)
-- Dependencies: 315
-- Name: connections_new_id_seq; Type: SEQUENCE OWNED BY; Schema: config; Owner: postgres
--

ALTER SEQUENCE config.connections_new_id_seq OWNED BY config.connections.id;


--
-- TOC entry 302 (class 1259 OID 21526)
-- Name: dl_ai_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.dl_ai_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    config_name character varying(120) DEFAULT 'Naamloze AI-config'::character varying NOT NULL,
    analysis_type text NOT NULL,
    model_provider text DEFAULT 'openai'::text NOT NULL,
    model_name text NOT NULL,
    model_version text,
    temperature numeric(3,2) DEFAULT 0.0,
    max_tokens integer DEFAULT 2048,
    top_p numeric(3,2) DEFAULT 1.0,
    frequency_penalty numeric(3,2) DEFAULT 0.0,
    presence_penalty numeric(3,2) DEFAULT 0.0,
    path_filter text,
    format_whitelist text,
    partition_filter text,
    include_hidden_files boolean DEFAULT false NOT NULL,
    infer_schema boolean DEFAULT true NOT NULL,
    runner_concurrency integer DEFAULT 2 NOT NULL,
    propagation_mode character varying(20) DEFAULT 'auto'::character varying NOT NULL,
    overwrite_policy character varying(32) DEFAULT 'fill_empty'::character varying NOT NULL,
    confidence_threshold numeric(4,3) DEFAULT 0.700 NOT NULL,
    respect_human_locks boolean DEFAULT true NOT NULL,
    model_profile text,
    prompt_pack text,
    notes text,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text,
    CONSTRAINT ck_freq_penalty_range CHECK (((frequency_penalty IS NULL) OR ((frequency_penalty >= ('-2'::integer)::numeric) AND (frequency_penalty <= (2)::numeric)))),
    CONSTRAINT ck_overwrite_policy CHECK (((overwrite_policy)::text = ANY ((ARRAY['fill_empty'::character varying, 'overwrite_if_confident'::character varying, 'never'::character varying])::text[]))),
    CONSTRAINT ck_presence_penalty_range CHECK (((presence_penalty IS NULL) OR ((presence_penalty >= ('-2'::integer)::numeric) AND (presence_penalty <= (2)::numeric)))),
    CONSTRAINT ck_propagation_mode CHECK (((propagation_mode)::text = ANY ((ARRAY['auto'::character varying, 'suggest_only'::character varying, 'off'::character varying])::text[]))),
    CONSTRAINT ck_temperature_range CHECK (((temperature IS NULL) OR ((temperature >= (0)::numeric) AND (temperature <= (2)::numeric)))),
    CONSTRAINT ck_top_p_range CHECK (((top_p IS NULL) OR ((top_p > (0)::numeric) AND (top_p <= (1)::numeric))))
);


ALTER TABLE config.dl_ai_config OWNER TO postgres;

--
-- TOC entry 301 (class 1259 OID 21525)
-- Name: dl_ai_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

ALTER TABLE config.dl_ai_config ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME config.dl_ai_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 308 (class 1259 OID 21626)
-- Name: dl_catalog_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.dl_catalog_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    config_name character varying(100) DEFAULT 'Naamloze config'::character varying NOT NULL,
    path_filter text,
    format_whitelist text,
    partition_filter text,
    include_hidden_files boolean DEFAULT false NOT NULL,
    infer_schema boolean DEFAULT true NOT NULL,
    notes text,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    last_test_status character varying(20),
    last_tested_at timestamp without time zone,
    last_test_notes text,
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text
);


ALTER TABLE config.dl_catalog_config OWNER TO postgres;

--
-- TOC entry 307 (class 1259 OID 21625)
-- Name: dl_catalog_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

ALTER TABLE config.dl_catalog_config ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME config.dl_catalog_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 312 (class 1259 OID 21685)
-- Name: dl_connection_details; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.dl_connection_details (
    connection_id integer NOT NULL,
    storage_type character varying(20) NOT NULL,
    endpoint_url text,
    bucket_or_container text,
    base_path text,
    auth_method character varying(20),
    secret_ref text,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE config.dl_connection_details OWNER TO postgres;

--
-- TOC entry 298 (class 1259 OID 21414)
-- Name: dw_ai_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.dw_ai_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    config_name character varying(120) DEFAULT 'Naamloze AI-config'::character varying NOT NULL,
    analysis_type text NOT NULL,
    model_provider text DEFAULT 'openai'::text NOT NULL,
    model_name text NOT NULL,
    model_version text,
    temperature numeric(3,2) DEFAULT 0.0,
    max_tokens integer DEFAULT 2048,
    top_p numeric(3,2) DEFAULT 1.0,
    frequency_penalty numeric(3,2) DEFAULT 0.0,
    presence_penalty numeric(3,2) DEFAULT 0.0,
    database_filter text,
    schema_filter text,
    table_filter text,
    runner_concurrency integer DEFAULT 2 NOT NULL,
    propagation_mode character varying(20) DEFAULT 'auto'::character varying NOT NULL,
    overwrite_policy character varying(32) DEFAULT 'fill_empty'::character varying NOT NULL,
    confidence_threshold numeric(4,3) DEFAULT 0.700 NOT NULL,
    respect_human_locks boolean DEFAULT true NOT NULL,
    model_profile text,
    prompt_pack text,
    notes text,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text,
    CONSTRAINT ck_freq_penalty_range CHECK (((frequency_penalty IS NULL) OR ((frequency_penalty >= ('-2'::integer)::numeric) AND (frequency_penalty <= (2)::numeric)))),
    CONSTRAINT ck_overwrite_policy CHECK (((overwrite_policy)::text = ANY ((ARRAY['fill_empty'::character varying, 'overwrite_if_confident'::character varying, 'never'::character varying])::text[]))),
    CONSTRAINT ck_presence_penalty_range CHECK (((presence_penalty IS NULL) OR ((presence_penalty >= ('-2'::integer)::numeric) AND (presence_penalty <= (2)::numeric)))),
    CONSTRAINT ck_propagation_mode CHECK (((propagation_mode)::text = ANY ((ARRAY['auto'::character varying, 'suggest_only'::character varying, 'off'::character varying])::text[]))),
    CONSTRAINT ck_temperature_range CHECK (((temperature IS NULL) OR ((temperature >= (0)::numeric) AND (temperature <= (2)::numeric)))),
    CONSTRAINT ck_top_p_range CHECK (((top_p IS NULL) OR ((top_p > (0)::numeric) AND (top_p <= (1)::numeric))))
);


ALTER TABLE config.dw_ai_config OWNER TO postgres;

--
-- TOC entry 297 (class 1259 OID 21413)
-- Name: dw_ai_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

ALTER TABLE config.dw_ai_config ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME config.dw_ai_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 304 (class 1259 OID 21585)
-- Name: dw_catalog_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.dw_catalog_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    config_name character varying(100) DEFAULT 'Naamloze config'::character varying NOT NULL,
    database_filter text,
    schema_filter text,
    table_filter text,
    include_views boolean DEFAULT false,
    include_system_objects boolean DEFAULT false,
    notes text,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    last_test_status character varying(20),
    last_tested_at timestamp without time zone,
    last_test_notes text,
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text
);


ALTER TABLE config.dw_catalog_config OWNER TO postgres;

--
-- TOC entry 303 (class 1259 OID 21584)
-- Name: dw_catalog_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

ALTER TABLE config.dw_catalog_config ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME config.dw_catalog_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 309 (class 1259 OID 21645)
-- Name: dw_connection_details; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.dw_connection_details (
    connection_id integer NOT NULL,
    engine_type character varying(20) NOT NULL,
    host text,
    port integer,
    default_database text,
    username text,
    ssl_mode character varying(10),
    updated_at timestamp without time zone DEFAULT now(),
    secret_ref text
);


ALTER TABLE config.dw_connection_details OWNER TO postgres;

--
-- TOC entry 300 (class 1259 OID 21487)
-- Name: pbi_ai_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.pbi_ai_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    config_name character varying(120) DEFAULT 'Naamloze AI-config'::character varying NOT NULL,
    analysis_type text NOT NULL,
    model_provider text DEFAULT 'openai'::text NOT NULL,
    model_name text NOT NULL,
    model_version text,
    temperature numeric(3,2) DEFAULT 0.0,
    max_tokens integer DEFAULT 2048,
    top_p numeric(3,2) DEFAULT 1.0,
    frequency_penalty numeric(3,2) DEFAULT 0.0,
    presence_penalty numeric(3,2) DEFAULT 0.0,
    workspace_filter text,
    model_filter text,
    table_filter text,
    include_tmdl boolean DEFAULT true NOT NULL,
    include_model_bim boolean DEFAULT false NOT NULL,
    respect_perspectives boolean DEFAULT true NOT NULL,
    runner_concurrency integer DEFAULT 2 NOT NULL,
    propagation_mode character varying(20) DEFAULT 'auto'::character varying NOT NULL,
    overwrite_policy character varying(32) DEFAULT 'fill_empty'::character varying NOT NULL,
    confidence_threshold numeric(4,3) DEFAULT 0.700 NOT NULL,
    respect_human_locks boolean DEFAULT true NOT NULL,
    model_profile text,
    prompt_pack text,
    notes text,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text,
    CONSTRAINT ck_freq_penalty_range CHECK (((frequency_penalty IS NULL) OR ((frequency_penalty >= ('-2'::integer)::numeric) AND (frequency_penalty <= (2)::numeric)))),
    CONSTRAINT ck_overwrite_policy CHECK (((overwrite_policy)::text = ANY ((ARRAY['fill_empty'::character varying, 'overwrite_if_confident'::character varying, 'never'::character varying])::text[]))),
    CONSTRAINT ck_presence_penalty_range CHECK (((presence_penalty IS NULL) OR ((presence_penalty >= ('-2'::integer)::numeric) AND (presence_penalty <= (2)::numeric)))),
    CONSTRAINT ck_propagation_mode CHECK (((propagation_mode)::text = ANY ((ARRAY['auto'::character varying, 'suggest_only'::character varying, 'off'::character varying])::text[]))),
    CONSTRAINT ck_temperature_range CHECK (((temperature IS NULL) OR ((temperature >= (0)::numeric) AND (temperature <= (2)::numeric)))),
    CONSTRAINT ck_top_p_range CHECK (((top_p IS NULL) OR ((top_p > (0)::numeric) AND (top_p <= (1)::numeric))))
);


ALTER TABLE config.pbi_ai_config OWNER TO postgres;

--
-- TOC entry 299 (class 1259 OID 21486)
-- Name: pbi_ai_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

ALTER TABLE config.pbi_ai_config ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME config.pbi_ai_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 306 (class 1259 OID 21605)
-- Name: pbi_catalog_config; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.pbi_catalog_config (
    id integer NOT NULL,
    connection_id integer NOT NULL,
    config_name character varying(100) DEFAULT 'Naamloze config'::character varying NOT NULL,
    workspace_filter text,
    model_filter text,
    table_filter text,
    include_tmdl boolean DEFAULT true NOT NULL,
    include_model_bim boolean DEFAULT false NOT NULL,
    respect_perspectives boolean DEFAULT true NOT NULL,
    notes text,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    last_test_status character varying(20),
    last_tested_at timestamp without time zone,
    last_test_notes text,
    deleted_at timestamp without time zone,
    deleted_by text,
    delete_reason text
);


ALTER TABLE config.pbi_catalog_config OWNER TO postgres;

--
-- TOC entry 305 (class 1259 OID 21604)
-- Name: pbi_catalog_config_id_seq; Type: SEQUENCE; Schema: config; Owner: postgres
--

ALTER TABLE config.pbi_catalog_config ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME config.pbi_catalog_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 310 (class 1259 OID 21658)
-- Name: pbi_local_connection_details; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.pbi_local_connection_details (
    connection_id integer NOT NULL,
    folder_path text NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE config.pbi_local_connection_details OWNER TO postgres;

--
-- TOC entry 311 (class 1259 OID 21671)
-- Name: pbi_service_connection_details; Type: TABLE; Schema: config; Owner: postgres
--

CREATE TABLE config.pbi_service_connection_details (
    connection_id integer NOT NULL,
    tenant_id text,
    client_id text,
    auth_method character varying(20) DEFAULT 'DEVICE_CODE'::character varying NOT NULL,
    secret_ref text,
    default_workspace_id text,
    default_workspace_name text,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE config.pbi_service_connection_details OWNER TO postgres;

--
-- TOC entry 334 (class 1259 OID 49218)
-- Name: edge; Type: TABLE; Schema: rel; Owner: postgres
--

CREATE TABLE rel.edge (
    edge_id bigint NOT NULL,
    src_node_id bigint NOT NULL,
    dst_node_id bigint NOT NULL,
    edge_type text NOT NULL,
    weight real,
    props jsonb,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE rel.edge OWNER TO postgres;

--
-- TOC entry 333 (class 1259 OID 49217)
-- Name: edge_edge_id_seq; Type: SEQUENCE; Schema: rel; Owner: postgres
--

CREATE SEQUENCE rel.edge_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE rel.edge_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5482 (class 0 OID 0)
-- Dependencies: 333
-- Name: edge_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: rel; Owner: postgres
--

ALTER SEQUENCE rel.edge_edge_id_seq OWNED BY rel.edge.edge_id;


--
-- TOC entry 335 (class 1259 OID 49241)
-- Name: fk; Type: TABLE; Schema: rel; Owner: postgres
--

CREATE TABLE rel.fk (
    src_table_node_id bigint NOT NULL,
    dst_table_node_id bigint NOT NULL,
    src_column_node_id bigint NOT NULL,
    dst_column_node_id bigint NOT NULL
);


ALTER TABLE rel.fk OWNER TO postgres;

--
-- TOC entry 313 (class 1259 OID 21721)
-- Name: secrets_plain; Type: TABLE; Schema: security; Owner: postgres
--

CREATE TABLE security.secrets_plain (
    ref_key text NOT NULL,
    secret_value text NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE security.secrets_plain OWNER TO postgres;

--
-- TOC entry 5051 (class 2604 OID 49379)
-- Name: catalog_runs id; Type: DEFAULT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.catalog_runs ALTER COLUMN id SET DEFAULT nextval('catalog.catalog_runs_id_seq'::regclass);


--
-- TOC entry 5045 (class 2604 OID 49204)
-- Name: node_descriptions id; Type: DEFAULT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_descriptions ALTER COLUMN id SET DEFAULT nextval('catalog.node_descriptions_id_seq'::regclass);


--
-- TOC entry 5040 (class 2604 OID 49016)
-- Name: nodes node_id; Type: DEFAULT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.nodes ALTER COLUMN node_id SET DEFAULT nextval('catalog.nodes_node_id_seq'::regclass);


--
-- TOC entry 4950 (class 2604 OID 18558)
-- Name: ai_analyzer_connection_config id; Type: DEFAULT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.ai_analyzer_connection_config ALTER COLUMN id SET DEFAULT nextval('config.ai_analyzer_connection_config_id_seq'::regclass);


--
-- TOC entry 4955 (class 2604 OID 18575)
-- Name: catalog_connection_config id; Type: DEFAULT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.catalog_connection_config ALTER COLUMN id SET DEFAULT nextval('config.catalog_connection_config_id_seq'::regclass);


--
-- TOC entry 5035 (class 2604 OID 21818)
-- Name: connections id; Type: DEFAULT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.connections ALTER COLUMN id SET DEFAULT nextval('config.connections_new_id_seq'::regclass);


--
-- TOC entry 5049 (class 2604 OID 49221)
-- Name: edge edge_id; Type: DEFAULT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.edge ALTER COLUMN edge_id SET DEFAULT nextval('rel.edge_edge_id_seq'::regclass);


--
-- TOC entry 5443 (class 0 OID 49376)
-- Dependencies: 341
-- Data for Name: catalog_runs; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.catalog_runs (id, run_type, connection_id, source_label, started_at, completed_at, status, mode, triggered_by, objects_total, nodes_created, nodes_updated, nodes_deleted, context, error_message, log_filename) FROM stdin;
\.


--
-- TOC entry 5425 (class 0 OID 49087)
-- Dependencies: 323
-- Data for Name: node_column; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_column (node_id, table_node_id, column_name, data_type, is_nullable) FROM stdin;
\.


--
-- TOC entry 5422 (class 0 OID 49034)
-- Dependencies: 320
-- Data for Name: node_database; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_database (node_id, server_name, database_name) FROM stdin;
\.


--
-- TOC entry 5434 (class 0 OID 49201)
-- Dependencies: 332
-- Data for Name: node_descriptions; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_descriptions (id, node_id, description_type, description, source, status, ai_result_id, propagate_method, is_current, created_at, updated_at, author_created, author_updated) FROM stdin;
\.


--
-- TOC entry 5438 (class 0 OID 49266)
-- Dependencies: 336
-- Data for Name: node_dl_container; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_dl_container (node_id, account_name, container_name, props) FROM stdin;
\.


--
-- TOC entry 5440 (class 0 OID 49299)
-- Dependencies: 338
-- Data for Name: node_dl_file; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_dl_file (node_id, folder_node_id, file_name, file_format, size_bytes, props) FROM stdin;
\.


--
-- TOC entry 5439 (class 0 OID 49280)
-- Dependencies: 337
-- Data for Name: node_dl_folder; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_dl_folder (node_id, container_node_id, folder_path, props) FROM stdin;
\.


--
-- TOC entry 5432 (class 0 OID 49188)
-- Dependencies: 330
-- Data for Name: node_etl_job; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_etl_job (node_id, engine, job_name, schedule_cron, props) FROM stdin;
\.


--
-- TOC entry 5431 (class 0 OID 49176)
-- Dependencies: 329
-- Data for Name: node_etl_notebook; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_etl_notebook (node_id, workspace_name, notebook_path, props) FROM stdin;
\.


--
-- TOC entry 5428 (class 0 OID 49135)
-- Dependencies: 326
-- Data for Name: node_etl_pipeline; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_etl_pipeline (node_id, engine, pipeline_name, location, props) FROM stdin;
\.


--
-- TOC entry 5430 (class 0 OID 49164)
-- Dependencies: 328
-- Data for Name: node_etl_script; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_etl_script (node_id, language, file_path, props, content) FROM stdin;
\.


--
-- TOC entry 5429 (class 0 OID 49147)
-- Dependencies: 327
-- Data for Name: node_etl_step; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_etl_step (node_id, pipeline_node_id, step_name, step_type, step_order, props) FROM stdin;
\.


--
-- TOC entry 5427 (class 0 OID 49118)
-- Dependencies: 325
-- Data for Name: node_pbi_measure; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_pbi_measure (node_id, table_node_id, measure_name, data_type, dax_expression, format_string, is_hidden) FROM stdin;
\.


--
-- TOC entry 5426 (class 0 OID 49106)
-- Dependencies: 324
-- Data for Name: node_pbi_model; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_pbi_model (node_id, workspace_name, dataset_name, props) FROM stdin;
\.


--
-- TOC entry 5441 (class 0 OID 49334)
-- Dependencies: 339
-- Data for Name: node_pbi_query; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_pbi_query (node_id, model_node_id, table_node_id, query_name, source_kind, source_path, m_code, props) FROM stdin;
\.


--
-- TOC entry 5423 (class 0 OID 49048)
-- Dependencies: 321
-- Data for Name: node_schema; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_schema (node_id, database_node_id, schema_name) FROM stdin;
\.


--
-- TOC entry 5424 (class 0 OID 49067)
-- Dependencies: 322
-- Data for Name: node_table; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.node_table (node_id, schema_node_id, table_name, table_type) FROM stdin;
\.


--
-- TOC entry 5421 (class 0 OID 49013)
-- Dependencies: 319
-- Data for Name: nodes; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.nodes (node_id, node_type, name, qualified_name, description_short, description_long, description_status, props, created_at, updated_at, is_current, created_in_run_id, last_seen_run_id, deleted_in_run_id, deleted_at) FROM stdin;
\.


--
-- TOC entry 5419 (class 0 OID 49005)
-- Dependencies: 317
-- Data for Name: object_type; Type: TABLE DATA; Schema: catalog; Owner: postgres
--

COPY catalog.object_type (object_type_code, name, description, level_hint, is_physical, is_tabular, supports_embedding, default_props) FROM stdin;
DB_DATABASE	Database	Database binnen een server/instance	0	t	f	t	{}
DB_SCHEMA	Schema	Schema binnen een database	1	t	f	f	{}
DB_TABLE	Tabel	Fysieke tabel in een database	2	t	t	t	{}
DB_VIEW	View	View in een database	2	t	t	t	{}
DB_COLUMN	Kolom	Kolom in een tabel of view	3	t	t	t	{}
PBI_MODEL	Power BI model	Power BI dataset / semantic model	0	f	f	t	{}
PBI_TABLE	Power BI tabel	Tabel in een Power BI model	2	f	t	t	{}
PBI_COLUMN	Power BI kolom	Kolom in een Power BI tabel	3	f	t	t	{}
PBI_MEASURE	Power BI measure	DAX-measure in een Power BI model	4	f	f	t	{}
PBI_QUERY	Power BI query	M-code query (Power Query) die data laadt in het model	1	f	f	t	{}
DL_CONTAINER	Data Lake container	Container/bucket in een data lake account	0	t	f	f	{}
DL_FOLDER	Data Lake folder	Folder/pad binnen een data lake container	1	t	f	f	{}
DL_FILE	Data Lake bestand	Bestand in een data lake (Parquet, CSV, JSON, ...)	2	t	f	f	{}
DL_TABLE	Data Lake tabel	Logische/lakehouse/Delta-tabel	2	t	t	t	{}
DL_COLUMN	Data Lake kolom	Kolom in een data lake tabel of schema van een bestand	3	t	t	t	{}
ETL_PIPELINE	ETL pipeline	Pipeline / dataflow / DAG (ADF, Databricks, Airflow, ...)	0	f	f	t	{}
ETL_STEP	ETL stap	Stap / activity binnen een ETL pipeline	1	f	f	t	{}
ETL_SCRIPT	ETL script	Scriptbestand (SQL, Python, PySpark, GRIP .grp, ...)	2	f	f	t	{}
ETL_NOTEBOOK	ETL notebook	Notebook (Databricks, Synapse, ...)	2	f	f	t	{}
ETL_JOB	ETL job	Job/schedule/trigger die pipelines aanstuurt	1	f	f	t	{}
\.


--
-- TOC entry 5396 (class 0 OID 18555)
-- Dependencies: 246
-- Data for Name: ai_analyzer_connection_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.ai_analyzer_connection_config (id, connection_id, use_for_ai, ai_database_filter, ai_schema_filter, ai_table_filter, ai_model_version, notes, updated_at, is_active, config_name, last_test_status, last_tested_at, last_test_notes) FROM stdin;
1	6	t	ENK_DEV1	stg	ods_alias_l*	default		2025-07-02 14:43:22.257068	t	ENK_DEV1 met alleen ods_alias* tabellen in schema stg	success	2025-07-03 10:11:25.686259	✅ Server-level connection successful (PostgreSQL)\n✅ Database 'ENK_DEV1': OK (Tables: 716, Views: 0)
\.


--
-- TOC entry 5398 (class 0 OID 18572)
-- Dependencies: 248
-- Data for Name: catalog_connection_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.catalog_connection_config (id, connection_id, use_for_catalog, catalog_database_filter, catalog_schema_filter, catalog_table_filter, include_views, include_system_objects, notes, updated_at, is_active, config_name, last_test_status, last_tested_at, last_test_notes) FROM stdin;
2	6	t	ENK_DEV1	stg	\N	f	f	Samengevoegd vanuit connection_id 29	2025-07-01 14:35:57.542206	f	Naamloze config	untested	\N	\N
3	6	t	ENK_DEV1			t	f		2025-07-02 14:45:49.039772	t	Alleen ENK_DEV1	success	2025-07-03 09:43:46.4399	✅ Server-level connection successful (PostgreSQL)\n✅ Database 'ENK_DEV1': OK (Tables: 716, Views: 0)
\.


--
-- TOC entry 5416 (class 0 OID 21797)
-- Dependencies: 314
-- Data for Name: connection_type_registry; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.connection_type_registry (connection_type, data_source_category, display_name, is_active, created_at) FROM stdin;
AZURE_SQL_SERVER	DATABASE_DATAWAREHOUSE	Azure SQL Server (T-SQL)	t	2025-08-15 15:31:43.107
POWERBI_LOCAL	POWERBI	Power BI (Local PBIP/TMDL)	t	2025-08-15 15:31:43.107
POSTGRESQL	DATABASE_DATAWAREHOUSE	PostgreSQL	t	2025-08-15 15:31:43.107
POWERBI_SERVICE	POWERBI	Power BI Service	f	2025-08-15 15:31:43.107
POWERBI_SHAREPOINT	POWERBI	Power BI (SharePoint/Graph)	f	2025-08-15 15:31:43.107
ADLS	DATA_LAKE	Azure Data Lake Storage	f	2025-08-15 15:31:43.107
S3	DATA_LAKE	Amazon S3	f	2025-08-15 15:31:43.107
GCS	DATA_LAKE	Google Cloud Storage	f	2025-08-15 15:31:43.107
\.


--
-- TOC entry 5418 (class 0 OID 21815)
-- Dependencies: 316
-- Data for Name: connections; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.connections (id, connection_name, data_source_category, connection_type, short_code, created_at, is_active, execution_mode, updated_at, last_test_status, last_tested_at, last_test_notes, type_specific_config, deleted_at, deleted_by, delete_reason) FROM stdin;
24	SSM SQL	POWERBI	POWERBI_LOCAL	pbi	2025-06-10 11:40:32.976024	t	manual	2025-09-01 13:30:33.479456	failed	2025-09-11 09:36:30.797467	❌ Unsupported connection type: Power BI Semantic Model	\N	\N	\N	\N
8	bisqq.database.windows.net	DATABASE_DATAWAREHOUSE	AZURE_SQL_SERVER	dw	2025-06-10 10:07:51.792555	t	manual	2025-08-18 12:24:40.971102	success	2025-09-11 09:36:34.800058	✅ Server-level connection successful (Azure SQL Server)	\N	\N	\N	\N
6	VPS1	DATABASE_DATAWAREHOUSE	POSTGRESQL	dw	2025-06-10 09:58:39.623817	t	scheduled	2025-09-01 15:14:29.259867	success	2025-09-11 09:36:38.436527	✅ Server-level connection successful (PostgreSQL)	\N	\N	\N	\N
23	SSM postgres	POWERBI	POWERBI_LOCAL	pbi	2025-06-10 11:40:21.824879	t	manual	2025-09-01 13:17:30.067563	failed	2025-08-18 12:31:44.603017	❌ Unsupported connection type: Power BI Semantic Model	\N	\N	\N	\N
\.


--
-- TOC entry 5404 (class 0 OID 21526)
-- Dependencies: 302
-- Data for Name: dl_ai_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.dl_ai_config (id, connection_id, config_name, analysis_type, model_provider, model_name, model_version, temperature, max_tokens, top_p, frequency_penalty, presence_penalty, path_filter, format_whitelist, partition_filter, include_hidden_files, infer_schema, runner_concurrency, propagation_mode, overwrite_policy, confidence_threshold, respect_human_locks, model_profile, prompt_pack, notes, is_active, updated_at, deleted_at, deleted_by, delete_reason) FROM stdin;
\.


--
-- TOC entry 5410 (class 0 OID 21626)
-- Dependencies: 308
-- Data for Name: dl_catalog_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.dl_catalog_config (id, connection_id, config_name, path_filter, format_whitelist, partition_filter, include_hidden_files, infer_schema, notes, is_active, updated_at, last_test_status, last_tested_at, last_test_notes, deleted_at, deleted_by, delete_reason) FROM stdin;
\.


--
-- TOC entry 5414 (class 0 OID 21685)
-- Dependencies: 312
-- Data for Name: dl_connection_details; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.dl_connection_details (connection_id, storage_type, endpoint_url, bucket_or_container, base_path, auth_method, secret_ref, updated_at) FROM stdin;
\.


--
-- TOC entry 5400 (class 0 OID 21414)
-- Dependencies: 298
-- Data for Name: dw_ai_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.dw_ai_config (id, connection_id, config_name, analysis_type, model_provider, model_name, model_version, temperature, max_tokens, top_p, frequency_penalty, presence_penalty, database_filter, schema_filter, table_filter, runner_concurrency, propagation_mode, overwrite_policy, confidence_threshold, respect_human_locks, model_profile, prompt_pack, notes, is_active, updated_at, deleted_at, deleted_by, delete_reason) FROM stdin;
\.


--
-- TOC entry 5406 (class 0 OID 21585)
-- Dependencies: 304
-- Data for Name: dw_catalog_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.dw_catalog_config (id, connection_id, config_name, database_filter, schema_filter, table_filter, include_views, include_system_objects, notes, is_active, updated_at, last_test_status, last_tested_at, last_test_notes, deleted_at, deleted_by, delete_reason) FROM stdin;
\.


--
-- TOC entry 5411 (class 0 OID 21645)
-- Dependencies: 309
-- Data for Name: dw_connection_details; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.dw_connection_details (connection_id, engine_type, host, port, default_database, username, ssl_mode, updated_at, secret_ref) FROM stdin;
8	AZURE_SQL_SERVER	bisqq.database.windows.net	1433	\N	server_admin	\N	2025-08-15 14:39:48.387415	connection/8/db_password
6	POSTGRESQL	10.3.152.2	5432	\N	postgres	\N	2025-08-15 14:39:48.387415	connection/6/db_password
\.


--
-- TOC entry 5402 (class 0 OID 21487)
-- Dependencies: 300
-- Data for Name: pbi_ai_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.pbi_ai_config (id, connection_id, config_name, analysis_type, model_provider, model_name, model_version, temperature, max_tokens, top_p, frequency_penalty, presence_penalty, workspace_filter, model_filter, table_filter, include_tmdl, include_model_bim, respect_perspectives, runner_concurrency, propagation_mode, overwrite_policy, confidence_threshold, respect_human_locks, model_profile, prompt_pack, notes, is_active, updated_at, deleted_at, deleted_by, delete_reason) FROM stdin;
\.


--
-- TOC entry 5408 (class 0 OID 21605)
-- Dependencies: 306
-- Data for Name: pbi_catalog_config; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.pbi_catalog_config (id, connection_id, config_name, workspace_filter, model_filter, table_filter, include_tmdl, include_model_bim, respect_perspectives, notes, is_active, updated_at, last_test_status, last_tested_at, last_test_notes, deleted_at, deleted_by, delete_reason) FROM stdin;
1	24	test	\N	\N	\N	t	t	t	\N	t	2025-09-09 12:15:42.519082	\N	\N	\N	\N	\N	\N
\.


--
-- TOC entry 5412 (class 0 OID 21658)
-- Dependencies: 310
-- Data for Name: pbi_local_connection_details; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.pbi_local_connection_details (connection_id, folder_path, updated_at) FROM stdin;
23	C:\\semanticmodel_catalog\\SSM_postgres	2025-08-15 14:41:25.902008
24	C:\\semanticmodel_catalog\\SSM_SQL	2025-09-08 14:14:20.065454
\.


--
-- TOC entry 5413 (class 0 OID 21671)
-- Dependencies: 311
-- Data for Name: pbi_service_connection_details; Type: TABLE DATA; Schema: config; Owner: postgres
--

COPY config.pbi_service_connection_details (connection_id, tenant_id, client_id, auth_method, secret_ref, default_workspace_id, default_workspace_name, updated_at) FROM stdin;
\.


--
-- TOC entry 5436 (class 0 OID 49218)
-- Dependencies: 334
-- Data for Name: edge; Type: TABLE DATA; Schema: rel; Owner: postgres
--

COPY rel.edge (edge_id, src_node_id, dst_node_id, edge_type, weight, props, created_at) FROM stdin;
\.


--
-- TOC entry 5437 (class 0 OID 49241)
-- Dependencies: 335
-- Data for Name: fk; Type: TABLE DATA; Schema: rel; Owner: postgres
--

COPY rel.fk (src_table_node_id, dst_table_node_id, src_column_node_id, dst_column_node_id) FROM stdin;
\.


--
-- TOC entry 5415 (class 0 OID 21721)
-- Dependencies: 313
-- Data for Name: secrets_plain; Type: TABLE DATA; Schema: security; Owner: postgres
--

COPY security.secrets_plain (ref_key, secret_value, created_at, updated_at) FROM stdin;
connection/8/db_password	2p^!g3egE7?my	2025-08-15 15:11:27.99353	2025-08-15 15:11:27.99353
connection/6/db_password	gripopdata	2025-08-15 15:11:27.99353	2025-08-15 15:11:27.99353
\.


--
-- TOC entry 5484 (class 0 OID 0)
-- Dependencies: 340
-- Name: catalog_runs_id_seq; Type: SEQUENCE SET; Schema: catalog; Owner: postgres
--

SELECT pg_catalog.setval('catalog.catalog_runs_id_seq', 1, false);


--
-- TOC entry 5485 (class 0 OID 0)
-- Dependencies: 331
-- Name: node_descriptions_id_seq; Type: SEQUENCE SET; Schema: catalog; Owner: postgres
--

SELECT pg_catalog.setval('catalog.node_descriptions_id_seq', 1, false);


--
-- TOC entry 5486 (class 0 OID 0)
-- Dependencies: 318
-- Name: nodes_node_id_seq; Type: SEQUENCE SET; Schema: catalog; Owner: postgres
--

SELECT pg_catalog.setval('catalog.nodes_node_id_seq', 1, false);


--
-- TOC entry 5487 (class 0 OID 0)
-- Dependencies: 245
-- Name: ai_analyzer_connection_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.ai_analyzer_connection_config_id_seq', 3, true);


--
-- TOC entry 5488 (class 0 OID 0)
-- Dependencies: 247
-- Name: catalog_connection_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.catalog_connection_config_id_seq', 3, true);


--
-- TOC entry 5489 (class 0 OID 0)
-- Dependencies: 315
-- Name: connections_new_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.connections_new_id_seq', 24, true);


--
-- TOC entry 5490 (class 0 OID 0)
-- Dependencies: 301
-- Name: dl_ai_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.dl_ai_config_id_seq', 1, false);


--
-- TOC entry 5491 (class 0 OID 0)
-- Dependencies: 307
-- Name: dl_catalog_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.dl_catalog_config_id_seq', 1, false);


--
-- TOC entry 5492 (class 0 OID 0)
-- Dependencies: 297
-- Name: dw_ai_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.dw_ai_config_id_seq', 1, false);


--
-- TOC entry 5493 (class 0 OID 0)
-- Dependencies: 303
-- Name: dw_catalog_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.dw_catalog_config_id_seq', 1, false);


--
-- TOC entry 5494 (class 0 OID 0)
-- Dependencies: 299
-- Name: pbi_ai_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.pbi_ai_config_id_seq', 1, false);


--
-- TOC entry 5495 (class 0 OID 0)
-- Dependencies: 305
-- Name: pbi_catalog_config_id_seq; Type: SEQUENCE SET; Schema: config; Owner: postgres
--

SELECT pg_catalog.setval('config.pbi_catalog_config_id_seq', 1, true);


--
-- TOC entry 5496 (class 0 OID 0)
-- Dependencies: 333
-- Name: edge_edge_id_seq; Type: SEQUENCE SET; Schema: rel; Owner: postgres
--

SELECT pg_catalog.setval('rel.edge_edge_id_seq', 1, false);


--
-- TOC entry 5196 (class 2606 OID 49387)
-- Name: catalog_runs catalog_runs_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.catalog_runs
    ADD CONSTRAINT catalog_runs_pkey PRIMARY KEY (id);


--
-- TOC entry 5154 (class 2606 OID 49093)
-- Name: node_column node_column_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_column
    ADD CONSTRAINT node_column_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5156 (class 2606 OID 49095)
-- Name: node_column node_column_table_node_id_column_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_column
    ADD CONSTRAINT node_column_table_node_id_column_name_key UNIQUE (table_node_id, column_name);


--
-- TOC entry 5142 (class 2606 OID 49040)
-- Name: node_database node_database_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_database
    ADD CONSTRAINT node_database_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5144 (class 2606 OID 49042)
-- Name: node_database node_database_server_name_database_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_database
    ADD CONSTRAINT node_database_server_name_database_name_key UNIQUE (server_name, database_name);


--
-- TOC entry 5172 (class 2606 OID 49211)
-- Name: node_descriptions node_descriptions_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_descriptions
    ADD CONSTRAINT node_descriptions_pkey PRIMARY KEY (id);


--
-- TOC entry 5182 (class 2606 OID 49274)
-- Name: node_dl_container node_dl_container_account_name_container_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_container
    ADD CONSTRAINT node_dl_container_account_name_container_name_key UNIQUE (account_name, container_name);


--
-- TOC entry 5184 (class 2606 OID 49272)
-- Name: node_dl_container node_dl_container_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_container
    ADD CONSTRAINT node_dl_container_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5190 (class 2606 OID 49307)
-- Name: node_dl_file node_dl_file_folder_node_id_file_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_file
    ADD CONSTRAINT node_dl_file_folder_node_id_file_name_key UNIQUE (folder_node_id, file_name);


--
-- TOC entry 5192 (class 2606 OID 49305)
-- Name: node_dl_file node_dl_file_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_file
    ADD CONSTRAINT node_dl_file_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5186 (class 2606 OID 49288)
-- Name: node_dl_folder node_dl_folder_container_node_id_folder_path_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_folder
    ADD CONSTRAINT node_dl_folder_container_node_id_folder_path_key UNIQUE (container_node_id, folder_path);


--
-- TOC entry 5188 (class 2606 OID 49286)
-- Name: node_dl_folder node_dl_folder_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_folder
    ADD CONSTRAINT node_dl_folder_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5170 (class 2606 OID 49194)
-- Name: node_etl_job node_etl_job_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_job
    ADD CONSTRAINT node_etl_job_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5168 (class 2606 OID 49182)
-- Name: node_etl_notebook node_etl_notebook_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_notebook
    ADD CONSTRAINT node_etl_notebook_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5162 (class 2606 OID 49141)
-- Name: node_etl_pipeline node_etl_pipeline_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_pipeline
    ADD CONSTRAINT node_etl_pipeline_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5166 (class 2606 OID 49170)
-- Name: node_etl_script node_etl_script_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_script
    ADD CONSTRAINT node_etl_script_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5164 (class 2606 OID 49153)
-- Name: node_etl_step node_etl_step_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_step
    ADD CONSTRAINT node_etl_step_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5160 (class 2606 OID 49124)
-- Name: node_pbi_measure node_pbi_measure_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_measure
    ADD CONSTRAINT node_pbi_measure_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5158 (class 2606 OID 49112)
-- Name: node_pbi_model node_pbi_model_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_model
    ADD CONSTRAINT node_pbi_model_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5194 (class 2606 OID 49340)
-- Name: node_pbi_query node_pbi_query_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_query
    ADD CONSTRAINT node_pbi_query_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5146 (class 2606 OID 49056)
-- Name: node_schema node_schema_database_node_id_schema_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_schema
    ADD CONSTRAINT node_schema_database_node_id_schema_name_key UNIQUE (database_node_id, schema_name);


--
-- TOC entry 5148 (class 2606 OID 49054)
-- Name: node_schema node_schema_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_schema
    ADD CONSTRAINT node_schema_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5150 (class 2606 OID 49074)
-- Name: node_table node_table_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_table
    ADD CONSTRAINT node_table_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5152 (class 2606 OID 49076)
-- Name: node_table node_table_schema_node_id_table_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_table
    ADD CONSTRAINT node_table_schema_node_id_table_name_key UNIQUE (schema_node_id, table_name);


--
-- TOC entry 5138 (class 2606 OID 49026)
-- Name: nodes nodes_node_type_qualified_name_key; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.nodes
    ADD CONSTRAINT nodes_node_type_qualified_name_key UNIQUE (node_type, qualified_name);


--
-- TOC entry 5140 (class 2606 OID 49024)
-- Name: nodes nodes_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (node_id);


--
-- TOC entry 5134 (class 2606 OID 49011)
-- Name: object_type object_type_pkey; Type: CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.object_type
    ADD CONSTRAINT object_type_pkey PRIMARY KEY (object_type_code);


--
-- TOC entry 5078 (class 2606 OID 18565)
-- Name: ai_analyzer_connection_config ai_analyzer_connection_config_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.ai_analyzer_connection_config
    ADD CONSTRAINT ai_analyzer_connection_config_pkey PRIMARY KEY (id);


--
-- TOC entry 5081 (class 2606 OID 18584)
-- Name: catalog_connection_config catalog_connection_config_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.catalog_connection_config
    ADD CONSTRAINT catalog_connection_config_pkey PRIMARY KEY (id);


--
-- TOC entry 5119 (class 2606 OID 21896)
-- Name: connection_type_registry connection_type_registry_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.connection_type_registry
    ADD CONSTRAINT connection_type_registry_pkey PRIMARY KEY (connection_type);


--
-- TOC entry 5125 (class 2606 OID 21826)
-- Name: connections connections_new_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.connections
    ADD CONSTRAINT connections_new_pkey PRIMARY KEY (id);


--
-- TOC entry 5104 (class 2606 OID 21637)
-- Name: dl_catalog_config dl_catalog_config_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dl_catalog_config
    ADD CONSTRAINT dl_catalog_config_pkey PRIMARY KEY (id);


--
-- TOC entry 5114 (class 2606 OID 21692)
-- Name: dl_connection_details dl_connection_details_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dl_connection_details
    ADD CONSTRAINT dl_connection_details_pkey PRIMARY KEY (connection_id);


--
-- TOC entry 5096 (class 2606 OID 21596)
-- Name: dw_catalog_config dw_catalog_config_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dw_catalog_config
    ADD CONSTRAINT dw_catalog_config_pkey PRIMARY KEY (id);


--
-- TOC entry 5108 (class 2606 OID 21652)
-- Name: dw_connection_details dw_connection_details_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dw_connection_details
    ADD CONSTRAINT dw_connection_details_pkey PRIMARY KEY (connection_id);


--
-- TOC entry 5102 (class 2606 OID 21617)
-- Name: pbi_catalog_config pbi_catalog_config_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_catalog_config
    ADD CONSTRAINT pbi_catalog_config_pkey PRIMARY KEY (id);


--
-- TOC entry 5110 (class 2606 OID 21665)
-- Name: pbi_local_connection_details pbi_local_connection_details_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_local_connection_details
    ADD CONSTRAINT pbi_local_connection_details_pkey PRIMARY KEY (connection_id);


--
-- TOC entry 5112 (class 2606 OID 21679)
-- Name: pbi_service_connection_details pbi_service_connection_details_pkey; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_service_connection_details
    ADD CONSTRAINT pbi_service_connection_details_pkey PRIMARY KEY (connection_id);


--
-- TOC entry 5094 (class 2606 OID 21554)
-- Name: dl_ai_config pk_dl_ai_config; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dl_ai_config
    ADD CONSTRAINT pk_dl_ai_config PRIMARY KEY (id);


--
-- TOC entry 5086 (class 2606 OID 21440)
-- Name: dw_ai_config pk_dw_ai_config; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dw_ai_config
    ADD CONSTRAINT pk_dw_ai_config PRIMARY KEY (id);


--
-- TOC entry 5090 (class 2606 OID 21516)
-- Name: pbi_ai_config pk_pbi_ai_config; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_ai_config
    ADD CONSTRAINT pk_pbi_ai_config PRIMARY KEY (id);


--
-- TOC entry 5123 (class 2606 OID 21912)
-- Name: connection_type_registry uq_connection_type_registry_display_name; Type: CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.connection_type_registry
    ADD CONSTRAINT uq_connection_type_registry_display_name UNIQUE (display_name);


--
-- TOC entry 5174 (class 2606 OID 49226)
-- Name: edge edge_pkey; Type: CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.edge
    ADD CONSTRAINT edge_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 5180 (class 2606 OID 49245)
-- Name: fk fk_pkey; Type: CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.fk
    ADD CONSTRAINT fk_pkey PRIMARY KEY (src_column_node_id, dst_column_node_id);


--
-- TOC entry 5117 (class 2606 OID 21729)
-- Name: secrets_plain secrets_plain_pkey; Type: CONSTRAINT; Schema: security; Owner: postgres
--

ALTER TABLE ONLY security.secrets_plain
    ADD CONSTRAINT secrets_plain_pkey PRIMARY KEY (ref_key);


--
-- TOC entry 5135 (class 1259 OID 49033)
-- Name: ix_nodes_qualified; Type: INDEX; Schema: catalog; Owner: postgres
--

CREATE INDEX ix_nodes_qualified ON catalog.nodes USING btree (qualified_name);


--
-- TOC entry 5136 (class 1259 OID 49032)
-- Name: ix_nodes_type_name; Type: INDEX; Schema: catalog; Owner: postgres
--

CREATE INDEX ix_nodes_type_name ON catalog.nodes USING btree (node_type, name);


--
-- TOC entry 5079 (class 1259 OID 18604)
-- Name: idx_ai_analyzer_connection_config_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_ai_analyzer_connection_config_active ON config.ai_analyzer_connection_config USING btree (is_active);


--
-- TOC entry 5082 (class 1259 OID 18602)
-- Name: idx_catalog_connection_config_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_catalog_connection_config_active ON config.catalog_connection_config USING btree (is_active);


--
-- TOC entry 5126 (class 1259 OID 21923)
-- Name: idx_connections_deleted_at; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_connections_deleted_at ON config.connections USING btree (deleted_at);


--
-- TOC entry 5127 (class 1259 OID 21829)
-- Name: idx_connections_new_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_connections_new_active ON config.connections USING btree (is_active);


--
-- TOC entry 5128 (class 1259 OID 21830)
-- Name: idx_connections_new_connection_type; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_connections_new_connection_type ON config.connections USING btree (connection_type);


--
-- TOC entry 5129 (class 1259 OID 21831)
-- Name: idx_connections_new_data_source_category; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_connections_new_data_source_category ON config.connections USING btree (data_source_category);


--
-- TOC entry 5130 (class 1259 OID 21832)
-- Name: idx_connections_new_name; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_connections_new_name ON config.connections USING btree (connection_name);


--
-- TOC entry 5131 (class 1259 OID 21833)
-- Name: idx_connections_new_short_code; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_connections_new_short_code ON config.connections USING btree (short_code);


--
-- TOC entry 5120 (class 1259 OID 21913)
-- Name: idx_ctr_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_ctr_active ON config.connection_type_registry USING btree (is_active) WHERE is_active;


--
-- TOC entry 5121 (class 1259 OID 21905)
-- Name: idx_ctr_category; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_ctr_category ON config.connection_type_registry USING btree (data_source_category);


--
-- TOC entry 5091 (class 1259 OID 21561)
-- Name: idx_dl_ai_config_conn_type_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dl_ai_config_conn_type_active ON config.dl_ai_config USING btree (connection_id, analysis_type, is_active, updated_at DESC);


--
-- TOC entry 5092 (class 1259 OID 21560)
-- Name: idx_dl_ai_config_connection; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dl_ai_config_connection ON config.dl_ai_config USING btree (connection_id);


--
-- TOC entry 5105 (class 1259 OID 21644)
-- Name: idx_dl_catalog_config_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dl_catalog_config_active ON config.dl_catalog_config USING btree (is_active);


--
-- TOC entry 5106 (class 1259 OID 21643)
-- Name: idx_dl_catalog_config_connection; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dl_catalog_config_connection ON config.dl_catalog_config USING btree (connection_id);


--
-- TOC entry 5083 (class 1259 OID 21447)
-- Name: idx_dw_ai_config_conn_type_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dw_ai_config_conn_type_active ON config.dw_ai_config USING btree (connection_id, analysis_type, is_active, updated_at DESC);


--
-- TOC entry 5084 (class 1259 OID 21446)
-- Name: idx_dw_ai_config_connection; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dw_ai_config_connection ON config.dw_ai_config USING btree (connection_id);


--
-- TOC entry 5097 (class 1259 OID 21603)
-- Name: idx_dw_catalog_config_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dw_catalog_config_active ON config.dw_catalog_config USING btree (is_active);


--
-- TOC entry 5098 (class 1259 OID 21602)
-- Name: idx_dw_catalog_config_connection; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_dw_catalog_config_connection ON config.dw_catalog_config USING btree (connection_id);


--
-- TOC entry 5087 (class 1259 OID 21523)
-- Name: idx_pbi_ai_config_conn_type_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_pbi_ai_config_conn_type_active ON config.pbi_ai_config USING btree (connection_id, analysis_type, is_active, updated_at DESC);


--
-- TOC entry 5088 (class 1259 OID 21522)
-- Name: idx_pbi_ai_config_connection; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_pbi_ai_config_connection ON config.pbi_ai_config USING btree (connection_id);


--
-- TOC entry 5099 (class 1259 OID 21624)
-- Name: idx_pbi_catalog_config_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_pbi_catalog_config_active ON config.pbi_catalog_config USING btree (is_active);


--
-- TOC entry 5100 (class 1259 OID 21623)
-- Name: idx_pbi_catalog_config_connection; Type: INDEX; Schema: config; Owner: postgres
--

CREATE INDEX idx_pbi_catalog_config_connection ON config.pbi_catalog_config USING btree (connection_id);


--
-- TOC entry 5132 (class 1259 OID 21922)
-- Name: uq_connections_name_active; Type: INDEX; Schema: config; Owner: postgres
--

CREATE UNIQUE INDEX uq_connections_name_active ON config.connections USING btree (connection_name) WHERE (deleted_at IS NULL);


--
-- TOC entry 5175 (class 1259 OID 49238)
-- Name: ix_edge_dst; Type: INDEX; Schema: rel; Owner: postgres
--

CREATE INDEX ix_edge_dst ON rel.edge USING btree (dst_node_id);


--
-- TOC entry 5176 (class 1259 OID 49237)
-- Name: ix_edge_src; Type: INDEX; Schema: rel; Owner: postgres
--

CREATE INDEX ix_edge_src ON rel.edge USING btree (src_node_id);


--
-- TOC entry 5177 (class 1259 OID 49240)
-- Name: ix_edge_type_dst; Type: INDEX; Schema: rel; Owner: postgres
--

CREATE INDEX ix_edge_type_dst ON rel.edge USING btree (edge_type, dst_node_id);


--
-- TOC entry 5178 (class 1259 OID 49239)
-- Name: ix_edge_type_src; Type: INDEX; Schema: rel; Owner: postgres
--

CREATE INDEX ix_edge_type_src ON rel.edge USING btree (edge_type, src_node_id);


--
-- TOC entry 5115 (class 1259 OID 21730)
-- Name: idx_secrets_plain_updated_at; Type: INDEX; Schema: security; Owner: postgres
--

CREATE INDEX idx_secrets_plain_updated_at ON security.secrets_plain USING btree (updated_at);


--
-- TOC entry 5243 (class 2620 OID 21926)
-- Name: connections tr_connections_prevent_activate_when_deleted; Type: TRIGGER; Schema: config; Owner: postgres
--

CREATE TRIGGER tr_connections_prevent_activate_when_deleted BEFORE UPDATE OF is_active, deleted_at ON config.connections FOR EACH ROW EXECUTE FUNCTION config.fn_connections_prevent_activate_when_deleted();


--
-- TOC entry 5244 (class 2620 OID 21921)
-- Name: connections tr_connections_sync_from_registry; Type: TRIGGER; Schema: config; Owner: postgres
--

CREATE TRIGGER tr_connections_sync_from_registry BEFORE INSERT OR UPDATE OF connection_type, data_source_category, short_code ON config.connections FOR EACH ROW EXECUTE FUNCTION config.fn_connections_sync_from_registry();


--
-- TOC entry 5245 (class 2620 OID 21928)
-- Name: connections tr_prevent_activate_deleted; Type: TRIGGER; Schema: config; Owner: postgres
--

CREATE TRIGGER tr_prevent_activate_deleted BEFORE UPDATE OF is_active ON config.connections FOR EACH ROW EXECUTE FUNCTION config.fn_prevent_activate_deleted();


--
-- TOC entry 5217 (class 2606 OID 49096)
-- Name: node_column node_column_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_column
    ADD CONSTRAINT node_column_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5218 (class 2606 OID 49101)
-- Name: node_column node_column_table_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_column
    ADD CONSTRAINT node_column_table_node_id_fkey FOREIGN KEY (table_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5212 (class 2606 OID 49043)
-- Name: node_database node_database_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_database
    ADD CONSTRAINT node_database_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5228 (class 2606 OID 49212)
-- Name: node_descriptions node_descriptions_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_descriptions
    ADD CONSTRAINT node_descriptions_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5235 (class 2606 OID 49275)
-- Name: node_dl_container node_dl_container_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_container
    ADD CONSTRAINT node_dl_container_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5238 (class 2606 OID 49313)
-- Name: node_dl_file node_dl_file_folder_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_file
    ADD CONSTRAINT node_dl_file_folder_node_id_fkey FOREIGN KEY (folder_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5239 (class 2606 OID 49308)
-- Name: node_dl_file node_dl_file_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_file
    ADD CONSTRAINT node_dl_file_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5236 (class 2606 OID 49294)
-- Name: node_dl_folder node_dl_folder_container_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_folder
    ADD CONSTRAINT node_dl_folder_container_node_id_fkey FOREIGN KEY (container_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5237 (class 2606 OID 49289)
-- Name: node_dl_folder node_dl_folder_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_dl_folder
    ADD CONSTRAINT node_dl_folder_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5227 (class 2606 OID 49195)
-- Name: node_etl_job node_etl_job_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_job
    ADD CONSTRAINT node_etl_job_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5226 (class 2606 OID 49183)
-- Name: node_etl_notebook node_etl_notebook_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_notebook
    ADD CONSTRAINT node_etl_notebook_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5222 (class 2606 OID 49142)
-- Name: node_etl_pipeline node_etl_pipeline_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_pipeline
    ADD CONSTRAINT node_etl_pipeline_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5225 (class 2606 OID 49171)
-- Name: node_etl_script node_etl_script_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_script
    ADD CONSTRAINT node_etl_script_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5223 (class 2606 OID 49154)
-- Name: node_etl_step node_etl_step_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_step
    ADD CONSTRAINT node_etl_step_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5224 (class 2606 OID 49159)
-- Name: node_etl_step node_etl_step_pipeline_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_etl_step
    ADD CONSTRAINT node_etl_step_pipeline_node_id_fkey FOREIGN KEY (pipeline_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5220 (class 2606 OID 49125)
-- Name: node_pbi_measure node_pbi_measure_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_measure
    ADD CONSTRAINT node_pbi_measure_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5221 (class 2606 OID 49130)
-- Name: node_pbi_measure node_pbi_measure_table_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_measure
    ADD CONSTRAINT node_pbi_measure_table_node_id_fkey FOREIGN KEY (table_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5219 (class 2606 OID 49113)
-- Name: node_pbi_model node_pbi_model_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_model
    ADD CONSTRAINT node_pbi_model_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5240 (class 2606 OID 49346)
-- Name: node_pbi_query node_pbi_query_model_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_query
    ADD CONSTRAINT node_pbi_query_model_node_id_fkey FOREIGN KEY (model_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5241 (class 2606 OID 49341)
-- Name: node_pbi_query node_pbi_query_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_query
    ADD CONSTRAINT node_pbi_query_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5242 (class 2606 OID 49351)
-- Name: node_pbi_query node_pbi_query_table_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_pbi_query
    ADD CONSTRAINT node_pbi_query_table_node_id_fkey FOREIGN KEY (table_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5213 (class 2606 OID 49062)
-- Name: node_schema node_schema_database_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_schema
    ADD CONSTRAINT node_schema_database_node_id_fkey FOREIGN KEY (database_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5214 (class 2606 OID 49057)
-- Name: node_schema node_schema_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_schema
    ADD CONSTRAINT node_schema_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5215 (class 2606 OID 49077)
-- Name: node_table node_table_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_table
    ADD CONSTRAINT node_table_node_id_fkey FOREIGN KEY (node_id) REFERENCES catalog.nodes(node_id) ON DELETE CASCADE;


--
-- TOC entry 5216 (class 2606 OID 49082)
-- Name: node_table node_table_schema_node_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.node_table
    ADD CONSTRAINT node_table_schema_node_id_fkey FOREIGN KEY (schema_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5211 (class 2606 OID 49027)
-- Name: nodes nodes_node_type_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: postgres
--

ALTER TABLE ONLY catalog.nodes
    ADD CONSTRAINT nodes_node_type_fkey FOREIGN KEY (node_type) REFERENCES catalog.object_type(object_type_code);


--
-- TOC entry 5197 (class 2606 OID 21854)
-- Name: ai_analyzer_connection_config ai_analyzer_connection_config_connection_id_fkey; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.ai_analyzer_connection_config
    ADD CONSTRAINT ai_analyzer_connection_config_connection_id_fkey FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5198 (class 2606 OID 21859)
-- Name: catalog_connection_config catalog_connection_config_connection_id_fkey; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.catalog_connection_config
    ADD CONSTRAINT catalog_connection_config_connection_id_fkey FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5209 (class 2606 OID 21906)
-- Name: connections connections_connection_type_fkey; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.connections
    ADD CONSTRAINT connections_connection_type_fkey FOREIGN KEY (connection_type) REFERENCES config.connection_type_registry(connection_type);


--
-- TOC entry 5210 (class 2606 OID 21914)
-- Name: connections fk_connections_connection_type; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.connections
    ADD CONSTRAINT fk_connections_connection_type FOREIGN KEY (connection_type) REFERENCES config.connection_type_registry(connection_type);


--
-- TOC entry 5201 (class 2606 OID 21874)
-- Name: dl_ai_config fk_dl_ai_config_connection; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dl_ai_config
    ADD CONSTRAINT fk_dl_ai_config_connection FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5204 (class 2606 OID 21889)
-- Name: dl_catalog_config fk_dl_catalog_config_connection; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dl_catalog_config
    ADD CONSTRAINT fk_dl_catalog_config_connection FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5208 (class 2606 OID 21849)
-- Name: dl_connection_details fk_dl_conn_details; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dl_connection_details
    ADD CONSTRAINT fk_dl_conn_details FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5199 (class 2606 OID 21864)
-- Name: dw_ai_config fk_dw_ai_config_connection; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dw_ai_config
    ADD CONSTRAINT fk_dw_ai_config_connection FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5202 (class 2606 OID 21879)
-- Name: dw_catalog_config fk_dw_catalog_config_connection; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dw_catalog_config
    ADD CONSTRAINT fk_dw_catalog_config_connection FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5205 (class 2606 OID 21834)
-- Name: dw_connection_details fk_dw_conn_details; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.dw_connection_details
    ADD CONSTRAINT fk_dw_conn_details FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5200 (class 2606 OID 21869)
-- Name: pbi_ai_config fk_pbi_ai_config_connection; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_ai_config
    ADD CONSTRAINT fk_pbi_ai_config_connection FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5203 (class 2606 OID 21884)
-- Name: pbi_catalog_config fk_pbi_catalog_config_connection; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_catalog_config
    ADD CONSTRAINT fk_pbi_catalog_config_connection FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5206 (class 2606 OID 21839)
-- Name: pbi_local_connection_details fk_pbi_local_conn; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_local_connection_details
    ADD CONSTRAINT fk_pbi_local_conn FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5207 (class 2606 OID 21844)
-- Name: pbi_service_connection_details fk_pbi_service_conn; Type: FK CONSTRAINT; Schema: config; Owner: postgres
--

ALTER TABLE ONLY config.pbi_service_connection_details
    ADD CONSTRAINT fk_pbi_service_conn FOREIGN KEY (connection_id) REFERENCES config.connections(id) ON DELETE CASCADE;


--
-- TOC entry 5229 (class 2606 OID 49232)
-- Name: edge edge_dst_node_id_fkey; Type: FK CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.edge
    ADD CONSTRAINT edge_dst_node_id_fkey FOREIGN KEY (dst_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5230 (class 2606 OID 49227)
-- Name: edge edge_src_node_id_fkey; Type: FK CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.edge
    ADD CONSTRAINT edge_src_node_id_fkey FOREIGN KEY (src_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5231 (class 2606 OID 49261)
-- Name: fk fk_dst_column_node_id_fkey; Type: FK CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.fk
    ADD CONSTRAINT fk_dst_column_node_id_fkey FOREIGN KEY (dst_column_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5232 (class 2606 OID 49251)
-- Name: fk fk_dst_table_node_id_fkey; Type: FK CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.fk
    ADD CONSTRAINT fk_dst_table_node_id_fkey FOREIGN KEY (dst_table_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5233 (class 2606 OID 49256)
-- Name: fk fk_src_column_node_id_fkey; Type: FK CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.fk
    ADD CONSTRAINT fk_src_column_node_id_fkey FOREIGN KEY (src_column_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5234 (class 2606 OID 49246)
-- Name: fk fk_src_table_node_id_fkey; Type: FK CONSTRAINT; Schema: rel; Owner: postgres
--

ALTER TABLE ONLY rel.fk
    ADD CONSTRAINT fk_src_table_node_id_fkey FOREIGN KEY (src_table_node_id) REFERENCES catalog.nodes(node_id);


--
-- TOC entry 5449 (class 0 OID 0)
-- Dependencies: 6
-- Name: SCHEMA config; Type: ACL; Schema: -; Owner: postgres
--

GRANT ALL ON SCHEMA config TO catalog_admin;


--
-- TOC entry 5451 (class 0 OID 0)
-- Dependencies: 5
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT USAGE ON SCHEMA public TO catalog_admin;


--
-- TOC entry 5452 (class 0 OID 0)
-- Dependencies: 13
-- Name: SCHEMA security; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA security TO catalog_admin;


--
-- TOC entry 5456 (class 0 OID 0)
-- Dependencies: 246
-- Name: TABLE ai_analyzer_connection_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.ai_analyzer_connection_config TO catalog_admin;


--
-- TOC entry 5458 (class 0 OID 0)
-- Dependencies: 245
-- Name: SEQUENCE ai_analyzer_connection_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.ai_analyzer_connection_config_id_seq TO catalog_admin;


--
-- TOC entry 5459 (class 0 OID 0)
-- Dependencies: 248
-- Name: TABLE catalog_connection_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.catalog_connection_config TO catalog_admin;


--
-- TOC entry 5461 (class 0 OID 0)
-- Dependencies: 247
-- Name: SEQUENCE catalog_connection_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.catalog_connection_config_id_seq TO catalog_admin;


--
-- TOC entry 5462 (class 0 OID 0)
-- Dependencies: 314
-- Name: TABLE connection_type_registry; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.connection_type_registry TO catalog_admin;


--
-- TOC entry 5463 (class 0 OID 0)
-- Dependencies: 316
-- Name: TABLE connections; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.connections TO catalog_admin;


--
-- TOC entry 5465 (class 0 OID 0)
-- Dependencies: 315
-- Name: SEQUENCE connections_new_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.connections_new_id_seq TO catalog_admin;


--
-- TOC entry 5466 (class 0 OID 0)
-- Dependencies: 302
-- Name: TABLE dl_ai_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.dl_ai_config TO catalog_admin;


--
-- TOC entry 5467 (class 0 OID 0)
-- Dependencies: 301
-- Name: SEQUENCE dl_ai_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.dl_ai_config_id_seq TO catalog_admin;


--
-- TOC entry 5468 (class 0 OID 0)
-- Dependencies: 308
-- Name: TABLE dl_catalog_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.dl_catalog_config TO catalog_admin;


--
-- TOC entry 5469 (class 0 OID 0)
-- Dependencies: 307
-- Name: SEQUENCE dl_catalog_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.dl_catalog_config_id_seq TO catalog_admin;


--
-- TOC entry 5470 (class 0 OID 0)
-- Dependencies: 312
-- Name: TABLE dl_connection_details; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.dl_connection_details TO catalog_admin;


--
-- TOC entry 5471 (class 0 OID 0)
-- Dependencies: 298
-- Name: TABLE dw_ai_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.dw_ai_config TO catalog_admin;


--
-- TOC entry 5472 (class 0 OID 0)
-- Dependencies: 297
-- Name: SEQUENCE dw_ai_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.dw_ai_config_id_seq TO catalog_admin;


--
-- TOC entry 5473 (class 0 OID 0)
-- Dependencies: 304
-- Name: TABLE dw_catalog_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.dw_catalog_config TO catalog_admin;


--
-- TOC entry 5474 (class 0 OID 0)
-- Dependencies: 303
-- Name: SEQUENCE dw_catalog_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.dw_catalog_config_id_seq TO catalog_admin;


--
-- TOC entry 5475 (class 0 OID 0)
-- Dependencies: 309
-- Name: TABLE dw_connection_details; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.dw_connection_details TO catalog_admin;


--
-- TOC entry 5476 (class 0 OID 0)
-- Dependencies: 300
-- Name: TABLE pbi_ai_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.pbi_ai_config TO catalog_admin;


--
-- TOC entry 5477 (class 0 OID 0)
-- Dependencies: 299
-- Name: SEQUENCE pbi_ai_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.pbi_ai_config_id_seq TO catalog_admin;


--
-- TOC entry 5478 (class 0 OID 0)
-- Dependencies: 306
-- Name: TABLE pbi_catalog_config; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.pbi_catalog_config TO catalog_admin;


--
-- TOC entry 5479 (class 0 OID 0)
-- Dependencies: 305
-- Name: SEQUENCE pbi_catalog_config_id_seq; Type: ACL; Schema: config; Owner: postgres
--

GRANT ALL ON SEQUENCE config.pbi_catalog_config_id_seq TO catalog_admin;


--
-- TOC entry 5480 (class 0 OID 0)
-- Dependencies: 310
-- Name: TABLE pbi_local_connection_details; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.pbi_local_connection_details TO catalog_admin;


--
-- TOC entry 5481 (class 0 OID 0)
-- Dependencies: 311
-- Name: TABLE pbi_service_connection_details; Type: ACL; Schema: config; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE config.pbi_service_connection_details TO catalog_admin;


--
-- TOC entry 5483 (class 0 OID 0)
-- Dependencies: 313
-- Name: TABLE secrets_plain; Type: ACL; Schema: security; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE security.secrets_plain TO catalog_admin;


--
-- TOC entry 2348 (class 826 OID 17648)
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: config; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA config GRANT ALL ON SEQUENCES TO catalog_admin;


--
-- TOC entry 2347 (class 826 OID 17647)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: config; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA config GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO catalog_admin;


--
-- TOC entry 2352 (class 826 OID 17652)
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO catalog_admin;


--
-- TOC entry 2351 (class 826 OID 17651)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO catalog_admin;


--
-- TOC entry 2353 (class 826 OID 21894)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: security; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA security GRANT SELECT,INSERT,UPDATE ON TABLES TO catalog_admin;


-- Completed on 2025-11-24 16:54:14

--
-- PostgreSQL database dump complete
--

