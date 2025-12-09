import os
import pytest
import builtins
from dotenv import load_dotenv
from ai_analyzer.runners.table_runner import run_batch_tables_by_config
import logging

# pytest-compatible print
print = builtins.print

# Logging config
logging.basicConfig(level=logging.DEBUG)

# Load .env
load_dotenv(dotenv_path=".env", override=True)

RUN_REAL_DB_TESTS = os.getenv("RUN_REAL_DB_TESTS") == "1"
NAV_DB_HOST = os.getenv("NAV_DB_HOST")
NAV_DB_NAME = os.getenv("NAV_DB_NAME")


pytestmark = pytest.mark.skipif(
    not RUN_REAL_DB_TESTS or not NAV_DB_HOST or not NAV_DB_NAME,
    reason="Real DB tests uitgeschakeld of NAV_DB_* niet geconfigureerd"
)


# Herbruikbare functie
def _run_live_analysis(analysis_type: str):
    print(f"\n=== START: {analysis_type} ===")
    run_batch_tables_by_config(
        ai_config_id=1,
        analysis_type=analysis_type,
        author="pytest_user",
        dry_run=False
    )
    print(f"=== END: {analysis_type} ===\n")


# Tests per analysis_type
def test_column_classification():
    _run_live_analysis("column_classification")


def test_base_table_analysis():
    _run_live_analysis("base_table_analysis")


def test_data_quality_check():
    _run_live_analysis("data_quality_check")


def test_data_presence_analysis():
    _run_live_analysis("data_presence_analysis")


def test_view_definition_analysis():
    _run_live_analysis("view_definition_analysis")
