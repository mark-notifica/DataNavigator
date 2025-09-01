import os
import logging
from functools import lru_cache
from typing import Optional
import sqlalchemy as sa
from dotenv import load_dotenv
from pathlib import Path

# ---------------------------------------
# Load environment + logger
# ---------------------------------------
load_dotenv()
logger = logging.getLogger(__name__)


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


# ---------------- Low-level DB helpers ----------------
def q_all(sql: str, params: dict | None = None) -> list[sa.Row]:
    with engine.connect() as c:
        return list(c.execute(sa.text(sql), params or {}))

def q_one(sql: str, params: dict | None = None) -> Optional[sa.Row]:
    with engine.connect() as c:
        res = c.execute(sa.text(sql), params or {})
        row = res.fetchone()
        return row

def exec_tx(sql: str, params: dict | None = None):
    with engine.begin() as c:
        c.execute(sa.text(sql), params or {})