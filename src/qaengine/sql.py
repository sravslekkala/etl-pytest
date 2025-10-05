
from typing import Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd

def run_query(engine: Engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params or {})
    return df

def fetch_scalar(engine: Engine, sql: str, params: dict | None = None) -> Any:
    with engine.connect() as conn:
        res = conn.execute(text(sql), params or {})
        row = res.fetchone()
        return None if row is None else (row[0] if len(row) else None)
