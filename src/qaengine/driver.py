
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json, os
from jsonschema import validate
import pandas as pd

from .config import load_settings
from .connections import get_engine
from .sql import run_query, fetch_scalar
from .files import load_file
from .validators import (
    assert_no_nulls_df, assert_no_duplicates_df, assert_expected_schema_df, scd2_health_df
)

@dataclass
class Case:
    suite: str
    name: str
    type: str
    params: dict[str, Any]

def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_schema() -> dict:
    return _load_json(Path("cases/schema.json"))

def find_case_files(suite: str) -> list[Path]:
    root = Path("cases") / suite
    return sorted([p for p in root.glob("*.json")])

def parse_cases(suite: str) -> list[Case]:
    schema = load_schema()
    out: list[Case] = []
    for f in find_case_files(suite):
        doc = _load_json(f)
        validate(instance=doc, schema=schema)
        for item in doc.get("cases", []):
            out.append(Case(suite=suite, name=item["name"], type=item["type"], params=item["params"]))
    return out

def _resolve_engine(alias: str):
    settings = load_settings()
    info = settings.connections[alias]
    return get_engine(alias, info.url), info.schema

def _df_from_source(spec: dict) -> pd.DataFrame:
    # spec:
    #   kind: "sql" | "file"
    #   conn_alias: "local_sqlite"
    #   query: "SELECT ..."
    #   path: "relative/or/absolute"
    if spec["kind"] == "sql":
        eng, _ = _resolve_engine(spec["conn_alias"])
        return run_query(eng, spec["query"])
    elif spec["kind"] == "file":
        st = load_settings()
        p = Path(spec["path"])
        if not p.is_absolute():
            p = st.data_dir / p
        return load_file(p)
    else:
        raise ValueError(f"Unknown source kind: {spec['kind']}")

def execute_case(case: Case):
    t = case.type
    p = case.params

    if t == "row_count":
        src = _df_from_source(p["source"])
        tgt = _df_from_source(p["target"])
        tol = float(p.get("tolerance_pct", 0))
        s, g = len(src), len(tgt)
        if tol == 0:
            assert s == g, f"Row count mismatch: source={s}, target={g}"
        else:
            delta = abs(s - g) / max(1, s) * 100
            assert delta <= tol, f"Row count delta {delta:.2f}% exceeds tolerance {tol}%"
        return

    if t == "query_expect":
        eng, _ = _resolve_engine(p["conn_alias"])
        mode = p.get("mode", "scalar")
        if mode == "scalar":
            got = fetch_scalar(eng, p["query"], p.get("params"))
            exp = p.get("expected")
            assert got == exp, f"Expected {exp}, got {got}"
        elif mode == "rowcount":
            df = run_query(eng, p["query"], p.get("params"))
            exp = int(p["expected"])
            assert len(df) == exp, f"Expected {exp} rows, got {len(df)}"
        else:
            raise ValueError(f"Unsupported query_expect mode: {mode}")
        return

    if t == "null_check":
        df = _df_from_source(p["source"])
        assert_no_nulls_df(df, cols=p.get("columns"))
        return

    if t == "duplicate_check":
        df = _df_from_source(p["source"])
        assert_no_duplicates_df(df, subset=p["key_columns"])
        return

    if t == "schema_check":
        df = _df_from_source(p["source"])
        expect = p["expected_columns"]
        assert_expected_schema_df(list(df.columns), expect, exact=bool(p.get("exact", False)))
        return

    if t == "scd2_health":
        df = _df_from_source(p["target"])
        scd = p["scd2"]
        scd2_health_df(df, bk=scd["bk"], eff_from=scd["eff_from"], eff_to=scd["eff_to"], is_current=scd["is_current"])
        return

    raise ValueError(f"Unknown case type: {t}")
