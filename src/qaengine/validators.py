
from __future__ import annotations
from typing import Iterable
import pandas as pd

def assert_no_nulls_df(df: pd.DataFrame, cols: Iterable[str] | None = None):
    cols = list(cols) if cols else list(df.columns)
    bad = {c: int(df[c].isna().sum()) for c in cols if df[c].isna().any()}
    assert not bad, f"Nulls found: {bad}"

def assert_no_duplicates_df(df: pd.DataFrame, subset: Iterable[str] | None = None):
    dups = df.duplicated(subset=list(subset) if subset else None)
    n = int(dups.sum())
    assert n == 0, f"{n} duplicate rows found{(' on ' + ','.join(subset)) if subset else ''}"

def assert_expected_schema_df(actual_cols: list[str], expected_cols: list[str], exact: bool = False):
    missing = [c for c in expected_cols if c not in actual_cols]
    extra = [c for c in actual_cols if c not in expected_cols]
    if exact:
        assert not missing and not extra, f"Schema mismatch. Missing: {missing} Extra: {extra}"
    else:
        assert not missing, f"Missing expected columns: {missing}"

def scd2_health_df(target: pd.DataFrame, bk: str, eff_from: str, eff_to: str, is_current: str):
    t = target.copy()
    t[is_current] = t[is_current].astype(int)
    grp = t.groupby(bk, as_index=False)[is_current].sum()
    bad = grp[grp[is_current] != 1]
    assert bad.empty, f"SCD2 violation: keys without exactly one current row: {bad[bk].tolist()}"
    def _overlaps(g: pd.DataFrame) -> bool:
        g = g.sort_values(eff_from).reset_index(drop=True)
        overlaps = (g[eff_from] < g[eff_to].shift(1)) & g[eff_to].shift(1).notna()
        return bool(overlaps.any())
    ov = t.groupby(bk).apply(_overlaps)
    bad_keys = ov[ov].index.tolist()
    assert not bad_keys, f"SCD2 violation: overlapping ranges for {bad_keys}"
