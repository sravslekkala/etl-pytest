
from pathlib import Path
import pandas as pd

def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)

def load_file(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".csv":
        return read_csv(path)
    if suf in (".parquet", ".pq"):
        return read_parquet(path)
    raise ValueError(f"Unsupported file type: {suf}")
