
from dataclasses import dataclass
from pathlib import Path
import os, yaml
from dotenv import load_dotenv

load_dotenv()

@dataclass
class ConnectionInfo:
    alias: str
    url: str
    schema: str | None = None

@dataclass
class Settings:
    default_conn_alias: str
    data_dir: Path
    connections: dict[str, ConnectionInfo]

def load_settings() -> Settings:
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()
    default_conn_alias = os.getenv("DEFAULT_CONN_ALIAS", "local_sqlite")
    cfg_file = Path("configs/connections.yaml")
    with open(cfg_file, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    conns: dict[str, ConnectionInfo] = {}
    for alias, cfg in (raw.get("connections") or {}).items():
        conns[alias] = ConnectionInfo(alias=alias, url=cfg["url"], schema=cfg.get("schema"))
    return Settings(default_conn_alias=default_conn_alias, data_dir=data_dir, connections=conns)
