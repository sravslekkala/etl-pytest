
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_engine_cache: dict[str, Engine] = {}

def get_engine(alias: str, url: str) -> Engine:
    key = f"{alias}|{url}"
    if key not in _engine_cache:
        _engine_cache[key] = create_engine(url, future=True)
    return _engine_cache[key]
