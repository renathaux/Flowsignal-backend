from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATABASE_DIR = BASE_DIR / "database"
CACHE_DIR = BASE_DIR / "cache"
CANDLE_CACHE_DIR = CACHE_DIR / "candle_cache"


def ensure_runtime_dirs():
    for path in [DATA_DIR, DATABASE_DIR, CACHE_DIR, CANDLE_CACHE_DIR]:
        path.mkdir(parents=True, exist_ok=True)


ensure_runtime_dirs()
