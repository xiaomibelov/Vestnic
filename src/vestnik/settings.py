import os

def env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val

def env_int(name: str, default: int = 0) -> int:
    raw = env(name, None)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default

def env_bool(name: str, default: bool = False) -> bool:
    raw = env(name, None)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}

BOT_TOKEN = env("BOT_TOKEN", "")
DATABASE_URL = env("DATABASE_URL", "postgresql+asyncpg://vestnik:vestnik@db:5432/vestnik")
REDIS_URL = env("REDIS_URL", "redis://redis:6379/0")
ADMIN_TG_CHAT_ID = env("ADMIN_TG_CHAT_ID", "")

TG_API_ID = env_int("TG_API_ID", 0)
TG_API_HASH = env("TG_API_HASH", "")
TG_SESSION = env("TG_SESSION", "")

HARVESTER_ENABLED = env_bool("HARVESTER_ENABLED", False)
HARVEST_INTERVAL_SEC = env_int("HARVEST_INTERVAL_SEC", 60)
HARVEST_LIMIT_PER_CHANNEL = env_int("HARVEST_LIMIT_PER_CHANNEL", 50)
POST_CACHE_TTL_HOURS = env_int("POST_CACHE_TTL_HOURS", 48)
