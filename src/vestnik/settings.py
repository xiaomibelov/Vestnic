import os

def env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val

BOT_TOKEN = env("BOT_TOKEN", "")
DATABASE_URL = env("DATABASE_URL", "postgresql+asyncpg://vestnik:vestnik@db:5432/vestnik")
REDIS_URL = env("REDIS_URL", "redis://redis:6379/0")
ADMIN_TG_CHAT_ID = env("ADMIN_TG_CHAT_ID", "")
