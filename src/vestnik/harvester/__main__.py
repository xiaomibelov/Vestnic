import asyncio
import binascii
import logging
import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import Integer, cast, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from vestnik.db import session_scope
from vestnik.schema import ensure_schema
from vestnik.models import Channel, PostCache
from vestnik.settings import (
    HARVESTER_ENABLED,
    HARVEST_INTERVAL_SEC,
    HARVEST_LIMIT_PER_CHANNEL,
    POST_CACHE_TTL_HOURS,
    TG_API_HASH,
    TG_API_ID,
    TG_SESSION,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vestnik.harvester")

_POSTCACHE_COLS: set[str] | None = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _postcache_cols() -> set[str]:
    global _POSTCACHE_COLS
    if _POSTCACHE_COLS is None:
        _POSTCACHE_COLS = set(PostCache.__table__.columns.keys())
        logger.info("posts_cache columns=%s", ",".join(sorted(_POSTCACHE_COLS)))
    return _POSTCACHE_COLS


def _ttl_expires_at(now: datetime) -> datetime:
    ttl = POST_CACHE_TTL_HOURS
    if not isinstance(ttl, int) or ttl <= 0:
        ttl = 48
    return now + timedelta(hours=ttl)


def _sanitize_tg_session(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return s

    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        s = s[1:-1].strip()

    s = "".join(s.split())

    if "/" in s or s.endswith(".session"):
        return s

    if len(s) >= 2 and s[0].isdigit():
        ver = s[0]
        payload = s[1:]
        pad = (-len(payload)) % 4
        if pad:
            payload = payload + ("=" * pad)
        return ver + payload

    pad = (-len(s)) % 4
    if pad:
        s = s + ("=" * pad)
    return s


def _make_client() -> TelegramClient | None:
    if not TG_API_ID or not TG_API_HASH:
        logger.warning("missing TG_API_ID/TG_API_HASH")
        return None
    if not TG_SESSION:
        logger.warning("missing TG_SESSION")
        return None

    sess = _sanitize_tg_session(TG_SESSION)
    if not sess:
        logger.warning("empty TG_SESSION after sanitize")
        return None

    if "/" in sess or sess.endswith(".session"):
        return TelegramClient(sess, TG_API_ID, TG_API_HASH)

    try:
        return TelegramClient(StringSession(sess), TG_API_ID, TG_API_HASH)
    except (binascii.Error, ValueError) as e:
        logger.error("invalid TG_SESSION: %s", e)
        return None


async def _fetch_active_channels() -> list[Channel]:
    async with session_scope() as session:
        q = select(Channel).where(Channel.is_active == True).order_by(Channel.id)
        return (await session.execute(q)).scalars().all()


async def _last_message_id(channel_ref: str) -> int:
    async with session_scope() as session:
        q = select(func.max(cast(PostCache.message_id, Integer))).where(
            PostCache.channel_ref == channel_ref, PostCache.is_deleted == False
        )
        val = (await session.execute(q)).scalar()
        try:
            return int(val or 0)
        except Exception:
            return 0


async def _cleanup_expired() -> int:
    now = _now_utc()
    async with session_scope() as session:
        res = await session.execute(delete(PostCache).where(PostCache.expires_at < now))
        await session.commit()
        try:
            return int(res.rowcount or 0)
        except Exception:
            return 0


def _project_postcache_row(raw: dict) -> dict:
    cols = _postcache_cols()
    out: dict = {}

    for k in ("channel_ref", "message_id", "text", "is_deleted"):
        if k in cols and k in raw:
            out[k] = raw[k]

    pub = raw.get("published_at")
    if pub is not None:
        for k in ("published_at", "posted_at", "message_date", "date"):
            if k in cols and k not in out:
                out[k] = pub
                break

    fetched = raw.get("fetched_at") or _now_utc()
    for k in ("fetched_at", "created_at", "ingested_at", "inserted_at"):
        if k in cols and k not in out:
            out[k] = fetched
            break

    exp = raw.get("expires_at")
    if exp is not None:
        for k in ("expires_at", "expire_at", "expires"):
            if k in cols and k not in out:
                out[k] = exp
                break

    return out


async def _upsert_posts(channel_ref: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    projected = [_project_postcache_row(r) for r in rows]
    projected = [r for r in projected if r]

    if not projected:
        logger.warning("no insertable columns for posts_cache; rows dropped")
        return 0

    cols = _postcache_cols()
    conflict_cols = [c for c in ("channel_ref", "message_id") if c in cols]

    async with session_scope() as session:
        stmt = insert(PostCache).values(projected)
        if len(conflict_cols) == 2:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
        res = await session.execute(stmt)
        await session.commit()
        try:
            return int(res.rowcount or 0)
        except Exception:
            return 0


async def _maybe_update_channel_meta(channel_id: int, tg_channel_id: int | None, title: str | None) -> None:
    async with session_scope() as session:
        ch = (await session.execute(select(Channel).where(Channel.id == channel_id))).scalars().first()
        if not ch:
            return

        changed = False

        if tg_channel_id is not None and hasattr(ch, "tg_channel_id"):
            cur = getattr(ch, "tg_channel_id", None)
            if not cur:
                setattr(ch, "tg_channel_id", tg_channel_id)
                changed = True

        if title and hasattr(ch, "title"):
            cur_title = (getattr(ch, "title", None) or "").strip()
            if not cur_title or cur_title == getattr(ch, "username", ""):
                setattr(ch, "title", title)
                changed = True

        if changed:
            await session.commit()


async def _harvest_one_channel(client: TelegramClient, ch: Channel) -> tuple[int, int]:
    channel_ref = ch.username.lstrip("@")
    last_id = await _last_message_id(channel_ref)

    logger.info("harvest: start channel=%s last_id=%s", channel_ref, last_id)

    entity = await client.get_entity(channel_ref)
    await _maybe_update_channel_meta(ch.id, getattr(entity, "id", None), getattr(entity, "title", None))

    now = _now_utc()
    expires_at = _ttl_expires_at(now)

    limit = HARVEST_LIMIT_PER_CHANNEL
    if not isinstance(limit, int) or limit <= 0:
        limit = 50

    scanned = 0
    collected: list[dict] = []

    kwargs = {"limit": limit}
    if last_id > 0:
        kwargs["min_id"] = last_id

    async for msg in client.iter_messages(entity, **kwargs):
        scanned += 1
        if not msg or not getattr(msg, "id", None):
            continue

        text = (getattr(msg, "raw_text", None) or getattr(msg, "message", None) or "").strip()
        if not text:
            continue

        collected.append(
            {
                "channel_ref": channel_ref,
                "message_id": str(msg.id),
                "url": f"https://t.me/{channel_ref}/{msg.id}",
                "text": text,
                "published_at": getattr(msg, "date", None) or now,
                "fetched_at": now,
                "expires_at": expires_at,
                "is_deleted": False,
            }
        )

    inserted = await _upsert_posts(channel_ref, collected)
    logger.info("harvest: done channel=%s scanned=%s inserted=%s", channel_ref, scanned, inserted)
    return inserted, scanned


async def _harvest_cycle(client: TelegramClient) -> int:
    async with session_scope() as session:
        await ensure_schema(session)
    channels = await _fetch_active_channels()
    logger.info("cycle: channels=%s", len(channels))

    cleaned = await _cleanup_expired()
    if cleaned:
        logger.info("cleaned expired posts_cache rows=%s", cleaned)

    total_inserted = 0
    for ch in channels:
        try:
            inserted, _scanned = await _harvest_one_channel(client, ch)
            total_inserted += inserted
            await asyncio.sleep(0.2)
        except FloodWaitError as e:
            wait_s = int(getattr(e, "seconds", 0) or 0)
            wait_s = max(wait_s, 1)
            logger.warning("flood wait seconds=%s channel=%s", wait_s, ch.username)
            await asyncio.sleep(wait_s + random.random())
        except Exception as e:
            logger.exception("harvest error channel=%s err=%s", ch.username, e)
            await asyncio.sleep(1)

    logger.info("cycle: inserted_total=%s", total_inserted)
    return total_inserted


async def _run_loop() -> None:
    logger.info("harvester: start enabled=%s", bool(HARVESTER_ENABLED))

    if not HARVESTER_ENABLED:
        logger.warning("HARVESTER_ENABLED=0; harvester is idle")

    while True:
        if not HARVESTER_ENABLED:
            await asyncio.sleep(5)
            continue

        client = _make_client()
        if client is None:
            logger.warning("harvester disabled by config (TG_* missing/invalid)")
            await asyncio.sleep(10)
            continue

        try:
            async with client:
                await _harvest_cycle(client)
        except Exception as e:
            logger.exception("harvester cycle error err=%s", e)

        sleep_s = HARVEST_INTERVAL_SEC if isinstance(HARVEST_INTERVAL_SEC, int) else 60
        if sleep_s <= 0:
            sleep_s = 60
        await asyncio.sleep(sleep_s)


async def _cmd_login() -> None:
    if not TG_API_ID or not TG_API_HASH:
        raise SystemExit("TG_API_ID/TG_API_HASH are required for login. Put them into .env")
    logger.info("starting telegram login (interactive)")
    client = TelegramClient(StringSession(), TG_API_ID, TG_API_HASH)
    await client.start()
    session_str = client.session.save()
    await client.disconnect()
    print("")
    print("Put this into .env")
    print(f"TG_SESSION={session_str}")
    print("HARVESTER_ENABLED=1")
    print("")


async def _cmd_oneshot() -> None:
    client = _make_client()
    if client is None:
        raise SystemExit("TG_* config missing/invalid; cannot run oneshot")
    async with client:
        inserted = await _harvest_cycle(client)
    print(f"ONESHOOT: inserted_total={inserted}")


def main() -> None:
    import sys

    cmd = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if cmd in {"login", "auth"}:
        asyncio.run(_cmd_login())
        return
    if cmd in {"oneshot", "seed"}:
        asyncio.run(_cmd_oneshot())
        return
    asyncio.run(_run_loop())


if __name__ == "__main__":
    main()
