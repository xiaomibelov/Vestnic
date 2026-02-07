import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import Integer, cast, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from vestnik.db import session_scope
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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ttl_expires_at(now: datetime) -> datetime:
    ttl = POST_CACHE_TTL_HOURS
    if ttl <= 0:
        ttl = 48
    return now + timedelta(hours=ttl)


def _make_client() -> TelegramClient | None:
    if not TG_API_ID or not TG_API_HASH:
        return None
    if TG_SESSION:
        if "/" in TG_SESSION or TG_SESSION.endswith(".session"):
            return TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
        return TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)
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


async def _upsert_posts(channel_ref: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    async with session_scope() as session:
        stmt = insert(PostCache).values(rows)
        stmt = stmt.on_conflict_do_nothing(index_elements=["channel_ref", "message_id"])
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
        if tg_channel_id and not ch.tg_channel_id:
            ch.tg_channel_id = tg_channel_id
            changed = True
        if title and (not ch.title or ch.title.strip() == "" or ch.title == ch.username):
            ch.title = title
            changed = True
        if changed:
            await session.commit()


async def _harvest_one_channel(client: TelegramClient, ch: Channel) -> int:
    channel_ref = ch.username.lstrip("@")
    last_id = await _last_message_id(channel_ref)
    entity = await client.get_entity(channel_ref)
    await _maybe_update_channel_meta(ch.id, getattr(entity, "id", None), getattr(entity, "title", None))

    now = _now_utc()
    expires_at = _ttl_expires_at(now)

    collected: list[dict] = []
    async for msg in client.iter_messages(entity, min_id=last_id, limit=HARVEST_LIMIT_PER_CHANNEL, reverse=True):
        if not msg:
            continue
        text = (msg.message or "").strip()
        if not text:
            continue
        url = f"https://t.me/{channel_ref}/{msg.id}" if channel_ref else ""
        collected.append(
            {
                "channel_ref": channel_ref,
                "message_id": str(msg.id),
                "url": url,
                "text": text,
                "expires_at": expires_at,
            }
        )

    inserted = await _upsert_posts(channel_ref, collected)
    return inserted


async def _run_loop() -> None:
    logger.info(
        "harvester loop starting enabled=%s interval=%s limit=%s ttl_h=%s",
        HARVESTER_ENABLED,
        HARVEST_INTERVAL_SEC,
        HARVEST_LIMIT_PER_CHANNEL,
        POST_CACHE_TTL_HOURS,
    )

    while True:
        if not HARVESTER_ENABLED:
            await asyncio.sleep(5)
            continue

        client = _make_client()
        if client is None:
            logger.warning("harvester disabled by config: TG_API_ID/TG_API_HASH/TG_SESSION not set")
            await asyncio.sleep(10)
            continue

        try:
            async with client:
                channels = await _fetch_active_channels()
                if not channels:
                    await asyncio.sleep(max(5, HARVEST_INTERVAL_SEC))
                    continue

                cleaned = await _cleanup_expired()
                if cleaned:
                    logger.info("cleaned expired posts_cache rows=%s", cleaned)

                total_inserted = 0
                for ch in channels:
                    try:
                        inserted = await _harvest_one_channel(client, ch)
                        total_inserted += inserted
                        if inserted:
                            logger.info("harvested channel=%s inserted=%s", ch.username, inserted)
                        await asyncio.sleep(0.2)
                    except FloodWaitError as e:
                        wait_s = int(getattr(e, "seconds", 0) or 0)
                        wait_s = max(wait_s, 1)
                        logger.warning("flood wait seconds=%s channel=%s", wait_s, ch.username)
                        await asyncio.sleep(wait_s + random.random())
                    except Exception as e:
                        logger.exception("harvest error channel=%s err=%s", ch.username, e)
                        await asyncio.sleep(1)

                if total_inserted:
                    logger.info("harvest cycle done inserted_total=%s", total_inserted)

        except Exception as e:
            logger.exception("harvester cycle error err=%s", e)

        sleep_s = HARVEST_INTERVAL_SEC
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


async def _cmd_login_qr() -> None:
    if not TG_API_ID or not TG_API_HASH:
        raise SystemExit("TG_API_ID/TG_API_HASH are required for login. Put them into .env")
    logger.info("starting telegram login (qr)")
    client = TelegramClient(StringSession(), TG_API_ID, TG_API_HASH)
    await client.connect()
    try:
        qr = await client.qr_login()
        url = getattr(qr, "url", "") or ""
        if not url:
            raise RuntimeError("qr login did not return a login url")

        https_url = url
        if url.startswith("tg://login?token="):
            https_url = "https://t.me/login?token=" + url.split("token=", 1)[1]

        print("")
        print("Approve login on your phone (same Telegram account).")
        print("Option A: open this link on the phone (it will open Telegram):")
        print(url)
        if https_url != url:
            print("")
            print("Option B (if tg:// link is not clickable):")
            print(https_url)
        print("")
        print("Waiting for approval... (60s)")
        try:
            await qr.wait(timeout=60)
        except asyncio.TimeoutError:
            print("")
            print("Timed out waiting for approval.")
            print("Run the command again to get a fresh link.")
            raise SystemExit(2)

        session_str = client.session.save()
    finally:
        await client.disconnect()

    print("")
    print("Put this into .env")
    print(f"TG_SESSION={session_str}")
    print("HARVESTER_ENABLED=1")
    print("")


def main() -> None:
    import sys
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if cmd in {"login", "auth"}:
        asyncio.run(_cmd_login())
        return
    if cmd in {"login-qr", "auth-qr", "qr"}:
        asyncio.run(_cmd_login_qr())
        return
    asyncio.run(_run_loop())


if __name__ == "__main__":
    main()
