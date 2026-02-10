import asyncio
import inspect
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from aiogram import Bot
from sqlalchemy import text

from vestnik.db import session_scope
from vestnik.schema import ensure_schema
from vestnik.settings import BOT_TOKEN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vestnik.worker")


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


def _safe_ident(name: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9_]+", name or ""):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


async def _list_tables(session) -> set[str]:
    q = text(
        "select table_name from information_schema.tables "
        "where table_schema='public' and table_type='BASE TABLE'"
    )
    res = await session.execute(q)
    return {r[0] for r in res.all()}


async def _table_cols(session, table: str) -> set[str]:
    q = text(
        "select column_name from information_schema.columns "
        "where table_schema='public' and table_name=:t"
    )
    res = await session.execute(q, {"t": table})
    return {r[0] for r in res.all()}


def _pick_table(tables: set[str], candidates: list[str]) -> str | None:
    for t in candidates:
        if t in tables:
            return t
    return None


async def _resolve_pack_tables(session) -> tuple[str, str]:
    tables = await _list_tables(session)

    user_packs_t = _pick_table(tables, ["user_packs", "user_pack", "users_packs"])
    pack_channels_t = _pick_table(tables, ["pack_channels", "pack_channel", "packs_channels"])

    missing = []
    if user_packs_t is None:
        missing.append("user_packs")
    if pack_channels_t is None:
        missing.append("pack_channels")

    if missing:
        raise RuntimeError(f"missing tables: {missing}. existing={sorted(tables)}")

    return user_packs_t, pack_channels_t


async def _ensure_deliveries_table(session) -> None:
    await session.execute(
        text(
            """
            create table if not exists deliveries (
              id serial primary key,
              user_id integer not null,
              channel_ref varchar(255) not null,
              message_id varchar(64) not null,
              sent_at timestamptz not null default now(),
              unique (user_id, channel_ref, message_id)
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_deliveries_user_id on deliveries(user_id);"))
    await session.execute(text("create index if not exists ix_deliveries_sent_at on deliveries(sent_at);"))
    await session.execute(text("create unique index if not exists ux_deliveries_pair on deliveries(user_id, channel_ref, message_id);"))
    await session.commit()


async def _ensure_report_deliveries_table(session) -> None:
    await session.execute(
        text(
            """
            create table if not exists report_deliveries (
              id serial primary key,
              user_id integer not null,
              report_id integer not null,
              sent_at timestamptz not null default now(),
              unique (user_id, report_id)
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_report_deliveries_user_id on report_deliveries(user_id);"))
    await session.execute(text("create index if not exists ix_report_deliveries_sent_at on report_deliveries(sent_at);"))
    await session.execute(text("create unique index if not exists ux_report_deliveries_pair on report_deliveries(user_id, report_id);"))
    await session.commit()


async def _ensure_user_settings(session) -> None:
    await session.execute(
        text(
            """
            create table if not exists user_settings (
              user_id integer primary key,
              delivery_enabled boolean not null default true,
              digest_interval_sec integer null,
              last_sent_at timestamptz null,
              pause_until timestamptz null,
              format_mode varchar(16) not null default 'digest'
            );
            """
        )
    )
    await session.execute(text("alter table user_settings add column if not exists pause_until timestamptz;"))
    await session.execute(text("alter table user_settings add column if not exists format_mode varchar(16);"))
    await session.execute(text("update user_settings set format_mode='digest' where format_mode is null;"))
    await session.execute(text("create index if not exists ix_user_settings_delivery_enabled on user_settings(delivery_enabled);"))
    await session.execute(text("create index if not exists ix_user_settings_pause_until on user_settings(pause_until);"))
    await session.commit()


async def _ensure_user_settings_row(session, user_id: int) -> None:
    await session.execute(
        text("insert into user_settings (user_id) values (:uid) on conflict do nothing"),
        {"uid": user_id},
    )
    await session.commit()


async def _get_user_settings(session, user_id: int) -> tuple[bool, int | None, datetime | None, datetime | None, str]:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    row = (
        await session.execute(
            text("select delivery_enabled, digest_interval_sec, last_sent_at, pause_until, format_mode from user_settings where user_id=:uid"),
            {"uid": user_id},
        )
    ).first()
    if not row:
        return True, None, None, None, "digest"
    return bool(row[0]), (int(row[1]) if row[1] is not None else None), row[2], row[3], (str(row[4]) if row[4] else "digest")


@dataclass(frozen=True)
class UserRow:
    id: int
    tg_id: int


@dataclass(frozen=True)
class PostRow:
    channel_ref: str
    message_id: str
    text: str
    url: str


async def _fetch_users(session) -> list[UserRow]:
    await _ensure_user_settings(session)
    now = datetime.now(timezone.utc)
    res = await session.execute(
        text(
            """
            select u.id, u.tg_id
            from users u
            left join user_settings s on s.user_id = u.id
            where u.tg_id is not null
              and coalesce(s.delivery_enabled, true) = true
              and (s.pause_until is null or s.pause_until <= :now)
            order by u.id
            """
        ),
        {"now": now},
    )
    out: list[UserRow] = []
    for r in res.all():
        out.append(UserRow(id=int(r[0]), tg_id=int(r[1])))
    return out


async def _selected_pack_ids(session, user_id: int, user_packs_t: str) -> list[int]:
    cols = await _table_cols(session, user_packs_t)

    user_id_col = "user_id" if "user_id" in cols else None
    pack_id_col = "pack_id" if "pack_id" in cols else None
    enabled_col = "is_enabled" if "is_enabled" in cols else ("enabled" if "enabled" in cols else None)

    if not user_id_col or not pack_id_col:
        raise RuntimeError(f"user_packs table {user_packs_t!r} missing user_id/pack_id; cols={sorted(cols)}")

    where = f"where {_safe_ident(user_id_col)} = :uid"
    if enabled_col:
        where += f" and {_safe_ident(enabled_col)} = true"

    sql = f"select {_safe_ident(pack_id_col)} from {_safe_ident(user_packs_t)} {where}"
    res = await session.execute(text(sql), {"uid": user_id})
    return [int(r[0]) for r in res.all()]


async def _packs_for_ids(session, pack_ids: list[int]) -> list[dict[str, Any]]:
    if not pack_ids:
        return []
    cols = await _table_cols(session, "packs")

    id_col = "id" if "id" in cols else None
    key_col = "key" if "key" in cols else ("pack_key" if "pack_key" in cols else ("slug" if "slug" in cols else None))
    title_col = "title" if "title" in cols else ("name" if "name" in cols else ("pack_name" if "pack_name" in cols else None))

    if not id_col:
        raise RuntimeError(f"packs table missing id; cols={sorted(cols)}")
    if not key_col:
        raise RuntimeError(f"packs table missing key/pack_key/slug; cols={sorted(cols)}")

    sel = f"select {id_col}, {key_col}"
    if title_col:
        sel += f", {title_col}"
    sel += " from packs where id = any(:pids) order by id"

    res = await session.execute(text(sel), {"pids": pack_ids})
    out: list[dict[str, Any]] = []
    for r in res.all():
        out.append(
            {
                "id": int(r[0]),
                "pack_key": str(r[1]),
                "pack_title": (str(r[2]) if title_col and r[2] is not None else str(r[1])),
            }
        )
    return out


async def _channels_for_pack_ids(session, pack_ids: list[int], pack_channels_t: str) -> list[str]:
    if not pack_ids:
        return []
    cols = await _table_cols(session, pack_channels_t)

    pack_id_col = "pack_id" if "pack_id" in cols else None
    channel_id_col = "channel_id" if "channel_id" in cols else None
    if not pack_id_col or not channel_id_col:
        raise RuntimeError(f"pack_channels table {pack_channels_t!r} missing pack_id/channel_id; cols={sorted(cols)}")

    sql = (
        f"select distinct c.username "
        f"from {_safe_ident(pack_channels_t)} pc "
        f"join channels c on c.id = pc.{_safe_ident(channel_id_col)} "
        f"where pc.{_safe_ident(pack_id_col)} = any(:pids) "
        f"and c.is_active = true"
    )
    res = await session.execute(text(sql), {"pids": pack_ids})
    usernames = [str(r[0]) for r in res.all()]
    return [u.lstrip("@") for u in usernames]


async def _fetch_unsent_posts(session, user_id: int, channel_refs: list[str], limit: int) -> list[PostRow]:
    if not channel_refs:
        return []
    now = datetime.now(timezone.utc)

    sql = text(
        """
        select p.channel_ref, p.message_id, p.text, p.url
        from posts_cache p
        left join deliveries d
          on d.user_id = :uid
         and d.channel_ref = p.channel_ref
         and d.message_id = p.message_id
        where d.id is null
          and p.is_deleted = false
          and p.expires_at > :now
          and p.channel_ref = any(:refs)
        order by p.parsed_at desc
        limit :lim
        """
    )
    res = await session.execute(sql, {"uid": user_id, "now": now, "refs": channel_refs, "lim": limit})
    out: list[PostRow] = []
    for r in res.all():
        out.append(PostRow(channel_ref=str(r[0]), message_id=str(r[1]), text=str(r[2] or ""), url=str(r[3] or "")))
    return out


async def _mark_delivered_posts(session, user_id: int, posts: list[PostRow]) -> None:
    if not posts:
        return
    rows = [{"uid": user_id, "cr": p.channel_ref, "mid": p.message_id} for p in posts]
    await session.execute(
        text(
            """
            insert into deliveries (user_id, channel_ref, message_id)
            values (:uid, :cr, :mid)
            on conflict do nothing
            """
        ),
        rows,
    )
    await session.commit()


async def _touch_last_sent(session, user_id: int) -> None:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    await session.execute(
        text("update user_settings set last_sent_at=now() where user_id=:uid"),
        {"uid": user_id},
    )
    await session.commit()


def _build_message_posts(posts: list[PostRow], mode: str, max_chars: int = 3800) -> str:
    mode = (mode or "digest").strip().lower()
    if mode == "compact":
        out = "Лента (компакт):\n\n"
        for p in posts:
            t = (p.text or "").strip().replace("\n", " ")
            if len(t) > 120:
                t = t[:120] + "…"
            url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
            chunk = f"• @{p.channel_ref}: {t}\n{url}\n\n"
            if len(out) + len(chunk) > max_chars:
                break
            out += chunk
        return out.strip()

    out = "Авто-дайджест:\n\n"
    for p in posts:
        t = (p.text or "").strip().replace("\n", " ")
        if len(t) > 180:
            t = t[:180] + "…"
        url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
        chunk = f"@{p.channel_ref}: {t}\n{url}\n\n"
        if len(out) + len(chunk) > max_chars:
            break
        out += chunk
    return out.strip()


def _dry_preview_lines_posts(posts: list[PostRow], n: int) -> str:
    if n <= 0:
        return ""
    lines: list[str] = []
    for p in posts[:n]:
        url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
        lines.append(f"@{p.channel_ref}/{p.message_id} {url}")
    return " | ".join(lines)


def _coerce_dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v
    return None


async def _brain_generate_report_compat(
    session,
    *,
    pack_key: str,
    hours: int,
    limit: int,
    period_end: datetime,
    snap: str,
    user_tg_id: int,
) -> Any:
    # Import lazily to avoid tight coupling.
    from vestnik.brain import pipeline as bp  # type: ignore

    fn = getattr(bp, "generate_report", None)
    if fn is None:
        raise RuntimeError("vestnik.brain.pipeline.generate_report not found")

    sig = inspect.signature(fn)
    kwargs: dict[str, Any] = {}

    def _has(name: str) -> bool:
        return name in sig.parameters

    if _has("pack_key"):
        kwargs["pack_key"] = pack_key
    elif _has("pack"):
        kwargs["pack"] = pack_key
    elif _has("pack_ref"):
        kwargs["pack_ref"] = pack_key
    else:
        kwargs["pack_key"] = pack_key

    if _has("hours"):
        kwargs["hours"] = int(hours)
    if _has("limit"):
        kwargs["limit"] = int(limit)

    if _has("period_end"):
        kwargs["period_end"] = period_end
    elif _has("period_end_iso"):
        kwargs["period_end_iso"] = period_end.isoformat()
    elif _has("end"):
        kwargs["end"] = period_end

    if _has("snap"):
        kwargs["snap"] = snap

    if _has("user_tg_id"):
        kwargs["user_tg_id"] = int(user_tg_id)
    elif _has("tg_id"):
        kwargs["tg_id"] = int(user_tg_id)
    elif _has("user_tg"):
        kwargs["user_tg"] = int(user_tg_id)

    if _has("save"):
        kwargs["save"] = True

    if _has("session"):
        return await fn(session=session, **kwargs)

    sig = None
    try:
        import inspect
        sig = inspect.signature(fn)
    except Exception:
        sig = None

    if sig is not None:
        import inspect
        params = list(sig.parameters.values())
        accepts_positional = any(
            p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
            for p in params
        )
        if accepts_positional:
            return await fn(session, **kwargs)

    return await fn(**kwargs)


async def _find_report_id(
    session,
    *,
    user_id: int,
    pack_key: str,
    period_start: datetime | None,
    period_end: datetime | None,
    input_hash: str | None,
) -> int | None:
    where = "where user_id=:uid and pack_key=:pk"
    params: dict[str, Any] = {"uid": user_id, "pk": pack_key}

    if period_start and period_end:
        where += " and period_start=:ps and period_end=:pe"
        params["ps"] = period_start
        params["pe"] = period_end

    if input_hash:
        where += " and input_hash=:h"
        params["h"] = input_hash

    q = text(
        f"""
        select id
        from reports
        {where}
        order by id desc
        limit 1
        """
    )
    row = (await session.execute(q, params)).first()
    if row:
        return int(row[0])

    # Fallback: latest for user+pack
    q2 = text(
        """
        select id
        from reports
        where user_id=:uid and pack_key=:pk
        order by id desc
        limit 1
        """
    )
    row2 = (await session.execute(q2, {"uid": user_id, "pk": pack_key})).first()
    return int(row2[0]) if row2 else None


async def _reserve_report_delivery(session, *, user_id: int, report_id: int) -> bool:
    # True => reserved (not sent before). False => already sent.
    row = (
        await session.execute(
            text(
                """
                insert into report_deliveries (user_id, report_id)
                values (:uid, :rid)
                on conflict do nothing
                returning id
                """
            ),
            {"uid": user_id, "rid": report_id},
        )
    ).first()
    await session.commit()
    return bool(row)


def _clip_telegram(text_s: str, max_chars: int = 4096) -> str:
    s = (text_s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars]


def _first_line(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return s.splitlines()[0][:200]


async def _oneshot() -> None:
    enabled = _env_bool("WORKER_ENABLED", True)
    dry = _env_bool("WORKER_DRY_RUN", False)
    max_posts = _env_int("WORKER_MAX_POSTS_PER_USER", 10)
    preview_n = _env_int("WORKER_DRY_RUN_PREVIEW_N", 0)

    mode = _env_str("WORKER_MODE", "posts").strip().lower()
    default_interval_sec = _env_int("WORKER_DEFAULT_INTERVAL_SEC", 86400)

    brain_hours = _env_int("WORKER_BRAIN_HOURS", 24)
    brain_limit = _env_int("WORKER_BRAIN_LIMIT", max_posts)
    brain_snap = _env_str("WORKER_BRAIN_SNAP", "minute")
    brain_period_end = os.environ.get("WORKER_BRAIN_PERIOD_END", "").strip()

    async with session_scope() as session:
        await ensure_schema(session)

    target_tg = os.environ.get("WORKER_TARGET_TG_ID", "").strip()

    if not enabled:
        logger.warning("WORKER_ENABLED=0; idle")
        return

    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required")

    bot = Bot(token=BOT_TOKEN)

    async with session_scope() as session:
        await _ensure_user_settings(session)

        user_packs_t, pack_channels_t = await _resolve_pack_tables(session)
        users = await _fetch_users(session)

        if target_tg:
            try:
                tg = int(target_tg)
                users = [u for u in users if u.tg_id == tg]
            except Exception:
                logger.warning("WORKER_TARGET_TG_ID invalid: %s", target_tg)

        logger.info(
            "oneshot: users=%s dry_run=%s mode=%s max_posts=%s preview_n=%s",
            len(users),
            dry,
            mode,
            max_posts,
            preview_n,
        )

        sent_users = 0
        for u in users:
            try:
                delivery_enabled, interval_sec, last_sent_at, pause_until, format_mode = await _get_user_settings(session, u.id)
                if not delivery_enabled:
                    continue

                if pause_until:
                    try:
                        pu = pause_until
                        if pu.tzinfo is None:
                            pu = pu.replace(tzinfo=timezone.utc)
                        if pu > datetime.now(timezone.utc):
                            continue
                    except Exception:
                        pass

                # Default interval guard (if DB null) to avoid spamming.
                if interval_sec is None:
                    interval_sec = int(default_interval_sec)

                if interval_sec and last_sent_at:
                    delta = (datetime.now(timezone.utc) - last_sent_at).total_seconds()
                    if delta < float(interval_sec):
                        continue

                pack_ids = await _selected_pack_ids(session, u.id, user_packs_t)
                if not pack_ids:
                    continue

                if mode in {"brain", "report", "digest"}:
                    await _ensure_report_deliveries_table(session)

                    packs = await _packs_for_ids(session, pack_ids)
                    if not packs:
                        continue

                    # Determine period_end
                    pe = datetime.now(timezone.utc)
                    if brain_period_end:
                        try:
                            # Let brain accept ISO; we also keep datetime for our local logs.
                            pe = datetime.fromisoformat(brain_period_end.replace("Z", "+00:00"))
                            if pe.tzinfo is None:
                                pe = pe.replace(tzinfo=timezone.utc)
                        except Exception:
                            pe = datetime.now(timezone.utc)

                    any_sent = False
                    for p in packs:
                        pack_key = str(p["pack_key"])
                        pack_title = str(p.get("pack_title") or pack_key)

                        res = await _brain_generate_report_compat(
                            session,
                            pack_key=pack_key,
                            hours=int(brain_hours),
                            limit=int(brain_limit),
                            period_end=pe,
                            snap=str(brain_snap),
                            user_tg_id=int(u.tg_id),
                        )

                        report_text = ""
                        input_hash = None
                        ps = None
                        pe2 = None

                        if isinstance(res, tuple) and res:
                            # not expected, but keep safe
                            report_text = str(res[0] or "")
                        else:
                            report_text = str(getattr(res, "report_text", "") or getattr(res, "text", "") or "")
                            input_hash = getattr(res, "input_hash", None) or getattr(res, "prehash", None)
                            ps = _coerce_dt(getattr(res, "period_start", None) or getattr(res, "start", None))
                            pe2 = _coerce_dt(getattr(res, "period_end", None) or getattr(res, "end", None))

                        report_text = _clip_telegram(report_text, 4096)
                        if not report_text:
                            continue

                        report_id = await _find_report_id(
                            session,
                            user_id=u.id,
                            pack_key=pack_key,
                            period_start=ps,
                            period_end=pe2,
                            input_hash=(str(input_hash) if input_hash else None),
                        )
                        if report_id is None:
                            logger.warning("brain report: cannot resolve report_id user_id=%s pack=%s", u.id, pack_key)
                            continue

                        if dry:
                            logger.info(
                                "DRY brain report user_tg=%s pack=%s report_id=%s first_line=%s",
                                u.tg_id,
                                pack_key,
                                report_id,
                                _first_line(report_text),
                            )
                            any_sent = True
                            continue

                        ok = await _reserve_report_delivery(session, user_id=u.id, report_id=int(report_id))
                        if not ok:
                            logger.info("skip already-sent report user_tg=%s report_id=%s pack=%s", u.tg_id, report_id, pack_key)
                            continue

                        await bot.send_message(u.tg_id, report_text)
                        any_sent = True
                        await asyncio.sleep(0.4)

                    if any_sent:
                        await _touch_last_sent(session, u.id)
                        sent_users += 1
                    continue

                # Default: posts mode (existing behaviour)
                await _ensure_deliveries_table(session)
                channel_refs = await _channels_for_pack_ids(session, pack_ids, pack_channels_t)
                if not channel_refs:
                    continue

                posts = await _fetch_unsent_posts(session, u.id, channel_refs, max_posts)
                if not posts:
                    continue

                if dry:
                    preview = _dry_preview_lines_posts(posts, preview_n)
                    if preview:
                        logger.info("DRY preview user_tg=%s sample=%s", u.tg_id, preview)
                    logger.info("DRY (no side effects) user_tg=%s would_send=%s", u.tg_id, len(posts))
                    continue

                msg = _build_message_posts(posts, format_mode)
                await bot.send_message(u.tg_id, msg)
                await _mark_delivered_posts(session, u.id, posts)
                await _touch_last_sent(session, u.id)
                sent_users += 1

                await asyncio.sleep(0.4)

            except Exception as e:
                logger.exception("user send error tg_id=%s err=%s", u.tg_id, e)
                await asyncio.sleep(0.2)

    await bot.session.close()
    logger.info("oneshot done: users_sent=%s", sent_users)


async def _loop() -> None:
    interval = _env_int("WORKER_INTERVAL_SEC", 300)
    if interval <= 0:
        interval = 300
    logger.info("worker loop started interval=%ss", interval)
    while True:
        await _oneshot()
        await asyncio.sleep(interval)


def main() -> None:
    import sys

    cmd = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if cmd in {"-h", "--help", "help"}:
        print(
            "Usage:\n"
            "  python -m vestnik.worker oneshot\n"
            "  python -m vestnik.worker\n\n"
            "Env:\n"
            "  WORKER_MODE=posts|brain\n"
            "  WORKER_DRY_RUN=1\n"
            "  WORKER_TARGET_TG_ID=...\n"
            "  WORKER_DEFAULT_INTERVAL_SEC=86400\n"
            "  WORKER_BRAIN_HOURS=24\n"
            "  WORKER_BRAIN_LIMIT=10\n"
            "  WORKER_BRAIN_SNAP=minute\n"
            "  WORKER_BRAIN_PERIOD_END=2026-02-09T20:00:00+03:00\n"
        )
        return

    if cmd in {"oneshot", "run-once"}:
        asyncio.run(_oneshot())
        return
    asyncio.run(_loop())


if __name__ == "__main__":
    main()
