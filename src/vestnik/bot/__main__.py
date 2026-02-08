import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select, text

from vestnik.db import session_scope
from vestnik.models import Channel, PostCache, User
from vestnik.settings import BOT_TOKEN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vestnik.bot")

dp = Dispatcher()


@dataclass(frozen=True)
class PackRow:
    id: int
    title: str


def packs_keyboard(packs: list[PackRow], selected_ids: set[int]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for p in packs:
        mark = "✅" if p.id in selected_ids else "➕"
        rows.append([InlineKeyboardButton(text=f"{mark} {p.title}", callback_data=f"pack:{p.id}")])
    rows.append([InlineKeyboardButton(text="Обновить", callback_data="packs:refresh")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def safe_edit_reply_markup(cb: CallbackQuery, markup: InlineKeyboardMarkup) -> None:
    if not cb.message:
        return
    try:
        await cb.message.edit_reply_markup(reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


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


async def _resolve_pack_tables(session) -> tuple[str, str, str]:
    tables = await _list_tables(session)
    packs_t = _pick_table(tables, ["packs", "pack"])
    user_packs_t = _pick_table(tables, ["user_packs", "user_pack", "users_packs"])
    pack_channels_t = _pick_table(tables, ["pack_channels", "pack_channel", "packs_channels"])

    missing = [name for name, t in [("packs", packs_t), ("user_packs", user_packs_t), ("pack_channels", pack_channels_t)] if t is None]
    if missing:
        raise RuntimeError(f"missing tables: {missing}. existing={sorted(tables)}")

    return packs_t, user_packs_t, pack_channels_t


async def ensure_user(tg_id: int) -> User:
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalars().first()
        if user:
            return user
        user = User(tg_id=tg_id, role="guest")
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


async def _fetch_packs(session) -> list[PackRow]:
    packs_t, _user_packs_t, _pack_channels_t = await _resolve_pack_tables(session)
    cols = await _table_cols(session, packs_t)

    id_col = "id" if "id" in cols else None
    if not id_col:
        raise RuntimeError(f"packs table {packs_t!r} has no id column; cols={sorted(cols)}")

    title_col = "title" if "title" in cols else ("name" if "name" in cols else ("slug" if "slug" in cols else None))
    if not title_col:
        title_col = id_col

    where_sql = ""
    if "is_active" in cols:
        where_sql = "where is_active = true"

    sql = f"select {_safe_ident(id_col)} as id, {_safe_ident(title_col)} as title from {_safe_ident(packs_t)} {where_sql} order by {_safe_ident(id_col)}"
    res = await session.execute(text(sql))
    rows = res.all()
    out: list[PackRow] = []
    for r in rows:
        out.append(PackRow(id=int(r[0]), title=str(r[1])))
    return out


async def _selected_pack_ids(session, user_id: int) -> set[int]:
    _packs_t, user_packs_t, _pack_channels_t = await _resolve_pack_tables(session)
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
    return {int(r[0]) for r in res.all()}


async def get_packs_and_selected(tg_id: int) -> tuple[list[PackRow], set[int]]:
    await ensure_user(tg_id)
    async with session_scope() as session:
        packs = await _fetch_packs(session)
        user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalars().first()
        if not user:
            return packs, set()
        selected = await _selected_pack_ids(session, user.id)
        return packs, selected


async def _toggle_pack(session, user_id: int, pack_id: int) -> None:
    _packs_t, user_packs_t, _pack_channels_t = await _resolve_pack_tables(session)
    cols = await _table_cols(session, user_packs_t)

    user_id_col = "user_id" if "user_id" in cols else None
    pack_id_col = "pack_id" if "pack_id" in cols else None
    enabled_col = "is_enabled" if "is_enabled" in cols else ("enabled" if "enabled" in cols else None)

    if not user_id_col or not pack_id_col:
        raise RuntimeError(f"user_packs table {user_packs_t!r} missing user_id/pack_id; cols={sorted(cols)}")

    # find existing row
    sql_find = (
        f"select {_safe_ident(user_id_col)}, {_safe_ident(pack_id_col)}"
        + (f", {_safe_ident(enabled_col)}" if enabled_col else "")
        + f" from {_safe_ident(user_packs_t)} where {_safe_ident(user_id_col)}=:uid and {_safe_ident(pack_id_col)}=:pid limit 1"
    )
    row = (await session.execute(text(sql_find), {"uid": user_id, "pid": pack_id})).first()

    if enabled_col:
        if row is None:
            sql_ins = (
                f"insert into {_safe_ident(user_packs_t)} ({_safe_ident(user_id_col)}, {_safe_ident(pack_id_col)}, {_safe_ident(enabled_col)}) "
                f"values (:uid, :pid, true)"
            )
            await session.execute(text(sql_ins), {"uid": user_id, "pid": pack_id})
        else:
            cur_enabled = bool(row[2])
            sql_upd = (
                f"update {_safe_ident(user_packs_t)} set {_safe_ident(enabled_col)} = :val "
                f"where {_safe_ident(user_id_col)}=:uid and {_safe_ident(pack_id_col)}=:pid"
            )
            await session.execute(text(sql_upd), {"val": (not cur_enabled), "uid": user_id, "pid": pack_id})
    else:
        # no enabled column: toggle by insert/delete
        if row is None:
            sql_ins = (
                f"insert into {_safe_ident(user_packs_t)} ({_safe_ident(user_id_col)}, {_safe_ident(pack_id_col)}) "
                f"values (:uid, :pid)"
            )
            await session.execute(text(sql_ins), {"uid": user_id, "pid": pack_id})
        else:
            sql_del = (
                f"delete from {_safe_ident(user_packs_t)} where {_safe_ident(user_id_col)}=:uid and {_safe_ident(pack_id_col)}=:pid"
            )
            await session.execute(text(sql_del), {"uid": user_id, "pid": pack_id})

    await session.commit()


async def _channels_for_pack_ids(session, pack_ids: list[int]) -> list[str]:
    if not pack_ids:
        return []
    _packs_t, _user_packs_t, pack_channels_t = await _resolve_pack_tables(session)
    pc_cols = await _table_cols(session, pack_channels_t)

    pack_id_col = "pack_id" if "pack_id" in pc_cols else None
    channel_id_col = "channel_id" if "channel_id" in pc_cols else None
    if not pack_id_col or not channel_id_col:
        raise RuntimeError(f"pack_channels table {pack_channels_t!r} missing pack_id/channel_id; cols={sorted(pc_cols)}")

    # channels table is ORM-backed; columns should exist as in MVP
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


@dp.message(CommandStart())
async def start(m: Message):
    logger.info("start tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    packs, selected = await get_packs_and_selected(m.from_user.id)
    if not packs:
        await m.answer("Паки пока не настроены.")
        return
    await m.answer("Выбери паки для дайджеста:", reply_markup=packs_keyboard(packs, selected))


@dp.message(Command("packs"))
async def packs_cmd(m: Message):
    logger.info("packs tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    packs, selected = await get_packs_and_selected(m.from_user.id)
    await m.answer("Твои паки:", reply_markup=packs_keyboard(packs, selected))


@dp.callback_query(F.data == "packs:refresh")
async def packs_refresh(cb: CallbackQuery):
    logger.info("packs_refresh tg_id=%s", cb.from_user.id)
    packs, selected = await get_packs_and_selected(cb.from_user.id)
    await safe_edit_reply_markup(cb, packs_keyboard(packs, selected))
    await cb.answer("OK")


@dp.callback_query(F.data.startswith("pack:"))
async def pack_toggle(cb: CallbackQuery):
    pack_id = int(cb.data.split(":", 1)[1])
    logger.info("pack_toggle tg_id=%s pack_id=%s", cb.from_user.id, pack_id)
    await ensure_user(cb.from_user.id)

    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == cb.from_user.id))).scalars().first()
        if not user:
            await cb.answer("user not found", show_alert=True)
            return
        await _toggle_pack(session, user.id, pack_id)

    packs, selected = await get_packs_and_selected(cb.from_user.id)
    await safe_edit_reply_markup(cb, packs_keyboard(packs, selected))
    await cb.answer("OK")


@dp.message(Command("account"))
async def account(m: Message):
    logger.info("account tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == m.from_user.id))).scalars().first()
        if not user:
            await m.answer("Пользователь не найден.")
            return

    exp = getattr(user, "subscription_expires_at", None)
    exp_s = exp.isoformat() if exp else "-"
    role = getattr(user, "role", "guest")
    await m.answer(f"Роль: {role}\nПодписка до: {exp_s}\nКоманды: /packs /digest")


@dp.message(Command("digest"))
async def digest(m: Message):
    logger.info("digest tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    now = datetime.now(timezone.utc)

    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == m.from_user.id))).scalars().first()
        if not user:
            await m.answer("Пользователь не найден.")
            return

        # selected pack ids
        try:
            selected = await _selected_pack_ids(session, user.id)
        except Exception as e:
            logger.exception("digest selected packs error: %s", e)
            await m.answer("Схема паков не готова (таблицы packs/user_packs/pack_channels).")
            return

        if not selected:
            await m.answer("Паки не выбраны. Используй /packs.")
            return

        channel_refs = await _channels_for_pack_ids(session, list(selected))
        if not channel_refs:
            await m.answer("Для выбранных паков нет активных каналов.")
            return

        posts = (
            await session.execute(
                select(PostCache)
                .where(
                    PostCache.channel_ref.in_(list(channel_refs)),
                    PostCache.is_deleted == False,
                    PostCache.expires_at > now,
                )
                .order_by(PostCache.parsed_at.desc())
                .limit(15)
            )
        ).scalars().all()

    if not posts:
        await m.answer("Нет свежих постов. Harvester ещё не собрал данные или не настроен.")
        return

    out = "Дайджест (последние посты):\n\n"
    for p in posts:
        text0 = (p.text or "").strip().replace("\n", " ")
        if len(text0) > 140:
            text0 = text0[:140] + "…"
        url = (p.url or "").strip()
        chunk = f"{p.channel_ref}: {text0}\n{url}\n\n"
        if len(out) + len(chunk) > 3800:
            break
        out += chunk

    await m.answer(out)


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required")
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
