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


async def _ensure_user_settings(session) -> None:
    await session.execute(
        text(
            """
            create table if not exists user_settings (
              user_id integer primary key,
              delivery_enabled boolean not null default true,
              digest_interval_sec integer null,
              last_sent_at timestamptz null
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_user_settings_delivery_enabled on user_settings(delivery_enabled);"))
    await session.commit()


async def _ensure_user_settings_row(session, user_id: int) -> None:
    await session.execute(
        text(
            """
            insert into user_settings (user_id)
            values (:uid)
            on conflict do nothing
            """
        ),
        {"uid": user_id},
    )
    await session.commit()


async def _get_user_settings(session, user_id: int) -> tuple[bool, int | None, datetime | None]:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    row = (
        await session.execute(
            text("select delivery_enabled, digest_interval_sec, last_sent_at from user_settings where user_id=:uid"),
            {"uid": user_id},
        )
    ).first()
    if not row:
        return True, None, None
    return bool(row[0]), (int(row[1]) if row[1] is not None else None), row[2]


async def _toggle_delivery(session, user_id: int) -> bool:
    enabled, _, _ = await _get_user_settings(session, user_id)
    new_val = not enabled
    await session.execute(
        text("update user_settings set delivery_enabled=:v where user_id=:uid"),
        {"v": new_val, "uid": user_id},
    )
    await session.commit()
    return new_val


async def _set_interval_minutes(session, user_id: int, minutes: int) -> None:
    sec = max(int(minutes) * 60, 60)
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    await session.execute(
        text("update user_settings set digest_interval_sec=:sec where user_id=:uid"),
        {"sec": sec, "uid": user_id},
    )
    await session.commit()


def _settings_text(delivery_enabled: bool, interval_sec: int | None, last_sent_at) -> str:
    st = "ВКЛ ✅" if delivery_enabled else "ВЫКЛ ⛔️"
    if interval_sec:
        mins = max(int(interval_sec // 60), 1)
        iv = f"{mins} мин"
    else:
        iv = "глобальная (env)"
    last = last_sent_at.isoformat() if last_sent_at else "-"
    return f"Настройки:\nРассылка: {st}\nИнтервал: {iv}\nПоследняя отправка: {last}"


def _settings_kb(delivery_enabled: bool) -> InlineKeyboardMarkup:
    btn = "Отключить рассылку" if delivery_enabled else "Включить рассылку"
    rows = [
        [InlineKeyboardButton(text=btn, callback_data="delivery:toggle")],
        [InlineKeyboardButton(text="Паки", callback_data="ui:packs")],
        [InlineKeyboardButton(text="Дайджест сейчас", callback_data="ui:digest_now")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def packs_keyboard(packs: list[PackRow], selected_ids: set[int], delivery_enabled: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for p in packs:
        mark = "✅" if p.id in selected_ids else "➕"
        rows.append([InlineKeyboardButton(text=f"{mark} {p.title}", callback_data=f"pack:{p.id}")])

    delivery_txt = "Рассылка: ВКЛ ✅" if delivery_enabled else "Рассылка: ВЫКЛ ⛔️"
    rows.append([InlineKeyboardButton(text=delivery_txt, callback_data="delivery:toggle")])
    rows.append([InlineKeyboardButton(text="Обновить", callback_data="packs:refresh")])
    rows.append([InlineKeyboardButton(text="Настройки", callback_data="ui:settings")])
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
    return [PackRow(id=int(r[0]), title=str(r[1])) for r in rows]


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


async def _toggle_pack(session, user_id: int, pack_id: int) -> None:
    _packs_t, user_packs_t, _pack_channels_t = await _resolve_pack_tables(session)
    cols = await _table_cols(session, user_packs_t)

    user_id_col = "user_id" if "user_id" in cols else None
    pack_id_col = "pack_id" if "pack_id" in cols else None
    enabled_col = "is_enabled" if "is_enabled" in cols else ("enabled" if "enabled" in cols else None)

    if not user_id_col or not pack_id_col:
        raise RuntimeError(f"user_packs table {user_packs_t!r} missing user_id/pack_id; cols={sorted(cols)}")

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


async def _render_packs_ui(tg_id: int) -> tuple[str, InlineKeyboardMarkup]:
    await ensure_user(tg_id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalars().first()
        if not user:
            return "Пользователь не найден.", InlineKeyboardMarkup(inline_keyboard=[])

        delivery_enabled, interval_sec, last_sent = await _get_user_settings(session, user.id)

        packs = await _fetch_packs(session)
        selected = await _selected_pack_ids(session, user.id)

    title = _settings_text(delivery_enabled, interval_sec, last_sent) + "\n\nВыбери паки:"
    return title, packs_keyboard(packs, selected, delivery_enabled)


@dp.message(CommandStart())
async def start(m: Message):
    logger.info("start tg_id=%s", m.from_user.id)
    title, kb = await _render_packs_ui(m.from_user.id)
    await m.answer(title, reply_markup=kb)


@dp.message(Command("packs"))
async def packs_cmd(m: Message):
    logger.info("packs tg_id=%s", m.from_user.id)
    title, kb = await _render_packs_ui(m.from_user.id)
    await m.answer(title, reply_markup=kb)


@dp.message(Command("settings"))
async def settings_cmd(m: Message):
    logger.info("settings tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == m.from_user.id))).scalars().first()
        if not user:
            await m.answer("Пользователь не найден.")
            return
        enabled, interval_sec, last_sent = await _get_user_settings(session, user.id)
    await m.answer(_settings_text(enabled, interval_sec, last_sent), reply_markup=_settings_kb(enabled))


@dp.message(Command("interval"))
async def interval_cmd(m: Message):
    logger.info("interval tg_id=%s", m.from_user.id)
    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        await m.answer("Использование: /interval 15  (минуты). Чтобы сбросить: /interval 0")
        return
    try:
        minutes = int(parts[1])
    except Exception:
        await m.answer("Нужно число минут. Пример: /interval 15")
        return

    await ensure_user(m.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == m.from_user.id))).scalars().first()
        if not user:
            await m.answer("Пользователь не найден.")
            return
        await _ensure_user_settings(session)
        await _ensure_user_settings_row(session, user.id)

        if minutes <= 0:
            await session.execute(text("update user_settings set digest_interval_sec=null where user_id=:uid"), {"uid": user.id})
            await session.commit()
            enabled, interval_sec, last_sent = await _get_user_settings(session, user.id)
            await m.answer("Интервал сброшен. Теперь используется глобальная настройка.", reply_markup=_settings_kb(enabled))
            return

        await _set_interval_minutes(session, user.id, minutes)
        enabled, interval_sec, last_sent = await _get_user_settings(session, user.id)
        await m.answer(f"Ок. Персональный интервал: {max(interval_sec // 60, 1)} мин", reply_markup=_settings_kb(enabled))


@dp.callback_query(F.data == "ui:settings")
async def ui_settings(cb: CallbackQuery):
    await ensure_user(cb.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == cb.from_user.id))).scalars().first()
        if not user:
            await cb.answer("user not found", show_alert=True)
            return
        enabled, interval_sec, last_sent = await _get_user_settings(session, user.id)
    if cb.message:
        await cb.message.answer(_settings_text(enabled, interval_sec, last_sent), reply_markup=_settings_kb(enabled))
    await cb.answer("OK")


@dp.callback_query(F.data == "ui:packs")
async def ui_packs(cb: CallbackQuery):
    title, kb = await _render_packs_ui(cb.from_user.id)
    if cb.message:
        await cb.message.answer(title, reply_markup=kb)
    await cb.answer("OK")


@dp.callback_query(F.data == "delivery:toggle")
async def delivery_toggle(cb: CallbackQuery):
    await ensure_user(cb.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == cb.from_user.id))).scalars().first()
        if not user:
            await cb.answer("user not found", show_alert=True)
            return
        new_val = await _toggle_delivery(session, user.id)
        enabled, interval_sec, last_sent = await _get_user_settings(session, user.id)

    if cb.message:
        await cb.message.answer(_settings_text(enabled, interval_sec, last_sent), reply_markup=_settings_kb(new_val))
    await cb.answer("OK")


@dp.callback_query(F.data == "packs:refresh")
async def packs_refresh(cb: CallbackQuery):
    title, kb = await _render_packs_ui(cb.from_user.id)
    if cb.message:
        try:
            await cb.message.edit_text(title, reply_markup=kb)
        except TelegramBadRequest:
            await safe_edit_reply_markup(cb, kb)
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

    title, kb = await _render_packs_ui(cb.from_user.id)
    if cb.message:
        try:
            await cb.message.edit_text(title, reply_markup=kb)
        except TelegramBadRequest:
            await safe_edit_reply_markup(cb, kb)
    await cb.answer("OK")


@dp.callback_query(F.data == "ui:digest_now")
async def ui_digest_now(cb: CallbackQuery):
    if cb.message:
        await cb.message.answer("Собираю дайджест…")
    await cb.answer("OK")
    await _send_digest_to_user(cb.from_user.id, cb.message)


async def _send_digest_to_user(tg_id: int, msg_ctx: Message | None) -> None:
    await ensure_user(tg_id)
    now = datetime.now(timezone.utc)

    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalars().first()
        if not user:
            if msg_ctx:
                await msg_ctx.answer("Пользователь не найден.")
            return

        selected = await _selected_pack_ids(session, user.id)
        if not selected:
            if msg_ctx:
                await msg_ctx.answer("Паки не выбраны. Используй /packs.")
            return

        channel_refs = await _channels_for_pack_ids(session, list(selected))
        if not channel_refs:
            if msg_ctx:
                await msg_ctx.answer("Для выбранных паков нет активных каналов.")
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
        if msg_ctx:
            await msg_ctx.answer("Нет свежих постов (harvester ещё не собрал новые данные).")
        return

    out = "Дайджест (ручной):\n\n"
    for p in posts:
        text0 = (p.text or "").strip().replace("\n", " ")
        if len(text0) > 180:
            text0 = text0[:180] + "…"
        url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
        chunk = f"{p.channel_ref}: {text0}\n{url}\n\n"
        if len(out) + len(chunk) > 3800:
            break
        out += chunk

    if msg_ctx:
        await msg_ctx.answer(out)


@dp.message(Command("digest"))
async def digest_cmd(m: Message):
    logger.info("digest tg_id=%s", m.from_user.id)
    await _send_digest_to_user(m.from_user.id, m)


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required")
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
