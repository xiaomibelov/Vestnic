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
from vestnik.models import PostCache, User
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


def _chunks(items: list, n: int) -> list[list]:
    if n <= 0:
        return [items]
    return [items[i : i + n] for i in range(0, len(items), n)]


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
              last_sent_at timestamptz null,
              menu_chat_id bigint null,
              menu_message_id integer null
            );
            """
        )
    )
    await session.execute(text("alter table user_settings add column if not exists menu_chat_id bigint;"))
    await session.execute(text("alter table user_settings add column if not exists menu_message_id integer;"))
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


async def _get_user_settings(session, user_id: int) -> dict:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    row = (
        await session.execute(
            text(
                """
                select delivery_enabled, digest_interval_sec, last_sent_at, menu_chat_id, menu_message_id
                from user_settings where user_id=:uid
                """
            ),
            {"uid": user_id},
        )
    ).first()
    if not row:
        return {"delivery_enabled": True, "digest_interval_sec": None, "last_sent_at": None, "menu_chat_id": None, "menu_message_id": None}

    return {
        "delivery_enabled": bool(row[0]),
        "digest_interval_sec": (int(row[1]) if row[1] is not None else None),
        "last_sent_at": row[2],
        "menu_chat_id": row[3],
        "menu_message_id": row[4],
    }


async def _set_menu_message(session, user_id: int, chat_id: int, message_id: int) -> None:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    await session.execute(
        text("update user_settings set menu_chat_id=:c, menu_message_id=:m where user_id=:uid"),
        {"c": int(chat_id), "m": int(message_id), "uid": int(user_id)},
    )
    await session.commit()


async def _toggle_delivery(session, user_id: int) -> bool:
    cur = await _get_user_settings(session, user_id)
    new_val = not bool(cur["delivery_enabled"])
    await session.execute(
        text("update user_settings set delivery_enabled=:v where user_id=:uid"),
        {"v": new_val, "uid": user_id},
    )
    await session.commit()
    return new_val


async def _set_interval_minutes(session, user_id: int, minutes: int | None) -> None:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)

    if minutes is None or minutes <= 0:
        await session.execute(text("update user_settings set digest_interval_sec=null where user_id=:uid"), {"uid": user_id})
        await session.commit()
        return

    sec = max(int(minutes) * 60, 60)
    await session.execute(text("update user_settings set digest_interval_sec=:sec where user_id=:uid"), {"sec": sec, "uid": user_id})
    await session.commit()


def _fmt_settings(s: dict) -> str:
    st = "ВКЛ ✅" if s["delivery_enabled"] else "ВЫКЛ ⛔️"
    if s["digest_interval_sec"]:
        mins = max(int(s["digest_interval_sec"] // 60), 1)
        iv = f"{mins} мин"
    else:
        iv = "глобальная (env)"
    last = s["last_sent_at"].isoformat() if s["last_sent_at"] else "-"
    return f"Рассылка: {st}\nИнтервал: {iv}\nПоследняя отправка: {last}"


def _kb_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📦 Паки", callback_data="scr:packs:0")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="scr:settings")],
        [InlineKeyboardButton(text="📰 Дайджест сейчас", callback_data="act:digest_now")],
        [InlineKeyboardButton(text="📡 Каналы", callback_data="scr:channels")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="scr:stats")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="scr:help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_back(to: str = "menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"scr:{to}")]])


def _kb_settings(s: dict) -> InlineKeyboardMarkup:
    toggle_txt = "Отключить рассылку" if s["delivery_enabled"] else "Включить рассылку"
    rows = [
        [InlineKeyboardButton(text=toggle_txt, callback_data="act:delivery_toggle:settings")],
        [
            InlineKeyboardButton(text="⏱ 5м", callback_data="act:iv:5:settings"),
            InlineKeyboardButton(text="⏱ 15м", callback_data="act:iv:15:settings"),
            InlineKeyboardButton(text="⏱ 60м", callback_data="act:iv:60:settings"),
        ],
        [InlineKeyboardButton(text="⟲ Сбросить интервал", callback_data="act:iv_reset:settings")],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="scr:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_help() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="scr:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _safe_edit_text(cb: CallbackQuery, text0: str, kb: InlineKeyboardMarkup) -> None:
    if not cb.message:
        return
    try:
        await cb.message.edit_text(text0, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        # если нельзя редактировать текст — попробуем только клавиатуру
        try:
            await cb.message.edit_reply_markup(reply_markup=kb)
        except TelegramBadRequest as e2:
            if "message is not modified" in str(e2):
                return
            raise


async def ensure_user(tg_id: int) -> User:
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalars().first()
        if user:
            await _ensure_user_settings(session)
            await _ensure_user_settings_row(session, user.id)
            return user
        user = User(tg_id=tg_id, role="guest")
        session.add(user)
        await session.commit()
        await session.refresh(user)
        await _ensure_user_settings(session)
        await _ensure_user_settings_row(session, user.id)
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


def _kb_packs(packs: list[PackRow], selected: set[int], page: int, pages: int, delivery_enabled: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for p in packs:
        mark = "✅" if p.id in selected else "➕"
        rows.append([InlineKeyboardButton(text=f"{mark} {p.title}", callback_data=f"act:pk:{p.id}:{page}")])

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"scr:packs:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{max(pages,1)}", callback_data="noop"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"scr:packs:{page+1}"))
    rows.append(nav)

    d_txt = "Рассылка: ВКЛ ✅" if delivery_enabled else "Рассылка: ВЫКЛ ⛔️"
    rows.append([InlineKeyboardButton(text=d_txt, callback_data=f"act:delivery_toggle:packs:{page}")])
    rows.append([InlineKeyboardButton(text="⚙️ Настройки", callback_data="scr:settings")])
    rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="scr:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
    text0 = "Честный вестник\n\n" + _fmt_settings(s)
    return text0, _kb_menu()


async def _render_settings(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
    text0 = "⚙️ Настройки\n\n" + _fmt_settings(s) + "\n\nБыстрые действия:"
    return text0, _kb_settings(s)


async def _render_packs(user_id: int, page: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
        packs_all = await _fetch_packs(session)
        selected = await _selected_pack_ids(session, user_id)

    per_page = 10
    pages = max((len(packs_all) + per_page - 1) // per_page, 1)
    page = max(min(page, pages - 1), 0)
    chunk = packs_all[page * per_page : (page + 1) * per_page]

    text0 = f"📦 Паки (выбрано: {len(selected)})\n\nНажми, чтобы включить/выключить."
    return text0, _kb_packs(chunk, selected, page, pages, bool(s["delivery_enabled"]))


async def _render_channels(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        selected = await _selected_pack_ids(session, user_id)
        if not selected:
            return "📡 Каналы\n\nПаки не выбраны. Сначала выбери паки.", _kb_back("menu")

        refs = await _channels_for_pack_ids(session, list(selected))
        if not refs:
            return "📡 Каналы\n\nДля выбранных паков нет активных каналов.", _kb_back("menu")

        sql = text(
            """
            select channel_ref, count(*) as cnt, max(message_id) as max_mid
            from posts_cache
            where is_deleted=false and channel_ref = any(:refs)
            group by channel_ref
            order by cnt desc, channel_ref asc
            """
        )
        res = await session.execute(sql, {"refs": refs})
        rows = res.all()

    lines = ["📡 Каналы (по выбранным пакам):", ""]
    for r in rows[:40]:
        lines.append(f"@{r[0]} — {int(r[1])} постов (max id {r[2]})")
    if len(rows) > 40:
        lines.append("")
        lines.append(f"…и ещё {len(rows)-40}")

    return "\n".join(lines).strip(), _kb_back("menu")


async def _render_stats(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
        selected = await _selected_pack_ids(session, user_id)

        posts_total = (await session.execute(text("select count(*) from posts_cache"))).scalar_one()
        deliveries_total = (await session.execute(text("select count(*) from deliveries"))).scalar_one()

        unsent = (await session.execute(
            text(
                """
                select count(*)
                from posts_cache p
                left join deliveries d
                  on d.user_id = :uid
                 and d.channel_ref = p.channel_ref
                 and d.message_id = p.message_id
                where d.id is null
                  and p.is_deleted=false
                  and p.expires_at > now()
                """
            ),
            {"uid": user_id},
        )).scalar_one()

    text0 = (
        "📊 Статистика\n\n"
        f"{_fmt_settings(s)}\n\n"
        f"Выбрано паков: {len(selected)}\n"
        f"posts_cache: {int(posts_total)}\n"
        f"deliveries (все): {int(deliveries_total)}\n"
        f"не доставлено (тебе): {int(unsent)}"
    )
    return text0, _kb_back("menu")


async def _render_help() -> tuple[str, InlineKeyboardMarkup]:
    text0 = (
        "ℹ️ Помощь\n\n"
        "Команды:\n"
        "/menu — открыть меню\n"
        "/packs — паки\n"
        "/settings — настройки\n"
        "/digest — ручной дайджест\n\n"
        "Логика:\n"
        "• Harvester собирает посты в БД (posts_cache)\n"
        "• Worker рассылает (deliveries — защита от дублей)\n"
        "• Включение/интервал — в настройках (user_settings)\n"
    )
    return text0, _kb_help()


async def _render_screen(user_id: int, screen: str, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    if screen == "menu":
        return await _render_menu(user_id)
    if screen == "settings":
        return await _render_settings(user_id)
    if screen == "packs":
        return await _render_packs(user_id, page)
    if screen == "channels":
        return await _render_channels(user_id)
    if screen == "stats":
        return await _render_stats(user_id)
    if screen == "help":
        return await _render_help()
    return await _render_menu(user_id)


async def _manual_digest(user_id: int, msg_ctx: Message) -> None:
    now = datetime.now(timezone.utc)
    async with session_scope() as session:
        selected = await _selected_pack_ids(session, user_id)
        if not selected:
            await msg_ctx.answer("Паки не выбраны. Открой /packs.")
            return
        refs = await _channels_for_pack_ids(session, list(selected))
        if not refs:
            await msg_ctx.answer("Для выбранных паков нет активных каналов.")
            return

        posts = (
            await session.execute(
                select(PostCache)
                .where(
                    PostCache.channel_ref.in_(list(refs)),
                    PostCache.is_deleted == False,
                    PostCache.expires_at > now,
                )
                .order_by(PostCache.parsed_at.desc())
                .limit(15)
            )
        ).scalars().all()

    if not posts:
        await msg_ctx.answer("Нет свежих постов (harvester ещё не собрал новые данные).")
        return

    out = "📰 Дайджест (ручной):\n\n"
    for p in posts:
        text0 = (p.text or "").strip().replace("\n", " ")
        if len(text0) > 180:
            text0 = text0[:180] + "…"
        url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
        chunk = f"@{p.channel_ref}: {text0}\n{url}\n\n"
        if len(out) + len(chunk) > 3800:
            break
        out += chunk
    await msg_ctx.answer(out.strip())


async def _open_menu_message(bot: Bot, tg_id: int, chat_id: int, prefer_edit: bool = True) -> None:
    user = await ensure_user(tg_id)
    async with session_scope() as session:
        s = await _get_user_settings(session, user.id)
        menu_chat_id = s["menu_chat_id"]
        menu_message_id = s["menu_message_id"]

    text0, kb = await _render_screen(user.id, "menu")

    if prefer_edit and menu_chat_id and menu_message_id and int(menu_chat_id) == int(chat_id):
        try:
            await bot.edit_message_text(text0, chat_id=int(chat_id), message_id=int(menu_message_id), reply_markup=kb)
            return
        except Exception:
            pass

    m = await bot.send_message(chat_id, text0, reply_markup=kb)
    async with session_scope() as session:
        await _set_menu_message(session, user.id, int(chat_id), int(m.message_id))


@dp.message(CommandStart())
async def start(m: Message):
    logger.info("start tg_id=%s", m.from_user.id)
    bot = m.bot
    await _open_menu_message(bot, m.from_user.id, m.chat.id, prefer_edit=True)


@dp.message(Command("menu"))
async def menu_cmd(m: Message):
    bot = m.bot
    await _open_menu_message(bot, m.from_user.id, m.chat.id, prefer_edit=True)


@dp.message(Command("packs"))
async def packs_cmd(m: Message):
    user = await ensure_user(m.from_user.id)
    text0, kb = await _render_screen(user.id, "packs", page=0)
    await m.answer(text0, reply_markup=kb)


@dp.message(Command("settings"))
async def settings_cmd(m: Message):
    user = await ensure_user(m.from_user.id)
    text0, kb = await _render_screen(user.id, "settings")
    await m.answer(text0, reply_markup=kb)


@dp.message(Command("digest"))
async def digest_cmd(m: Message):
    user = await ensure_user(m.from_user.id)
    await _manual_digest(user.id, m)


@dp.callback_query(F.data == "noop")
async def noop(cb: CallbackQuery):
    await cb.answer("")


@dp.callback_query(F.data.startswith("scr:"))
async def screen_router(cb: CallbackQuery):
    user = await ensure_user(cb.from_user.id)
    parts = (cb.data or "").split(":")
    screen = parts[1] if len(parts) > 1 else "menu"
    page = 0
    if screen == "packs" and len(parts) > 2:
        try:
            page = int(parts[2])
        except Exception:
            page = 0

    text0, kb = await _render_screen(user.id, screen, page=page)
    await _safe_edit_text(cb, text0, kb)
    await cb.answer("OK")


@dp.callback_query(F.data.startswith("act:"))
async def action_router(cb: CallbackQuery):
    user = await ensure_user(cb.from_user.id)
    parts = (cb.data or "").split(":")
    act = parts[1] if len(parts) > 1 else ""
    screen = parts[2] if len(parts) > 2 else "menu"
    page = 0
    if len(parts) > 3:
        try:
            page = int(parts[3])
        except Exception:
            page = 0

    if act == "delivery_toggle":
        async with session_scope() as session:
            await _toggle_delivery(session, user.id)
        text0, kb = await _render_screen(user.id, screen, page=page)
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "iv":
        # act:iv:<minutes>:<screen>[:page]
        minutes = 0
        if len(parts) > 2:
            try:
                minutes = int(parts[2])
            except Exception:
                minutes = 0
        screen = parts[3] if len(parts) > 3 else "settings"
        page = 0
        if len(parts) > 4:
            try:
                page = int(parts[4])
            except Exception:
                page = 0
        async with session_scope() as session:
            await _set_interval_minutes(session, user.id, minutes)
        text0, kb = await _render_screen(user.id, screen, page=page)
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "iv_reset":
        async with session_scope() as session:
            await _set_interval_minutes(session, user.id, None)
        text0, kb = await _render_screen(user.id, "settings")
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "pk":
        # act:pk:<pack_id>:<page>
        pack_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        async with session_scope() as session:
            await _toggle_pack(session, user.id, pack_id)
        text0, kb = await _render_screen(user.id, "packs", page=page)
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "digest_now":
        if cb.message:
            await cb.message.answer("Собираю дайджест…")
            await _manual_digest(user.id, cb.message)
        await cb.answer("OK")
        return

    await cb.answer("OK")


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required")
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
