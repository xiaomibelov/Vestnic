import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

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


@dataclass(frozen=True)
class PostRow:
    channel_ref: str
    message_id: str
    text: str
    url: str
    parsed_at: datetime | None


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
              menu_chat_id bigint null,
              menu_message_id integer null,
              pause_until timestamptz null,
              format_mode varchar(16) not null default 'digest'
            );
            """
        )
    )
    await session.execute(text("alter table user_settings add column if not exists menu_chat_id bigint;"))
    await session.execute(text("alter table user_settings add column if not exists menu_message_id integer;"))
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


async def _get_user_settings(session, user_id: int) -> dict:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    row = (
        await session.execute(
            text(
                """
                select delivery_enabled, digest_interval_sec, last_sent_at, menu_chat_id, menu_message_id, pause_until, format_mode
                from user_settings where user_id=:uid
                """
            ),
            {"uid": user_id},
        )
    ).first()
    if not row:
        return {
            "delivery_enabled": True,
            "digest_interval_sec": None,
            "last_sent_at": None,
            "menu_chat_id": None,
            "menu_message_id": None,
            "pause_until": None,
            "format_mode": "digest",
        }
    return {
        "delivery_enabled": bool(row[0]),
        "digest_interval_sec": (int(row[1]) if row[1] is not None else None),
        "last_sent_at": row[2],
        "menu_chat_id": row[3],
        "menu_message_id": row[4],
        "pause_until": row[5],
        "format_mode": (str(row[6]) if row[6] else "digest"),
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


async def _toggle_format_mode(session, user_id: int) -> str:
    cur = await _get_user_settings(session, user_id)
    mode = (cur.get("format_mode") or "digest").strip().lower()
    new_mode = "compact" if mode != "compact" else "digest"
    await session.execute(
        text("update user_settings set format_mode=:m where user_id=:uid"),
        {"m": new_mode, "uid": user_id},
    )
    await session.commit()
    return new_mode


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


async def _pause_for_seconds(session, user_id: int, seconds: int) -> None:
    until = datetime.now(timezone.utc) + timedelta(seconds=max(int(seconds), 60))
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    await session.execute(text("update user_settings set pause_until=:u where user_id=:uid"), {"u": until, "uid": user_id})
    await session.commit()


async def _pause_clear(session, user_id: int) -> None:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    await session.execute(text("update user_settings set pause_until=null where user_id=:uid"), {"uid": user_id})
    await session.commit()


async def _reset_deliveries_for_user(session, user_id: int) -> int:
    await _ensure_deliveries_table(session)
    res = await session.execute(text("delete from deliveries where user_id=:uid"), {"uid": user_id})
    await session.commit()
    return int(res.rowcount or 0)


async def _touch_last_sent(session, user_id: int) -> None:
    await _ensure_user_settings(session)
    await _ensure_user_settings_row(session, user_id)
    await session.execute(text("update user_settings set last_sent_at=now() where user_id=:uid"), {"uid": user_id})
    await session.commit()


def _fmt_settings(s: dict) -> str:
    st = "ВКЛ ✅" if s["delivery_enabled"] else "ВЫКЛ ⛔️"

    if s["digest_interval_sec"]:
        mins = max(int(s["digest_interval_sec"] // 60), 1)
        iv = f"{mins} мин"
    else:
        iv = "глобальная (env)"

    mode = "компакт" if (s.get("format_mode") == "compact") else "дайджест"

    last = s["last_sent_at"].isoformat() if s["last_sent_at"] else "-"

    pause_until = s.get("pause_until")
    if pause_until:
        try:
            if pause_until.tzinfo is None:
                pause_until = pause_until.replace(tzinfo=timezone.utc)
            if pause_until > datetime.now(timezone.utc):
                pause = f"до {pause_until.isoformat(timespec='seconds')}"
            else:
                pause = "нет"
        except Exception:
            pause = "нет"
    else:
        pause = "нет"

    return f"Рассылка: {st}\nИнтервал: {iv}\nФормат: {mode}\nПауза: {pause}\nПоследняя отправка: {last}"


def _kb_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📦 Паки", callback_data="scr:packs:0")],
        [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data="scr:send")],
        [InlineKeyboardButton(text="🧾 Очередь", callback_data="scr:queue")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="scr:settings")],
        [InlineKeyboardButton(text="📡 Каналы", callback_data="scr:channels")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="scr:stats")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="scr:help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_back(to: str = "menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"scr:{to}")]])


def _kb_settings(s: dict) -> InlineKeyboardMarkup:
    toggle_txt = "Отключить рассылку" if s["delivery_enabled"] else "Включить рассылку"

    paused = False
    pu = s.get("pause_until")
    if pu:
        try:
            if pu.tzinfo is None:
                pu = pu.replace(tzinfo=timezone.utc)
            paused = pu > datetime.now(timezone.utc)
        except Exception:
            paused = False

    pause_txt = "▶️ Возобновить" if paused else "⏸ Пауза 1 час"
    mode_txt = "Формат: компакт" if (s.get("format_mode") == "compact") else "Формат: дайджест"

    rows = [
        [InlineKeyboardButton(text=toggle_txt, callback_data="act:delivery_toggle:settings")],
        [InlineKeyboardButton(text=pause_txt, callback_data="act:pause_toggle:settings")],
        [InlineKeyboardButton(text=mode_txt, callback_data="act:mode_toggle:settings")],
        [
            InlineKeyboardButton(text="⏱ 5м", callback_data="act:iv:5:settings"),
            InlineKeyboardButton(text="⏱ 15м", callback_data="act:iv:15:settings"),
            InlineKeyboardButton(text="⏱ 60м", callback_data="act:iv:60:settings"),
        ],
        [InlineKeyboardButton(text="⟲ Сбросить интервал", callback_data="act:iv_reset:settings")],
        [InlineKeyboardButton(text="♻️ Сбросить доставку (мне)", callback_data="scr:reset_confirm")],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="scr:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_reset_confirm() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="✅ Да, сбросить", callback_data="act:reset_deliveries")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="scr:settings")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_send(s: dict) -> InlineKeyboardMarkup:
    mode_txt = "Формат: компакт" if (s.get("format_mode") == "compact") else "Формат: дайджест"
    rows = [
        [InlineKeyboardButton(text=mode_txt, callback_data="act:mode_toggle:send")],
        [
            InlineKeyboardButton(text="1", callback_data="act:send:1"),
            InlineKeyboardButton(text="5", callback_data="act:send:5"),
            InlineKeyboardButton(text="10", callback_data="act:send:10"),
            InlineKeyboardButton(text="25", callback_data="act:send:25"),
        ],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="scr:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_help() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="scr:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


async def _safe_edit_text(cb: CallbackQuery, text0: str, kb: InlineKeyboardMarkup) -> None:
    if not cb.message:
        return
    try:
        await cb.message.edit_text(text0, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
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


async def _fetch_unsent_posts(session, user_id: int, channel_refs: list[str], limit: int) -> list[PostRow]:
    if not channel_refs:
        return []
    await _ensure_deliveries_table(session)
    now = datetime.now(timezone.utc)

    sql = text(
        """
        select p.channel_ref, p.message_id, p.text, p.url, p.parsed_at
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
        out.append(PostRow(channel_ref=str(r[0]), message_id=str(r[1]), text=str(r[2] or ""), url=str(r[3] or ""), parsed_at=r[4]))
    return out


async def _mark_delivered(session, user_id: int, posts: list[PostRow]) -> None:
    if not posts:
        return
    await _ensure_deliveries_table(session)
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


def _build_message(posts: list[PostRow], mode: str) -> str:
    mode = (mode or "digest").strip().lower()
    if mode == "compact":
        out = "Лента (компакт):\n\n"
        for p in posts:
            t = (p.text or "").strip().replace("\n", " ")
            if len(t) > 120:
                t = t[:120] + "…"
            url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
            line = f"• @{p.channel_ref}: {t}\n{url}\n\n"
            if len(out) + len(line) > 3800:
                break
            out += line
        return out.strip()

    out = "Авто-дайджест:\n\n"
    for p in posts:
        t = (p.text or "").strip().replace("\n", " ")
        if len(t) > 180:
            t = t[:180] + "…"
        url = (p.url or "").strip() or f"https://t.me/{p.channel_ref}/{p.message_id}"
        chunk = f"@{p.channel_ref}: {t}\n{url}\n\n"
        if len(out) + len(chunk) > 3800:
            break
        out += chunk
    return out.strip()


async def _render_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
    text0 = "Честный вестник\n\n" + _fmt_settings(s)
    return text0, _kb_menu()


async def _render_settings(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
    text0 = "⚙️ Настройки\n\n" + _fmt_settings(s)
    return text0, _kb_settings(s)


async def _render_reset_confirm(_user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text0 = "♻️ Сбросить доставку\n\nЭто удалит историю доставок *только для тебя*.\nПосле этого worker сможет прислать посты заново.\n\nПодтвердить?"
    return text0, _kb_reset_confirm()


async def _render_send(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
        selected = await _selected_pack_ids(session, user_id)
        refs: list[str] = []
        if selected:
            refs = await _channels_for_pack_ids(session, list(selected))
        unsent = 0
        if refs:
            unsent = len(await _fetch_unsent_posts(session, user_id, refs, 9999))
    text0 = "🚀 Отправить сейчас\n\n" + _fmt_settings(s) + f"\n\nВ очереди (тебе): {unsent}\n\nСколько постов отправить?"
    return text0, _kb_send(s)


async def _render_queue(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)
        selected = await _selected_pack_ids(session, user_id)
        if not selected:
            return "🧾 Очередь\n\nПаки не выбраны. Сначала выбери /packs.", _kb_back("menu")

        refs = await _channels_for_pack_ids(session, list(selected))
        if not refs:
            return "🧾 Очередь\n\nДля выбранных паков нет активных каналов.", _kb_back("menu")

        await _ensure_deliveries_table(session)
        now = datetime.now(timezone.utc)

        total_unsent = (await session.execute(
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
                  and p.expires_at > :now
                  and p.channel_ref = any(:refs)
                """
            ),
            {"uid": user_id, "now": now, "refs": refs},
        )).scalar_one()

        per = await session.execute(
            text(
                """
                select p.channel_ref, count(*) as cnt
                from posts_cache p
                left join deliveries d
                  on d.user_id = :uid
                 and d.channel_ref = p.channel_ref
                 and d.message_id = p.message_id
                where d.id is null
                  and p.is_deleted=false
                  and p.expires_at > :now
                  and p.channel_ref = any(:refs)
                group by p.channel_ref
                order by cnt desc, p.channel_ref asc
                limit 12
                """
            ),
            {"uid": user_id, "now": now, "refs": refs},
        )
        per_rows = per.all()

        prev = await session.execute(
            text(
                """
                select p.channel_ref, p.message_id, p.text, p.url, p.parsed_at
                from posts_cache p
                left join deliveries d
                  on d.user_id = :uid
                 and d.channel_ref = p.channel_ref
                 and d.message_id = p.message_id
                where d.id is null
                  and p.is_deleted=false
                  and p.expires_at > :now
                  and p.channel_ref = any(:refs)
                order by p.parsed_at desc
                limit 5
                """
            ),
            {"uid": user_id, "now": now, "refs": refs},
        )
        prev_rows = prev.all()

    lines = ["🧾 Очередь", "", _fmt_settings(s), "", f"Всего не доставлено (тебе): {int(total_unsent)}", ""]
    if per_rows:
        lines.append("Топ каналов:")
        for r in per_rows:
            lines.append(f"@{r[0]} — {int(r[1])}")
        lines.append("")

    if prev_rows:
        lines.append("Ближайшие посты:")
        for r in prev_rows:
            ch = str(r[0])
            mid = str(r[1])
            t = str(r[2] or "").strip().replace("\n", " ")
            if len(t) > 90:
                t = t[:90] + "…"
            url = (str(r[3] or "").strip()) or f"https://t.me/{ch}/{mid}"
            lines.append(f"• @{ch}: {t}")
            lines.append(url)
        lines.append("")

    lines.append("Действия: 🚀 Отправить сейчас → в меню")
    return "\n".join(lines).strip(), _kb_back("menu")


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
        "/settings — настройки\n\n"
        "Экраны:\n"
        "• 🚀 Отправить сейчас — ручная отправка N постов\n"
        "• 🧾 Очередь — сколько осталось и превью\n\n"
        "Логика:\n"
        "• Harvester собирает посты в БД (posts_cache)\n"
        "• Worker/бот отправляют и пишут deliveries (анти-дубли)\n"
        "• Настройки/пауза/формат — в user_settings\n"
    )
    return text0, _kb_help()


async def _render_screen(user_id: int, screen: str, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    if screen == "menu":
        return await _render_menu(user_id)
    if screen == "settings":
        return await _render_settings(user_id)
    if screen == "reset_confirm":
        return await _render_reset_confirm(user_id)
    if screen == "send":
        return await _render_send(user_id)
    if screen == "queue":
        return await _render_queue(user_id)
    if screen == "packs":
        return await _render_packs(user_id, page)
    if screen == "channels":
        return await _render_channels(user_id)
    if screen == "stats":
        return await _render_stats(user_id)
    if screen == "help":
        return await _render_help()
    return await _render_menu(user_id)


async def _send_now(bot: Bot, user_id: int, tg_id: int, n: int) -> int:
    n = max(min(int(n), 50), 1)
    async with session_scope() as session:
        s = await _get_user_settings(session, user_id)

        pu = s.get("pause_until")
        if pu:
            try:
                if pu.tzinfo is None:
                    pu = pu.replace(tzinfo=timezone.utc)
                if pu > datetime.now(timezone.utc):
                    return 0
            except Exception:
                pass

        if not s.get("delivery_enabled", True):
            return 0

        selected = await _selected_pack_ids(session, user_id)
        if not selected:
            return 0
        refs = await _channels_for_pack_ids(session, list(selected))
        if not refs:
            return 0

        posts = await _fetch_unsent_posts(session, user_id, refs, n)
        if not posts:
            return 0

        msg = _build_message(posts, s.get("format_mode") or "digest")

        await bot.send_message(tg_id, msg)
        await _mark_delivered(session, user_id, posts)
        await _touch_last_sent(session, user_id)
        return len(posts)


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
    await _open_menu_message(m.bot, m.from_user.id, m.chat.id, prefer_edit=True)


@dp.message(Command("menu"))
async def menu_cmd(m: Message):
    await _open_menu_message(m.bot, m.from_user.id, m.chat.id, prefer_edit=True)


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

    if act == "pause_toggle":
        async with session_scope() as session:
            s = await _get_user_settings(session, user.id)
            pu = s.get("pause_until")
            paused = False
            if pu:
                try:
                    if pu.tzinfo is None:
                        pu = pu.replace(tzinfo=timezone.utc)
                    paused = pu > datetime.now(timezone.utc)
                except Exception:
                    paused = False
            if paused:
                await _pause_clear(session, user.id)
            else:
                await _pause_for_seconds(session, user.id, 3600)
        text0, kb = await _render_screen(user.id, screen, page=page)
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "mode_toggle":
        async with session_scope() as session:
            await _toggle_format_mode(session, user.id)
        text0, kb = await _render_screen(user.id, screen, page=page)
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "iv":
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
        pack_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        async with session_scope() as session:
            await _toggle_pack(session, user.id, pack_id)
        text0, kb = await _render_screen(user.id, "packs", page=page)
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "reset_deliveries":
        async with session_scope() as session:
            n = await _reset_deliveries_for_user(session, user.id)
        if cb.message:
            await cb.message.answer(f"Ок. Сброшено доставок: {n}.")
        text0, kb = await _render_screen(user.id, "settings")
        await _safe_edit_text(cb, text0, kb)
        await cb.answer("OK")
        return

    if act == "send":
        n = 5
        if len(parts) > 2:
            try:
                n = int(parts[2])
            except Exception:
                n = 5
        try:
            sent = await _send_now(cb.bot, user.id, cb.from_user.id, n)
            if sent <= 0:
                await cb.answer("Нечего отправлять", show_alert=False)
            else:
                await cb.answer(f"Отправлено: {sent}", show_alert=False)
        except Exception:
            logger.exception("send_now error")
            await cb.answer("Ошибка отправки", show_alert=True)

        # refresh send screen
        text0, kb = await _render_screen(user.id, "send")
        await _safe_edit_text(cb, text0, kb)
        return

    await cb.answer("OK")


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required")
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
