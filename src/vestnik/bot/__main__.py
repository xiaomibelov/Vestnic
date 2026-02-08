import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from vestnik.db import session_scope
import vestnik.models as models

def _model_by_tablename(tablename: str):
    for v in models.__dict__.values():
        if getattr(v, "__tablename__", None) == tablename:
            return v
    available = sorted({getattr(v, "__tablename__", None) for v in models.__dict__.values() if getattr(v, "__tablename__", None)})
    raise ImportError(f"Model for table {tablename!r} not found. Available tablenames: {available}")

Channel = _model_by_tablename("channels")
Pack = _model_by_tablename("packs")
PackChannel = _model_by_tablename("pack_channels")
PostCache = _model_by_tablename("posts_cache")
User = _model_by_tablename("users")
UserPack = _model_by_tablename("user_packs")

from vestnik.settings import BOT_TOKEN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vestnik.bot")

dp = Dispatcher()


def packs_keyboard(packs: list[Pack], selected_ids: set[int]) -> InlineKeyboardMarkup:
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


async def get_packs_and_selected(tg_id: int) -> tuple[list[Pack], set[int]]:
    async with session_scope() as session:
        packs = (await session.execute(select(Pack).where(Pack.is_active == True).order_by(Pack.id))).scalars().all()
        user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalars().first()
        if not user:
            return packs, set()
        selected = (
            await session.execute(
                select(UserPack.pack_id).where(UserPack.user_id == user.id, UserPack.is_enabled == True)
            )
        ).scalars().all()
        return packs, set(selected)


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

        row = (
            await session.execute(select(UserPack).where(UserPack.user_id == user.id, UserPack.pack_id == pack_id))
        ).scalars().first()

        if row and row.is_enabled:
            row.is_enabled = False
        elif row and not row.is_enabled:
            row.is_enabled = True
        else:
            session.add(UserPack(user_id=user.id, pack_id=pack_id, is_enabled=True))

        await session.commit()

    packs, selected = await get_packs_and_selected(cb.from_user.id)
    await safe_edit_reply_markup(cb, packs_keyboard(packs, selected))
    await cb.answer("OK")


@dp.message(Command("my_packs"))
async def my_packs(m: Message):
    logger.info("my_packs tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == m.from_user.id))).scalars().first()
        if not user:
            await m.answer("Пользователь не найден.")
            return
        q = (
            select(Pack)
            .join(UserPack, UserPack.pack_id == Pack.id)
            .where(UserPack.user_id == user.id, UserPack.is_enabled == True, Pack.is_active == True)
            .order_by(Pack.id)
        )
        packs = (await session.execute(q)).scalars().all()

    if not packs:
        await m.answer("Паки не выбраны. Используй /packs.")
        return

    lines = [f"• {p.title}" for p in packs]
    await m.answer("Выбранные паки:\n" + "\n".join(lines))


@dp.message(Command("account"))
async def account(m: Message):
    logger.info("account tg_id=%s", m.from_user.id)
    await ensure_user(m.from_user.id)
    async with session_scope() as session:
        user = (await session.execute(select(User).where(User.tg_id == m.from_user.id))).scalars().first()
        if not user:
            await m.answer("Пользователь не найден.")
            return

    exp = user.subscription_expires_at.isoformat() if user.subscription_expires_at else "-"
    await m.answer(f"Роль: {user.role}\nПодписка до: {exp}\nКоманды: /packs /my_packs /digest")


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

        pack_ids = (
            await session.execute(select(UserPack.pack_id).where(UserPack.user_id == user.id, UserPack.is_enabled == True))
        ).scalars().all()

        if not pack_ids:
            await m.answer("Паки не выбраны. Используй /packs.")
            return

        channel_usernames = (
            await session.execute(
                select(Channel.username)
                .join(PackChannel, PackChannel.channel_id == Channel.id)
                .where(PackChannel.pack_id.in_(list(pack_ids)), Channel.is_active == True)
            )
        ).scalars().all()

        channel_usernames = [u.lstrip('@') for u in channel_usernames]

        if not channel_usernames:
            await m.answer("Для выбранных паков нет активных каналов.")
            return

        posts = (
            await session.execute(
                select(PostCache)
                .where(
                    PostCache.channel_ref.in_(list(channel_usernames)),
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
        text = (p.text or "").strip().replace("\n", " ")
        if len(text) > 140:
            text = text[:140] + "…"
        chunk = f"{p.channel_ref}: {text}\n{p.url}\n\n"
        if len(out) + len(chunk) > 3800:
            break
        out += chunkhunkhunk

    await m.answer(out.strip())


async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is empty. Put it into .env")
    logger.info("bot starting polling")
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
