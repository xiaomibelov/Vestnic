import asyncio
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

from vestnik.settings import BOT_TOKEN
from vestnik.db import db_ping

dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Чистый Вестник: бот поднят. Дальше подключим паки, парсер и генерацию отчётов.")

async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is empty. Put it into .env")

    ok = await db_ping()
    if not ok:
        print("db ping failed")

    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
