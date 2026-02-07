Чистый Вестник (локальный bootstrap)

Старт

1) env
cp .env.example .env
заполни BOT_TOKEN

2) старт сервисов
docker compose up -d --build

3) проверка
docker compose ps
curl -s http://127.0.0.1:8001/health

Harvester (MTProto)

1) заполни TG_API_ID и TG_API_HASH в .env
2) получи TG_SESSION (интерактивно)
docker compose run --rm harvester python -m vestnik.harvester login
3) вставь TG_SESSION в .env и поставь HARVESTER_ENABLED=1
4) перезапусти harvester
docker compose up -d --build harvester

Проверка: в Telegram
/packs
/digest

Сервисы
- bot: Telegram bot (aiogram)
- harvester: MTProto сборщик (telethon)
- worker: фоновые задачи (stub)
- web: web/admin API (FastAPI)
- db: PostgreSQL
- redis: Redis
