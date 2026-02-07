Чистый Вестник (локальный bootstrap)

Старт

1) env
cp .env.example .env

2) старт сервисов
docker compose up -d --build

3) проверка
docker compose ps
docker compose logs -f bot

Сервисы
- bot: Telegram bot (aiogram)
- harvester: MTProto сборщик (stub)
- worker: фоновые задачи (stub)
- web: web/admin API (stub)
- db: PostgreSQL
- redis: Redis
