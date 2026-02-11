# Vestnik deploy checklist

## Policy
- **Runtime DDL запрещён**: все сервисы в обычном запуске должны работать с `VESTNIK_SCHEMA_AUTO=0`.
- **DDL только отдельным шагом**: `python -m vestnik.schema init`.

## Preflight
- `git status -sb` (чисто)
- при необходимости: `docker compose pull` (если у тебя так принято)

## Build
- `docker compose build --no-cache worker`

## DB schema (единственный DDL шаг)
- `docker compose run --rm -e VESTNIK_SCHEMA_AUTO=1 worker python -m vestnik.schema init`
- проверка:
  - `docker compose run --rm worker python -m vestnik.schema check`

## Runtime (DDL off)
- В `.env`/compose:
  - `VESTNIK_SCHEMA_AUTO=0`
- Старт сервисов (по твоей схеме: `up -d`, systemd, etc.)

## Smoke
- `docker compose run --rm -e VESTNIK_SCHEMA_AUTO=0 worker python -m vestnik.worker oneshot`
- Если нужна быстрая проверка БД:
  - `docker compose run --rm worker python -m vestnik.schema check`
