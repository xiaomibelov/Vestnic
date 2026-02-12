from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vestnik.db import session_scope
from vestnik.settings import env_bool


async def maybe_ensure_schema(session: AsyncSession) -> None:
    # По умолчанию авто-DDL в рантайме выключен, чтобы не ловить lock waits.
    if not env_bool("VESTNIK_SCHEMA_AUTO", False):
        return
    await ensure_schema(session)
    # DDL транзакционный; страхуемся явным commit.
    try:
        await session.commit()
    except Exception:
        pass


async def _get_table_columns(session: AsyncSession, table: str) -> set[str]:
    res = await session.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema = current_schema()
              and table_name = :t
            """
        ),
        {"t": table},
    )
    return {str(r[0]) for r in res.fetchall()}


async def _ensure_column(session: AsyncSession, cols: set[str], table: str, col: str, ddl: str) -> None:
    # ВАЖНО: избегаем "ALTER ... IF NOT EXISTS" в steady-state,
    # потому что сам ALTER берёт AccessExclusiveLock даже когда ничего не меняет.
    if col in cols:
        return
    await session.execute(text(ddl))


async def ensure_schema(session: AsyncSession) -> None:
    # users
    await session.execute(
        text(
            """
            create table if not exists users (
              id serial primary key,
              tg_id bigint not null,
              username varchar null,
              role varchar(32) not null default 'guest',
              subscription_expires_at timestamptz null,
              referrer_tg_id bigint null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    users_cols = await _get_table_columns(session, "users")
    await _ensure_column(session, users_cols, "users", "tg_id", "alter table users add column tg_id bigint;")
    await _ensure_column(session, users_cols, "users", "username", "alter table users add column username varchar;")
    await _ensure_column(session, users_cols, "users", "role", "alter table users add column role varchar(32);")
    await _ensure_column(
        session,
        users_cols,
        "users",
        "subscription_expires_at",
        "alter table users add column subscription_expires_at timestamptz;",
    )
    await _ensure_column(session, users_cols, "users", "referrer_tg_id", "alter table users add column referrer_tg_id bigint;")
    await _ensure_column(session, users_cols, "users", "created_at", "alter table users add column created_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_users_tg_id on users(tg_id);"))
    await session.execute(text("create index if not exists ix_users_role on users(role);"))

    # prompts
    await session.execute(
        text(
            """
            create table if not exists prompts (
              id serial primary key,
              key varchar(64) not null,
              text text not null default '',
              updated_at timestamptz not null default now()
            );
            """
        )
    )
    prompts_cols = await _get_table_columns(session, "prompts")
    await _ensure_column(session, prompts_cols, "prompts", "key", "alter table prompts add column key varchar(64);")
    await _ensure_column(session, prompts_cols, "prompts", "text", "alter table prompts add column text text;")
    await _ensure_column(session, prompts_cols, "prompts", "updated_at", "alter table prompts add column updated_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_prompts_key on prompts(key);"))
    await session.execute(text("create index if not exists ix_prompts_updated_at on prompts(updated_at);"))

    # packs
    await session.execute(
        text(
            """
            create table if not exists packs (
              id serial primary key,
              key varchar not null,
              title varchar not null,
              description text not null default '',
              tier varchar(32) not null default 'free',
              prompt_id integer null references prompts(id) on delete set null,
              schedule_time varchar(8) null,
              is_active boolean not null default true,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    packs_cols = await _get_table_columns(session, "packs")
    await _ensure_column(session, packs_cols, "packs", "key", "alter table packs add column key varchar;")
    await _ensure_column(session, packs_cols, "packs", "title", "alter table packs add column title varchar;")
    await _ensure_column(session, packs_cols, "packs", "description", "alter table packs add column description text;")
    await _ensure_column(session, packs_cols, "packs", "tier", "alter table packs add column tier varchar(32);")
    await _ensure_column(session, packs_cols, "packs", "prompt_id", "alter table packs add column prompt_id integer;")
    await _ensure_column(session, packs_cols, "packs", "schedule_time", "alter table packs add column schedule_time varchar(8);")
    await _ensure_column(session, packs_cols, "packs", "is_active", "alter table packs add column is_active boolean;")
    await _ensure_column(session, packs_cols, "packs", "created_at", "alter table packs add column created_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_packs_key on packs(key);"))
    await session.execute(text("create index if not exists ix_packs_tier on packs(tier);"))
    await session.execute(text("create index if not exists ix_packs_is_active on packs(is_active);"))

    # channels
    await session.execute(
        text(
            """
            create table if not exists channels (
              id serial primary key,
              tg_channel_id bigint not null,
              username varchar null,
              title varchar null,
              is_public boolean not null default false,
              is_active boolean not null default true,
              added_by integer null references users(id) on delete set null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    channels_cols = await _get_table_columns(session, "channels")
    await _ensure_column(session, channels_cols, "channels", "tg_channel_id", "alter table channels add column tg_channel_id bigint;")
    await _ensure_column(session, channels_cols, "channels", "username", "alter table channels add column username varchar;")
    await _ensure_column(session, channels_cols, "channels", "title", "alter table channels add column title varchar;")
    await _ensure_column(session, channels_cols, "channels", "is_public", "alter table channels add column is_public boolean;")
    await _ensure_column(session, channels_cols, "channels", "is_active", "alter table channels add column is_active boolean;")
    await _ensure_column(session, channels_cols, "channels", "added_by", "alter table channels add column added_by integer;")
    await _ensure_column(session, channels_cols, "channels", "created_at", "alter table channels add column created_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_channels_tg_channel_id on channels(tg_channel_id);"))
    await session.execute(text("create index if not exists ix_channels_is_public on channels(is_public);"))
    await session.execute(text("create index if not exists ix_channels_is_active on channels(is_active);"))

    # pack_channels
    await session.execute(
        text(
            """
            create table if not exists pack_channels (
              id serial primary key,
              pack_id integer not null references packs(id) on delete cascade,
              channel_id integer not null references channels(id) on delete cascade,
              created_at timestamptz not null default now(),
              unique (pack_id, channel_id)
            );
            """
        )
    )
    pack_channels_cols = await _get_table_columns(session, "pack_channels")
    await _ensure_column(session, pack_channels_cols, "pack_channels", "pack_id", "alter table pack_channels add column pack_id integer;")
    await _ensure_column(session, pack_channels_cols, "pack_channels", "channel_id", "alter table pack_channels add column channel_id integer;")
    await _ensure_column(session, pack_channels_cols, "pack_channels", "created_at", "alter table pack_channels add column created_at timestamptz;")
    await session.execute(text("create index if not exists ix_pack_channels_pack_id on pack_channels(pack_id);"))
    await session.execute(text("create index if not exists ix_pack_channels_channel_id on pack_channels(channel_id);"))

    # user_packs
    await session.execute(
        text(
            """
            create table if not exists user_packs (
              id serial primary key,
              user_id integer not null references users(id) on delete cascade,
              pack_id integer not null references packs(id) on delete cascade,
              is_enabled boolean not null default true,
              created_at timestamptz not null default now(),
              unique (user_id, pack_id)
            );
            """
        )
    )
    user_packs_cols = await _get_table_columns(session, "user_packs")
    await _ensure_column(session, user_packs_cols, "user_packs", "user_id", "alter table user_packs add column user_id integer;")
    await _ensure_column(session, user_packs_cols, "user_packs", "pack_id", "alter table user_packs add column pack_id integer;")
    await _ensure_column(session, user_packs_cols, "user_packs", "is_enabled", "alter table user_packs add column is_enabled boolean;")
    await _ensure_column(session, user_packs_cols, "user_packs", "created_at", "alter table user_packs add column created_at timestamptz;")
    await session.execute(text("create index if not exists ix_user_packs_user_id on user_packs(user_id);"))
    await session.execute(text("create index if not exists ix_user_packs_pack_id on user_packs(pack_id);"))

    # posts_cache
    await session.execute(
        text(
            """
            create table if not exists posts_cache (
              id serial primary key,
              channel_id integer not null references channels(id) on delete cascade,
              message_id_int bigint not null,
              message_date timestamptz null,
              message_text text null,
              created_at timestamptz not null default now(),
              unique (channel_id, message_id_int)
            );
            """
        )
    )
    posts_cache_cols = await _get_table_columns(session, "posts_cache")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "channel_id", "alter table posts_cache add column channel_id integer;")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_id_int", "alter table posts_cache add column message_id_int bigint;")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_date", "alter table posts_cache add column message_date timestamptz;")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_text", "alter table posts_cache add column message_text text;")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "created_at", "alter table posts_cache add column created_at timestamptz;")
    await session.execute(text("create index if not exists ix_posts_cache_channel_id on posts_cache(channel_id);"))
    await session.execute(text("create index if not exists ix_posts_cache_message_date on posts_cache(message_date);"))

    # deliveries
    await session.execute(
        text(
            """
            create table if not exists deliveries (
              id serial primary key,
              user_id integer not null references users(id) on delete cascade,
              pack_id integer not null references packs(id) on delete cascade,
              channel_id integer null references channels(id) on delete set null,
              post_id varchar null,
              status varchar(32) not null default 'sent',
              error text null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    
    deliveries_cols = await _get_table_columns(session, "deliveries")
    await _ensure_column(session, deliveries_cols, "deliveries", "user_id", "alter table deliveries add column user_id integer;")
    await _ensure_column(session, deliveries_cols, "deliveries", "pack_id", "alter table deliveries add column pack_id integer;")
    await _ensure_column(session, deliveries_cols, "deliveries", "channel_id", "alter table deliveries add column channel_id integer;")
    await _ensure_column(session, deliveries_cols, "deliveries", "post_id", "alter table deliveries add column post_id varchar;")
    await _ensure_column(session, deliveries_cols, "deliveries", "status", "alter table deliveries add column status varchar(32);")
    await _ensure_column(session, deliveries_cols, "deliveries", "error", "alter table deliveries add column error text;")
    await _ensure_column(session, deliveries_cols, "deliveries", "created_at", "alter table deliveries add column created_at timestamptz;")

    await session.execute(text("create index if not exists ix_deliveries_user_id on deliveries(user_id);"))
    await session.execute(text("create index if not exists ix_deliveries_pack_id on deliveries(pack_id);"))
    await session.execute(text("create index if not exists ix_deliveries_status on deliveries(status);"))

    # user_settings
    await session.execute(
        text(
            """
            create table if not exists user_settings (
              id serial primary key,
              user_id integer not null references users(id) on delete cascade,
              pause_until timestamptz null,
              format_mode varchar(16) not null default 'plain',
              menu_chat_id bigint null,
              menu_message_id bigint null,
              created_at timestamptz not null default now(),
              unique (user_id)
            );
            """
        )
    )
    user_settings_cols = await _get_table_columns(session, "user_settings")
    await _ensure_column(session, user_settings_cols, "user_settings", "pause_until", "alter table user_settings add column pause_until timestamptz;")
    await _ensure_column(session, user_settings_cols, "user_settings", "format_mode", "alter table user_settings add column format_mode varchar(16);")
    await _ensure_column(session, user_settings_cols, "user_settings", "menu_chat_id", "alter table user_settings add column menu_chat_id bigint;")
    await _ensure_column(session, user_settings_cols, "user_settings", "menu_message_id", "alter table user_settings add column menu_message_id bigint;")
    await session.execute(text("create index if not exists ix_user_settings_pause_until on user_settings(pause_until);"))

    # subscriptions
    await session.execute(
        text(
            """
            create table if not exists subscriptions (
              id serial primary key,
              user_id integer not null references users(id) on delete cascade,
              tier varchar(32) not null,
              starts_at timestamptz not null default now(),
              ends_at timestamptz null,
              source varchar(32) not null default 'manual',
              created_at timestamptz not null default now()
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_subscriptions_user_id on subscriptions(user_id);"))
    await session.execute(text("create index if not exists ix_subscriptions_tier on subscriptions(tier);"))
    
    subscriptions_cols = await _get_table_columns(session, "subscriptions")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "user_id", "alter table subscriptions add column user_id integer;")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "starts_at", "alter table subscriptions add column starts_at timestamptz;")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "ends_at", "alter table subscriptions add column ends_at timestamptz;")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "status", "alter table subscriptions add column status varchar(32);")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "created_at", "alter table subscriptions add column created_at timestamptz;")

    await session.execute(text("create index if not exists ix_subscriptions_ends_at on subscriptions(ends_at);"))

    # user_channels
    await session.execute(
        text(
            """
            create table if not exists user_channels (
              id serial primary key,
              user_id integer not null references users(id) on delete cascade,
              channel_id integer not null references channels(id) on delete cascade,
              created_at timestamptz not null default now(),
              unique (user_id, channel_id)
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_user_channels_user_id on user_channels(user_id);"))
    await session.execute(text("create index if not exists ix_user_channels_channel_id on user_channels(channel_id);"))

    # referral_balance
    await session.execute(
        text(
            """
            create table if not exists referral_balance (
              id serial primary key,
              user_id integer not null references users(id) on delete cascade,
              balance_rub integer not null default 0,
              updated_at timestamptz not null default now(),
              unique (user_id)
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_referral_balance_user_id on referral_balance(user_id);"))

    # payments_log
    await session.execute(
        text(
            """
            create table if not exists payments_log (
              id serial primary key,
              user_id integer null references users(id) on delete set null,
              provider varchar(32) not null,
              payload jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_payments_log_user_id on payments_log(user_id);"))
    await session.execute(text("create index if not exists ix_payments_log_created_at on payments_log(created_at);"))

    # reports
    await session.execute(
        text(
            """
            create table if not exists reports (
              id serial primary key,
              user_id integer null references users(id) on delete set null,
              input_hash varchar(64) not null,
              stage1_count integer not null default 0,
              stage2_model varchar(64) null,
              payload jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    reports_cols = await _get_table_columns(session, "reports")
    await _ensure_column(session, reports_cols, "reports", "input_hash", "alter table reports add column input_hash varchar(64);")
    await _ensure_column(session, reports_cols, "reports", "stage1_count", "alter table reports add column stage1_count integer;")
    await _ensure_column(session, reports_cols, "reports", "stage2_model", "alter table reports add column stage2_model varchar(64);")
    await session.execute(text("create index if not exists ix_reports_user_id on reports(user_id);"))
    await session.execute(text("create index if not exists ix_reports_input_hash on reports(input_hash);"))
    await session.execute(text("create index if not exists ix_reports_created_at on reports(created_at);"))


async def _list_tables(session: AsyncSession) -> set[str]:
    res = await session.execute(
        text(
            """
            select table_name
            from information_schema.tables
            where table_schema = current_schema()
              and table_type = 'BASE TABLE'
            """
        )
    )
    return {str(r[0]) for r in res.fetchall()}


async def check_schema(session: AsyncSession) -> dict[str, Any]:
    tables = await _list_tables(session)

    required_tables = [
        "users",
        "prompts",
        "packs",
        "channels",
        "pack_channels",
        "user_packs",
        "posts_cache",
        "deliveries",
        "user_settings",
        "subscriptions",
        "user_channels",
        "referral_balance",
        "payments_log",
        "reports",
    ]

    # Минимально необходимые колонки (без проверки индексов/constraints).
    required_cols: dict[str, list[str]] = {
        "users": ["tg_id", "username", "role", "subscription_expires_at", "referrer_tg_id", "created_at"],
        "prompts": ["key", "text", "updated_at"],
        "packs": ["key", "title", "description", "tier", "prompt_id", "schedule_time", "is_active", "created_at"],
        "channels": ["tg_channel_id", "username", "title", "is_public", "is_active", "added_by", "created_at"],
        "pack_channels": ["pack_id", "channel_id", "created_at"],
        "user_packs": ["user_id", "pack_id", "is_enabled", "created_at"],
        "posts_cache": ["channel_id", "message_id_int", "message_date"],
        "subscriptions": ["user_id", "starts_at", "ends_at", "status", "created_at"],
        "user_settings": ["pause_until", "format_mode", "menu_chat_id", "menu_message_id"],
        "reports": ["input_hash", "stage1_count", "stage2_model"],
    }

    missing_tables = [t for t in required_tables if t not in tables]
    missing_cols: dict[str, list[str]] = {}

    for t, cols in required_cols.items():
        if t in missing_tables:
            continue
        have = await _get_table_columns(session, t)
        miss = [c for c in cols if c not in have]
        if miss:
            missing_cols[t] = miss

    ok = (not missing_tables) and (not missing_cols)
    return {"ok": ok, "missing_tables": missing_tables, "missing_cols": missing_cols}


async def init_schema() -> None:
    async with session_scope() as session:
        await ensure_schema(session)
        await session.commit()


async def run_check() -> int:
    async with session_scope() as session:
        res = await check_schema(session)
        await session.commit()
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res.get("ok") else 2


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m vestnik.schema")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Create/upgrade DB schema (idempotent).")
    sub.add_parser("check", help="Check required tables/columns exist; prints JSON; exit 0 if ok else 2.")

    args = p.parse_args(argv)

    if args.cmd == "init":
        asyncio.run(init_schema())
        print("ok")
        return 0

    if args.cmd == "check":
        return asyncio.run(run_check())

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
