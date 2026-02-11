from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


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
              tier varchar not null default 'tier1',
              prompt_id integer null,
              schedule_time time null,
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
    await _ensure_column(session, packs_cols, "packs", "tier", "alter table packs add column tier varchar;")
    await _ensure_column(session, packs_cols, "packs", "prompt_id", "alter table packs add column prompt_id integer;")
    await _ensure_column(session, packs_cols, "packs", "schedule_time", "alter table packs add column schedule_time time;")
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
              tg_channel_id bigint null,
              username varchar not null,
              title varchar not null default '',
              is_active boolean not null default true,
              is_public boolean null,
              added_by varchar(32) null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    channels_cols = await _get_table_columns(session, "channels")
    await _ensure_column(session, channels_cols, "channels", "tg_channel_id", "alter table channels add column tg_channel_id bigint;")
    await _ensure_column(session, channels_cols, "channels", "username", "alter table channels add column username varchar;")
    await _ensure_column(session, channels_cols, "channels", "title", "alter table channels add column title varchar;")
    await _ensure_column(session, channels_cols, "channels", "is_active", "alter table channels add column is_active boolean;")
    await _ensure_column(session, channels_cols, "channels", "is_public", "alter table channels add column is_public boolean;")
    await _ensure_column(session, channels_cols, "channels", "added_by", "alter table channels add column added_by varchar(32);")
    await _ensure_column(session, channels_cols, "channels", "created_at", "alter table channels add column created_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_channels_username on channels(username);"))
    await session.execute(
        text(
            "create unique index if not exists ux_channels_tg_channel_id on channels(tg_channel_id) "
            "where tg_channel_id is not null;"
        )
    )
    await session.execute(text("create index if not exists ix_channels_is_active on channels(is_active);"))

    # pack_channels
    await session.execute(
        text(
            """
            create table if not exists pack_channels (
              id serial primary key,
              pack_id integer not null,
              channel_id integer not null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    pack_channels_cols = await _get_table_columns(session, "pack_channels")
    await _ensure_column(session, pack_channels_cols, "pack_channels", "pack_id", "alter table pack_channels add column pack_id integer;")
    await _ensure_column(session, pack_channels_cols, "pack_channels", "channel_id", "alter table pack_channels add column channel_id integer;")
    await _ensure_column(session, pack_channels_cols, "pack_channels", "created_at", "alter table pack_channels add column created_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_pack_channels_pair on pack_channels(pack_id, channel_id);"))
    await session.execute(text("create index if not exists ix_pack_channels_pack_id on pack_channels(pack_id);"))

    # user_packs
    await session.execute(
        text(
            """
            create table if not exists user_packs (
              id serial primary key,
              user_id integer not null,
              pack_id integer not null,
              is_enabled boolean not null default true,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    user_packs_cols = await _get_table_columns(session, "user_packs")
    await _ensure_column(session, user_packs_cols, "user_packs", "user_id", "alter table user_packs add column user_id integer;")
    await _ensure_column(session, user_packs_cols, "user_packs", "pack_id", "alter table user_packs add column pack_id integer;")
    await _ensure_column(session, user_packs_cols, "user_packs", "is_enabled", "alter table user_packs add column is_enabled boolean;")
    await _ensure_column(session, user_packs_cols, "user_packs", "created_at", "alter table user_packs add column created_at timestamptz;")
    await session.execute(text("create unique index if not exists ux_user_packs_pair on user_packs(user_id, pack_id);"))
    await session.execute(text("create index if not exists ix_user_packs_user_id on user_packs(user_id);"))

    # posts_cache (+ nullable columns for gradual TZ alignment)
    await session.execute(
        text(
            """
            create table if not exists posts_cache (
              id serial primary key,
              channel_ref varchar(255) not null,
              message_id varchar(64) not null,
              url varchar(512) not null default '',
              text text not null default '',
              parsed_at timestamptz not null default now(),
              expires_at timestamptz not null,
              is_deleted boolean not null default false,
              channel_id integer null,
              message_id_int integer null
            );
            """
        )
    )
    posts_cache_cols = await _get_table_columns(session, "posts_cache")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "channel_id", "alter table posts_cache add column channel_id integer;")
    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_id_int", "alter table posts_cache add column message_id_int integer;")
    await session.execute(text("create index if not exists ix_posts_cache_channel_ref on posts_cache(channel_ref);"))
    await session.execute(text("create index if not exists ix_posts_cache_expires_at on posts_cache(expires_at);"))
    await session.execute(text("create unique index if not exists ux_posts_cache_pair on posts_cache(channel_ref, message_id);"))

    # deliveries
    await session.execute(
        text(
            """
            create table if not exists deliveries (
              id serial primary key,
              user_id integer not null,
              channel_ref varchar(255) not null,
              message_id varchar(64) not null,
              sent_at timestamptz not null default now()
            );
            """
        )
    )
    await session.execute(text("create unique index if not exists ux_deliveries_pair on deliveries(user_id, channel_ref, message_id);"))
    await session.execute(text("create index if not exists ix_deliveries_user_id on deliveries(user_id);"))
    await session.execute(text("create index if not exists ix_deliveries_sent_at on deliveries(sent_at);"))

    # user_settings
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
    user_settings_cols = await _get_table_columns(session, "user_settings")
    await _ensure_column(session, user_settings_cols, "user_settings", "menu_chat_id", "alter table user_settings add column menu_chat_id bigint;")
    await _ensure_column(session, user_settings_cols, "user_settings", "menu_message_id", "alter table user_settings add column menu_message_id integer;")
    await _ensure_column(session, user_settings_cols, "user_settings", "pause_until", "alter table user_settings add column pause_until timestamptz;")
    await _ensure_column(session, user_settings_cols, "user_settings", "format_mode", "alter table user_settings add column format_mode varchar(16);")
    await session.execute(text("update user_settings set format_mode='digest' where format_mode is null;"))
    await session.execute(text("create index if not exists ix_user_settings_delivery_enabled on user_settings(delivery_enabled);"))
    await session.execute(text("create index if not exists ix_user_settings_pause_until on user_settings(pause_until);"))

    # subscriptions (создаём сейчас как контракт под этап 2)
    await session.execute(
        text(
            """
            create table if not exists subscriptions (
              id serial primary key,
              user_id integer not null,
              pack_id integer null,
              tier varchar(16) not null,
              started_at timestamptz not null default now(),
              expires_at timestamptz not null,
              payment_provider varchar null,
              auto_renew boolean not null default true
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_subscriptions_user_id on subscriptions(user_id);"))
    await session.execute(text("create index if not exists ix_subscriptions_expires_at on subscriptions(expires_at);"))

    # user_channels (PRO)
    await session.execute(
        text(
            """
            create table if not exists user_channels (
              user_id integer not null,
              channel_id integer not null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    await session.execute(text("create unique index if not exists ux_user_channels_pair on user_channels(user_id, channel_id);"))
    await session.execute(text("create index if not exists ix_user_channels_user_id on user_channels(user_id);"))

    # referral_balance (этап 2)
    await session.execute(
        text(
            """
            create table if not exists referral_balance (
              user_id integer primary key,
              total_earned numeric not null default 0,
              total_withdrawn numeric not null default 0
            );
            """
        )
    )

    # payments_log (этап 2)
    await session.execute(
        text(
            """
            create table if not exists payments_log (
              id serial primary key,
              user_id integer not null,
              amount numeric not null,
              currency varchar(16) not null,
              provider varchar null,
              status varchar(16) not null,
              created_at timestamptz not null default now()
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_payments_log_user_id on payments_log(user_id);"))
    await session.execute(text("create index if not exists ix_payments_log_created_at on payments_log(created_at);"))

    # reports (готовые отчёты; расширение ER-модели, поля не удаляем)
    await session.execute(
        text(
            """
            create table if not exists reports (
              id serial primary key,
              user_id integer not null,
              pack_id integer null,
              pack_key varchar(64) null,
              period_start timestamptz not null,
              period_end timestamptz not null,
              sources_json text not null default '',
              report_text text not null default '',
              created_at timestamptz not null default now()
            );
            """
        )
    )
    await session.execute(text("create index if not exists ix_reports_user_id on reports(user_id);"))
    await session.execute(text("create index if not exists ix_reports_created_at on reports(created_at);"))

    # --- ai cache ddl (non-destructive) ---
    await session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS post_facts (
              id SERIAL PRIMARY KEY,
              channel_ref TEXT NOT NULL,
              message_id TEXT NOT NULL,
              text_sha256 TEXT NOT NULL,
              summary TEXT NOT NULL DEFAULT '',
              url TEXT NOT NULL DEFAULT '',
              channel_name TEXT NOT NULL DEFAULT '',
              model TEXT NOT NULL DEFAULT '',
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              UNIQUE(channel_ref, message_id)
            )
            """
        )
    )
    await session.execute(text("CREATE INDEX IF NOT EXISTS idx_post_facts_updated_at ON post_facts(updated_at DESC)"))

    reports_cols = await _get_table_columns(session, "reports")
    await _ensure_column(session, reports_cols, "reports", "input_hash", "ALTER TABLE reports ADD COLUMN input_hash TEXT")
    await _ensure_column(session, reports_cols, "reports", "stage2_model", "ALTER TABLE reports ADD COLUMN stage2_model TEXT")
    await _ensure_column(session, reports_cols, "reports", "stage1_count", "ALTER TABLE reports ADD COLUMN stage1_count INTEGER")

    await session.execute(text("CREATE INDEX IF NOT EXISTS idx_reports_pack_period ON reports(pack_key, period_start, period_end)"))
    await session.execute(text("CREATE INDEX IF NOT EXISTS idx_reports_input_hash ON reports(input_hash)"))

    await session.commit()
