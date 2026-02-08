from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


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
    await session.execute(text("alter table users add column if not exists tg_id bigint;"))
    await session.execute(text("alter table users add column if not exists username varchar;"))
    await session.execute(text("alter table users add column if not exists role varchar(32);"))
    await session.execute(text("alter table users add column if not exists subscription_expires_at timestamptz;"))
    await session.execute(text("alter table users add column if not exists referrer_tg_id bigint;"))
    await session.execute(text("alter table users add column if not exists created_at timestamptz;"))
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
    await session.execute(text("alter table prompts add column if not exists key varchar(64);"))
    await session.execute(text("alter table prompts add column if not exists text text;"))
    await session.execute(text("alter table prompts add column if not exists updated_at timestamptz;"))
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
    await session.execute(text("alter table packs add column if not exists key varchar;"))
    await session.execute(text("alter table packs add column if not exists title varchar;"))
    await session.execute(text("alter table packs add column if not exists description text;"))
    await session.execute(text("alter table packs add column if not exists tier varchar;"))
    await session.execute(text("alter table packs add column if not exists prompt_id integer;"))
    await session.execute(text("alter table packs add column if not exists schedule_time time;"))
    await session.execute(text("alter table packs add column if not exists is_active boolean;"))
    await session.execute(text("alter table packs add column if not exists created_at timestamptz;"))
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
    await session.execute(text("alter table channels add column if not exists tg_channel_id bigint;"))
    await session.execute(text("alter table channels add column if not exists username varchar;"))
    await session.execute(text("alter table channels add column if not exists title varchar;"))
    await session.execute(text("alter table channels add column if not exists is_active boolean;"))
    await session.execute(text("alter table channels add column if not exists is_public boolean;"))
    await session.execute(text("alter table channels add column if not exists added_by varchar(32);"))
    await session.execute(text("alter table channels add column if not exists created_at timestamptz;"))
    await session.execute(text("create unique index if not exists ux_channels_username on channels(username);"))
    await session.execute(text("create unique index if not exists ux_channels_tg_channel_id on channels(tg_channel_id) where tg_channel_id is not null;"))
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
    await session.execute(text("alter table pack_channels add column if not exists pack_id integer;"))
    await session.execute(text("alter table pack_channels add column if not exists channel_id integer;"))
    await session.execute(text("alter table pack_channels add column if not exists created_at timestamptz;"))
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
    await session.execute(text("alter table user_packs add column if not exists user_id integer;"))
    await session.execute(text("alter table user_packs add column if not exists pack_id integer;"))
    await session.execute(text("alter table user_packs add column if not exists is_enabled boolean;"))
    await session.execute(text("alter table user_packs add column if not exists created_at timestamptz;"))
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
    await session.execute(text("alter table posts_cache add column if not exists channel_id integer;"))
    await session.execute(text("alter table posts_cache add column if not exists message_id_int integer;"))
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
    await session.execute(text("alter table user_settings add column if not exists menu_chat_id bigint;"))
    await session.execute(text("alter table user_settings add column if not exists menu_message_id integer;"))
    await session.execute(text("alter table user_settings add column if not exists pause_until timestamptz;"))
    await session.execute(text("alter table user_settings add column if not exists format_mode varchar(16);"))
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
    await session.execute(text('''
-- --- AI cache: per-post facts (Stage 1) ---
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
);

CREATE INDEX IF NOT EXISTS idx_post_facts_updated_at ON post_facts(updated_at DESC);

-- --- AI cache: report metadata (Stage 2 reuse) ---
ALTER TABLE reports ADD COLUMN IF NOT EXISTS input_hash TEXT;
ALTER TABLE reports ADD COLUMN IF NOT EXISTS stage2_model TEXT;
ALTER TABLE reports ADD COLUMN IF NOT EXISTS stage1_count INTEGER;

CREATE INDEX IF NOT EXISTS idx_reports_pack_period ON reports(pack_key, period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_reports_input_hash ON reports(input_hash);
    '''))


    await session.commit()
