from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, String, DateTime, Integer, Text, Boolean, func


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[str | None] = mapped_column(String, nullable=True)
    role: Mapped[str] = mapped_column(String(32), default="guest")
    subscription_expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    referrer_tg_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Prompt(Base):
    __tablename__ = "prompts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    text: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PostCache(Base):
    __tablename__ = "posts_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    channel_ref: Mapped[str] = mapped_column(String(255), index=True)
    message_id: Mapped[str] = mapped_column(String(64), index=True)
    url: Mapped[str] = mapped_column(String(512), default="")
    text: Mapped[str] = mapped_column(Text, default="")
    parsed_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True))
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    channel_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    message_id_int: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tg_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255), default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_public: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    added_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
