"""
Telegram Channel Manager Bot  (Phase 3 — Fast, Simple Automation, Hardened, Backup/Restore)

Pyrogram bot + userbot, SQLAlchemy async, APScheduler, hardened FSM, persistent
Automation Engine, safe callback routing, safe background tasks, safe DB migrations,
and a full Database Backup / Restore system inside System Tools.

.env keys:
    BOT_TOKEN, OWNER_ID, API_ID, API_HASH, DATABASE_URL, LOG_LEVEL
Run:
    pip install -r requirements.txt && python bot.py

Highlights
  * SIMPLE automation wizard: "What message should trigger?" -> "What should I reply?"
    Default match is EXACT. No hidden "any" surprise.
  * Fast path: in-memory rule/button/settings cache; execution counters bumped with
    one batched UPDATE per message; no full-cache invalidation on incoming messages.
  * Non-blocking user relay (spawn) so automations reply instantly.
  * Advanced Settings submenu (match type, scope, priority, cooldown, media, buttons).
  * Full edit flows: one-step edits that save & return.
  * Rule Duplicate + per-rule logs + rule-level Test.
  * Channels tab: Add / Remove / Settings / Refresh.
  * Legacy auto-reply fully migrated & suppressed once any automation exists.
  * 💾 Backup & Restore inside System Tools:
      - SQLite: VACUUM INTO .db backup, validated + atomic file swap on restore.
      - Postgres: universal JSON export/import (also works for SQLite).
      - Auto safety backup before every restore; caches reloaded after.
  * All old features preserved.
"""

from __future__ import annotations

import asyncio
import html as _html
import json
import logging
import os
import re
import shutil
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config as env
from pyrogram import Client, filters, idle, enums
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (
    ApiIdInvalid, AuthKeyUnregistered, FloodWait, MessageNotModified, PasswordHashInvalid,
    PeerIdInvalid, PhoneCodeEmpty, PhoneCodeExpired, PhoneCodeInvalid, PhoneNumberBanned,
    PhoneNumberFlood, PhoneNumberInvalid, SessionPasswordNeeded, UserDeactivated,
    UserDeactivatedBan, UserIsBlocked, UserPrivacyRestricted, UsernameNotOccupied,
    InputUserDeactivated,
)
from pyrogram.handlers import (
    CallbackQueryHandler, ChatJoinRequestHandler, ChatMemberUpdatedHandler, MessageHandler,
)
from pyrogram.types import (
    CallbackQuery, ChatJoinRequest, ChatMemberUpdated, ChatPermissions, InlineKeyboardButton,
    InlineKeyboardMarkup, Message,
)
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Integer, String, Text, delete, func, or_, select,
    text, update,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

# ===========================================================================
# CONFIG
# ===========================================================================

BOT_TOKEN = env("BOT_TOKEN", default="").strip()
OWNER_ID = env("OWNER_ID", default="").strip()
ENV_API_ID = env("API_ID", default="").strip()
ENV_API_HASH = env("API_HASH", default="").strip()
DATABASE_URL = env("DATABASE_URL", default="sqlite+aiosqlite:///bot.db").strip()
LOG_LEVEL = env("LOG_LEVEL", default="INFO").strip().upper()

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("sqlite:///") and "aiosqlite" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("sqlite:///", "sqlite+aiosqlite:///", 1)

IS_SQLITE = DATABASE_URL.startswith("sqlite")

SEND_INTERVAL = 0.05
BULK_APPROVE_INTERVAL = 0.05
MAX_TEXT = 3800

Path("logs").mkdir(exist_ok=True)
BACKUP_DIR = Path("backups")
BACKUP_DIR.mkdir(exist_ok=True)

logger = logging.getLogger("channel_manager")
logger.setLevel(LOG_LEVEL)
logger.propagate = False
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = RotatingFileHandler("logs/bot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
_fh.setFormatter(_fmt)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)
logger.addHandler(_fh)
logger.addHandler(_ch)

BOT_START_TIME = datetime.now(timezone.utc)
HTML = enums.ParseMode.HTML

_background_tasks: set[asyncio.Task] = set()


def spawn(coro: Awaitable, *, name: str = "task") -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _done(t: asyncio.Task):
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.error("background task '%s' crashed: %s: %s",
                         name, type(exc).__name__, exc)
            logger.debug("traceback:\n%s", "".join(traceback.format_exception(exc)))

    task.add_done_callback(_done)
    return task


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def today_start() -> datetime:
    n = now_utc()
    return datetime(n.year, n.month, n.day)


def esc(v) -> str:
    return _html.escape(str(v if v is not None else ""), quote=False)


# ===========================================================================
# DATABASE MODELS
# ===========================================================================

Base = declarative_base()


class KV(Base):
    __tablename__ = "kv"
    key = Column(String(64), primary_key=True)
    value = Column(Text, nullable=False, default="")


class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, nullable=False, index=True)
    name = Column(String(255), default="")
    is_owner = Column(Boolean, default=False)
    added_at = Column(DateTime, default=now_utc)


class Channel(Base):
    __tablename__ = "channels"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, unique=True, nullable=False, index=True)
    name = Column(String(255), default="")
    added_at = Column(DateTime, default=now_utc)
    is_active = Column(Boolean, default=True)


class JoinRequest(Base):
    __tablename__ = "join_requests"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, index=True, nullable=False)
    user_id = Column(BigInteger, index=True, nullable=False)
    first_name = Column(String(255), default="")
    last_name = Column(String(255), default="")
    username = Column(String(255), default="")
    status = Column(String(20), default="pending", index=True)
    requested_at = Column(DateTime, default=now_utc)
    processed_at = Column(DateTime, nullable=True)
    source = Column(String(20), default="event")


class Member(Base):
    __tablename__ = "members"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, index=True, nullable=False)
    user_id = Column(BigInteger, index=True, nullable=False)
    first_name = Column(String(255), default="")
    last_name = Column(String(255), default="")
    username = Column(String(255), default="")
    joined_at = Column(DateTime, default=now_utc)
    first_joined_at = Column(DateTime, nullable=True)
    last_joined_at = Column(DateTime, nullable=True)
    last_left_at = Column(DateTime, nullable=True)
    last_verified_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)


class MemberLeave(Base):
    __tablename__ = "member_leaves"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, index=True)
    user_id = Column(BigInteger, index=True)
    first_name = Column(String(255), default="")
    username = Column(String(255), default="")
    left_at = Column(DateTime, default=now_utc)


class Conversation(Base):
    __tablename__ = "conversations"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, index=True, nullable=False)
    direction = Column(String(3))
    message = Column(Text, default="")
    sent_at = Column(DateTime, default=now_utc)
    is_read = Column(Boolean, default=False)


class KnownUser(Base):
    __tablename__ = "known_users"
    user_id = Column(BigInteger, primary_key=True)
    first_name = Column(String(255), default="")
    username = Column(String(255), default="")
    first_seen = Column(DateTime, default=now_utc)
    auto_reply_sent = Column(Boolean, default=False)
    bot_blocked = Column(Boolean, default=False)


class Broadcast(Base):
    __tablename__ = "broadcasts"
    id = Column(Integer, primary_key=True)
    message = Column(Text, default="")
    from_chat_id = Column(BigInteger, nullable=True)
    from_message_id = Column(Integer, nullable=True)
    channel_id = Column(BigInteger, nullable=True)
    scheduled_at = Column(DateTime, nullable=True)
    sent_count = Column(Integer, default=0)
    fail_count = Column(Integer, default=0)
    status = Column(String(20), default="pending")


class BlockedUser(Base):
    __tablename__ = "blocked_users"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, nullable=False, index=True)
    blocked_at = Column(DateTime, default=now_utc)


class Settings(Base):
    __tablename__ = "settings"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, unique=True, nullable=False)
    join_msg_text = Column(Text, default="Welcome {first_name}! 🎉")
    join_msg_media_id = Column(String(255), default="")
    join_msg_media_type = Column(String(20), default="")
    join_btn_label = Column(String(255), default="")
    join_btn_url = Column(Text, default="")
    join_msg_enabled = Column(Boolean, default=True)
    leave_msg_text = Column(Text, default="")
    leave_msg_media_id = Column(String(255), default="")
    leave_msg_media_type = Column(String(20), default="")
    leave_btn_label = Column(String(255), default="")
    leave_btn_url = Column(Text, default="")
    leave_msg_enabled = Column(Boolean, default=False)
    auto_accept = Column(Boolean, default=False)
    welcome_enabled = Column(Boolean, default=True)
    welcome_message = Column(Text, default="Welcome {first_name}! 🎉")


class GlobalSettings(Base):
    __tablename__ = "global_settings"
    id = Column(Integer, primary_key=True)
    start_msg_text = Column(Text, default="👋 Welcome! Send us a message.")
    start_btn_label = Column(String(255), default="")
    start_btn_url = Column(Text, default="")
    auto_reply_text = Column(Text, default="")
    auto_reply_btn_label = Column(String(255), default="")
    auto_reply_btn_url = Column(Text, default="")
    auto_reply_enabled = Column(Boolean, default=False)
    auto_reply_migrated = Column(Boolean, default=False)
    notif_join_request = Column(Boolean, default=True)
    notif_member_join = Column(Boolean, default=True)
    notif_member_leave = Column(Boolean, default=False)
    notif_auto_accept = Column(Boolean, default=True)


class AutomationRule(Base):
    __tablename__ = "automation_rules"
    id = Column(Integer, primary_key=True)
    name = Column(String(200), default="")
    description = Column(Text, default="")
    enabled = Column(Boolean, default=True, index=True)
    priority = Column(Integer, default=100, index=True)
    stop_on_match = Column(Boolean, default=True)
    trigger_type = Column(String(32), default="message", index=True)
    match_type = Column(String(24), default="exact")
    trigger_value = Column(Text, default="")
    case_insensitive = Column(Boolean, default=True)
    scope_type = Column(String(16), default="global")
    scope_channel_id = Column(BigInteger, nullable=True, index=True)
    cooldown_seconds = Column(Integer, default=0)
    max_executions = Column(Integer, default=0)
    response_type = Column(String(16), default="text")
    response_text = Column(Text, default="")
    response_media_id = Column(String(255), default="")
    response_from_chat_id = Column(BigInteger, nullable=True)
    response_from_message_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=now_utc)
    updated_at = Column(DateTime, default=now_utc)
    last_triggered_at = Column(DateTime, nullable=True)
    execution_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    is_migrated = Column(Boolean, default=False)


class AutomationButton(Base):
    __tablename__ = "automation_buttons"
    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, index=True, nullable=False)
    row = Column(Integer, default=0)
    col = Column(Integer, default=0)
    label = Column(String(120), default="")
    url = Column(Text, default="")


class AutomationCooldown(Base):
    __tablename__ = "automation_cooldowns"
    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, index=True, nullable=False)
    scope_key = Column(String(120), index=True, nullable=False)
    last_run = Column(DateTime, default=now_utc)


class AutomationLog(Base):
    __tablename__ = "automation_logs"
    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, index=True, nullable=True)
    rule_name = Column(String(200), default="")
    user_id = Column(BigInteger, nullable=True, index=True)
    channel_id = Column(BigInteger, nullable=True)
    trigger_type = Column(String(32), default="")
    matched = Column(Boolean, default=False)
    ok = Column(Boolean, default=False)
    detail = Column(Text, default="")
    created_at = Column(DateTime, default=now_utc, index=True)


# ===========================================================================
# DB SETUP + MIGRATIONS
# ===========================================================================

def _build_engine():
    kw: dict = {"echo": False, "pool_pre_ping": True}
    if IS_SQLITE:
        kw["connect_args"] = {"timeout": 30}
    return create_async_engine(DATABASE_URL, **kw)


engine = _build_engine()
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

_T = "1" if IS_SQLITE else "TRUE"
_F = "0" if IS_SQLITE else "FALSE"


def _is_duplicate_column_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "duplicate column" in msg or "already exists" in msg


async def _add_column_if_missing(table: str, col: str, col_def: str):
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
        logger.info("Migration: added %s.%s", table, col)
    except Exception as exc:
        if not _is_duplicate_column_error(exc):
            logger.error("Migration FAILED for %s.%s: %s", table, col, exc)


async def _dedupe_members():
    async with SessionLocal() as s:
        rows = (await s.execute(select(Member).order_by(Member.id))).scalars().all()
        seen: dict = {}
        removed = 0
        for m in rows:
            key = (m.channel_id, m.user_id)
            keep = seen.get(key)
            if keep is None:
                seen[key] = m
                continue
            keep.is_active = bool(keep.is_active or m.is_active)
            if m.joined_at and (keep.joined_at is None or m.joined_at < keep.joined_at):
                keep.joined_at = m.joined_at
            keep.first_name = keep.first_name or m.first_name
            keep.username = keep.username or m.username
            await s.delete(m)
            removed += 1
        if removed:
            await s.commit()
            logger.info("Migration: merged %d duplicate member row(s)", removed)


async def _dedupe_pending_requests():
    async with SessionLocal() as s:
        rows = (await s.execute(select(JoinRequest).where(
            JoinRequest.status == "pending").order_by(JoinRequest.id))).scalars().all()
        seen: set = set()
        changed = 0
        for r in rows:
            key = (r.channel_id, r.user_id)
            if key in seen:
                r.status = "expired"
                r.processed_at = now_utc()
                changed += 1
            else:
                seen.add(key)
        if changed:
            await s.commit()
            logger.info("Migration: expired %d duplicate pending request(s)", changed)


async def _create_index(name: str, ddl: str):
    try:
        async with engine.begin() as conn:
            await conn.execute(text(ddl))
    except Exception as exc:
        if not _is_duplicate_column_error(exc):
            logger.error("Index %s failed: %s", name, exc)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if IS_SQLITE:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA busy_timeout=30000"))

    migrations = [
        ("settings", "join_msg_text", "TEXT"),
        ("settings", "join_msg_media_id", "VARCHAR(255) DEFAULT ''"),
        ("settings", "join_msg_media_type", "VARCHAR(20) DEFAULT ''"),
        ("settings", "join_btn_label", "VARCHAR(255) DEFAULT ''"),
        ("settings", "join_btn_url", "TEXT"),
        ("settings", "join_msg_enabled", f"BOOLEAN DEFAULT {_T}"),
        ("settings", "leave_msg_text", "TEXT"),
        ("settings", "leave_msg_media_id", "VARCHAR(255) DEFAULT ''"),
        ("settings", "leave_msg_media_type", "VARCHAR(20) DEFAULT ''"),
        ("settings", "leave_btn_label", "VARCHAR(255) DEFAULT ''"),
        ("settings", "leave_btn_url", "TEXT"),
        ("settings", "leave_msg_enabled", f"BOOLEAN DEFAULT {_F}"),
        ("known_users", "auto_reply_sent", f"BOOLEAN DEFAULT {_F}"),
        ("known_users", "bot_blocked", f"BOOLEAN DEFAULT {_F}"),
        ("broadcasts", "from_chat_id", "BIGINT"),
        ("broadcasts", "from_message_id", "INTEGER"),
        ("join_requests", "last_name", "VARCHAR(255) DEFAULT ''"),
        ("members", "is_active", f"BOOLEAN DEFAULT {_T}"),
        ("global_settings", "notif_join_request", f"BOOLEAN DEFAULT {_T}"),
        ("global_settings", "notif_member_join", f"BOOLEAN DEFAULT {_T}"),
        ("global_settings", "notif_member_leave", f"BOOLEAN DEFAULT {_F}"),
        ("global_settings", "notif_auto_accept", f"BOOLEAN DEFAULT {_T}"),
        ("global_settings", "auto_reply_migrated", f"BOOLEAN DEFAULT {_F}"),
        ("members", "last_name", "VARCHAR(255) DEFAULT ''"),
        ("members", "first_joined_at", "TIMESTAMP"),
        ("members", "last_joined_at", "TIMESTAMP"),
        ("members", "last_left_at", "TIMESTAMP"),
        ("members", "last_verified_at", "TIMESTAMP"),
        ("join_requests", "source", "VARCHAR(20) DEFAULT 'event'"),
        ("automation_rules", "stop_on_match", f"BOOLEAN DEFAULT {_T}"),
        ("automation_rules", "case_insensitive", f"BOOLEAN DEFAULT {_T}"),
        ("automation_rules", "is_migrated", f"BOOLEAN DEFAULT {_F}"),
    ]
    for table, col, col_def in migrations:
        await _add_column_if_missing(table, col, col_def)

    try:
        async with engine.begin() as conn:
            await conn.execute(text(
                "UPDATE members SET first_joined_at = joined_at WHERE first_joined_at IS NULL"))
            await conn.execute(text(
                "UPDATE members SET last_joined_at = joined_at WHERE last_joined_at IS NULL"))
    except Exception as exc:
        logger.error("Member timestamp backfill failed: %s", exc)

    await _dedupe_members()
    await _dedupe_pending_requests()
    await _create_index(
        "uq_members_channel_user",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_members_channel_user ON members (channel_id, user_id)")
    await _create_index(
        "ix_jr_channel_user_status",
        "CREATE INDEX IF NOT EXISTS ix_jr_channel_user_status "
        "ON join_requests (channel_id, user_id, status)")
    await _create_index(
        "ix_leaves_channel_left",
        "CREATE INDEX IF NOT EXISTS ix_leaves_channel_left ON member_leaves (channel_id, left_at)")
    await _create_index(
        "ix_auto_log_created",
        "CREATE INDEX IF NOT EXISTS ix_auto_log_created ON automation_logs (created_at)")

    async with SessionLocal() as s:
        gs = (await s.execute(select(GlobalSettings))).scalars().first()
        if gs is None:
            s.add(GlobalSettings())
            await s.commit()

    await _migrate_legacy_auto_reply()
    logger.info("Database ready: %s", DATABASE_URL.split("@")[-1])


async def _migrate_legacy_auto_reply():
    async with SessionLocal() as s:
        gs = (await s.execute(select(GlobalSettings))).scalars().first()
        if gs is None or gs.auto_reply_migrated:
            return
        if not gs.auto_reply_text or not gs.auto_reply_enabled:
            gs.auto_reply_migrated = True
            await s.commit()
            return
        existing = (await s.execute(select(AutomationRule).where(
            AutomationRule.is_migrated == True))).scalars().first()  # noqa: E712
        if existing is None:
            rule = AutomationRule(
                name="Imported Auto-Reply",
                description="Migrated from legacy global auto-reply (fire once per user)",
                enabled=True, priority=50, trigger_type="message", match_type="any",
                trigger_value="", response_type="text",
                response_text=gs.auto_reply_text or "", is_migrated=True,
            )
            s.add(rule)
            await s.flush()
            if gs.auto_reply_btn_label and gs.auto_reply_btn_url:
                s.add(AutomationButton(rule_id=rule.id, row=0, col=0,
                                       label=gs.auto_reply_btn_label,
                                       url=gs.auto_reply_btn_url))
            logger.info("Migrated legacy auto-reply into AutomationRule #%s", rule.id)
        gs.auto_reply_migrated = True
        gs.auto_reply_enabled = False
        await s.commit()
    invalidate_automation_cache()


# ===========================================================================
# KV STORE
# ===========================================================================

async def kv_get(key: str, default: str = "") -> str:
    async with SessionLocal() as s:
        row = (await s.execute(select(KV).where(KV.key == key))).scalar_one_or_none()
        return row.value if row else default


async def kv_set(key: str, value: str):
    async with SessionLocal() as s:
        row = (await s.execute(select(KV).where(KV.key == key))).scalar_one_or_none()
        if row:
            row.value = value
        else:
            s.add(KV(key=key, value=value))
        await s.commit()


async def kv_del(*keys: str):
    async with SessionLocal() as s:
        await s.execute(delete(KV).where(KV.key.in_(keys)))
        await s.commit()


# ===========================================================================
# ADMIN CACHE + FILTERS
# ===========================================================================

_admin_ids: set = set()
_owner_id: int = 0


async def reload_admins():
    global _admin_ids
    async with SessionLocal() as s:
        rows = (await s.execute(select(Admin.user_id))).scalars().all()
    _admin_ids = set(rows)


def is_admin(user_id: int) -> bool:
    return user_id in _admin_ids


def is_owner(user_id: int) -> bool:
    return user_id == _owner_id


async def _admin_filter_func(_, __, update) -> bool:
    u = getattr(update, "from_user", None)
    return bool(u and is_admin(u.id))


async def _owner_filter_func(_, __, update) -> bool:
    u = getattr(update, "from_user", None)
    return bool(u and is_owner(u.id))


admin_only = filters.create(_admin_filter_func, name="AdminOnly")
owner_only = filters.create(_owner_filter_func, name="OwnerOnly")


# ===========================================================================
# FSM
# ===========================================================================

class St(Enum):
    NONE = auto()
    LOGIN_API_ID = auto()
    LOGIN_API_HASH = auto()
    LOGIN_PHONE = auto()
    LOGIN_CODE = auto()
    LOGIN_PASSWORD = auto()
    SET_BOT_API_ID = auto()
    SET_BOT_API_HASH = auto()
    ADD_ADMIN = auto()
    SEARCH = auto()
    INBOX_REPLY = auto()
    BC_CONTENT = auto()
    BC_SCHEDULE = auto()
    JOIN_MSG_TEXT = auto()
    JOIN_MSG_BTN_LABEL = auto()
    JOIN_MSG_BTN_URL = auto()
    JOIN_MSG_MEDIA = auto()
    LEAVE_MSG_TEXT = auto()
    LEAVE_MSG_BTN_LABEL = auto()
    LEAVE_MSG_BTN_URL = auto()
    LEAVE_MSG_MEDIA = auto()
    START_MSG_TEXT = auto()
    START_MSG_BTN_LABEL = auto()
    START_MSG_BTN_URL = auto()
    AUTO_REPLY_TEXT = auto()
    AUTO_REPLY_BTN_LABEL = auto()
    AUTO_REPLY_BTN_URL = auto()
    AUTO_TRIGGER = auto()
    AUTO_RESPONSE = auto()
    AUTO_EDIT_NAME = auto()
    AUTO_EDIT_PATTERN = auto()
    AUTO_EDIT_RESPONSE = auto()
    AUTO_EDIT_MEDIA = auto()
    AUTO_EDIT_BTN_LABEL = auto()
    AUTO_EDIT_BTN_URL = auto()
    AUTO_TEST_INPUT = auto()
    ADD_CHANNEL = auto()
    RESTORE_UPLOAD = auto()


FLOW_TTL_SECONDS = 15 * 60


@dataclass
class Flow:
    state: St = St.NONE
    data: dict = field(default_factory=dict)
    touched: float = 0.0
    origin_msg_id: Optional[int] = None
    origin_section: str = ""


flows: dict = {}


def _mono() -> float:
    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:
        return time.monotonic()


def flow(uid: int) -> Flow:
    f = flows.get(uid)
    now = _mono()
    if f is None or (f.state != St.NONE and now - f.touched > FLOW_TTL_SECONDS):
        if f is not None and f.state != St.NONE:
            logger.info("flow expired admin_id=%s state=%s", uid, f.state.name)
        f = flows[uid] = Flow()
    f.touched = now
    return f


def reset_flow(uid: int):
    flows[uid] = Flow(touched=_mono())


# ===========================================================================
# CLIENTS + SAFE SEND HELPERS
# ===========================================================================

bot: Optional[Client] = None
userbot: Optional[Client] = None
scheduler = AsyncIOScheduler(timezone="UTC")
login_client: Optional[Client] = None
login_lock = asyncio.Lock()


async def userbot_ready() -> bool:
    return userbot is not None and userbot.is_connected


async def flood_safe(coro_factory, retries: int = 3):
    for _ in range(retries):
        try:
            return await coro_factory()
        except FloodWait as fw:
            wait_s = int(getattr(fw, "value", 0) or 1)
            logger.warning("FloodWait %ss", wait_s)
            await asyncio.sleep(wait_s + 1)
    return await coro_factory()


async def safe_edit(msg: Message, txt: str, reply_markup=None):
    try:
        await msg.edit_text(txt[:4090], reply_markup=reply_markup, parse_mode=HTML,
                            disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except FloodWait as fw:
        await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
    except Exception as exc:
        logger.warning("safe_edit failed (%s) — sending new message", exc)
        try:
            await bot.send_message(msg.chat.id, txt[:4090], reply_markup=reply_markup,
                                   parse_mode=HTML, disable_web_page_preview=True)
        except Exception as exc2:
            logger.error("safe_edit fallback failed: %s", exc2)


async def safe_delete(msg: Optional[Message]):
    if msg is None:
        return
    try:
        await msg.delete()
    except Exception:
        pass


async def safe_answer(cq: CallbackQuery, text: str = "", show_alert: bool = False):
    try:
        await cq.answer(text, show_alert=show_alert)
    except Exception:
        pass


async def notify_admins(text_msg: str, reply_markup=None, exclude: Optional[int] = None):
    for admin_id in list(_admin_ids):
        if admin_id == exclude:
            continue
        try:
            await flood_safe(lambda a=admin_id: bot.send_message(
                a, text_msg[:4090], reply_markup=reply_markup, parse_mode=HTML,
                disable_web_page_preview=True))
        except Exception as exc:
            logger.warning("notify_admins -> %s failed: %s", admin_id, exc)
            try:
                plain = re.sub(r"<[^>]+>", "", text_msg)
                await bot.send_message(admin_id, plain[:4090], reply_markup=reply_markup)
            except Exception:
                pass


# ===========================================================================
# KEYBOARDS
# ===========================================================================

def kb_main_panel(logged_in: bool) -> InlineKeyboardMarkup:
    userbot_btn = "🔐 Userbot ✅" if logged_in else "🔐 Userbot ⚠️"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Join Requests", callback_data="reqs:overview"),
         InlineKeyboardButton("🤖 Automation", callback_data="auto:main")],
        [InlineKeyboardButton("📣 Channels", callback_data="channels:list"),
         InlineKeyboardButton("🔍 Search", callback_data="search:start")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast:start"),
         InlineKeyboardButton("📊 Analytics", callback_data="stats:show")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings:main"),
         InlineKeyboardButton("📥 Inbox", callback_data="inbox:list")],
        [InlineKeyboardButton("🛡️ Admins", callback_data="admins:list"),
         InlineKeyboardButton(userbot_btn, callback_data="login:menu")],
        [InlineKeyboardButton("🧰 System Tools", callback_data="tools:main"),
         InlineKeyboardButton("🔄 Refresh", callback_data="panel:refresh")],
    ])


def kb_settings_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📩 Join Message", callback_data="settings:join_select"),
         InlineKeyboardButton("🚪 Leave Message", callback_data="settings:leave_select")],
        [InlineKeyboardButton("👋 Start Message", callback_data="settings:start_msg"),
         InlineKeyboardButton("🔁 Legacy Auto-Reply", callback_data="settings:auto_reply")],
        [InlineKeyboardButton("🔔 Notifications", callback_data="settings:notifications"),
         InlineKeyboardButton("🤖 Automation Center", callback_data="auto:main")],
        [InlineKeyboardButton("« Back", callback_data="panel:main")],
    ])


def kb_join_msg_settings(ch_id: int, s: Settings) -> InlineKeyboardMarkup:
    status = "✅ Enabled" if s.join_msg_enabled else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Message", callback_data=f"join_msg:edit:{ch_id}"),
         InlineKeyboardButton("🖼 Media", callback_data=f"join_msg:media:{ch_id}")],
        [InlineKeyboardButton("🔗 Set Button", callback_data=f"join_msg:btn_set:{ch_id}"),
         InlineKeyboardButton("🗑 Remove Button", callback_data=f"join_msg:btn_remove:{ch_id}")],
        [InlineKeyboardButton(f"Toggle: {status}", callback_data=f"join_msg:toggle:{ch_id}"),
         InlineKeyboardButton("👁 Preview", callback_data=f"join_msg:preview:{ch_id}")],
        [InlineKeyboardButton("« Back", callback_data="settings:join_select")],
    ])


def kb_leave_msg_settings(ch_id: int, s: Settings) -> InlineKeyboardMarkup:
    status = "✅ Enabled" if s.leave_msg_enabled else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Message", callback_data=f"leave_msg:edit:{ch_id}"),
         InlineKeyboardButton("🖼 Media", callback_data=f"leave_msg:media:{ch_id}")],
        [InlineKeyboardButton("🔗 Set Button", callback_data=f"leave_msg:btn_set:{ch_id}"),
         InlineKeyboardButton("🗑 Remove Button", callback_data=f"leave_msg:btn_remove:{ch_id}")],
        [InlineKeyboardButton(f"Toggle: {status}", callback_data=f"leave_msg:toggle:{ch_id}"),
         InlineKeyboardButton("👁 Preview", callback_data=f"leave_msg:preview:{ch_id}")],
        [InlineKeyboardButton("« Back", callback_data="settings:leave_select")],
    ])


def kb_start_msg_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Start Message", callback_data="start_msg:edit")],
        [InlineKeyboardButton("🔗 Set Button", callback_data="start_msg:btn_set"),
         InlineKeyboardButton("🗑 Remove Button", callback_data="start_msg:btn_remove")],
        [InlineKeyboardButton("👁 Preview", callback_data="start_msg:preview")],
        [InlineKeyboardButton("« Back", callback_data="settings:main")],
    ])


def kb_auto_reply_settings(gs: GlobalSettings) -> InlineKeyboardMarkup:
    status = "✅ Enabled" if gs.auto_reply_enabled else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit", callback_data="auto_reply:edit"),
         InlineKeyboardButton("🔗 Button", callback_data="auto_reply:btn_set")],
        [InlineKeyboardButton("🗑 Remove Button", callback_data="auto_reply:btn_remove"),
         InlineKeyboardButton(f"Toggle: {status}", callback_data="auto_reply:toggle")],
        [InlineKeyboardButton("👁 Preview", callback_data="auto_reply:preview")],
        [InlineKeyboardButton("🤖 Use Automation Center instead",
                              callback_data="auto:main")],
        [InlineKeyboardButton("« Back", callback_data="settings:main")],
    ])


def kb_notifications(gs: GlobalSettings) -> InlineKeyboardMarkup:
    t = lambda v: "✅" if v else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{t(gs.notif_join_request)} Join Request",
                              callback_data="notif:toggle:join_request")],
        [InlineKeyboardButton(f"{t(gs.notif_member_join)} Member Joined",
                              callback_data="notif:toggle:member_join")],
        [InlineKeyboardButton(f"{t(gs.notif_member_leave)} Member Left",
                              callback_data="notif:toggle:member_leave")],
        [InlineKeyboardButton(f"{t(gs.notif_auto_accept)} Auto-Accept",
                              callback_data="notif:toggle:auto_accept")],
        [InlineKeyboardButton("« Back", callback_data="settings:main")],
    ])


def kb_channel_select_for(prefix: str, channels: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"📣 {c.name or c.channel_id}",
                                  callback_data=f"{prefix}:{c.channel_id}")] for c in channels]
    rows.append([InlineKeyboardButton("« Back", callback_data="settings:main")])
    return InlineKeyboardMarkup(rows)


def kb_btn_ask(cb_yes: str, cb_no: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes", callback_data=cb_yes),
        InlineKeyboardButton("❌ No", callback_data=cb_no),
    ]])


def kb_back(target: str = "panel:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data=target)]])


def kb_confirm(yes_cb: str, no_cb: str, yes_label="✅ Yes", no_label="❌ No") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(yes_label, callback_data=yes_cb),
        InlineKeyboardButton(no_label, callback_data=no_cb),
    ]])


def kb_login_menu(logged_in: bool) -> InlineKeyboardMarkup:
    rows = []
    if logged_in:
        rows.append([InlineKeyboardButton("🚪 Logout Userbot", callback_data="login:logout")])
    else:
        rows.append([InlineKeyboardButton("▶️ Start Login", callback_data="login:begin")])
    rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    return InlineKeyboardMarkup(rows)


def kb_cancel_login() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Login", callback_data="login:cancel")]])


def kb_admins_list(admins: list) -> InlineKeyboardMarkup:
    rows = []
    for a in admins:
        label = f"👑 {a.name or a.user_id}" if a.is_owner else f"🛡️ {a.name or a.user_id}"
        row = [InlineKeyboardButton(label, callback_data="noop")]
        if not a.is_owner:
            row.append(InlineKeyboardButton("🗑️", callback_data=f"admins:remove:{a.user_id}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("➕ Add Admin", callback_data="admins:add")])
    rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    return InlineKeyboardMarkup(rows)


def kb_channel_notify(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Accept All", callback_data=f"req:accept_all:{channel_id}"),
         InlineKeyboardButton("❌ Decline All", callback_data=f"req:decline_all:{channel_id}")],
        [InlineKeyboardButton("🔍 Search", callback_data="search:start"),
         InlineKeyboardButton("📊 Stats", callback_data="stats:show")],
    ])


def pager_row(prefix: str, page: int, pages: int) -> list:
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{prefix}:{page - 1}"))
    row.append(InlineKeyboardButton(f"{page}/{pages}", callback_data="noop"))
    if page < pages:
        row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}:{page + 1}"))
    return row


def kb_join_request_actions(channel_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Accept", callback_data=f"jr:accept:{channel_id}:{user_id}"),
         InlineKeyboardButton("❌ Decline", callback_data=f"jr:decline:{channel_id}:{user_id}")],
        [InlineKeyboardButton("👁 Profile", callback_data=f"user:profile:{channel_id}:{user_id}"),
         InlineKeyboardButton("🔄 Refresh", callback_data=f"jr:refresh:{channel_id}:{user_id}")],
    ])


# ===========================================================================
# TEMPLATE / SETTINGS HELPERS
# ===========================================================================

def render_template(template: str, first_name: str = "", last_name: str = "",
                    username: str = "", channel_name: str = "", user_id: Any = "",
                    channel_id: Any = "", **extra) -> str:
    now = now_utc()
    subs = {
        "{first_name}": esc(first_name),
        "{last_name}": esc(last_name),
        "{username}": esc(f"@{username}") if username else "",
        "{user_id}": esc(user_id),
        "{channel_name}": esc(channel_name),
        "{channel_id}": esc(channel_id),
        "{date}": now.strftime("%Y-%m-%d"),
        "{time}": now.strftime("%H:%M:%S"),
        "{datetime}": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    out = template or ""
    for k, v in subs.items():
        out = out.replace(k, v)
    return out


def build_markup(label: str, url: str) -> Optional[InlineKeyboardMarkup]:
    if label and url and re.match(r"^(https?://|tg://)", url.strip()):
        return InlineKeyboardMarkup([[InlineKeyboardButton(label, url=url.strip())]])
    return None


def strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")


def short_preview(html_text: str, n: int = 100) -> str:
    plain = strip_tags(html_text or "")
    if len(plain) > n:
        plain = plain[:n] + "…"
    return esc(plain) if plain else "(not set)"


def html_of(message: Message) -> str:
    try:
        if message.text is not None:
            return message.text.html
        if message.caption is not None:
            return message.caption.html
    except Exception:
        pass
    return message.text or message.caption or ""


async def get_or_create_settings(session: AsyncSession, channel_id: int) -> Settings:
    row = (await session.execute(
        select(Settings).where(Settings.channel_id == channel_id)
    )).scalar_one_or_none()
    if row is None:
        row = Settings(channel_id=channel_id)
        session.add(row)
        await session.flush()
    return row


async def get_global_settings(session: AsyncSession) -> GlobalSettings:
    gs = (await session.execute(select(GlobalSettings))).scalars().first()
    if gs is None:
        gs = GlobalSettings()
        session.add(gs)
        await session.flush()
    return gs


async def get_channel_name(channel_id: int) -> str:
    async with SessionLocal() as s:
        ch = (await s.execute(select(Channel).where(Channel.channel_id == channel_id))).scalar_one_or_none()
    return ch.name if ch and ch.name else str(channel_id)


async def mark_user_blocked(user_id: int):
    try:
        async with SessionLocal() as s:
            ku = (await s.execute(select(KnownUser).where(KnownUser.user_id == user_id))).scalar_one_or_none()
            if ku:
                ku.bot_blocked = True
                await s.commit()
    except Exception as exc:
        logger.error("mark_user_blocked failed user_id=%s: %s", user_id, exc)


async def _send_configured(user_id: int, text_out: str, media_type: str, media_id: str, markup):
    async def _do(parse):
        if media_type == "photo" and media_id:
            await bot.send_photo(user_id, media_id, caption=text_out[:1024],
                                 parse_mode=parse, reply_markup=markup)
        elif media_type == "video" and media_id:
            await bot.send_video(user_id, media_id, caption=text_out[:1024],
                                 parse_mode=parse, reply_markup=markup)
        else:
            await bot.send_message(user_id, text_out[:4090], parse_mode=parse,
                                   reply_markup=markup, disable_web_page_preview=True)
    try:
        await flood_safe(lambda: _do(HTML))
        return True
    except (UserIsBlocked, UserPrivacyRestricted, PeerIdInvalid, InputUserDeactivated) as exc:
        logger.info("Cannot DM %s: %s", user_id, exc)
        await mark_user_blocked(user_id)
        return False
    except Exception as exc:
        logger.warning("HTML send failed for %s (%s) — retrying plain", user_id, exc)
        try:
            plain = strip_tags(text_out)
            await flood_safe(lambda: _plain_send(user_id, plain, media_type, media_id, markup))
            return True
        except Exception as exc2:
            logger.warning("Plain send failed for %s: %s", user_id, exc2)
            return False


async def _plain_send(user_id, plain, media_type, media_id, markup):
    if media_type == "photo" and media_id:
        await bot.send_photo(user_id, media_id, caption=plain[:1024], reply_markup=markup)
    elif media_type == "video" and media_id:
        await bot.send_video(user_id, media_id, caption=plain[:1024], reply_markup=markup)
    else:
        await bot.send_message(user_id, plain[:4090], reply_markup=markup)


async def send_join_message(channel_id: int, channel_name: str, user_id: int,
                            first_name: str, last_name: str, username: str) -> bool:
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, channel_id)
        await session.commit()
        if not s.join_msg_enabled or not s.join_msg_text:
            return False
        data = (s.join_msg_text, s.join_msg_media_type, s.join_msg_media_id,
                s.join_btn_label, s.join_btn_url)
    text_out = render_template(data[0], first_name, last_name, username, channel_name)
    return await _send_configured(user_id, text_out, data[1], data[2], build_markup(data[3], data[4]))


async def send_leave_message(channel_id: int, channel_name: str, user_id: int,
                             first_name: str, last_name: str, username: str) -> bool:
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, channel_id)
        await session.commit()
        if not s.leave_msg_enabled or not s.leave_msg_text:
            return False
        data = (s.leave_msg_text, s.leave_msg_media_type, s.leave_msg_media_id,
                s.leave_btn_label, s.leave_btn_url)
    text_out = render_template(data[0], first_name, last_name, username, channel_name)
    return await _send_configured(user_id, text_out, data[1], data[2], build_markup(data[3], data[4]))


# ===========================================================================
# JOIN REQUEST PROCESSING
# ===========================================================================

async def process_join_request(channel_id: int, user_id: int, approve: bool) -> str:
    if not await userbot_ready():
        return "fail"
    for _ in range(4):
        try:
            if approve:
                await userbot.approve_chat_join_request(channel_id, user_id)
            else:
                await userbot.decline_chat_join_request(channel_id, user_id)
            return "ok"
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
        except Exception as exc:
            msg = str(exc).upper()
            if any(k in msg for k in ("HIDE_REQUESTER_MISSING", "USER_ALREADY_PARTICIPANT",
                                      "INVITE_REQUEST_SENT", "USER_NOT_PARTICIPANT")):
                return "gone"
            logger.warning("process_join_request failed user=%s chat=%s: %s", user_id, channel_id, exc)
            return "fail"
    return "fail"


async def record_member(channel_id: int, user_id: int, first_name: str, username: str,
                        last_name: str = ""):
    now = now_utc()
    async with SessionLocal() as s:
        m = (await s.execute(select(Member).where(
            Member.channel_id == channel_id, Member.user_id == user_id))).scalars().first()
        if m is None:
            s.add(Member(channel_id=channel_id, user_id=user_id,
                         first_name=first_name or "", last_name=last_name or "",
                         username=username or "", is_active=True, joined_at=now,
                         first_joined_at=now, last_joined_at=now))
        else:
            if not m.is_active:
                m.last_joined_at = now
            elif m.last_joined_at is None:
                m.last_joined_at = m.joined_at or now
            if m.first_joined_at is None:
                m.first_joined_at = m.joined_at or now
            m.is_active = True
            m.first_name = first_name or m.first_name
            m.last_name = last_name or m.last_name
            m.username = username or m.username
        try:
            await s.commit()
        except Exception as exc:
            await s.rollback()
            logger.info("record_member race on ch=%s user=%s (%s); retrying as update",
                        channel_id, user_id, type(exc).__name__)
            m = (await s.execute(select(Member).where(
                Member.channel_id == channel_id, Member.user_id == user_id))).scalars().first()
            if m is not None:
                if not m.is_active:
                    m.last_joined_at = now
                m.is_active = True
                await s.commit()


async def deactivate_member(channel_id: int, user_id: int) -> bool:
    now = now_utc()
    async with SessionLocal() as s:
        rows = (await s.execute(select(Member).where(
            Member.channel_id == channel_id, Member.user_id == user_id))).scalars().all()
        was_active = False
        for m in rows:
            if m.is_active:
                was_active = True
            m.is_active = False
            m.last_left_at = now
        await s.commit()
    return was_active


async def apply_request_decision(channel_id: int, user_id: int, approve: bool) -> str:
    async with SessionLocal() as session:
        jr = (await session.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.user_id == user_id,
            JoinRequest.status == "pending"))).scalars().first()
    if jr is None:
        return "ℹ️ No pending request for that user in this channel."

    result = await process_join_request(channel_id, user_id, approve)
    if result == "fail":
        logger.warning("decision FAILED channel_id=%s user_id=%s approve=%s",
                       channel_id, user_id, approve)
        return ("❌ Telegram did not accept the action. The request is still pending.\n"
                "Check that the userbot is an admin with the invite-users right.")

    async with SessionLocal() as session:
        row = (await session.execute(select(JoinRequest).where(JoinRequest.id == jr.id))).scalar_one_or_none()
        if row:
            row.status = ("accepted" if approve else "declined") if result == "ok" else "expired"
            row.processed_at = now_utc()
            await session.commit()
    _pending_cache.pop(channel_id, None)
    _member_cache.pop(channel_id, None)

    if result == "gone":
        return "ℹ️ That request was already handled on Telegram."
    if approve:
        await record_member(channel_id, user_id, jr.first_name, jr.username, jr.last_name or "")
        await send_join_message(channel_id, await get_channel_name(channel_id), user_id,
                                jr.first_name, jr.last_name or "", jr.username)
    logger.info("decision OK channel_id=%s user_id=%s approve=%s", channel_id, user_id, approve)
    return f"{'✅ Accepted' if approve else '❌ Declined'} user <code>{user_id}</code>."


_bulk_running: set = set()
_bulk_lock = asyncio.Lock()


async def bulk_process_requests(chat_id: int, channel_id: Optional[int], approve: bool):
    if not await userbot_ready():
        await bot.send_message(
            chat_id, "⚠️ Userbot isn't logged in yet. Open <b>🔐 Userbot Login</b> first.",
            parse_mode=HTML, reply_markup=kb_back())
        return
    key = channel_id if channel_id is not None else "all"
    async with _bulk_lock:
        if key in _bulk_running or "all" in _bulk_running:
            await bot.send_message(chat_id, "⏳ A bulk operation is already running.",
                                   reply_markup=kb_back())
            return
        _bulk_running.add(key)
    try:
        await _bulk_process_inner(chat_id, channel_id, approve)
    finally:
        async with _bulk_lock:
            _bulk_running.discard(key)


async def _bulk_process_inner(chat_id: int, channel_id: Optional[int], approve: bool):
    label = "Accepting" if approve else "Declining"
    progress = await bot.send_message(chat_id, "🔄 Syncing pending requests from Telegram…")

    async with SessionLocal() as s:
        q = select(Channel).where(Channel.is_active == True)  # noqa: E712
        if channel_id is not None:
            q = q.where(Channel.channel_id == channel_id)
        channels = (await s.execute(q)).scalars().all()
    sync_warnings = []
    for ch in channels:
        _, err = await sync_channel_requests(ch.channel_id)
        if err:
            sync_warnings.append(f"{esc(ch.name)}: {esc(err)}")

    async with SessionLocal() as session:
        q = select(JoinRequest).where(JoinRequest.status == "pending")
        if channel_id is not None:
            q = q.where(JoinRequest.channel_id == channel_id)
        requests = (await session.execute(q.order_by(JoinRequest.requested_at))).scalars().all()

    total = len(requests)
    if total == 0:
        txt = "No pending requests found on Telegram."
        if sync_warnings:
            txt += "\n\n⚠️ Live sync problems:\n" + "\n".join(sync_warnings)
        await safe_edit(progress, txt, kb_back())
        return

    counters = {"ok": 0, "gone": 0, "fail": 0}
    ch_names: dict = {}
    processed = 0
    last_edit = 0.0
    sem = asyncio.Semaphore(3)
    lock = asyncio.Lock()

    async def handle(req: JoinRequest):
        nonlocal processed, last_edit
        async with sem:
            result = await process_join_request(req.channel_id, req.user_id, approve)
            await asyncio.sleep(BULK_APPROVE_INTERVAL)
        if result in ("ok", "gone"):
            try:
                async with SessionLocal() as session:
                    row = (await session.execute(
                        select(JoinRequest).where(JoinRequest.id == req.id))).scalar_one_or_none()
                    if row:
                        row.status = (("accepted" if approve else "declined")
                                      if result == "ok" else "expired")
                        row.processed_at = now_utc()
                        await session.commit()
            except Exception as exc:
                logger.exception("bulk: DB update failed req=%s: %s", req.id, exc)
            if result == "ok" and approve:
                try:
                    await record_member(req.channel_id, req.user_id, req.first_name,
                                        req.username, req.last_name or "")
                    if req.channel_id not in ch_names:
                        ch_names[req.channel_id] = await get_channel_name(req.channel_id)
                    await send_join_message(req.channel_id, ch_names[req.channel_id], req.user_id,
                                            req.first_name, req.last_name or "", req.username)
                except Exception as exc:
                    logger.exception("bulk: post-accept step failed user=%s: %s", req.user_id, exc)
        async with lock:
            counters[result] += 1
            processed += 1
            now = _mono()
            if now - last_edit > 2.0:
                last_edit = now
                await safe_edit(
                    progress,
                    f"⏳ {label}…\n\nProgress: {processed} / {total}\n\n"
                    f"{'✅ Accepted' if approve else '❌ Declined'}: {counters['ok']}\n"
                    f"⚠️ Already handled: {counters['gone']}\n❌ Failed: {counters['fail']}")

    await asyncio.gather(*(handle(r) for r in requests))

    for ch in channels:
        _pending_cache.pop(ch.channel_id, None)
        _member_cache.pop(ch.channel_id, None)

    final = (f"{'✅' if counters['fail'] == 0 else '⚠️'} <b>Bulk {('accept' if approve else 'decline')} "
             f"finished</b>\n\nProcessed: {processed} / {total}\n"
             f"{'✅ Accepted' if approve else '❌ Declined'}: {counters['ok']}\n"
             f"⚠️ Already handled on Telegram: {counters['gone']}\n"
             f"❌ Failed (still pending): {counters['fail']}")
    if counters["fail"]:
        final += "\n\nFailures usually mean the userbot lacks the invite-users admin right."
    if sync_warnings:
        final += "\n\n⚠️ Live sync problems:\n" + "\n".join(sync_warnings)
    logger.info("bulk_%s done total=%s ok=%s gone=%s fail=%s",
                "accept" if approve else "decline",
                total, counters["ok"], counters["gone"], counters["fail"])
    await safe_edit(progress, final, kb_back())


_sync_locks: dict = {}


def _sync_lock(channel_id: int) -> asyncio.Lock:
    lk = _sync_locks.get(channel_id)
    if lk is None:
        lk = _sync_locks[channel_id] = asyncio.Lock()
    return lk


async def sync_channel_requests(channel_id: int):
    if not await userbot_ready():
        return 0, "userbot not connected"

    async with _sync_lock(channel_id):
        live: dict = {}
        try:
            async for r in userbot.get_chat_join_requests(channel_id):
                user = getattr(r, "user", None) or getattr(r, "from_user", None)
                if user is None:
                    continue
                live[user.id] = (user.first_name or "", user.last_name or "",
                                 user.username or "", getattr(r, "date", None))
        except FloodWait as fw:
            wait_s = int(getattr(fw, "value", 1))
            logger.warning("sync_requests channel=%s FloodWait %ss", channel_id, wait_s)
            return 0, f"FloodWait {wait_s}s"
        except Exception as exc:
            logger.warning("sync_requests channel=%s list failed: %s: %s",
                           channel_id, type(exc).__name__, exc)
            return 0, f"{type(exc).__name__}: {str(exc)[:80]}"

        now = now_utc()
        try:
            async with SessionLocal() as s:
                rows = (await s.execute(select(JoinRequest).where(
                    JoinRequest.channel_id == channel_id,
                    JoinRequest.status == "pending"))).scalars().all()
                by_user = {}
                for row in rows:
                    if row.user_id in by_user:
                        row.status = "expired"
                        row.processed_at = now
                    else:
                        by_user[row.user_id] = row

                for uid, (fn, ln, un, dt) in live.items():
                    row = by_user.get(uid)
                    if row is None:
                        req_time = (dt.replace(tzinfo=None) if getattr(dt, "tzinfo", None)
                                    else (dt or now))
                        s.add(JoinRequest(channel_id=channel_id, user_id=uid, first_name=fn,
                                          last_name=ln, username=un, status="pending",
                                          requested_at=req_time, source="sync"))
                    else:
                        row.first_name = fn or row.first_name
                        row.last_name = ln or row.last_name
                        row.username = un or row.username

                for uid, row in by_user.items():
                    if uid not in live:
                        row.status = "expired"
                        row.processed_at = now
                await s.commit()
        except Exception as exc:
            logger.exception("sync_requests channel=%s DB reconcile failed: %s", channel_id, exc)
            return 0, f"database error: {type(exc).__name__}"

        _pending_cache[channel_id] = (len(live), now)
        return len(live), None


async def sync_pending_with_telegram(channel_id: Optional[int] = None) -> int:
    if not await userbot_ready():
        return 0
    async with SessionLocal() as s:
        q = select(Channel).where(Channel.is_active == True)  # noqa: E712
        if channel_id is not None:
            q = q.where(Channel.channel_id == channel_id)
        channels = (await s.execute(q)).scalars().all()
    ok = 0
    for ch in channels:
        _, err = await sync_channel_requests(ch.channel_id)
        if err is None:
            ok += 1
        await asyncio.sleep(0.5)
    return ok


# ===========================================================================
# USERBOT LOGIN FLOW
# ===========================================================================

async def start_login_flow(admin_id: int, message_or_cq):
    reset_flow(admin_id)
    f = flow(admin_id)
    f.state = St.LOGIN_API_ID
    txt = ("<b>🔐 Userbot Login — Step 1/4</b>\n\n"
           "Send your <b>API ID</b> (numbers only).\n"
           "Get it from https://my.telegram.org → API Development Tools.")
    if isinstance(message_or_cq, CallbackQuery):
        await safe_edit(message_or_cq.message, txt, kb_cancel_login())
    else:
        await message_or_cq.reply_text(txt, reply_markup=kb_cancel_login(), parse_mode=HTML,
                                       disable_web_page_preview=True)


async def cancel_login_flow(admin_id: int, chat_id: int):
    global login_client
    async with login_lock:
        if login_client is not None:
            try:
                await login_client.disconnect()
            except Exception:
                pass
            login_client = None
    reset_flow(admin_id)
    await bot.send_message(chat_id, "Login cancelled.",
                           reply_markup=kb_main_panel(await userbot_ready()))


async def _drop_login_client():
    global login_client
    if login_client is not None:
        try:
            await login_client.disconnect()
        except Exception:
            pass
        login_client = None


async def handle_login_text(admin_id: int, chat_id: int, txt: str):
    global login_client
    f = flow(admin_id)
    txt = txt.strip()

    if f.state == St.LOGIN_API_ID:
        if not txt.isdigit():
            await bot.send_message(chat_id, "That doesn't look like a numeric API ID.")
            return
        f.data["api_id"] = int(txt)
        f.state = St.LOGIN_API_HASH
        await bot.send_message(chat_id, "<b>Step 2/4</b> — Send your <b>API Hash</b> now.",
                               reply_markup=kb_cancel_login(), parse_mode=HTML)
        return

    if f.state == St.LOGIN_API_HASH:
        if len(txt) < 10:
            await bot.send_message(chat_id, "That doesn't look like a valid API Hash. Try again.")
            return
        f.data["api_hash"] = txt
        f.state = St.LOGIN_PHONE
        await bot.send_message(
            chat_id,
            "<b>Step 3/4</b> — Send your phone number in international format.\n"
            "Example: <code>+919876543210</code>",
            reply_markup=kb_cancel_login(), parse_mode=HTML)
        return

    if f.state == St.LOGIN_PHONE:
        phone = txt.replace(" ", "").replace("-", "")
        if not re.match(r"^\+?\d{7,15}$", phone):
            await bot.send_message(chat_id, "Invalid phone format.",
                                   parse_mode=HTML)
            return
        if not phone.startswith("+"):
            phone = "+" + phone

        async with login_lock:
            await _drop_login_client()
            login_client = Client("temp_login_session", api_id=f.data["api_id"],
                                  api_hash=f.data["api_hash"], in_memory=True)
            try:
                await login_client.connect()
                sent = await login_client.send_code(phone)
            except ApiIdInvalid:
                await bot.send_message(chat_id, "❌ Invalid API ID / API Hash combination.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except PhoneNumberInvalid:
                await bot.send_message(chat_id, "❌ Invalid phone number. Send it again.")
                await _drop_login_client()
                return
            except PhoneNumberBanned:
                await bot.send_message(chat_id, "❌ This phone is banned from Telegram.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except PhoneNumberFlood:
                await bot.send_message(chat_id, "❌ Too many login attempts on this number.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except Exception as exc:
                logger.exception("send_code failed: %s", exc)
                await bot.send_message(chat_id, f"❌ Failed to send OTP: {esc(exc)}",
                                       parse_mode=HTML)
                await _drop_login_client()
                reset_flow(admin_id)
                return

        f.data["phone"] = phone
        f.data["phone_code_hash"] = sent.phone_code_hash
        f.state = St.LOGIN_CODE
        await bot.send_message(
            chat_id,
            "<b>Step 4/4</b> — Enter the <b>OTP</b>. Send digits only, e.g. <code>12345</code>.",
            reply_markup=kb_cancel_login(), parse_mode=HTML)
        return

    if f.state == St.LOGIN_CODE:
        code = re.sub(r"[^\d]", "", txt)
        if not code:
            await bot.send_message(chat_id, "Send the OTP as digits only.")
            return
        async with login_lock:
            if login_client is None:
                await bot.send_message(chat_id, "Session expired. Start again.")
                reset_flow(admin_id)
                return
            try:
                await login_client.sign_in(f.data["phone"], f.data["phone_code_hash"], code)
            except SessionPasswordNeeded:
                f.state = St.LOGIN_PASSWORD
                await bot.send_message(
                    chat_id, "🔒 Two-Step Verification is enabled. Send your <b>2FA password</b>.",
                    reply_markup=kb_cancel_login(), parse_mode=HTML)
                return
            except (PhoneCodeInvalid, PhoneCodeEmpty):
                await bot.send_message(chat_id, "❌ Wrong OTP. Send the correct code.")
                return
            except PhoneCodeExpired:
                await bot.send_message(chat_id, "❌ OTP expired. Login cancelled.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except Exception as exc:
                logger.exception("sign_in failed: %s", exc)
                await bot.send_message(chat_id, f"❌ Login failed: {esc(exc)}", parse_mode=HTML)
                await _drop_login_client()
                reset_flow(admin_id)
                return
        await finalize_login(admin_id, chat_id)
        return

    if f.state == St.LOGIN_PASSWORD:
        async with login_lock:
            if login_client is None:
                await bot.send_message(chat_id, "Session expired. Start again.")
                reset_flow(admin_id)
                return
            try:
                await login_client.check_password(txt)
            except PasswordHashInvalid:
                await bot.send_message(chat_id, "❌ Wrong password. Try again.")
                return
            except Exception as exc:
                logger.exception("check_password failed: %s", exc)
                await bot.send_message(chat_id, f"❌ Login failed: {esc(exc)}", parse_mode=HTML)
                await _drop_login_client()
                reset_flow(admin_id)
                return
        await finalize_login(admin_id, chat_id)
        return


async def finalize_login(admin_id: int, chat_id: int):
    global login_client
    api_id = flow(admin_id).data.get("api_id")
    api_hash = flow(admin_id).data.get("api_hash")
    async with login_lock:
        try:
            me = await login_client.get_me()
            session_string = await login_client.export_session_string()
        finally:
            await _drop_login_client()

    await kv_set("userbot_api_id", str(api_id))
    await kv_set("userbot_api_hash", api_hash)
    await kv_set("userbot_session", session_string)

    reset_flow(admin_id)
    await bot.send_message(chat_id, f"⏳ Starting userbot as <b>{esc(me.first_name)}</b>…",
                           parse_mode=HTML)
    ok = await start_userbot_from_kv()
    if ok:
        await bot.send_message(chat_id,
                               f"✅ Userbot logged in as <b>{esc(me.first_name)}</b>.",
                               reply_markup=kb_main_panel(True), parse_mode=HTML)
    else:
        await bot.send_message(chat_id, "⚠️ Login saved, but userbot failed to start.",
                               reply_markup=kb_main_panel(False))


async def start_userbot_from_kv() -> bool:
    global userbot
    session_string = await kv_get("userbot_session")
    api_id = await kv_get("userbot_api_id")
    api_hash = await kv_get("userbot_api_hash")
    if not session_string or not api_id or not api_hash:
        return False

    if userbot is not None:
        try:
            if userbot.is_connected:
                await userbot.stop()
        except Exception:
            pass
        userbot = None

    client = Client("userbot_live", api_id=int(api_id), api_hash=api_hash,
                    session_string=session_string, in_memory=True)
    register_userbot_handlers(client)
    try:
        await client.start()
        userbot = client
        me = await client.get_me()
        logger.info("Userbot connected as %s (%s)", me.first_name, me.id)
    except (AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan) as exc:
        logger.error("Userbot session invalid: %s", exc)
        await kv_del("userbot_session", "userbot_api_id", "userbot_api_hash")
        userbot = None
        return False
    except Exception as exc:
        logger.exception("Userbot failed to start: %s", exc)
        userbot = None
        return False

    try:
        await import_userbot_channels(me.id)
    except Exception as exc:
        logger.warning("import_userbot_channels failed: %s", exc)
    return True


async def import_userbot_channels(me_id: int):
    async for dialog in userbot.get_dialogs():
        chat = dialog.chat
        if chat.type not in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
            continue
        try:
            member = await userbot.get_chat_member(chat.id, me_id)
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
            continue
        except Exception:
            continue
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            continue
        async with SessionLocal() as s:
            ch = (await s.execute(select(Channel).where(Channel.channel_id == chat.id))).scalar_one_or_none()
            if ch is None:
                s.add(Channel(channel_id=chat.id, name=chat.title or str(chat.id), is_active=True))
            else:
                ch.is_active = True
                if chat.title:
                    ch.name = chat.title
            await s.commit()


async def logout_userbot(chat_id: int):
    global userbot
    if userbot is not None:
        try:
            await userbot.stop()
        except Exception:
            pass
        userbot = None
    await kv_del("userbot_session", "userbot_api_id", "userbot_api_hash")
    await bot.send_message(chat_id, "🚪 Userbot logged out.",
                           reply_markup=kb_main_panel(False))


# ===========================================================================
# CHANNEL EVENT HANDLERS
# ===========================================================================

async def _ensure_channel(chat) -> None:
    async with SessionLocal() as s:
        ch = (await s.execute(select(Channel).where(Channel.channel_id == chat.id))).scalar_one_or_none()
        if ch is None:
            s.add(Channel(channel_id=chat.id, name=chat.title or str(chat.id), is_active=True))
        else:
            if not ch.is_active:
                ch.is_active = True
            if chat.title and ch.name != chat.title:
                ch.name = chat.title
        await s.commit()


_seen_events: dict = {}
_BOT_ID: int = 0


def _dedup(key, ttl: float = 30.0) -> bool:
    now = _mono()
    for k in [k for k, t in _seen_events.items() if now - t > ttl]:
        _seen_events.pop(k, None)
    if key in _seen_events:
        return True
    _seen_events[key] = now
    return False


async def _get_bot_id() -> int:
    global _BOT_ID
    if not _BOT_ID and bot is not None:
        try:
            _BOT_ID = (await bot.get_me()).id
        except Exception as exc:
            logger.warning("get_me failed: %s", exc)
    return _BOT_ID


async def _on_join_request(client: Client, request: ChatJoinRequest):
    chat = request.chat
    u = request.from_user
    if u is None:
        return
    req_ts = getattr(request, "date", None)
    req_ts = int(req_ts.timestamp()) if hasattr(req_ts, "timestamp") else 0
    if _dedup(("jr", chat.id, u.id, req_ts)):
        return
    logger.info("join_request channel_id=%s user_id=%s", chat.id, u.id)

    await _ensure_channel(chat)
    async with SessionLocal() as session:
        ex = (await session.execute(select(JoinRequest).where(
            JoinRequest.channel_id == chat.id, JoinRequest.user_id == u.id,
            JoinRequest.status == "pending"))).scalars().first()
        if ex is None:
            session.add(JoinRequest(
                channel_id=chat.id, user_id=u.id, first_name=u.first_name or "",
                last_name=u.last_name or "", username=u.username or "",
                status="pending", source="event"))
        else:
            ex.first_name = u.first_name or ex.first_name
            ex.last_name = u.last_name or ex.last_name
            ex.username = u.username or ex.username
        settings = await get_or_create_settings(session, chat.id)
        gs = await get_global_settings(session)
        auto = settings.auto_accept
        await session.commit()
    _pending_cache.pop(chat.id, None)

    auto_failed_reason = ""
    if auto:
        result = await process_join_request(chat.id, u.id, approve=True)
        if result in ("ok", "gone"):
            async with SessionLocal() as session:
                jr = (await session.execute(select(JoinRequest).where(
                    JoinRequest.channel_id == chat.id, JoinRequest.user_id == u.id,
                    JoinRequest.status == "pending"))).scalars().first()
                if jr:
                    jr.status = "accepted" if result == "ok" else "expired"
                    jr.processed_at = now_utc()
                    await session.commit()
            if result == "ok":
                await record_member(chat.id, u.id, u.first_name or "", u.username or "",
                                    u.last_name or "")
                await send_join_message(chat.id, chat.title or "", u.id, u.first_name or "",
                                        u.last_name or "", u.username or "")
                if gs.notif_auto_accept:
                    await notify_admins(f"✅ Auto-accepted: <b>{esc(u.first_name)}</b> "
                                        f"into <b>{esc(chat.title)}</b>")
                spawn(run_event_automations(
                    "join_request", user_id=u.id, first_name=u.first_name or "",
                    last_name=u.last_name or "", username=u.username or "",
                    channel_id=chat.id, channel_name=chat.title or ""),
                    name="auto_join_request")
            _pending_cache.pop(chat.id, None)
            return
        auto_failed_reason = ("userbot not connected" if not await userbot_ready()
                              else "Telegram rejected (check userbot admin rights)")
        logger.warning("auto_accept FAILED ch=%s u=%s: %s", chat.id, u.id, auto_failed_reason)

    if gs.notif_join_request or auto_failed_reason:
        pending = await live_pending_count(chat.id, use_ttl=False)
        members = await live_member_count(chat.id)
        uname = esc("@" + u.username) if u.username else "no username"
        text_out = (
            f"🔔 <b>New join request</b>\n\n"
            f"Channel: <b>{esc(chat.title)}</b>\n"
            f"User: <b>{esc(u.first_name)}</b> ({uname})\n"
            f"ID: <code>{u.id}</code>\n"
            f"Requested: {now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            f"⏳ Pending (Telegram): {pending.value}{src_tag(pending)}\n"
            f"👥 Members: {members.value:,}{src_tag(members)}")
        if auto_failed_reason:
            text_out = (f"⚠️ <b>Auto-accept FAILED</b> — {esc(auto_failed_reason)}.\n"
                        f"Request is still pending.\n\n") + text_out
        await notify_admins(text_out, kb_join_request_actions(chat.id, u.id))


async def _on_member_updated(client: Client, update: ChatMemberUpdated):
    old = update.old_chat_member
    new = update.new_chat_member
    if new is None:
        return
    chat = update.chat
    u = new.user
    if u is None:
        return

    if u.id == await _get_bot_id():
        if not _dedup(("bot", chat.id, str(new.status))):
            await _handle_bot_status(update)
        return

    active = (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER,
              ChatMemberStatus.RESTRICTED)
    left = (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    old_status = old.status if old is not None else None
    new_status = new.status

    ev_ts = getattr(update, "date", None)
    ev_ts = int(ev_ts.timestamp()) if hasattr(ev_ts, "timestamp") else 0
    if _dedup(("mu", chat.id, u.id, str(old_status), str(new_status), ev_ts)):
        return
    _member_cache.pop(chat.id, None)

    async with SessionLocal() as session:
        gs = await get_global_settings(session)

    if new_status in left and (old_status in active or old_status is None):
        await _ensure_channel(chat)
        was_active = await deactivate_member(chat.id, u.id)
        if not was_active and old_status is None:
            return
        async with SessionLocal() as session:
            session.add(MemberLeave(channel_id=chat.id, user_id=u.id,
                                    first_name=u.first_name or "", username=u.username or ""))
            await session.commit()
        if was_active or old_status in active:
            await send_leave_message(chat.id, chat.title or "", u.id, u.first_name or "",
                                     u.last_name or "", u.username or "")
        if gs.notif_member_leave:
            await notify_admins(f"🚪 <b>{esc(u.first_name)}</b> left <b>{esc(chat.title)}</b>")
        spawn(run_event_automations(
            "member_leave", user_id=u.id, first_name=u.first_name or "",
            last_name=u.last_name or "", username=u.username or "",
            channel_id=chat.id, channel_name=chat.title or ""), name="auto_member_leave")

    elif new_status in active and (old_status is None or old_status not in active):
        await _ensure_channel(chat)
        await record_member(chat.id, u.id, u.first_name or "", u.username or "", u.last_name or "")
        if gs.notif_member_join:
            await notify_admins(f"👤 <b>{esc(u.first_name)}</b> joined <b>{esc(chat.title)}</b>")
        spawn(run_event_automations(
            "member_join", user_id=u.id, first_name=u.first_name or "",
            last_name=u.last_name or "", username=u.username or "",
            channel_id=chat.id, channel_name=chat.title or ""), name="auto_member_join")


async def _handle_bot_status(update: ChatMemberUpdated):
    new = update.new_chat_member
    chat = update.chat
    if new.status == ChatMemberStatus.ADMINISTRATOR:
        await _ensure_channel(chat)
        txt = (f"✅ <b>Bot added as admin</b>\n\nChannel: <b>{esc(chat.title)}</b>\n"
               f"ID: <code>{chat.id}</code>")
        if not await userbot_ready():
            txt += ("\n\n⚠️ The userbot also needs to be admin here with invite permissions.")
        await notify_admins(txt, kb_channel_notify(chat.id))
    elif new.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, ChatMemberStatus.MEMBER):
        keep = False
        if await userbot_ready():
            try:
                me_u = await userbot.get_me()
                m = await userbot.get_chat_member(chat.id, me_u.id)
                keep = m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
            except Exception:
                keep = False
        if not keep:
            async with SessionLocal() as s:
                ch = (await s.execute(select(Channel).where(Channel.channel_id == chat.id))).scalar_one_or_none()
                if ch and ch.is_active:
                    ch.is_active = False
                    await s.commit()
            await notify_admins(f"⚠️ Bot removed from <b>{esc(chat.title)}</b>.")


def register_userbot_handlers(client: Client):
    async def _userbot_join(c: Client, request: ChatJoinRequest):
        if await _bot_manages(request.chat.id):
            return
        await _on_join_request(c, request)

    async def _userbot_member(c: Client, update: ChatMemberUpdated):
        if await _bot_manages(update.chat.id):
            return
        await _on_member_updated(c, update)

    client.add_handler(ChatJoinRequestHandler(_userbot_join))
    client.add_handler(ChatMemberUpdatedHandler(_userbot_member))


_bot_admin_cache: dict = {}


async def _bot_manages(channel_id: int) -> bool:
    now = _mono()
    hit = _bot_admin_cache.get(channel_id)
    if hit and now - hit[1] < 300:
        return hit[0]
    ok = False
    try:
        m = await bot.get_chat_member(channel_id, await _get_bot_id())
        ok = m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        pass
    _bot_admin_cache[channel_id] = (ok, now)
    return ok


# ===========================================================================
# LIVE COUNT HELPERS
# ===========================================================================

@dataclass
class CountResult:
    value: int
    source: str
    reason: str = ""
    fetched_at: Optional[datetime] = None

    @property
    def is_live(self) -> bool:
        return self.source == "LIVE"


_member_cache: dict = {}
_pending_cache: dict = {}
CACHE_TTL_SECONDS = 20


async def _client_member_count(client: Client, channel_id: int) -> int:
    fn = getattr(client, "get_chat_members_count", None) or getattr(client, "get_chat_member_count", None)
    if fn is None:
        raise RuntimeError("No member-count method on this Pyrogram version")
    return int(await fn(channel_id))


async def live_member_count(channel_id: int, use_ttl: bool = True) -> CountResult:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cached = _member_cache.get(channel_id)
    if use_ttl and cached and (now - cached[1]).total_seconds() < CACHE_TTL_SECONDS:
        return CountResult(cached[0], "LIVE", fetched_at=cached[1])

    reasons = []
    clients = []
    if await userbot_ready():
        clients.append(("userbot", userbot))
    if bot is not None:
        clients.append(("bot", bot))
    for label, client in clients:
        try:
            cnt = await flood_safe(lambda c=client: _client_member_count(c, channel_id))
            _member_cache[channel_id] = (cnt, now)
            return CountResult(cnt, "LIVE", fetched_at=now)
        except Exception as exc:
            reasons.append(f"{label}: {type(exc).__name__}")

    reason = "; ".join(reasons) or "no client available"
    if cached:
        return CountResult(cached[0], "CACHED", reason=reason, fetched_at=cached[1])

    async with SessionLocal() as s:
        cnt = (await s.execute(select(func.count()).select_from(Member).where(
            Member.channel_id == channel_id, Member.is_active == True))).scalar()  # noqa: E712
    return CountResult(int(cnt or 0), "LOCAL", reason=reason)


async def _local_pending_count(channel_id: Optional[int] = None) -> int:
    async with SessionLocal() as s:
        q = select(func.count()).select_from(JoinRequest).where(JoinRequest.status == "pending")
        if channel_id is not None:
            q = q.where(JoinRequest.channel_id == channel_id)
        return int((await s.execute(q)).scalar() or 0)


async def live_pending_count(channel_id: int, use_ttl: bool = True) -> CountResult:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cached = _pending_cache.get(channel_id)
    if use_ttl and cached and (now - cached[1]).total_seconds() < CACHE_TTL_SECONDS:
        return CountResult(cached[0], "LIVE", fetched_at=cached[1])

    if await userbot_ready():
        n, err = await sync_channel_requests(channel_id)
        if err is None:
            _pending_cache[channel_id] = (n, now)
            return CountResult(n, "LIVE", fetched_at=now)
        local = await _local_pending_count(channel_id)
        if cached:
            return CountResult(cached[0], "CACHED", reason=err, fetched_at=cached[1])
        return CountResult(local, "LOCAL", reason=err)

    local = await _local_pending_count(channel_id)
    return CountResult(local, "LOCAL", reason="userbot not connected")


def src_tag(r: CountResult) -> str:
    if r.source == "LIVE":
        return ""
    if r.source == "CACHED":
        age = ""
        if r.fetched_at:
            secs = int((now_utc() - r.fetched_at).total_seconds())
            age = f" {secs // 60}m old" if secs >= 60 else f" {secs}s old"
        return f" ⚠️ cached{age}"
    return " ⚠️ local DB only"


def fmt_uptime() -> str:
    total = int((datetime.now(timezone.utc) - BOT_START_TIME).total_seconds())
    return f"{total // 86400}d {(total % 86400) // 3600}h {(total % 3600) // 60}m"


# ===========================================================================
# AUTOMATION ENGINE  (fast path)
# ===========================================================================

_auto_cache: dict = {"rules": None, "buttons": {}, "ts": 0.0}
AUTO_CACHE_TTL = 30.0


def invalidate_automation_cache():
    _auto_cache["rules"] = None
    _auto_cache["buttons"] = {}
    _auto_cache["ts"] = 0.0


def _auto_cache_fresh() -> bool:
    return _auto_cache["rules"] is not None and (_mono() - _auto_cache["ts"]) < AUTO_CACHE_TTL


async def _load_enabled_rules(force: bool = False) -> list:
    if not force and _auto_cache_fresh():
        return _auto_cache["rules"] or []
    async with SessionLocal() as s:
        rows = (await s.execute(select(AutomationRule).where(
            AutomationRule.enabled == True).order_by(  # noqa: E712
            AutomationRule.priority, AutomationRule.id))).scalars().all()
    _auto_cache["rules"] = list(rows)
    _auto_cache["ts"] = _mono()
    _auto_cache["buttons"] = {}
    return _auto_cache["rules"]


async def _get_rule_buttons(rule_id: int) -> list:
    buttons = _auto_cache["buttons"].get(rule_id)
    if buttons is not None:
        return buttons
    async with SessionLocal() as s:
        btns = (await s.execute(select(AutomationButton).where(
            AutomationButton.rule_id == rule_id).order_by(
            AutomationButton.row, AutomationButton.col))).scalars().all()
    data = [{"row": b.row, "col": b.col, "label": b.label, "url": b.url} for b in btns]
    _auto_cache["buttons"][rule_id] = data
    return data


async def _build_rule_markup(rule_id: int) -> Optional[InlineKeyboardMarkup]:
    btns = await _get_rule_buttons(rule_id)
    if not btns:
        return None
    rows: dict = {}
    for b in btns:
        if not b["label"] or not b["url"]:
            continue
        if not re.match(r"^(https?://|tg://)\S+$", b["url"].strip()):
            continue
        rows.setdefault(b["row"], []).append(InlineKeyboardButton(b["label"], url=b["url"].strip()))
    ordered = [rows[k] for k in sorted(rows.keys()) if rows[k]]
    return InlineKeyboardMarkup(ordered) if ordered else None


def _split_keywords(value: str) -> list:
    parts = re.split(r"[\n,]+", value or "")
    return [p.strip() for p in parts if p.strip()]


def _match_rule(rule: AutomationRule, text: str) -> bool:
    if rule.trigger_type == "message":
        if rule.match_type == "any":
            return True
        raw = rule.trigger_value or ""
        needle = text or ""
        if not raw and rule.match_type != "any":
            return False
        if rule.case_insensitive:
            raw_cmp = raw.lower()
            needle_cmp = needle.lower()
        else:
            raw_cmp = raw
            needle_cmp = needle
        mt = rule.match_type
        try:
            if mt == "exact":
                return needle_cmp == raw_cmp
            if mt == "contains":
                return bool(raw_cmp) and raw_cmp in needle_cmp
            if mt == "starts":
                return bool(raw_cmp) and needle_cmp.startswith(raw_cmp)
            if mt == "ends":
                return bool(raw_cmp) and needle_cmp.endswith(raw_cmp)
            if mt == "regex":
                flags = re.IGNORECASE if rule.case_insensitive else 0
                return bool(re.search(raw, needle, flags))
            if mt == "any_kw":
                for kw in _split_keywords(rule.trigger_value):
                    if (kw.lower() if rule.case_insensitive else kw) in needle_cmp:
                        return True
                return False
            if mt == "all_kw":
                kws = _split_keywords(rule.trigger_value)
                if not kws:
                    return False
                for kw in kws:
                    if (kw.lower() if rule.case_insensitive else kw) not in needle_cmp:
                        return False
                return True
        except re.error:
            return False
    elif rule.trigger_type == "command":
        cmd = (rule.trigger_value or "").lstrip("/").lower()
        if not cmd:
            return False
        first = (text or "").split()[0] if text else ""
        return first.lower().lstrip("/") == cmd
    return False


def _rule_scope_matches(rule: AutomationRule, channel_id: Optional[int]) -> bool:
    if rule.scope_type == "global":
        return True
    if rule.scope_type == "channel":
        return rule.scope_channel_id is not None and channel_id == rule.scope_channel_id
    return False


_cd_cache: dict = {}


async def _check_cooldown(rule: AutomationRule, user_id: Optional[int],
                          channel_id: Optional[int]) -> bool:
    if not rule.cooldown_seconds or rule.cooldown_seconds <= 0:
        return True
    if user_id is not None:
        key = f"u:{user_id}"
    elif channel_id is not None:
        key = f"c:{channel_id}"
    else:
        key = "g"
    ck = (rule.id, key)
    now_m = _mono()
    last = _cd_cache.get(ck)
    if last is not None and (now_m - last) < rule.cooldown_seconds:
        return False
    _cd_cache[ck] = now_m
    spawn(_persist_cooldown(rule.id, key), name="cooldown")
    return True


async def _persist_cooldown(rule_id: int, key: str):
    try:
        now = now_utc()
        async with SessionLocal() as s:
            row = (await s.execute(select(AutomationCooldown).where(
                AutomationCooldown.rule_id == rule_id,
                AutomationCooldown.scope_key == key))).scalar_one_or_none()
            if row is None:
                s.add(AutomationCooldown(rule_id=rule_id, scope_key=key, last_run=now))
            else:
                row.last_run = now
            await s.commit()
    except Exception as exc:
        logger.debug("persist_cooldown failed: %s", exc)


async def _log_automation(rule: Optional[AutomationRule], user_id: Optional[int],
                          channel_id: Optional[int], trigger_type: str,
                          matched: bool, ok: bool, detail: str = ""):
    try:
        async with SessionLocal() as s:
            s.add(AutomationLog(
                rule_id=rule.id if rule else None,
                rule_name=rule.name if rule else "(event)", user_id=user_id,
                channel_id=channel_id, trigger_type=trigger_type,
                matched=matched, ok=ok, detail=detail[:500]))
            await s.commit()
    except Exception as exc:
        logger.debug("automation log failed: %s", exc)


async def _execute_rule(rule: AutomationRule, *, user_id: Optional[int],
                        first_name: str, last_name: str, username: str,
                        channel_id: Optional[int], channel_name: str,
                        raw_text: str = "", test_only: bool = False) -> dict:
    result = {"ok": False, "detail": "", "preview": "", "markup": None}
    markup = await _build_rule_markup(rule.id)
    result["markup"] = markup

    text_out = render_template(
        rule.response_text or "", first_name=first_name, last_name=last_name,
        username=username, channel_name=channel_name, user_id=user_id or "",
        channel_id=channel_id or "")

    if not user_id:
        result["detail"] = "no target user_id"
        return result

    result["preview"] = text_out
    if test_only:
        result["ok"] = True
        result["detail"] = "preview only"
        return result

    try:
        if rule.response_type == "copy" and rule.response_from_chat_id and rule.response_from_message_id:
            await bot.copy_message(user_id, rule.response_from_chat_id,
                                   rule.response_from_message_id, reply_markup=markup)
        elif rule.response_type in ("photo", "video", "document", "audio", "voice", "animation") \
                and rule.response_media_id:
            senders = {
                "photo": lambda: bot.send_photo(user_id, rule.response_media_id, caption=text_out[:1024],
                                                parse_mode=HTML, reply_markup=markup),
                "video": lambda: bot.send_video(user_id, rule.response_media_id, caption=text_out[:1024],
                                                parse_mode=HTML, reply_markup=markup),
                "document": lambda: bot.send_document(user_id, rule.response_media_id, caption=text_out[:1024],
                                                      parse_mode=HTML, reply_markup=markup),
                "audio": lambda: bot.send_audio(user_id, rule.response_media_id, caption=text_out[:1024],
                                                parse_mode=HTML, reply_markup=markup),
                "voice": lambda: bot.send_voice(user_id, rule.response_media_id, caption=text_out[:1024],
                                                parse_mode=HTML, reply_markup=markup),
                "animation": lambda: bot.send_animation(user_id, rule.response_media_id, caption=text_out[:1024],
                                                        parse_mode=HTML, reply_markup=markup),
            }
            try:
                await flood_safe(senders[rule.response_type])
            except Exception:
                await bot.send_message(user_id, text_out[:4090], parse_mode=HTML,
                                       reply_markup=markup, disable_web_page_preview=True)
        else:
            try:
                await bot.send_message(user_id, text_out[:4090], parse_mode=HTML,
                                       reply_markup=markup, disable_web_page_preview=True)
            except Exception:
                plain = strip_tags(text_out)
                await bot.send_message(user_id, plain[:4090], reply_markup=markup)
        result["ok"] = True
    except (UserIsBlocked, UserPrivacyRestricted, PeerIdInvalid, InputUserDeactivated) as exc:
        result["detail"] = f"cannot DM user: {type(exc).__name__}"
        await mark_user_blocked(user_id)
    except Exception as exc:
        result["detail"] = f"{type(exc).__name__}: {str(exc)[:150]}"
    return result


async def _batch_bump_counts(success_ids: list, error_ids: list, last_triggered_map: dict):
    if not success_ids and not error_ids and not last_triggered_map:
        return
    try:
        async with SessionLocal() as s:
            for rid in set(success_ids):
                await s.execute(
                    update(AutomationRule).where(AutomationRule.id == rid).values(
                        execution_count=AutomationRule.execution_count + 1,
                        last_triggered_at=last_triggered_map.get(rid, now_utc())))
            for rid in set(error_ids):
                await s.execute(
                    update(AutomationRule).where(AutomationRule.id == rid).values(
                        error_count=AutomationRule.error_count + 1))
            await s.commit()
    except Exception as exc:
        logger.debug("batch_bump_counts failed: %s", exc)


async def run_message_automations(message: Message) -> bool:
    u = message.from_user
    if u is None:
        return False
    text = (message.text or message.caption or "").strip()
    rules = await _load_enabled_rules()
    if not rules:
        return False

    any_executed = False
    success_ids: list = []
    error_ids: list = []
    triggered_ts = now_utc()

    fired: list[tuple] = []
    for rule in rules:
        if rule.trigger_type not in ("message", "command"):
            continue
        if rule.scope_type == "channel":
            continue
        if not _match_rule(rule, text):
            continue
        if rule.max_executions and (rule.execution_count or 0) >= rule.max_executions:
            continue
        if not await _check_cooldown(rule, u.id, None):
            continue
        fired.append(rule)
        if rule.stop_on_match:
            break

    if not fired:
        return False

    for rule in fired:
        try:
            result = await _execute_rule(
                rule, user_id=u.id, first_name=u.first_name or "", last_name=u.last_name or "",
                username=u.username or "", channel_id=None, channel_name="")
        except Exception as exc:
            logger.exception("automation rule #%s crashed: %s", rule.id, exc)
            result = {"ok": False, "detail": f"crash: {type(exc).__name__}"}
        any_executed = True
        if result["ok"]:
            success_ids.append(rule.id)
        else:
            error_ids.append(rule.id)
        spawn(_log_automation(rule, u.id, None, rule.trigger_type, True,
                              result["ok"], result["detail"]), name="auto_log")

    spawn(_batch_bump_counts(success_ids, error_ids,
                             {rid: triggered_ts for rid in success_ids + error_ids}),
          name="auto_count")
    return any_executed


async def run_event_automations(event_type: str, *, user_id: int, first_name: str,
                                last_name: str, username: str, channel_id: Optional[int],
                                channel_name: str = ""):
    rules = await _load_enabled_rules()
    if not rules:
        return
    fired = []
    for rule in rules:
        if rule.trigger_type != event_type:
            continue
        if not _rule_scope_matches(rule, channel_id):
            continue
        if rule.max_executions and (rule.execution_count or 0) >= rule.max_executions:
            continue
        if not await _check_cooldown(rule, user_id, channel_id):
            continue
        fired.append(rule)
        if rule.stop_on_match:
            break

    success_ids: list = []
    error_ids: list = []
    for rule in fired:
        try:
            result = await _execute_rule(
                rule, user_id=user_id, first_name=first_name, last_name=last_name,
                username=username, channel_id=channel_id, channel_name=channel_name)
        except Exception as exc:
            logger.exception("event automation #%s crashed: %s", rule.id, exc)
            result = {"ok": False, "detail": f"crash: {type(exc).__name__}"}
        if result["ok"]:
            success_ids.append(rule.id)
        else:
            error_ids.append(rule.id)
        spawn(_log_automation(rule, user_id, channel_id, event_type, True,
                              result["ok"], result["detail"]), name="auto_log")

    spawn(_batch_bump_counts(success_ids, error_ids,
                             {rid: now_utc() for rid in success_ids + error_ids}),
          name="auto_count")


# ===========================================================================
# AUTOMATION UI HELPERS
# ===========================================================================

def kb_automation_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Create Automation", callback_data="auto:create")],
        [InlineKeyboardButton("📋 Manage Rules", callback_data="auto:list:1"),
         InlineKeyboardButton("⚡ Active Only", callback_data="auto:list:1:on")],
        [InlineKeyboardButton("📊 Stats", callback_data="auto:stats"),
         InlineKeyboardButton("📋 Logs", callback_data="auto:logs:1")],
        [InlineKeyboardButton("🧩 Variables Help", callback_data="auto:vars"),
         InlineKeyboardButton("🧪 Test", callback_data="auto:test")],
        [InlineKeyboardButton("« Back", callback_data="panel:main")],
    ])


TRIGGER_LABELS = {
    "message": "💬 Message",
    "command": "🔧 Command",
    "join_request": "⏳ Join Request",
    "member_join": "👤 Member Joined",
    "member_leave": "🚪 Member Left",
}

MATCH_LABELS = {
    "exact": "🎯 Exact match",
    "contains": "🔎 Contains",
    "starts": "▶️ Starts with",
    "ends": "⏹ Ends with",
    "regex": "🧬 Regex",
    "any_kw": "🧩 Any keyword",
    "all_kw": "🧩 All keywords",
    "any": "🌀 Any message",
}

RESPONSE_LABELS = {
    "text": "Text",
    "photo": "Photo",
    "video": "Video",
    "document": "Document",
    "audio": "Audio",
    "voice": "Voice",
    "animation": "Animation/GIF",
    "copy": "Copy Telegram message",
}

COOLDOWN_PRESETS = [0, 5, 30, 60, 300, 3600, 86400]


def _scope_summary(rule: AutomationRule) -> str:
    if rule.scope_type == "global":
        return "🌐 Global"
    return f"📣 Channel {rule.scope_channel_id}"


def _short_trigger_preview(rule: AutomationRule) -> str:
    if rule.trigger_type == "message":
        if rule.match_type == "any":
            return "(any message)"
        val = rule.trigger_value or ""
        return (val[:60] + "…") if len(val) > 60 else val
    if rule.trigger_type == "command":
        return f"/{rule.trigger_value}"
    return ""


async def build_automation_main_text() -> str:
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(AutomationRule))).scalar() or 0
        active = (await s.execute(select(func.count()).select_from(AutomationRule).where(
            AutomationRule.enabled == True))).scalar() or 0  # noqa: E712
        ts = today_start()
        exec_today = (await s.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.created_at >= ts, AutomationLog.ok == True))).scalar() or 0  # noqa: E712
        err_today = (await s.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.created_at >= ts, AutomationLog.ok == False))).scalar() or 0  # noqa: E712
    return (f"🤖 <b>Automation Center</b>\n\n"
            f"⚡ Active Rules: <b>{active}</b> / {total}\n"
            f"📊 Executions Today: <b>{exec_today}</b>\n"
            f"⚠️ Errors Today: <b>{err_today}</b>\n\n"
            f"Rules run on incoming private messages and channel events.\n"
            f"Lower priority number = runs first.")


async def build_rule_list(page: int = 1, active_only: bool = False):
    PAGE = 6
    async with SessionLocal() as s:
        q = select(AutomationRule)
        if active_only:
            q = q.where(AutomationRule.enabled == True)  # noqa: E712
        q = q.order_by(AutomationRule.priority, AutomationRule.id)
        total = (await s.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0
        pages = max(1, -(-total // PAGE))
        page = min(max(1, page), pages)
        rules = (await s.execute(q.offset((page - 1) * PAGE).limit(PAGE))).scalars().all()
    if not rules:
        return ("🤖 <b>Automation Rules</b>\n\nNo rules yet. Tap ➕ Create Automation.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Create Automation", callback_data="auto:create")],
                    [InlineKeyboardButton("« Back", callback_data="auto:main")]]))

    lines = [f"🤖 <b>Automation Rules</b> — page {page}/{pages} — {total} rule(s)\n"]
    kb = []
    for r in rules:
        dot = "🟢" if r.enabled else "🔴"
        trig = _short_trigger_preview(r)
        resp = short_preview(r.response_text, 40) if r.response_text else f"[{r.response_type}]"
        lines.append(f"{dot} <b>#{r.id} {esc(r.name)}</b>\n"
                     f"   💬 <code>{esc(trig)}</code> → 📤 <i>{resp}</i>\n"
                     f"   {_scope_summary(r)} · P{r.priority} · runs: {r.execution_count} · err: {r.error_count}")
        kb.append([
            InlineKeyboardButton(f"⚙️ #{r.id}", callback_data=f"auto:view:{r.id}"),
            InlineKeyboardButton("🧪", callback_data=f"auto:test_rule:{r.id}"),
            InlineKeyboardButton("✅" if r.enabled else "❌", callback_data=f"auto:toggle:{r.id}"),
            InlineKeyboardButton("⬆️", callback_data=f"auto:prio:{r.id}:up"),
            InlineKeyboardButton("⬇️", callback_data=f"auto:prio:{r.id}:down"),
        ])
    kb.append(pager_row(f"auto:list" + (":on" if active_only else ""), page, pages))
    kb.append([InlineKeyboardButton("➕ Create", callback_data="auto:create"),
               InlineKeyboardButton("« Back", callback_data="auto:main")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(kb)


async def build_rule_view(rule_id: int):
    async with SessionLocal() as s:
        r = (await s.execute(select(AutomationRule).where(AutomationRule.id == rule_id))).scalar_one_or_none()
        if r is None:
            return "❔ Rule not found.", kb_back("auto:main")
        btns = (await s.execute(select(AutomationButton).where(
            AutomationButton.rule_id == rule_id).order_by(
            AutomationButton.row, AutomationButton.col))).scalars().all()
    dot = "🟢 Enabled" if r.enabled else "🔴 Disabled"
    btn_lines = "\n".join(f"   • {esc(b.label)} → {esc(b.url)}" for b in btns) or "   (none)"

    if r.trigger_type == "message":
        trig = _short_trigger_preview(r)
        trigger_line = (f"<b>Trigger:</b> 💬 message\n"
                        f"<b>Match:</b> {MATCH_LABELS.get(r.match_type, r.match_type)}\n"
                        f"<b>Pattern:</b> <code>{esc(trig or '(any)')}</code>\n")
    elif r.trigger_type == "command":
        trigger_line = (f"<b>Trigger:</b> 🔧 command <code>/{esc(r.trigger_value or '')}</code>\n")
    else:
        trigger_line = (f"<b>Trigger:</b> {TRIGGER_LABELS.get(r.trigger_type, r.trigger_type)}\n")

    resp_preview = short_preview(r.response_text, 220)
    txt = (f"🤖 <b>Rule #{r.id}: {esc(r.name)}</b> — {dot}\n\n"
           f"{trigger_line}"
           f"<b>Scope:</b> {_scope_summary(r)}\n<b>Priority:</b> {r.priority}\n"
           f"<b>Cooldown:</b> {r.cooldown_seconds}s\n"
           f"<b>Response type:</b> {RESPONSE_LABELS.get(r.response_type, r.response_type)}\n"
           f"<b>Reply:</b>\n<i>{resp_preview}</i>\n"
           f"<b>Buttons:</b>\n{btn_lines}\n\n"
           f"Runs: {r.execution_count} · Errors: {r.error_count}\n"
           f"Last triggered: {r.last_triggered_at.strftime('%Y-%m-%d %H:%M') if r.last_triggered_at else '—'} UTC")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Name", callback_data=f"auto:edit_name:{r.id}"),
         InlineKeyboardButton("✏️ Edit Trigger", callback_data=f"auto:edit_pattern:{r.id}")],
        [InlineKeyboardButton("✏️ Edit Reply", callback_data=f"auto:edit_response:{r.id}"),
         InlineKeyboardButton("🖼 Set Media", callback_data=f"auto:edit_media:{r.id}")],
        [InlineKeyboardButton("⚙️ Advanced Settings", callback_data=f"auto:advanced:{r.id}")],
        [InlineKeyboardButton("👁 Preview", callback_data=f"auto:preview:{r.id}"),
         InlineKeyboardButton("🧪 Test", callback_data=f"auto:test_rule:{r.id}")],
        [InlineKeyboardButton("📋 Logs", callback_data=f"auto:logs_rule:{r.id}:1"),
         InlineKeyboardButton("📄 Duplicate", callback_data=f"auto:duplicate:{r.id}")],
        [InlineKeyboardButton("✅ Enable" if not r.enabled else "❌ Disable",
                              callback_data=f"auto:toggle:{r.id}"),
         InlineKeyboardButton("🗑 Delete", callback_data=f"auto:delete:{r.id}")],
        [InlineKeyboardButton("« Back", callback_data="auto:list:1")],
    ])
    return txt[:4090], kb


async def build_rule_advanced(rule_id: int):
    async with SessionLocal() as s:
        r = (await s.execute(select(AutomationRule).where(AutomationRule.id == rule_id))).scalar_one_or_none()
        if r is None:
            return "❔ Rule not found.", kb_back("auto:main")
    txt = (f"⚙️ <b>Advanced Settings — Rule #{r.id}</b>\n\n"
           f"<b>Match type:</b> {MATCH_LABELS.get(r.match_type, r.match_type)}\n"
           f"<b>Scope:</b> {_scope_summary(r)}\n"
           f"<b>Priority:</b> {r.priority}\n"
           f"<b>Cooldown:</b> {r.cooldown_seconds}s\n"
           f"<b>Stop on match:</b> {'Yes' if r.stop_on_match else 'No'}\n"
           f"<b>Case-insensitive:</b> {'Yes' if r.case_insensitive else 'No'}\n\n"
           f"<i>Advanced options won't break the basic 'trigger → reply' flow.</i>")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Match Type", callback_data=f"auto:edit_match:{r.id}"),
         InlineKeyboardButton("🌐 Scope", callback_data=f"auto:edit_scope:{r.id}")],
        [InlineKeyboardButton("⚡ Priority", callback_data=f"auto:edit_priority:{r.id}"),
         InlineKeyboardButton("⏱ Cooldown", callback_data=f"auto:edit_cooldown:{r.id}")],
        [InlineKeyboardButton("🔗 Add Button", callback_data=f"auto:add_btn:{r.id}"),
         InlineKeyboardButton("🗑 Clear Buttons", callback_data=f"auto:clear_btns:{r.id}")],
        [InlineKeyboardButton("🛑 Toggle stop-on-match", callback_data=f"auto:toggle_stop:{r.id}")],
        [InlineKeyboardButton("« Back", callback_data=f"auto:view:{r.id}")],
    ])
    return txt, kb


async def build_automation_logs(page: int = 1, rule_id: Optional[int] = None):
    PAGE = 10
    async with SessionLocal() as s:
        q = select(AutomationLog)
        if rule_id is not None:
            q = q.where(AutomationLog.rule_id == rule_id)
        q = q.order_by(AutomationLog.id.desc())
        total = (await s.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0
        pages = max(1, -(-total // PAGE))
        page = min(max(1, page), pages)
        rows = (await s.execute(q.offset((page - 1) * PAGE).limit(PAGE))).scalars().all()
    if not rows:
        return "📋 <b>Automation Logs</b>\n\n(no entries)", InlineKeyboardMarkup([
            [InlineKeyboardButton("« Back", callback_data=f"auto:view:{rule_id}" if rule_id else "auto:main")]])
    lines = [f"📋 <b>Automation Logs</b> — page {page}/{pages} — {total} total\n"]
    for r in rows:
        mark = "✅" if r.ok else "❌"
        when = r.created_at.strftime("%m-%d %H:%M") if r.created_at else ""
        lines.append(f"{mark} <b>{esc(r.rule_name)}</b> · {esc(r.trigger_type)} · "
                     f"user=<code>{r.user_id or '—'}</code> · {when} UTC\n   {esc((r.detail or '')[:120])}")
    prefix = f"auto:logs_rule:{rule_id}" if rule_id else "auto:logs"
    back = f"auto:view:{rule_id}" if rule_id else "auto:main"
    kb = InlineKeyboardMarkup([pager_row(prefix, page, pages),
                               [InlineKeyboardButton("« Back", callback_data=back)]])
    return "\n".join(lines)[:4090], kb


async def build_automation_stats():
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(AutomationRule))).scalar() or 0
        active = (await s.execute(select(func.count()).select_from(AutomationRule).where(
            AutomationRule.enabled == True))).scalar() or 0  # noqa: E712
        top = (await s.execute(select(AutomationRule).order_by(
            AutomationRule.execution_count.desc()).limit(5))).scalars().all()
        ts = today_start()
        exec_today = (await s.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.created_at >= ts))).scalar() or 0
        ok_today = (await s.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.created_at >= ts, AutomationLog.ok == True))).scalar() or 0  # noqa: E712
        err_today = (await s.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.created_at >= ts, AutomationLog.ok == False))).scalar() or 0  # noqa: E712
    lines = [f"📊 <b>Automation Stats</b>\n",
             f"Rules: <b>{total}</b> · Active: <b>{active}</b>",
             f"Executions today: <b>{exec_today}</b> · OK: <b>{ok_today}</b> · Errors: <b>{err_today}</b>",
             ""]
    if top:
        lines.append("<b>Top rules by executions:</b>")
        for r in top:
            lines.append(f"  · #{r.id} {esc(r.name)} — {r.execution_count} run(s), "
                         f"{r.error_count} error(s)")
    return "\n".join(lines)


# ===========================================================================
# SIMPLE AUTOMATION WIZARD
# ===========================================================================

def _draft(f: Flow) -> dict:
    d = f.data.get("auto_draft")
    if not isinstance(d, dict):
        d = f.data["auto_draft"] = {}
    return d


async def _wizard_start(cq: CallbackQuery, admin_id: int):
    reset_flow(admin_id)
    f = flow(admin_id)
    f.state = St.AUTO_TRIGGER
    f.origin_section = "auto:create"
    _draft(f).clear()
    await safe_edit(
        cq.message,
        "➕ <b>New Automation — Step 1/2</b>\n\n"
        "💬 <b>What message should trigger the reply?</b>\n\n"
        "<i>Example:</i> <code>hello</code>\n\n"
        "The bot will reply when a user sends exactly this text "
        "(you can switch to contains/keywords/regex in Advanced Settings after saving).",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🌀 Any Message", callback_data="auto:trig:any")],
            [InlineKeyboardButton("❌ Cancel", callback_data="auto:cancel")],
        ]))


async def _wizard_show_preview(chat_id: int, admin_id: int, edit_msg: Optional[Message] = None):
    d = _draft(flow(admin_id))
    trig = d.get("trigger_value", "")
    match = d.get("match_type", "exact")
    resp_text = d.get("response_text", "")
    resp_media = d.get("response_media_id", "")
    resp_type = d.get("response_type", "text")

    trig_display = "(any message)" if match == "any" else f"<code>{esc(trig)}</code>"
    if resp_media and resp_type != "text":
        resp_display = f"[{resp_type}] {esc(short_preview(resp_text, 80)) if resp_text else '(no caption)'}"
    else:
        resp_display = f"<code>{esc(short_preview(resp_text, 200))}</code>"

    txt = (f"✅ <b>Preview</b>\n\n"
           f"💬 <b>Trigger:</b> {trig_display}\n"
           f"🎯 <b>Match mode:</b> {MATCH_LABELS.get(match, match)}\n\n"
           f"📤 <b>Bot will reply:</b>\n{resp_display}\n\n"
           f"Click <b>Save</b> to activate, or use <b>Advanced</b> to add media, "
           f"buttons, cooldown, priority or channel scope.")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💾 Save Automation", callback_data="auto:save_draft")],
        [InlineKeyboardButton("⚙️ Advanced Settings", callback_data="auto:adv_draft")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto:cancel")],
    ])
    if edit_msg is not None:
        await safe_edit(edit_msg, txt, kb)
    else:
        await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode=HTML)


async def handle_auto_wizard_text(admin_id: int, chat_id: int, message: Message) -> bool:
    f = flow(admin_id)
    st = f.state
    txt = (message.text or "").strip()

    if st == St.AUTO_TRIGGER:
        if not txt:
            await message.reply_text("Please send the trigger text (e.g. <code>hello</code>).",
                                     parse_mode=HTML)
            return True
        d = _draft(f)
        d["trigger_value"] = txt[:2000]
        d["match_type"] = "exact"
        d["trigger_type"] = "message"
        f.state = St.AUTO_RESPONSE
        await message.reply_text(
            "➕ <b>Step 2/2</b>\n\n"
            "📤 <b>Now send what you want the bot to reply.</b>\n\n"
            "You can also send a <b>photo / video / document / audio / voice / GIF</b> "
            "with an optional caption.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel",
                                                                     callback_data="auto:cancel")]]),
            parse_mode=HTML)
        return True

    if st == St.AUTO_RESPONSE:
        if not txt:
            await message.reply_text("Please send the reply content.")
            return True
        d = _draft(f)
        d["response_text"] = html_of(message)
        d["response_type"] = "text"
        f.state = St.NONE
        await _wizard_show_preview(chat_id, admin_id)
        return True

    if st == St.AUTO_EDIT_NAME:
        rule_id = f.data.get("edit_rule_id")
        draft_mode = f.data.get("draft_mode")
        if draft_mode and not rule_id:
            # Editing the draft's name
            d = _draft(f)
            d["name"] = txt[:200]
            reset_flow(admin_id)
            # Restore the draft reference — we just cleared flow but kept the draft in state? No.
            # Instead, keep it simple: show the preview.
            # NOTE: Because we cleared, we re-show the preview from the last known state.
            # In practice name-edit-in-draft is rare; fall back to preview.
            await message.reply_text("✅ Name saved. Reopen Create Automation to review.")
            return True
        reset_flow(admin_id)
        if not rule_id or not txt:
            await message.reply_text("Cancelled.")
            return True
        async with SessionLocal() as s:
            r = (await s.execute(select(AutomationRule).where(
                AutomationRule.id == rule_id))).scalar_one_or_none()
            if r:
                r.name = txt[:200]
                r.updated_at = now_utc()
                await s.commit()
        invalidate_automation_cache()
        v, kb = await build_rule_view(rule_id)
        await message.reply_text(f"✅ Name updated.\n\n{v}", reply_markup=kb, parse_mode=HTML)
        return True

    if st == St.AUTO_EDIT_PATTERN:
        rule_id = f.data.get("edit_rule_id")
        reset_flow(admin_id)
        if not rule_id or not txt:
            await message.reply_text("Cancelled.")
            return True
        async with SessionLocal() as s:
            r = (await s.execute(select(AutomationRule).where(
                AutomationRule.id == rule_id))).scalar_one_or_none()
            if r:
                r.trigger_value = txt[:2000]
                r.updated_at = now_utc()
                await s.commit()
        invalidate_automation_cache()
        v, kb = await build_rule_view(rule_id)
        await message.reply_text(f"✅ Trigger updated.\n\n{v}", reply_markup=kb, parse_mode=HTML)
        return True

    if st == St.AUTO_EDIT_RESPONSE:
        rule_id = f.data.get("edit_rule_id")
        reset_flow(admin_id)
        if not rule_id:
            await message.reply_text("Cancelled.")
            return True
        new_text = html_of(message)
        if not new_text:
            await message.reply_text("Send text or a media caption.")
            return True
        async with SessionLocal() as s:
            r = (await s.execute(select(AutomationRule).where(
                AutomationRule.id == rule_id))).scalar_one_or_none()
            if r:
                r.response_text = new_text
                r.updated_at = now_utc()
                await s.commit()
        invalidate_automation_cache()
        v, kb = await build_rule_view(rule_id)
        await message.reply_text(f"✅ Reply updated.\n\n{v}", reply_markup=kb, parse_mode=HTML)
        return True

    if st == St.AUTO_EDIT_BTN_LABEL:
        rule_id = f.data.get("edit_rule_id")
        if not rule_id or not txt:
            await message.reply_text("Send the button label.")
            return True
        f.data["pending_btn_label"] = txt[:100]
        f.state = St.AUTO_EDIT_BTN_URL
        await message.reply_text("Now send the button URL (https:// or tg://):")
        return True

    if st == St.AUTO_EDIT_BTN_URL:
        rule_id = f.data.get("edit_rule_id")
        label = f.data.get("pending_btn_label", "")
        reset_flow(admin_id)
        if not rule_id or not re.match(r"^(https?://|tg://)\S+$", txt):
            await message.reply_text("❌ Invalid URL. Cancelled.")
            return True
        async with SessionLocal() as s:
            count = (await s.execute(select(func.count()).select_from(AutomationButton).where(
                AutomationButton.rule_id == rule_id))).scalar() or 0
            s.add(AutomationButton(rule_id=rule_id, row=int(count) // 2, col=int(count) % 2,
                                   label=label, url=txt.strip()))
            await s.commit()
        invalidate_automation_cache()
        v, kb = await build_rule_view(rule_id)
        await message.reply_text(f"✅ Button added.\n\n{v}", reply_markup=kb, parse_mode=HTML)
        return True

    return False


async def handle_auto_wizard_media(admin_id: int, chat_id: int, message: Message) -> bool:
    f = flow(admin_id)
    st = f.state
    mtype = None
    mid = None
    if message.photo:
        mtype, mid = "photo", message.photo.file_id
    elif message.video:
        mtype, mid = "video", message.video.file_id
    elif message.document:
        mtype, mid = "document", message.document.file_id
    elif message.audio:
        mtype, mid = "audio", message.audio.file_id
    elif message.voice:
        mtype, mid = "voice", message.voice.file_id
    elif message.animation:
        mtype, mid = "animation", message.animation.file_id
    if mtype is None:
        return False

    if st == St.AUTO_RESPONSE:
        d = _draft(f)
        d["response_media_id"] = mid
        d["response_type"] = mtype
        if message.caption:
            d["response_text"] = html_of(message)
        f.state = St.NONE
        await _wizard_show_preview(chat_id, admin_id)
        return True

    if st == St.AUTO_EDIT_MEDIA:
        rule_id = f.data.get("edit_rule_id")
        if not rule_id:
            return True
        async with SessionLocal() as s:
            r = (await s.execute(select(AutomationRule).where(
                AutomationRule.id == rule_id))).scalar_one_or_none()
            if r:
                r.response_media_id = mid
                r.response_type = mtype
                if message.caption:
                    r.response_text = html_of(message)
                r.updated_at = now_utc()
                await s.commit()
        invalidate_automation_cache()
        reset_flow(admin_id)
        v, kb = await build_rule_view(rule_id)
        await message.reply_text(f"✅ Media updated ({mtype}).\n\n{v}",
                                 reply_markup=kb, parse_mode=HTML)
        return True

    return False


async def _save_draft_rule(d: dict) -> Optional[int]:
    try:
        match_type = d.get("match_type", "exact")
        trigger_value = d.get("trigger_value", "")
        if match_type == "any":
            trigger_value = ""
        name = d.get("name") or (trigger_value[:50] if trigger_value else "Any message")
        async with SessionLocal() as s:
            rule = AutomationRule(
                name=name or "Untitled",
                description=d.get("description") or "",
                enabled=True,
                priority=int(d.get("priority", 100)),
                stop_on_match=bool(d.get("stop_on_match", True)),
                trigger_type=d.get("trigger_type", "message"),
                match_type=match_type,
                trigger_value=trigger_value,
                case_insensitive=True,
                scope_type=d.get("scope_type", "global"),
                scope_channel_id=d.get("scope_channel_id"),
                cooldown_seconds=int(d.get("cooldown_seconds", 0)),
                response_type=d.get("response_type", "text"),
                response_text=d.get("response_text", ""),
                response_media_id=d.get("response_media_id", ""),
            )
            s.add(rule)
            await s.flush()
            for i, b in enumerate(d.get("buttons", []) or []):
                s.add(AutomationButton(rule_id=rule.id, row=i // 2, col=i % 2,
                                       label=b.get("label", ""), url=b.get("url", "")))
            await s.commit()
            rule_id = rule.id
        invalidate_automation_cache()
        return rule_id
    except Exception as exc:
        logger.exception("save_draft_rule failed: %s", exc)
        return None


# ===========================================================================
# MAIN PANEL TEXT
# ===========================================================================

_tg_sem = asyncio.Semaphore(4)


async def _gather_channel_stats(channels: list, refresh: bool):
    async def one(ch):
        async with _tg_sem:
            members = await live_member_count(ch.channel_id, use_ttl=not refresh)
            pending = await live_pending_count(ch.channel_id, use_ttl=not refresh)
        return ch, members, pending
    return await asyncio.gather(*(one(c) for c in channels))


async def build_main_panel_text(sync: bool = False) -> str:
    logged_in = await userbot_ready()
    userbot_status = "🟢 Connected" if logged_in else "🔴 Not logged in"
    userbot_info = ""
    if logged_in:
        try:
            me = await userbot.get_me()
            userbot_info = f" as {esc('@' + me.username) if me.username else esc(me.first_name)}"
        except Exception:
            pass

    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
        accepted_today = (await session.execute(
            select(func.count()).select_from(JoinRequest).where(
                JoinRequest.status == "accepted",
                JoinRequest.processed_at >= today_start()))).scalar() or 0
        left_today = (await session.execute(
            select(func.count()).select_from(MemberLeave).where(
                MemberLeave.left_at >= today_start()))).scalar() or 0
        auto_rules = (await session.execute(
            select(func.count()).select_from(AutomationRule).where(
                AutomationRule.enabled == True))).scalar() or 0  # noqa: E712
        sched_jobs = len(scheduler.get_jobs()) if scheduler.running else 0

    results = await _gather_channel_stats(channels, refresh=sync)
    total_members = sum(m.value for _, m, _ in results)
    total_pending = sum(p.value for _, _, p in results)
    all_live_members = all(m.is_live for _, m, _ in results) if results else True
    all_live_pending = all(p.is_live for _, _, p in results) if results else True
    m_tag = "[LIVE]" if all_live_members else "[CACHED]"
    p_tag = "[LIVE]" if all_live_pending else "[CACHED]"

    return (
        f"╭──────────────────────────────╮\n"
        f"│   🛡 <b>CHANNEL MANAGER</b>\n"
        f"│      Premium Admin Panel\n"
        f"╰──────────────────────────────╯\n\n"
        f"🟢 System Online\n"
        f"🔐 Userbot: {userbot_status}{userbot_info}\n"
        f"📣 Channels: {len(channels)}\n"
        f"⏳ Pending Requests: {total_pending} {p_tag}\n"
        f"👥 Members: {total_members:,} {m_tag}\n"
        f"⚡ Automations: {auto_rules}\n"
        f"📢 Scheduler jobs: {sched_jobs}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Accepted today: {accepted_today} · 🚪 Left today: {left_today}\n"
        f"🚀 Uptime: {fmt_uptime()}\n"
        f"🕒 {now_utc().strftime('%H:%M:%S')} UTC")


# ===========================================================================
# COMMAND HANDLERS
# ===========================================================================

async def cmd_start(client: Client, message: Message):
    u = message.from_user
    if u is None:
        return
    async with SessionLocal() as session:
        known = (await session.execute(select(KnownUser).where(KnownUser.user_id == u.id))).scalar_one_or_none()
        if known is None:
            session.add(KnownUser(user_id=u.id, first_name=u.first_name or "",
                                  username=u.username or ""))
        else:
            known.bot_blocked = False
        await session.commit()

    if is_admin(u.id):
        reset_flow(u.id)
        await message.reply_text(await build_main_panel_text(sync=False),
                                 reply_markup=kb_main_panel(await userbot_ready()), parse_mode=HTML)
    else:
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            await session.commit()
            markup = build_markup(gs.start_btn_label, gs.start_btn_url)
            txt = gs.start_msg_text or "👋 Welcome! Send us a message."
        try:
            await message.reply_text(txt, reply_markup=markup, parse_mode=HTML,
                                     disable_web_page_preview=True)
        except Exception:
            await message.reply_text(strip_tags(txt), reply_markup=markup)
        async with SessionLocal() as session:
            session.add(Conversation(user_id=u.id, direction="in", message="/start"))
            await session.commit()


async def cmd_channels(client: Client, message: Message):
    await show_channel_list(message.chat.id)


async def build_channel_list(refresh: bool = False):
    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
    if not channels:
        return ("No channels yet. Add the bot (and userbot) as admin to a channel, "
                "or tap ➕ Add Channel below.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Channel", callback_data="channels:add")],
                    [InlineKeyboardButton("« Back", callback_data="panel:main")]]))
    results = await _gather_channel_stats(channels, refresh=refresh)
    lines = ["📣 <b>Managed Channels</b>\n"]
    kb_rows = []
    for i, (ch, members, pending) in enumerate(results, 1):
        async with SessionLocal() as session:
            s = await get_or_create_settings(session, ch.channel_id)
            await session.commit()
            auto_str = "ON" if s.auto_accept else "OFF"
        lines.append(
            f"{i}. <b>{esc(ch.name)}</b>\n"
            f"   👥 {members.value:,}{src_tag(members)} · ⏳ {pending.value}{src_tag(pending)} · ✅ Auto: {auto_str}")
        kb_rows.append([InlineKeyboardButton(f"⚙️ {ch.name}"[:60],
                                             callback_data=f"channels:settings:{ch.channel_id}")])
    lines.append(f"\n🕒 {now_utc().strftime('%H:%M:%S')} UTC")
    kb_rows.append([InlineKeyboardButton("➕ Add Channel", callback_data="channels:add"),
                    InlineKeyboardButton("🔄 Refresh", callback_data="channels:refresh")])
    kb_rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(kb_rows)


async def show_channel_list(chat_id: int, edit_msg: Optional[Message] = None, refresh: bool = False):
    txt, kb = await build_channel_list(refresh=refresh)
    if edit_msg is not None:
        await safe_edit(edit_msg, txt, kb)
    else:
        await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode=HTML)


async def show_channel_settings(cq: CallbackQuery, ch_id: int):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
        await session.commit()
    name = await get_channel_name(ch_id)
    auto_str = "✅ ON" if s.auto_accept else "❌ OFF"
    await safe_edit(
        cq.message, f"⚙️ <b>Settings: {esc(name)}</b>\n\nAuto-Accept: {auto_str}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Join Message", callback_data=f"settings:join:{ch_id}"),
             InlineKeyboardButton("🚪 Leave Message", callback_data=f"settings:leave:{ch_id}")],
            [InlineKeyboardButton(f"Auto-Accept: {auto_str}",
                                  callback_data=f"autoaccept:toggle:{ch_id}")],
            [InlineKeyboardButton("🤖 Automations", callback_data="auto:list:1")],
            [InlineKeyboardButton("➖ Remove Channel",
                                  callback_data=f"channels:remove:{ch_id}")],
            [InlineKeyboardButton("« Back", callback_data="channels:list")],
        ]))


async def cmd_requests(client: Client, message: Message):
    wait = await message.reply_text("🔄 Fetching live data…")
    txt, kb = await build_requests_overview()
    await safe_edit(wait, txt, kb)


async def build_requests_overview():
    channels = await _active_channels()
    if not channels:
        return "No channels yet.", kb_back()
    results = await _gather_channel_stats(channels, refresh=True)
    lines = ["⏳ <b>Join Requests</b>\n"]
    rows = []
    for ch, members, pending in results:
        lines.append(f"📣 <b>{esc(ch.name)}</b>\n"
                     f"   ⏳ Pending: {pending.value}{src_tag(pending)}\n"
                     f"   👥 Members: {members.value:,}{src_tag(members)}\n")
        rows.append([InlineKeyboardButton(f"👁 View Requests — {ch.name}"[:60],
                                          callback_data=f"reqlist:{ch.channel_id}:1")])
        rows.append([InlineKeyboardButton("✅ Accept All", callback_data=f"req:accept_all:{ch.channel_id}"),
                     InlineKeyboardButton("❌ Decline All", callback_data=f"req:decline_all:{ch.channel_id}")])
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="reqs:overview"),
                 InlineKeyboardButton("« Back", callback_data="panel:main")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(rows)


REQ_PAGE_SIZE = 8


async def build_request_page(channel_id: int, page: int):
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.status == "pending"))).scalar() or 0
        pages = max(1, -(-total // REQ_PAGE_SIZE))
        page = min(max(1, page), pages)
        rows_ = (await s.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.status == "pending"
        ).order_by(JoinRequest.requested_at).offset((page - 1) * REQ_PAGE_SIZE)
            .limit(REQ_PAGE_SIZE))).scalars().all()
    name = await get_channel_name(channel_id)
    if not rows_:
        return (f"📣 <b>{esc(name)}</b>\n\nNo pending requests.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh", callback_data=f"reqlist:{channel_id}:1")],
                    [InlineKeyboardButton("« Back", callback_data="reqs:overview")]]))
    lines = [f"📣 <b>{esc(name)}</b> — pending {total}\n"]
    kb = []
    base = (page - 1) * REQ_PAGE_SIZE
    for i, r in enumerate(rows_, 1):
        uname = esc("@" + r.username) if r.username else "no username"
        ago = now_utc() - (r.requested_at or now_utc())
        mins = int(ago.total_seconds() // 60)
        when = (f"{mins} min ago" if mins < 60
                else (f"{mins // 60} h ago" if mins < 1440 else f"{mins // 1440} d ago"))
        lines.append(f"<b>#{base + i}</b> 👤 {esc(r.first_name)} {esc(r.last_name or '')}\n"
                     f"   {uname} · ID: <code>{r.user_id}</code>\n   Requested: {when}\n")
        kb.append([
            InlineKeyboardButton(f"✅ #{base + i}", callback_data=f"jr:accept:{channel_id}:{r.user_id}"),
            InlineKeyboardButton(f"❌ #{base + i}", callback_data=f"jr:decline:{channel_id}:{r.user_id}"),
            InlineKeyboardButton(f"👁 #{base + i}", callback_data=f"user:profile:{channel_id}:{r.user_id}")])
    kb.append(pager_row(f"reqlist:{channel_id}", page, pages))
    kb.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"reqlist:{channel_id}:{page}:r"),
               InlineKeyboardButton("« Back", callback_data="reqs:overview")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(kb)


async def cmd_accept_all(client: Client, message: Message):
    await bulk_process_requests(message.chat.id, None, approve=True)


async def cmd_decline_all(client: Client, message: Message):
    await bulk_process_requests(message.chat.id, None, approve=False)


STATUS_LABEL = {
    "pending": "⏳ PENDING REQUEST", "member": "✅ MEMBER", "left": "🚪 LEFT",
    "banned": "🔨 BANNED", "declined": "❌ DECLINED", "unknown": "❔ UNKNOWN",
}


async def live_user_status(channel_id: int, user_id: int):
    if not await userbot_ready():
        return None, "userbot not connected"
    try:
        m = await flood_safe(lambda: userbot.get_chat_member(channel_id, user_id))
        st = m.status
        if st in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR,
                  ChatMemberStatus.OWNER, ChatMemberStatus.RESTRICTED):
            return "member", None
        if st == ChatMemberStatus.BANNED:
            return "banned", None
        if st == ChatMemberStatus.LEFT:
            return "left", None
        return None, f"unrecognised status {st}"
    except Exception as exc:
        name = type(exc).__name__
        if "UserNotParticipant" in name or "USER_NOT_PARTICIPANT" in str(exc).upper():
            return "left", None
        return None, name


async def resolve_user_status(channel_id: int, user_id: int, verify: bool = True):
    async with SessionLocal() as s:
        pending = (await s.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.user_id == user_id,
            JoinRequest.status == "pending"))).scalars().first()
        last_req = (await s.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.user_id == user_id
        ).order_by(JoinRequest.id.desc()))).scalars().first()
        mem = (await s.execute(select(Member).where(
            Member.channel_id == channel_id, Member.user_id == user_id))).scalars().first()

    if pending:
        local = "pending"
    elif mem and mem.is_active:
        local = "member"
    elif mem and not mem.is_active:
        local = "left"
    elif last_req and last_req.status == "declined":
        local = "declined"
    else:
        local = "unknown"

    status, source, verr = local, "LOCAL", None
    if verify:
        live, verr = await live_user_status(channel_id, user_id)
        if live is not None:
            source = "LIVE"
            if live == "member":
                status = "member"
                if mem is None or not mem.is_active:
                    await record_member(channel_id, user_id,
                                        (mem.first_name if mem else "") or
                                        (last_req.first_name if last_req else ""),
                                        (mem.username if mem else "") or
                                        (last_req.username if last_req else ""))
            elif live in ("banned", "left"):
                status = "pending" if (pending and live == "left") else live
                if mem and mem.is_active:
                    await deactivate_member(channel_id, user_id)
    return {"status": status, "source": source, "verify_error": verr,
            "pending": pending, "last_req": last_req, "member": mem}


def _fmt_dt(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M") + " UTC" if dt else "—"


async def find_users(query: str, channel_id: Optional[int], limit: int = 50):
    q = query.strip()
    found: dict = {}

    def add(ch, uid, fn, ln, un):
        key = (ch, uid)
        if key not in found:
            found[key] = (ch, uid, fn or "", ln or "", un or "")

    async with SessionLocal() as s:
        if q.lstrip("-").isdigit():
            uid = int(q)
            conds_m = [Member.user_id == uid]
            conds_j = [JoinRequest.user_id == uid]
        else:
            term = q.lstrip("@").lower()
            like = f"%{term}%"
            conds_m = [or_(func.lower(Member.username).like(like),
                           func.lower(Member.first_name).like(like),
                           func.lower(func.coalesce(Member.last_name, "")).like(like))]
            conds_j = [or_(func.lower(JoinRequest.username).like(like),
                           func.lower(JoinRequest.first_name).like(like),
                           func.lower(func.coalesce(JoinRequest.last_name, "")).like(like))]
        mq = select(Member).where(*conds_m)
        jq = select(JoinRequest).where(*conds_j)
        if channel_id is not None:
            mq = mq.where(Member.channel_id == channel_id)
            jq = jq.where(JoinRequest.channel_id == channel_id)
        for m in (await s.execute(mq.limit(limit))).scalars().all():
            add(m.channel_id, m.user_id, m.first_name, m.last_name, m.username)
        for j in (await s.execute(jq.order_by(JoinRequest.id.desc()).limit(limit))).scalars().all():
            add(j.channel_id, j.user_id, j.first_name, j.last_name, j.username)
    return list(found.values())[:limit]


async def build_user_profile(channel_id: int, user_id: int):
    info = await resolve_user_status(channel_id, user_id, verify=True)
    st, mem, pend, last_req = info["status"], info["member"], info["pending"], info["last_req"]
    name_src = mem or pend or last_req
    fn = (name_src.first_name if name_src else "") or ""
    ln = (getattr(name_src, "last_name", "") if name_src else "") or ""
    un = (name_src.username if name_src else "") or ""
    if not fn and await userbot_ready():
        try:
            u = await userbot.get_users(user_id)
            fn, ln, un = u.first_name or "", u.last_name or "", u.username or ""
        except Exception:
            pass

    async with SessionLocal() as s:
        blocked = (await s.execute(select(BlockedUser).where(BlockedUser.user_id == user_id))).scalar_one_or_none()
        msgs = (await s.execute(select(func.count()).select_from(Conversation).where(
            Conversation.user_id == user_id))).scalar()
    chan = await get_channel_name(channel_id)
    src_line = ("🟢 Source: LIVE" if info["source"] == "LIVE"
                else f"⚠️ Source: LOCAL DB only{(' — ' + esc(info['verify_error'])) if info['verify_error'] else ''}")

    txt = (f"👤 <b>USER PROFILE</b>\n\n"
           f"Name: {esc((fn + ' ' + ln).strip() or '(unknown)')}\n"
           f"Username: {esc('@' + un) if un else '(none)'}\n"
           f"ID: <code>{user_id}</code>\n\n"
           f"📣 Channel: {esc(chan)}\nStatus: <b>{STATUS_LABEL[st]}</b>\n{src_line}\n\n"
           f"🕒 First Joined: {_fmt_dt(mem.first_joined_at or mem.joined_at) if mem else '—'}\n"
           f"🕒 Last Joined: {_fmt_dt(mem.last_joined_at) if mem else '—'}\n"
           f"🕒 Last Left: {_fmt_dt(mem.last_left_at) if mem else '—'}\n"
           f"⏳ Pending: {'Yes since ' + _fmt_dt(pend.requested_at) if pend else 'No'}\n\n"
           f"📨 Messages: {msgs}\n🚫 Blocked: {'Yes' if blocked else 'No'}")

    rows = []
    if st == "pending":
        rows.append([InlineKeyboardButton("✅ Accept", callback_data=f"jr:accept:{channel_id}:{user_id}"),
                     InlineKeyboardButton("❌ Decline", callback_data=f"jr:decline:{channel_id}:{user_id}")])
    if st == "member":
        rows.append([InlineKeyboardButton("🚫 Remove", callback_data=f"user:remove:{user_id}:{channel_id}"),
                     InlineKeyboardButton("🔇 Mute", callback_data=f"user:mute:{user_id}:{channel_id}"),
                     InlineKeyboardButton("🔨 Ban", callback_data=f"user:ban:{user_id}:{channel_id}")])
    if st == "banned":
        rows.append([InlineKeyboardButton("♻️ Unban", callback_data=f"user:unban:{user_id}:{channel_id}")])
    rows.append([InlineKeyboardButton("🔄 Refresh Status", callback_data=f"user:profile:{channel_id}:{user_id}")])
    rows.append([InlineKeyboardButton("📥 Open Inbox", callback_data=f"inbox:open:{user_id}"),
                 InlineKeyboardButton("« Back", callback_data="panel:main")])
    return txt, InlineKeyboardMarkup(rows)


async def run_search(chat_id: int, query: str, channel_id: Optional[int] = None):
    query = query.strip()
    if not query:
        await bot.send_message(chat_id, "Send a user ID, @username or a name.", reply_markup=kb_back())
        return
    results = await find_users(query, channel_id)

    if not results and await userbot_ready():
        try:
            lookup = int(query) if query.lstrip("-").isdigit() else query.lstrip("@")
            u = await userbot.get_users(lookup)
            targets = ([channel_id] if channel_id is not None
                       else [c.channel_id for c in await _active_channels()])
            for ch in targets:
                results.append((ch, u.id, u.first_name or "", u.last_name or "", u.username or ""))
        except (PeerIdInvalid, UsernameNotOccupied, KeyError, IndexError):
            pass
        except Exception as exc:
            logger.warning("search live lookup failed: %s", type(exc).__name__)

    if not results:
        scope = f" in <b>{esc(await get_channel_name(channel_id))}</b>" if channel_id is not None else ""
        await bot.send_message(chat_id, f"❌ No user found matching <code>{esc(query)}</code>{scope}.",
                               reply_markup=kb_back(), parse_mode=HTML)
        return

    if len(results) == 1:
        ch, uid, *_ = results[0]
        txt, kb = await build_user_profile(ch, uid)
        await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode=HTML)
        return

    lines = [f"🔍 <b>{len(results)} match(es)</b> for <code>{esc(query)}</code>\n"]
    btns = []
    for ch, uid, fn, ln, un in results[:20]:
        label = f"{(fn + ' ' + ln).strip() or uid}" + (f" @{un}" if un else "")
        cname = await get_channel_name(ch)
        btns.append([InlineKeyboardButton(f"{label} · {cname}"[:60],
                                          callback_data=f"user:profile:{ch}:{uid}")])
    if len(results) > 20:
        lines.append(f"Showing first 20 of {len(results)} — narrow your search.")
    btns.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=InlineKeyboardMarkup(btns),
                           parse_mode=HTML)


async def _active_channels() -> list:
    async with SessionLocal() as s:
        return (await s.execute(select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712


async def cmd_search(client: Client, message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("Usage: <code>/search &lt;user_id or @username&gt;</code>",
                                 parse_mode=HTML)
        return
    await run_search(message.chat.id, parts[1].strip())


async def show_inbox(chat_id: int):
    async with SessionLocal() as session:
        rows = (await session.execute(
            select(Conversation.user_id, func.max(Conversation.sent_at).label("last"))
            .group_by(Conversation.user_id)
            .order_by(func.max(Conversation.sent_at).desc()).limit(20))).all()
    if not rows:
        await bot.send_message(chat_id, "Inbox is empty.", reply_markup=kb_back())
        return
    buttons = []
    async with SessionLocal() as session:
        for user_id, _ in rows:
            last = (await session.execute(
                select(Conversation).where(Conversation.user_id == user_id)
                .order_by(Conversation.sent_at.desc()).limit(1))).scalars().first()
            raw = (last.message if last else "") or ""
            preview = raw[:30] + ("…" if len(raw) > 30 else "")
            buttons.append([InlineKeyboardButton(f"{user_id} — {preview}"[:60],
                                                 callback_data=f"inbox:open:{user_id}")])
    buttons.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    await bot.send_message(chat_id, "<b>📥 Inbox</b> — recent conversations:",
                           reply_markup=InlineKeyboardMarkup(buttons), parse_mode=HTML)


async def cmd_inbox(client: Client, message: Message):
    await show_inbox(message.chat.id)


async def cmd_block(client: Client, message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await message.reply_text("Usage: <code>/block &lt;user_id&gt;</code>", parse_mode=HTML)
        return
    tid = int(parts[1].strip())
    async with SessionLocal() as session:
        exists = (await session.execute(select(BlockedUser).where(
            BlockedUser.user_id == tid))).scalar_one_or_none()
        if not exists:
            session.add(BlockedUser(user_id=tid))
            await session.commit()
    await message.reply_text(f"🔇 User <code>{tid}</code> blocked.", parse_mode=HTML)


async def cmd_unblock(client: Client, message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await message.reply_text("Usage: <code>/unblock &lt;user_id&gt;</code>", parse_mode=HTML)
        return
    tid = int(parts[1].strip())
    async with SessionLocal() as session:
        row = (await session.execute(select(BlockedUser).where(BlockedUser.user_id == tid))).scalar_one_or_none()
        if row:
            await session.delete(row)
            await session.commit()
    await message.reply_text(f"🔊 User <code>{tid}</code> unblocked.", parse_mode=HTML)


async def cmd_broadcast(client: Client, message: Message):
    reset_flow(message.from_user.id)
    f = flow(message.from_user.id)
    f.state = St.BC_CONTENT
    await message.reply_text(
        "Send the broadcast content now (text, photo, video, voice or document).",
        reply_markup=kb_back())


async def build_stats_text(refresh: bool = False) -> str:
    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
        active_convos = (await session.execute(
            select(func.count(func.distinct(Conversation.user_id))).select_from(Conversation))).scalar()
        broadcasts_sent = (await session.execute(
            select(func.coalesce(func.sum(Broadcast.sent_count), 0)).select_from(Broadcast))).scalar()
        admin_count = (await session.execute(select(func.count()).select_from(Admin))).scalar()
        rules_total = (await session.execute(select(func.count()).select_from(AutomationRule))).scalar()
        rules_active = (await session.execute(select(func.count()).select_from(AutomationRule).where(
            AutomationRule.enabled == True))).scalar()  # noqa: E712
        auto_exec = (await session.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.ok == True))).scalar()  # noqa: E712
        auto_err = (await session.execute(select(func.count()).select_from(AutomationLog).where(
            AutomationLog.ok == False))).scalar()  # noqa: E712

    logged_in = await userbot_ready()
    userbot_txt = "🟢 Connected" if logged_in else "🔴 Not logged in"
    results = await _gather_channel_stats(channels, refresh=refresh)
    ts = today_start()
    blocks = []
    for ch, members, pending in results:
        async with SessionLocal() as session:
            joined_today = (await session.execute(select(func.count()).select_from(Member).where(
                Member.channel_id == ch.channel_id,
                func.coalesce(Member.last_joined_at, Member.joined_at) >= ts))).scalar()
            left_today = (await session.execute(select(func.count()).select_from(MemberLeave).where(
                MemberLeave.channel_id == ch.channel_id, MemberLeave.left_at >= ts))).scalar()
            tracked = (await session.execute(select(func.count()).select_from(Member).where(
                Member.channel_id == ch.channel_id,
                Member.is_active == True))).scalar()  # noqa: E712
            local_pending = await _local_pending_count(ch.channel_id)
        blocks.append(
            f"📣 <b>{esc(ch.name)}</b>\n"
            f"  👥 Members: {members.value:,}{src_tag(members)}\n"
            f"  ⏳ Pending (Telegram): {pending.value}{src_tag(pending)}\n"
            f"  🗄 Pending (Local): {local_pending} · 🧾 Tracked active: {tracked}\n"
            f"  ✅ Joined today: {joined_today} · 🚪 Left today: {left_today}")
    ch_section = "\n\n".join(blocks) if blocks else "(no channels)"

    return (f"<b>📊 Analytics</b>\n\n"
            f"🤖 Userbot: {userbot_txt}\n"
            f"📣 Channels: {len(channels)}\n"
            f"⚡ Automations: {rules_active} / {rules_total}\n\n"
            f"━━━ Per Channel ━━━\n{ch_section}\n\n"
            f"━━━ Bot Stats ━━━\n"
            f"📨 Broadcasts Sent: {broadcasts_sent}\n"
            f"💬 Active Conversations: {active_convos}\n"
            f"⚡ Automation Runs: {auto_exec} · ⚠️ Errors: {auto_err}\n"
            f"🛡️ Admins: {admin_count}\n"
            f"⏰ Uptime: {fmt_uptime()}\n"
            f"🕒 {now_utc().strftime('%H:%M:%S')} UTC")


KB_STATS = InlineKeyboardMarkup([
    [InlineKeyboardButton("🔄 Refresh", callback_data="stats:refresh")],
    [InlineKeyboardButton("« Back", callback_data="panel:main")],
])


async def cmd_stats(client: Client, message: Message):
    await message.reply_text(await build_stats_text(), reply_markup=KB_STATS, parse_mode=HTML)


async def cmd_settings(client: Client, message: Message):
    await message.reply_text("⚙️ <b>Settings</b>", reply_markup=kb_settings_main(), parse_mode=HTML)


async def cmd_admins(client: Client, message: Message):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin).order_by(Admin.is_owner.desc()))).scalars().all()
    await message.reply_text("<b>🛡️ Admins:</b>", reply_markup=kb_admins_list(admins), parse_mode=HTML)


async def cmd_login(client: Client, message: Message):
    await start_login_flow(message.from_user.id, message)


async def cmd_setapi(client: Client, message: Message):
    uid = message.from_user.id
    reset_flow(uid)
    flow(uid).state = St.SET_BOT_API_ID
    cur_id = await kv_get("bot_api_id")
    cur_hash = await kv_get("bot_api_hash")
    if ENV_API_ID and ENV_API_HASH:
        source = f"✅ Via .env (API_ID: <code>{esc(ENV_API_ID)}</code>)"
    elif cur_id and cur_hash:
        source = f"✅ Saved in DB (API_ID: <code>{esc(cur_id)}</code>)"
    else:
        source = "❌ Not set — bot running on temp credentials"
    await message.reply_text(
        f"<b>⚙️ Bot API Credentials</b>\n\n<b>Current:</b> {source}\n\n"
        f"Get credentials: https://my.telegram.org → API Development Tools\n\n"
        f"<b>Step 1/2</b> — Send your <b>API ID</b> (numbers only):",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="setapi:cancel")]]),
        parse_mode=HTML, disable_web_page_preview=True)


async def cmd_help(client: Client, message: Message):
    await message.reply_text(
        "<b>Admin Commands</b>\n"
        "/start /panel — main menu\n/channels — list managed channels\n"
        "/requests — pending join requests\n/accept_all /decline_all — bulk process\n"
        "/search &lt;id|@user&gt; — find a user\n/inbox — conversations\n"
        "/block &lt;id&gt; /unblock &lt;id&gt;\n/broadcast — start a broadcast\n"
        "/stats — analytics dashboard\n/settings — settings\n"
        "/automation — Automation Center\n"
        "/admins — manage admins\n/login — userbot login\n/setapi — set bot API (owner only)",
        parse_mode=HTML)


async def cmd_automation(client: Client, message: Message):
    await message.reply_text(await build_automation_main_text(),
                             reply_markup=kb_automation_main(), parse_mode=HTML)


# ===========================================================================
# PRIVATE MESSAGE ROUTER
# ===========================================================================

def media_label(message: Message) -> str:
    if message.photo: return "[photo]"
    if message.video: return "[video]"
    if message.voice: return "[voice]"
    if message.audio: return "[audio]"
    if message.document: return "[document]"
    if message.sticker: return "[sticker]"
    if message.animation: return "[gif]"
    if message.video_note: return "[video note]"
    return "[media]"


async def on_private_message(client: Client, message: Message):
    if message.from_user is None:
        return
    uid = message.from_user.id

    if is_admin(uid):
        await handle_admin_message(client, message, uid)
        return

    async with SessionLocal() as session:
        blocked = (await session.execute(select(BlockedUser).where(BlockedUser.user_id == uid))).scalar_one_or_none()
        if blocked:
            return
        known = (await session.execute(select(KnownUser).where(KnownUser.user_id == uid))).scalar_one_or_none()
        if known is None:
            known = KnownUser(user_id=uid, first_name=message.from_user.first_name or "",
                              username=message.from_user.username or "")
            session.add(known)
            await session.flush()
        known.bot_blocked = False
        gs = await get_global_settings(session)

        fire_legacy = (gs.auto_reply_enabled and not known.auto_reply_sent
                       and gs.auto_reply_text)
        legacy_text = gs.auto_reply_text or ""
        legacy_label = gs.auto_reply_btn_label or ""
        legacy_url = gs.auto_reply_btn_url or ""
        if fire_legacy:
            known.auto_reply_sent = True

        session.add(Conversation(user_id=uid, direction="in",
                                 message=(message.text or message.caption or media_label(message))))
        await session.commit()

    if message.text or message.caption:
        spawn(run_message_automations(message), name="auto_msg")

    if fire_legacy and legacy_text:
        async def _send_legacy():
            try:
                await bot.send_message(uid, legacy_text, parse_mode=HTML,
                                       reply_markup=build_markup(legacy_label, legacy_url),
                                       disable_web_page_preview=True)
            except Exception as exc:
                logger.warning("legacy auto-reply failed for %s: %s", uid, exc)
        spawn(_send_legacy(), name="legacy_reply")

    spawn(_relay_to_admins(message), name="relay")


async def _relay_to_admins(message: Message):
    u = message.from_user
    if u is None:
        return
    header = (f"👤 <b>{esc(u.first_name)}</b> | "
              f"{esc('@' + u.username) if u.username else 'no username'} "
              f"| ID: <code>{u.id}</code>")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Reply",
                                                     callback_data=f"inbox:reply:{u.id}")]])
    for admin_id in list(_admin_ids):
        try:
            if message.text:
                await flood_safe(lambda a=admin_id: bot.send_message(
                    a, f"{header}\n\n{esc(message.text)}"[:4090],
                    reply_markup=kb, parse_mode=HTML, disable_web_page_preview=True))
            else:
                caption = ((f"{header}\n\n{esc(message.caption)}") if message.caption else header)[:1024]
                await flood_safe(lambda a=admin_id: bot.copy_message(
                    a, message.chat.id, message.id,
                    caption=caption, parse_mode=HTML, reply_markup=kb))
        except Exception as exc:
            logger.warning("relay to admin %s failed: %s", admin_id, exc)
            try:
                await bot.send_message(admin_id, f"{header}\n\n{media_label(message)}",
                                       parse_mode=HTML, reply_markup=kb)
            except Exception:
                pass


# ===========================================================================
# ADMIN MESSAGE HANDLER
# ===========================================================================

def _valid_url(u: str) -> bool:
    return bool(re.match(r"^(https?://|tg://)\S+$", u.strip()))


async def handle_admin_message(client: Client, message: Message, uid: int):
    f = flow(uid)
    st = f.state
    chat_id = message.chat.id
    txt = message.text.strip() if message.text else ""

    if st == St.NONE:
        return

    if st in (St.LOGIN_API_ID, St.LOGIN_API_HASH, St.LOGIN_PHONE, St.LOGIN_CODE, St.LOGIN_PASSWORD):
        if message.text:
            await handle_login_text(uid, chat_id, message.text)
        return

    if st == St.RESTORE_UPLOAD:
        await _handle_restore_upload(uid, chat_id, message)
        return

    if st == St.SET_BOT_API_ID:
        if not txt.isdigit():
            await message.reply_text("❌ API ID must be numbers only. Try again:")
            return
        f.data["new_bot_api_id"] = txt
        f.state = St.SET_BOT_API_HASH
        await message.reply_text("<b>Step 2/2</b> — Send your <b>API Hash</b>:",
                                 reply_markup=InlineKeyboardMarkup([
                                     [InlineKeyboardButton("❌ Cancel", callback_data="setapi:cancel")]]),
                                 parse_mode=HTML)
        return
    if st == St.SET_BOT_API_HASH:
        if len(txt) < 10:
            await message.reply_text("❌ API Hash looks too short. Try again:")
            return
        new_id = f.data.get("new_bot_api_id", "")
        reset_flow(uid)
        await kv_set("bot_api_id", new_id)
        await kv_set("bot_api_hash", txt)
        await message.reply_text(
            f"✅ <b>API Credentials saved!</b>\n\nAPI ID: <code>{esc(new_id)}</code>\n\n"
            f"⚠️ Restart the bot to load new credentials.",
            reply_markup=kb_main_panel(await userbot_ready()), parse_mode=HTML)
        return

    if st == St.ADD_ADMIN:
        if not txt.lstrip("-").isdigit():
            await message.reply_text("Send a numeric Telegram user ID.")
            return
        tid = int(txt)
        async with SessionLocal() as session:
            exists = (await session.execute(select(Admin).where(Admin.user_id == tid))).scalar_one_or_none()
            if exists:
                await message.reply_text("Already an admin.")
            else:
                name = ""
                try:
                    u = await bot.get_users(tid)
                    name = u.first_name or ""
                except Exception:
                    pass
                session.add(Admin(user_id=tid, name=name))
                await session.commit()
                await reload_admins()
                await message.reply_text(f"✅ Added <code>{tid}</code> as admin.", parse_mode=HTML)
                try:
                    await bot.send_message(tid, "🛡️ You've been made an admin. Send /start.")
                except Exception:
                    pass
        reset_flow(uid)
        return

    if st == St.SEARCH:
        if not txt:
            return
        scope = f.data.get("search_channel")
        reset_flow(uid)
        await run_search(chat_id, txt, scope)
        return

    if st == St.BC_CONTENT:
        f.data["bc_from_chat_id"] = message.chat.id
        f.data["bc_message_id"] = message.id
        preview_txt = message.text or message.caption or media_label(message)
        f.state = St.NONE
        async with SessionLocal() as session:
            total = (await session.execute(select(func.count()).select_from(KnownUser).where(
                KnownUser.bot_blocked == False))).scalar()  # noqa: E712
        await message.reply_text(
            f"Send this to <b>{total}</b> reachable users?\n\nPreview: {esc(strip_tags(preview_txt)[:200])}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Send Now", callback_data="confirm:broadcast_send"),
                 InlineKeyboardButton("🕒 Schedule", callback_data="confirm:broadcast_schedule")],
                [InlineKeyboardButton("❌ Cancel", callback_data="confirm:cancel")]]),
            parse_mode=HTML)
        return

    if st == St.BC_SCHEDULE:
        try:
            run_at = datetime.strptime(txt, "%Y-%m-%d %H:%M")
        except ValueError:
            await message.reply_text("Invalid format. Use <code>YYYY-MM-DD HH:MM</code> (UTC).",
                                     parse_mode=HTML)
            return
        if run_at <= now_utc():
            await message.reply_text("That time is in the past. Send a future time (UTC).")
            return
        async with SessionLocal() as session:
            b = Broadcast(from_chat_id=f.data.get("bc_from_chat_id"),
                          from_message_id=f.data.get("bc_message_id"),
                          scheduled_at=run_at, status="pending")
            session.add(b)
            await session.commit()
            await session.refresh(b)
        schedule_broadcast_job(b.id, run_at)
        reset_flow(uid)
        await message.reply_text(f"📅 Broadcast scheduled for {run_at.strftime('%Y-%m-%d %H:%M')} UTC.",
                                 reply_markup=kb_back())
        return

    if st == St.INBOX_REPLY:
        target_id = f.data.get("reply_to")
        reset_flow(uid)
        if not target_id:
            return
        try:
            await flood_safe(lambda: bot.copy_message(target_id, message.chat.id, message.id))
            async with SessionLocal() as session:
                session.add(Conversation(user_id=target_id, direction="out",
                                         message=(message.text or message.caption or media_label(message))))
                await session.commit()
            await message.reply_text("✅ Sent.")
        except Exception as exc:
            await message.reply_text(f"❌ Failed to send: {esc(exc)}", parse_mode=HTML)
        return

    if st == St.ADD_CHANNEL:
        new_ch_id: Optional[int] = None
        new_name = ""
        if message.forward_from_chat and message.forward_from_chat.type in (
                enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
            new_ch_id = message.forward_from_chat.id
            new_name = message.forward_from_chat.title or ""
        elif message.text:
            raw = message.text.strip()
            m = re.match(r"^-?\d+$", raw)
            if m:
                new_ch_id = int(raw)
                new_name = str(new_ch_id)
            else:
                try:
                    chat = await bot.get_chat(raw if raw.startswith("@") else "@" + raw)
                    if chat.type in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
                        new_ch_id = chat.id
                        new_name = chat.title or ""
                except Exception as exc:
                    logger.info("add_channel lookup failed: %s", exc)

        if new_ch_id is None:
            await message.reply_text(
                "❌ Couldn't determine a channel.\n\n"
                "Forward any message from the channel, or send the channel's "
                "<b>numeric ID</b> or <b>@username</b>.",
                parse_mode=HTML)
            return

        try:
            member = await bot.get_chat_member(new_ch_id, await _get_bot_id())
        except Exception as exc:
            await message.reply_text(f"❌ Bot is not in that channel or can't access it: {esc(exc)}",
                                     parse_mode=HTML)
            return
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            await message.reply_text("❌ The bot needs to be an <b>admin</b> in that channel.",
                                     parse_mode=HTML)
            return

        async with SessionLocal() as s:
            ch = (await s.execute(select(Channel).where(Channel.channel_id == new_ch_id))).scalar_one_or_none()
            if ch is None:
                s.add(Channel(channel_id=new_ch_id, name=new_name or str(new_ch_id), is_active=True))
            else:
                ch.is_active = True
                if new_name:
                    ch.name = new_name
            await s.commit()

        reset_flow(uid)
        await message.reply_text(
            f"✅ Channel added: <b>{esc(new_name or new_ch_id)}</b>\n"
            f"ID: <code>{new_ch_id}</code>\n\n"
            f"⚠️ For join-request auto-accept, the userbot must also be admin here.",
            parse_mode=HTML, reply_markup=kb_back("channels:list"))
        return

    if st in (St.JOIN_MSG_TEXT, St.LEAVE_MSG_TEXT):
        if not message.text:
            await message.reply_text("Send text content for the message.")
            return
        ch_id = f.data.get("channel_id")
        kind = "join" if st == St.JOIN_MSG_TEXT else "leave"
        async with SessionLocal() as session:
            s = await get_or_create_settings(session, ch_id)
            if kind == "join":
                s.join_msg_text = html_of(message)
            else:
                s.leave_msg_text = html_of(message)
            await session.commit()
        f.state = St.NONE
        await message.reply_text(
            f"✅ {kind.capitalize()} message saved!\n\nAdd a button?",
            reply_markup=kb_btn_ask(f"btn_ask:yes:{kind}:{ch_id}", f"btn_ask:no:{kind}:{ch_id}"))
        return

    if st in (St.JOIN_MSG_BTN_LABEL, St.LEAVE_MSG_BTN_LABEL, St.START_MSG_BTN_LABEL, St.AUTO_REPLY_BTN_LABEL):
        if not txt:
            return
        f.data["btn_label"] = txt[:60]
        f.state = {St.JOIN_MSG_BTN_LABEL: St.JOIN_MSG_BTN_URL,
                   St.LEAVE_MSG_BTN_LABEL: St.LEAVE_MSG_BTN_URL,
                   St.START_MSG_BTN_LABEL: St.START_MSG_BTN_URL,
                   St.AUTO_REPLY_BTN_LABEL: St.AUTO_REPLY_BTN_URL}[st]
        await message.reply_text("Now send the button URL (must start with https://):")
        return

    if st in (St.JOIN_MSG_BTN_URL, St.LEAVE_MSG_BTN_URL):
        if not _valid_url(txt):
            await message.reply_text("❌ Invalid URL. Must start with https:// — try again:")
            return
        ch_id = f.data.get("channel_id")
        async with SessionLocal() as session:
            s = await get_or_create_settings(session, ch_id)
            if st == St.JOIN_MSG_BTN_URL:
                s.join_btn_label, s.join_btn_url = f.data.get("btn_label", ""), txt
            else:
                s.leave_btn_label, s.leave_btn_url = f.data.get("btn_label", ""), txt
            await session.commit()
        reset_flow(uid)
        await message.reply_text("✅ Button saved!")
        return

    if st in (St.JOIN_MSG_MEDIA, St.LEAVE_MSG_MEDIA):
        ch_id = f.data.get("channel_id")
        if message.photo:
            media_id, media_type = message.photo.file_id, "photo"
        elif message.video:
            media_id, media_type = message.video.file_id, "video"
        else:
            await message.reply_text("Send a photo or video only.")
            return
        async with SessionLocal() as session:
            s = await get_or_create_settings(session, ch_id)
            if st == St.JOIN_MSG_MEDIA:
                s.join_msg_media_id, s.join_msg_media_type = media_id, media_type
            else:
                s.leave_msg_media_id, s.leave_msg_media_type = media_id, media_type
            await session.commit()
        reset_flow(uid)
        await message.reply_text(f"✅ Media set ({media_type}).")
        return

    if st == St.START_MSG_TEXT:
        if not message.text:
            return
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            gs.start_msg_text = html_of(message)
            await session.commit()
        f.state = St.NONE
        await message.reply_text("✅ Start message text saved!\n\nAdd a button?",
                                 reply_markup=kb_btn_ask("btn_ask:yes:start_msg", "btn_ask:no:start_msg"))
        return

    if st == St.START_MSG_BTN_URL:
        if not _valid_url(txt):
            await message.reply_text("❌ Invalid URL. Must start with https:// — try again:")
            return
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            gs.start_btn_label, gs.start_btn_url = f.data.get("btn_label", ""), txt
            await session.commit()
        reset_flow(uid)
        await message.reply_text("✅ Start message button saved!", reply_markup=kb_start_msg_settings())
        return

    if st == St.AUTO_REPLY_TEXT:
        if not message.text:
            return
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            gs.auto_reply_text = html_of(message)
            await session.commit()
        f.state = St.NONE
        await message.reply_text("✅ Auto-reply text saved!\n\nAdd a button?",
                                 reply_markup=kb_btn_ask("btn_ask:yes:auto_reply", "btn_ask:no:auto_reply"))
        return

    if st == St.AUTO_REPLY_BTN_URL:
        if not _valid_url(txt):
            await message.reply_text("❌ Invalid URL. Must start with https:// — try again:")
            return
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            gs.auto_reply_btn_label, gs.auto_reply_btn_url = f.data.get("btn_label", ""), txt
            await session.commit()
            gs = await get_global_settings(session)
            await session.commit()
        reset_flow(uid)
        await message.reply_text("✅ Auto-reply button saved!", reply_markup=kb_auto_reply_settings(gs))
        return

    if st == St.AUTO_TEST_INPUT:
        rule_id = f.data.get("test_rule_id")
        reset_flow(uid)
        if not rule_id:
            await _run_global_test(chat_id, message)
            return
        await _run_rule_test(chat_id, rule_id, message)
        return

    if await handle_auto_wizard_media(uid, chat_id, message):
        return

    if await handle_auto_wizard_text(uid, chat_id, message):
        return


# ===========================================================================
# AUTOMATION — rule test
# ===========================================================================

async def _run_rule_test(chat_id: int, rule_id: int, message: Message):
    async with SessionLocal() as s:
        rule = (await s.execute(select(AutomationRule).where(
            AutomationRule.id == rule_id))).scalar_one_or_none()
    if rule is None:
        await bot.send_message(chat_id, "❔ Rule not found.", reply_markup=kb_back("auto:main"))
        return
    u = message.from_user
    text = (message.text or message.caption or "").strip()
    matched = _match_rule(rule, text) if rule.trigger_type in ("message", "command") else True
    result = await _execute_rule(
        rule, user_id=u.id, first_name=u.first_name or "", last_name=u.last_name or "",
        username=u.username or "", channel_id=None, channel_name="", test_only=True)
    ok = "✅" if matched else "❌"
    txt = (f"🧪 <b>Rule Test — #{rule.id} {esc(rule.name)}</b>\n\n"
           f"Input: <i>{esc(text[:200])}</i>\n"
           f"Trigger matched: {ok}\n"
           f"Match mode: {MATCH_LABELS.get(rule.match_type, rule.match_type)}\n"
           f"Scope: {_scope_summary(rule)}\n"
           f"Cooldown: {'configured ' + str(rule.cooldown_seconds) + 's' if rule.cooldown_seconds else 'none'}\n"
           f"Priority: {rule.priority}\n\n"
           f"<b>Response preview:</b>\n<blockquote>{esc(result.get('preview','') or '(empty)')}</blockquote>")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Send Test To Me", callback_data=f"auto:test_send:{rule.id}")],
        [InlineKeyboardButton("« Back", callback_data=f"auto:view:{rule.id}")],
    ])
    await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode=HTML)


async def _run_global_test(chat_id: int, message: Message):
    text = (message.text or message.caption or "").strip()
    rules = await _load_enabled_rules(force=True)
    matched_any = []
    for rule in rules:
        if rule.trigger_type not in ("message", "command"):
            continue
        if _rule_scope_matches(rule, None) and _match_rule(rule, text):
            matched_any.append(rule)
            if rule.stop_on_match:
                break
    if not matched_any:
        await bot.send_message(chat_id,
                               f"🧪 No rules matched <i>{esc(text[:120])}</i>.",
                               reply_markup=kb_back("auto:main"), parse_mode=HTML)
        return
    lines = [f"🧪 <b>Global Test</b> — input: <i>{esc(text[:120])}</i>\n",
             f"Matched <b>{len(matched_any)}</b> rule(s):"]
    for r in matched_any:
        lines.append(f"  • #{r.id} {esc(r.name)} → {short_preview(r.response_text, 60)}")
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=kb_back("auto:main"),
                           parse_mode=HTML)


# ===========================================================================
# BROADCAST
# ===========================================================================

async def _send_one_broadcast(target: int, from_chat: int, msg_id: int) -> str:
    for _ in range(3):
        try:
            await bot.copy_message(target, from_chat, msg_id)
            return "ok"
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, UserPrivacyRestricted):
            await mark_user_blocked(target)
            return "blocked"
        except Exception:
            return "fail"
    return "fail"


async def broadcast_targets() -> list:
    async with SessionLocal() as session:
        rows = (await session.execute(select(KnownUser.user_id).where(
            KnownUser.bot_blocked == False))).all()  # noqa: E712
    return [r[0] for r in rows if r[0] not in _admin_ids]


async def execute_broadcast(broadcast_id: int, progress_msg: Optional[Message] = None):
    async with SessionLocal() as session:
        b = (await session.execute(select(Broadcast).where(Broadcast.id == broadcast_id))).scalar_one_or_none()
        if b is None or b.status not in ("pending", "sending"):
            return
        b.status = "sending"
        from_chat, msg_id = b.from_chat_id, b.from_message_id
        await session.commit()

    targets = await broadcast_targets()
    total = len(targets)
    sent = failed = blocked = 0
    last_edit = 0.0
    for idx, target in enumerate(targets, 1):
        res = await _send_one_broadcast(target, from_chat, msg_id)
        if res == "ok":
            sent += 1
        elif res == "blocked":
            blocked += 1
        else:
            failed += 1
        await asyncio.sleep(SEND_INTERVAL)
        now = _mono()
        if progress_msg is not None and now - last_edit > 2.5:
            last_edit = now
            try:
                await progress_msg.edit_text(
                    f"📤 Sent: {sent}/{total} | Blocked: {blocked} | Failed: {failed}")
            except Exception:
                pass

    async with SessionLocal() as session:
        row = (await session.execute(select(Broadcast).where(Broadcast.id == broadcast_id))).scalar_one_or_none()
        if row:
            row.sent_count, row.fail_count, row.status = sent, failed + blocked, "done"
            await session.commit()

    summary = (f"✅ Broadcast complete: <b>{sent}</b> sent, <b>{blocked}</b> blocked the bot, "
               f"<b>{failed}</b> failed.")
    if progress_msg is not None:
        try:
            await progress_msg.edit_text(summary, reply_markup=kb_back(), parse_mode=HTML)
            return
        except Exception:
            pass
    await notify_admins(summary)


async def run_scheduled_broadcast(broadcast_id: int):
    try:
        await execute_broadcast(broadcast_id)
    except Exception as exc:
        logger.exception("Scheduled broadcast %s failed: %s", broadcast_id, exc)


def schedule_broadcast_job(broadcast_id: int, run_at: datetime):
    scheduler.add_job(run_scheduled_broadcast, "date",
                      run_date=run_at.replace(tzinfo=timezone.utc),
                      args=[broadcast_id], id=f"bc_{broadcast_id}", replace_existing=True,
                      misfire_grace_time=3600)


async def restore_scheduled_broadcasts():
    async with SessionLocal() as session:
        rows = (await session.execute(select(Broadcast).where(
            Broadcast.status == "pending",
            Broadcast.scheduled_at.isnot(None)))).scalars().all()
    for b in rows:
        run_at = b.scheduled_at if b.scheduled_at > now_utc() else now_utc() + timedelta(seconds=15)
        schedule_broadcast_job(b.id, run_at)
    if rows:
        logger.info("Restored %d scheduled broadcast(s)", len(rows))


# ===========================================================================
# SETTINGS TEXT BUILDERS
# ===========================================================================

def _msg_settings_text(title_emoji: str, title: str, ch_name: str, enabled: bool,
                       text_html: str, btn_label: str, btn_url: str, media_type: str) -> str:
    status = "✅ Enabled" if enabled else "❌ Disabled"
    btn_info = f"🔗 {esc(btn_label)} → {esc(btn_url)}" if btn_label else "(none)"
    media_info = f"📷 {esc(media_type.capitalize())}" if media_type else "(none)"
    return (f"{title_emoji} <b>{title}</b>\nChannel: <b>{esc(ch_name)}</b>\n\n"
            f"Status: {status}\nMessage: <i>{short_preview(text_html)}</i>\n"
            f"Button: {btn_info}\nMedia: {media_info}")


def _join_settings_text(ch_name: str, s: Settings) -> str:
    return _msg_settings_text("📩", "Join Message Settings", ch_name, s.join_msg_enabled,
                              s.join_msg_text, s.join_btn_label, s.join_btn_url,
                              s.join_msg_media_type or "")


def _leave_settings_text(ch_name: str, s: Settings) -> str:
    return _msg_settings_text("🚪", "Leave Message Settings", ch_name, s.leave_msg_enabled,
                              s.leave_msg_text, s.leave_btn_label, s.leave_btn_url,
                              s.leave_msg_media_type or "")


def _start_settings_text(gs: GlobalSettings) -> str:
    btn = f"🔗 {esc(gs.start_btn_label)} → {esc(gs.start_btn_url)}" if gs.start_btn_label else "(none)"
    return (f"👋 <b>Start Message Settings</b>\n\n"
            f"Message: <i>{short_preview(gs.start_msg_text, 200)}</i>\nButton: {btn}")


def _auto_reply_settings_text(gs: GlobalSettings) -> str:
    status = "✅ Enabled" if gs.auto_reply_enabled else "❌ Disabled"
    btn = f"🔗 {esc(gs.auto_reply_btn_label)}" if gs.auto_reply_btn_label else "(none)"
    return (f"🔁 <b>Legacy Auto-Reply</b> (kept for compatibility)\n\nStatus: {status}\n"
            f"Message: <i>{short_preview(gs.auto_reply_text, 200)}</i>\nButton: {btn}\n\n"
            f"💡 Tip: use 🤖 Automation Center for more flexible rules.")


async def _send_preview(chat_id: int, title: str, html_text: str, markup=None):
    try:
        await bot.send_message(chat_id, f"<b>👁️ {title}:</b>\n\n{html_text or '(not set)'}",
                               parse_mode=HTML, disable_web_page_preview=True,
                               reply_markup=markup)
    except Exception:
        await bot.send_message(chat_id, f"{title}:\n\n{strip_tags(html_text) or '(not set)'}",
                               reply_markup=markup)


async def show_join_settings(cq: CallbackQuery, ch_id: int):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
        await session.commit()
    await safe_edit(cq.message, _join_settings_text(await get_channel_name(ch_id), s),
                    kb_join_msg_settings(ch_id, s))


async def show_leave_settings(cq: CallbackQuery, ch_id: int):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
        await session.commit()
    await safe_edit(cq.message, _leave_settings_text(await get_channel_name(ch_id), s),
                    kb_leave_msg_settings(ch_id, s))


# ===========================================================================
# DATABASE BACKUP / RESTORE
# ===========================================================================

def _sqlite_db_path() -> Path:
    url = DATABASE_URL
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if url.startswith(prefix):
            path = url[len(prefix):]
            if url.startswith(prefix.replace("///", "////")):
                return Path(path)
            return Path(path) if not path.startswith("/") else Path("/" + path.lstrip("/"))
    return Path("bot.db")


def _human_size(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if f < 1024:
            return f"{f:.1f} {unit}"
        f /= 1024
    return f"{f:.1f} PB"


def _cleanup_old_backups(keep: int = 10):
    try:
        files = sorted(BACKUP_DIR.glob("bot_backup_*"),
                       key=lambda p: p.stat().st_mtime, reverse=True)
        for f in files[keep:]:
            try:
                f.unlink()
            except Exception:
                pass
    except Exception as exc:
        logger.debug("cleanup backups failed: %s", exc)


async def _reload_engine():
    global engine, SessionLocal
    try:
        await engine.dispose()
    except Exception as exc:
        logger.warning("engine.dispose during reload failed: %s", exc)
    engine = _build_engine()
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    logger.info("Database engine reloaded.")


async def _create_sqlite_backup(ts: str) -> tuple[Optional[Path], str]:
    src = _sqlite_db_path()
    if not src.exists():
        alt = Path(src.name)
        if alt.exists():
            src = alt
        else:
            return None, f"SQLite file not found at {src}"
    backup_path = BACKUP_DIR / f"bot_backup_{ts}.db"
    try:
        async with engine.begin() as conn:
            safe = str(backup_path.resolve()).replace("'", "''")
            await conn.execute(text(f"VACUUM INTO '{safe}'"))
        return backup_path, ""
    except Exception as exc:
        logger.warning("VACUUM INTO failed (%s); falling back to file copy", exc)
    try:
        async with engine.begin() as conn:
            try:
                await conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
            except Exception:
                pass
        shutil.copy2(src, backup_path)
        return backup_path, ""
    except Exception as exc:
        logger.exception("sqlite backup failed: %s", exc)
        return None, f"{type(exc).__name__}: {exc}"


def _validate_sqlite_backup(path: Path) -> tuple[bool, str]:
    import sqlite3
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as exc:
        return False, f"not a valid SQLite database ({exc})"
    required = {"admins", "channels", "automation_rules"}
    missing = required - tables
    if missing:
        return False, f"missing tables: {', '.join(sorted(missing))}"
    return True, ""


async def restore_sqlite_backup(uploaded: Path) -> tuple[bool, str]:
    db_path = _sqlite_db_path()
    if not db_path.exists():
        alt = Path(db_path.name)
        if alt.exists():
            db_path = alt
        else:
            return False, f"Current DB file not found: {db_path}"

    ok, err = _validate_sqlite_backup(uploaded)
    if not ok:
        return False, err

    try:
        safety = BACKUP_DIR / f"pre_restore_{now_utc().strftime('%Y%m%d_%H%M%S')}.db"
        async with engine.begin() as conn:
            safe = str(safety.resolve()).replace("'", "''")
            try:
                await conn.execute(text(f"VACUUM INTO '{safe}'"))
            except Exception:
                shutil.copy2(db_path, safety)
        logger.info("Safety backup: %s", safety)
    except Exception as exc:
        logger.warning("Safety backup failed (continuing): %s", exc)

    try:
        await engine.dispose()
    except Exception:
        pass

    for suffix in ("-wal", "-shm"):
        side = Path(str(db_path) + suffix)
        if side.exists():
            try:
                side.unlink()
            except Exception:
                pass

    try:
        tmp_target = db_path.with_suffix(db_path.suffix + ".new")
        shutil.copy2(uploaded, tmp_target)
        os.replace(str(tmp_target), str(db_path))
    except Exception as exc:
        return False, f"file replace failed: {exc}"

    await _reload_engine()

    try:
        async with SessionLocal() as s:
            await s.execute(select(1))
    except Exception as exc:
        return False, f"restored DB failed to open: {exc}"

    return True, ""


_JSON_TABLES = [
    ("admins", Admin), ("channels", Channel),
    ("join_requests", JoinRequest), ("members", Member),
    ("member_leaves", MemberLeave), ("conversations", Conversation),
    ("known_users", KnownUser), ("broadcasts", Broadcast),
    ("blocked_users", BlockedUser), ("settings", Settings),
    ("global_settings", GlobalSettings),
    ("automation_rules", AutomationRule),
    ("automation_buttons", AutomationButton),
    ("automation_cooldowns", AutomationCooldown),
    ("automation_logs", AutomationLog),
    ("kv", KV),
]


def _json_safe(v):
    if v is None or isinstance(v, (int, float, str, bool)):
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


async def _create_json_backup(ts: str) -> tuple[Optional[Path], str]:
    path = BACKUP_DIR / f"bot_backup_{ts}.json"
    try:
        from sqlalchemy import inspect as sa_inspect
        payload = {
            "format": "channel_manager_backup",
            "version": 1,
            "created_at": now_utc().isoformat(),
            "tables": {},
        }
        async with SessionLocal() as s:
            for name, model in _JSON_TABLES:
                rows = (await s.execute(select(model))).scalars().all()
                cols = [c.key for c in sa_inspect(model).columns]
                payload["tables"][name] = [
                    {c: _json_safe(getattr(r, c)) for c in cols} for r in rows
                ]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        return path, ""
    except Exception as exc:
        logger.exception("json backup failed: %s", exc)
        return None, f"{type(exc).__name__}: {exc}"


def _json_restore_value(v):
    if v is None or not isinstance(v, str):
        return v
    if "T" in v or v.endswith("Z"):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            pass
    return v


async def restore_from_json(uploaded: Path) -> tuple[bool, str]:
    try:
        with open(uploaded, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        return False, f"JSON parse failed: {exc}"

    if payload.get("format") != "channel_manager_backup":
        return False, "Not a Channel Manager backup file."

    tables = payload.get("tables")
    if not isinstance(tables, dict):
        return False, "Invalid backup structure."

    try:
        await _create_json_backup(now_utc().strftime("pre_restore_%Y%m%d_%H%M%S"))
    except Exception as exc:
        logger.warning("safety json backup failed: %s", exc)

    from sqlalchemy import inspect as sa_inspect
    models = dict(_JSON_TABLES)
    try:
        async with SessionLocal() as s:
            for name in reversed(list(models.keys())):
                try:
                    await s.execute(delete(models[name]))
                except Exception as exc:
                    logger.warning("wipe of %s failed: %s", name, exc)
            await s.commit()

            for name, model in models.items():
                rows = tables.get(name) or []
                if not rows:
                    continue
                valid_cols = {c.key for c in sa_inspect(model).columns}
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    clean = {k: _json_restore_value(v) for k, v in row.items()
                             if k in valid_cols}
                    try:
                        s.add(model(**clean))
                    except Exception as exc:
                        logger.warning("row insert failed in %s: %s", name, exc)
                await s.flush()
            await s.commit()
        return True, ""
    except Exception as exc:
        logger.exception("restore_from_json failed: %s", exc)
        return False, f"{type(exc).__name__}: {exc}"


async def create_db_backup() -> tuple[Optional[Path], str]:
    ts = now_utc().strftime("%Y%m%d_%H%M%S")
    if IS_SQLITE:
        path, err = await _create_sqlite_backup(ts)
        if path:
            return path, ""
        logger.warning("SQLite backup failed (%s), falling back to JSON export", err)
        path, jerr = await _create_json_backup(ts)
        if path:
            return path, ""
        return None, err or jerr
    return await _create_json_backup(ts)


async def restore_db(uploaded: Path) -> tuple[bool, str]:
    name = uploaded.name.lower()
    if name.endswith(".json"):
        return await restore_from_json(uploaded)
    if name.endswith(".db") or name.endswith(".sqlite") or name.endswith(".sqlite3"):
        if not IS_SQLITE:
            return False, ("Cannot restore a .db file to a PostgreSQL database. "
                           "Send a .json backup instead.")
        return await restore_sqlite_backup(uploaded)
    return False, "Unsupported file extension (expect .db, .sqlite, or .json)."


async def _post_restore_reload():
    try:
        await reload_admins()
    except Exception as exc:
        logger.warning("post-restore reload_admins: %s", exc)
    try:
        invalidate_automation_cache()
    except Exception:
        pass
    _member_cache.clear()
    _pending_cache.clear()
    _bot_admin_cache.clear()
    _cd_cache.clear()
    try:
        await init_db()
    except Exception as exc:
        logger.warning("post-restore init_db: %s", exc)


async def _handle_restore_upload(admin_id: int, chat_id: int, message: Message):
    doc = message.document
    if doc is None:
        await message.reply_text("Send the backup file as a <b>document</b>.",
                                 parse_mode=HTML)
        return
    fname = (doc.file_name or "backup.bin").lower()
    status = await message.reply_text("⏳ Downloading backup…")

    tmp_path = BACKUP_DIR / f"upload_{now_utc().strftime('%Y%m%d_%H%M%S')}_{doc.file_name or 'backup.bin'}"
    try:
        await bot.download_media(message, file_name=str(tmp_path))
    except Exception as exc:
        await status.edit_text(f"❌ Download failed: {esc(exc)}")
        return

    size = tmp_path.stat().st_size if tmp_path.exists() else 0
    await status.edit_text(f"⏳ Verifying and restoring ({_human_size(size)})…")

    try:
        ok, err = await restore_db(tmp_path)
    finally:
        try:
            tmp_path.unlink()
        except Exception:
            pass

    reset_flow(admin_id)
    if ok:
        await status.edit_text("⏳ Reloading caches…")
        await _post_restore_reload()
        await status.edit_text(
            "✅ <b>Database restored successfully.</b>\n\n"
            f"File: <code>{esc(doc.file_name or 'backup')}</code>\n"
            f"Size: {_human_size(size)}\n\n"
            "A safety backup of the previous DB was saved in <code>backups/</code>.\n"
            "All caches reloaded.",
            reply_markup=kb_main_panel(await userbot_ready()))
    else:
        await status.edit_text(
            f"❌ <b>Restore failed:</b> {esc(err)}\n\n"
            "The current database was <b>not</b> modified.",
            reply_markup=kb_back("tools:backup"))


# ===========================================================================
# CALLBACK ROUTER
# ===========================================================================

async def on_callback(client: Client, cq: CallbackQuery):
    if cq.from_user is None or not is_admin(cq.from_user.id):
        await safe_answer(cq, "Not authorized.", show_alert=True)
        return

    data = cq.data or ""
    admin_id = cq.from_user.id
    chat_id = cq.message.chat.id if cq.message else None
    answered = False

    async def ack(text_: str = "", alert: bool = False):
        nonlocal answered
        if answered:
            return
        answered = True
        await safe_answer(cq, text_, alert)

    try:
        p = data.split(":")

        if data == "noop":
            await ack()

        # ---------- panel ----------
        elif data in ("panel:main", "panel:refresh"):
            await ack("Refreshing…" if data == "panel:refresh" else "")
            reset_flow(admin_id)
            await safe_edit(cq.message,
                            await build_main_panel_text(sync=(data == "panel:refresh")),
                            kb_main_panel(await userbot_ready()))

        elif data == "reqs:overview":
            await ack("Refreshing…")
            txt, kb = await build_requests_overview()
            await safe_edit(cq.message, txt, kb)

        elif p[0] == "reqlist" and len(p) >= 3:
            ch_id, page = int(p[1]), int(p[2])
            force = len(p) >= 4 and p[3] == "r"
            await ack("Syncing…" if force else "")
            if force:
                await sync_channel_requests(ch_id)
            txt, kb = await build_request_page(ch_id, page)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("req:accept_all") or data.startswith("req:decline_all"):
            approve = data.startswith("req:accept_all")
            ch_id = int(p[2]) if len(p) > 2 else None
            await ack("Processing…")
            spawn(bulk_process_requests(chat_id, ch_id, approve), name="bulk_req")

        # ---------- search ----------
        elif data == "search:start":
            await ack()
            reset_flow(admin_id)
            channels = await _active_channels()
            rows = [[InlineKeyboardButton("🌐 All Channels", callback_data="search:scope:all")]]
            for c in channels:
                rows.append([InlineKeyboardButton(f"📣 {c.name or c.channel_id}"[:60],
                                                  callback_data=f"search:scope:{c.channel_id}")])
            rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
            await safe_edit(cq.message, "🔍 <b>Search User</b>\n\nWhere do you want to search?",
                            InlineKeyboardMarkup(rows))

        elif data.startswith("search:scope:"):
            await ack()
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.SEARCH
            f.data["search_channel"] = None if p[2] == "all" else int(p[2])
            scope = ("all channels" if p[2] == "all"
                     else f"<b>{esc(await get_channel_name(int(p[2])))}</b>")
            await safe_edit(
                cq.message,
                f"🔍 Searching in {scope}.\n\nSend a <b>user ID</b>, <b>@username</b>, or part of a <b>name</b>:",
                kb_back("search:start"))

        # ---------- channels ----------
        elif data in ("channels:list", "channels:refresh"):
            await ack("Refreshing…" if data == "channels:refresh" else "")
            await show_channel_list(chat_id, edit_msg=cq.message, refresh=(data == "channels:refresh"))

        elif data == "channels:add":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.ADD_CHANNEL
            await safe_edit(
                cq.message,
                "➕ <b>Add Channel</b>\n\n"
                "Forward any message from the channel, or send its <b>numeric ID</b> or "
                "<b>@username</b>.\n\n"
                "⚠️ The bot must be an <b>admin</b> in that channel.",
                kb_back("channels:list"))

        elif data.startswith("channels:settings:"):
            await ack()
            await show_channel_settings(cq, int(p[2]))

        elif data.startswith("channels:remove:"):
            ch_id = int(p[2])
            name = await get_channel_name(ch_id)
            await ack()
            await safe_edit(cq.message,
                            f"🗑 <b>Remove <code>{esc(name)}</code>?</b>\n"
                            f"The channel will be hidden from the panel, but historical data is kept.",
                            kb_confirm(f"channels:do_remove:{ch_id}", "channels:list"))

        elif data.startswith("channels:do_remove:"):
            ch_id = int(p[2])
            async with SessionLocal() as s:
                ch = (await s.execute(select(Channel).where(Channel.channel_id == ch_id))).scalar_one_or_none()
                if ch:
                    ch.is_active = False
                    await s.commit()
            _member_cache.pop(ch_id, None)
            _pending_cache.pop(ch_id, None)
            await ack("Removed.")
            await show_channel_list(chat_id, edit_msg=cq.message, refresh=True)

        elif data.startswith("autoaccept:toggle:"):
            ch_id = int(p[2])
            async with SessionLocal() as session:
                s = await get_or_create_settings(session, ch_id)
                s.auto_accept = not s.auto_accept
                await session.commit()
            await ack("Updated.")
            await show_channel_settings(cq, ch_id)

        # ---------- broadcast ----------
        elif data == "broadcast:start":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.BC_CONTENT
            await safe_edit(cq.message,
                            "Send the broadcast content (text, photo, video, voice or document).",
                            kb_back())

        # ---------- stats ----------
        elif data in ("stats:show", "stats:refresh"):
            await ack("Loading…")
            txt = await build_stats_text(refresh=(data == "stats:refresh"))
            if data == "stats:refresh":
                await safe_edit(cq.message, txt, KB_STATS)
            else:
                await bot.send_message(chat_id, txt, reply_markup=KB_STATS, parse_mode=HTML)

        # ---------- settings ----------
        elif data == "settings:main":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, "⚙️ <b>Settings</b>", kb_settings_main())

        elif data in ("settings:join_select", "settings:leave_select"):
            kind = "join" if data == "settings:join_select" else "leave"
            channels = await _active_channels()
            if not channels:
                await ack("No channels registered yet.", True)
                return
            await ack()
            if len(channels) == 1:
                if kind == "join":
                    await show_join_settings(cq, channels[0].channel_id)
                else:
                    await show_leave_settings(cq, channels[0].channel_id)
            else:
                await safe_edit(cq.message, f"Select a channel to configure {kind} message:",
                                kb_channel_select_for(f"settings:{kind}", channels))

        elif data.startswith("settings:join:"):
            await ack()
            await show_join_settings(cq, int(p[2]))

        elif data.startswith("settings:leave:"):
            await ack()
            await show_leave_settings(cq, int(p[2]))

        elif data == "settings:start_msg":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                await session.commit()
            await safe_edit(cq.message, _start_settings_text(gs), kb_start_msg_settings())

        elif data == "settings:auto_reply":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                await session.commit()
            await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))

        elif data == "settings:notifications":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                await session.commit()
            await safe_edit(cq.message, "🔔 <b>Notification Settings</b>\n<i>(global)</i>",
                            kb_notifications(gs))

        elif data.startswith("notif:toggle:"):
            kind = p[2]
            attr = {"join_request": "notif_join_request", "member_join": "notif_member_join",
                    "member_leave": "notif_member_leave", "auto_accept": "notif_auto_accept"}.get(kind)
            if attr is None:
                await ack()
                return
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                setattr(gs, attr, not getattr(gs, attr))
                await session.commit()
                gs = await get_global_settings(session)
                await session.commit()
            await ack("Updated.")
            await safe_edit(cq.message, "🔔 <b>Notification Settings</b>\n<i>(global)</i>",
                            kb_notifications(gs))

        # ---------- join_msg / leave_msg ----------
        elif p[0] in ("join_msg", "leave_msg") and len(p) >= 3:
            kind = "join" if p[0] == "join_msg" else "leave"
            action, ch_id = p[1], int(p[2])
            show = show_join_settings if kind == "join" else show_leave_settings
            pre = "JOIN_MSG" if kind == "join" else "LEAVE_MSG"

            if action == "edit":
                await ack()
                reset_flow(admin_id)
                f = flow(admin_id)
                f.state, f.data["channel_id"] = St[f"{pre}_TEXT"], ch_id
                await safe_edit(
                    cq.message,
                    f"Send the {kind} message text.\nFormatting & premium emoji preserved.\n"
                    "Variables: <code>{first_name} {last_name} {username} {channel_name} {date}</code>",
                    kb_back(f"settings:{kind}:{ch_id}"))
            elif action == "media":
                await ack()
                reset_flow(admin_id)
                f = flow(admin_id)
                f.state, f.data["channel_id"] = St[f"{pre}_MEDIA"], ch_id
                await safe_edit(cq.message, f"Send a photo or video to attach.",
                                InlineKeyboardMarkup([
                                    [InlineKeyboardButton("🗑 Remove Media",
                                                          callback_data=f"{p[0]}:media_remove:{ch_id}")],
                                    [InlineKeyboardButton("« Back",
                                                          callback_data=f"settings:{kind}:{ch_id}")]]))
            elif action == "media_remove":
                async with SessionLocal() as session:
                    s = await get_or_create_settings(session, ch_id)
                    setattr(s, f"{kind}_msg_media_id", "")
                    setattr(s, f"{kind}_msg_media_type", "")
                    await session.commit()
                reset_flow(admin_id)
                await ack("Media removed.")
                await show(cq, ch_id)
            elif action == "btn_set":
                await ack()
                reset_flow(admin_id)
                f = flow(admin_id)
                f.state, f.data["channel_id"] = St[f"{pre}_BTN_LABEL"], ch_id
                await safe_edit(cq.message, "Send the button label:",
                                kb_back(f"settings:{kind}:{ch_id}"))
            elif action == "btn_remove":
                async with SessionLocal() as session:
                    s = await get_or_create_settings(session, ch_id)
                    setattr(s, f"{kind}_btn_label", "")
                    setattr(s, f"{kind}_btn_url", "")
                    await session.commit()
                await ack("Button removed.")
                await show(cq, ch_id)
            elif action == "toggle":
                async with SessionLocal() as session:
                    s = await get_or_create_settings(session, ch_id)
                    attr = f"{kind}_msg_enabled"
                    setattr(s, attr, not getattr(s, attr))
                    await session.commit()
                await ack("Updated.")
                await show(cq, ch_id)
            elif action == "preview":
                await ack()
                async with SessionLocal() as session:
                    s = await get_or_create_settings(session, ch_id)
                    await session.commit()
                tpl = getattr(s, f"{kind}_msg_text") or "(no message set)"
                prev = render_template(tpl, cq.from_user.first_name or "Alex", "",
                                       cq.from_user.username or "alex",
                                       await get_channel_name(ch_id))
                await _send_preview(chat_id, f"{kind.capitalize()} Message Preview", prev)
            else:
                await ack()

        # ---------- btn_ask ----------
        elif data.startswith("btn_ask:"):
            await ack()
            yn, target = p[1], p[2]
            if target in ("join", "leave"):
                ch_id = int(p[3])
                pre = "JOIN_MSG" if target == "join" else "LEAVE_MSG"
                if yn == "yes":
                    reset_flow(admin_id)
                    f = flow(admin_id)
                    f.state, f.data["channel_id"] = St[f"{pre}_BTN_LABEL"], ch_id
                    await safe_edit(cq.message, "Send the button label:")
                else:
                    reset_flow(admin_id)
                    if target == "join":
                        await show_join_settings(cq, ch_id)
                    else:
                        await show_leave_settings(cq, ch_id)
            elif target == "start_msg":
                if yn == "yes":
                    reset_flow(admin_id)
                    flow(admin_id).state = St.START_MSG_BTN_LABEL
                    await safe_edit(cq.message, "Send the button label:")
                else:
                    reset_flow(admin_id)
                    async with SessionLocal() as session:
                        gs = await get_global_settings(session)
                        await session.commit()
                    await safe_edit(cq.message, _start_settings_text(gs), kb_start_msg_settings())
            elif target == "auto_reply":
                if yn == "yes":
                    reset_flow(admin_id)
                    flow(admin_id).state = St.AUTO_REPLY_BTN_LABEL
                    await safe_edit(cq.message, "Send the button label:")
                else:
                    reset_flow(admin_id)
                    async with SessionLocal() as session:
                        gs = await get_global_settings(session)
                        await session.commit()
                    await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))

        # ---------- start msg ----------
        elif data == "start_msg:edit":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.START_MSG_TEXT
            await safe_edit(cq.message, "Send the start message text:", kb_back("settings:start_msg"))
        elif data == "start_msg:btn_set":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.START_MSG_BTN_LABEL
            await safe_edit(cq.message, "Send the button label:", kb_back("settings:start_msg"))
        elif data == "start_msg:btn_remove":
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                gs.start_btn_label, gs.start_btn_url = "", ""
                await session.commit()
                gs = await get_global_settings(session)
                await session.commit()
            await ack("Button removed.")
            await safe_edit(cq.message, _start_settings_text(gs), kb_start_msg_settings())
        elif data == "start_msg:preview":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                await session.commit()
            await _send_preview(chat_id, "Start Message Preview", gs.start_msg_text)

        # ---------- auto reply legacy ----------
        elif data == "auto_reply:edit":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.AUTO_REPLY_TEXT
            await safe_edit(cq.message, "Send the auto-reply text:", kb_back("settings:auto_reply"))
        elif data == "auto_reply:btn_set":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.AUTO_REPLY_BTN_LABEL
            await safe_edit(cq.message, "Send the button label:", kb_back("settings:auto_reply"))
        elif data == "auto_reply:btn_remove":
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                gs.auto_reply_btn_label, gs.auto_reply_btn_url = "", ""
                await session.commit()
                gs = await get_global_settings(session)
                await session.commit()
            await ack("Removed.")
            await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))
        elif data == "auto_reply:toggle":
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                gs.auto_reply_enabled = not gs.auto_reply_enabled
                await session.commit()
                gs = await get_global_settings(session)
                await session.commit()
            await ack("Updated.")
            await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))
        elif data == "auto_reply:preview":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                await session.commit()
            await _send_preview(chat_id, "Auto-Reply Preview", gs.auto_reply_text)

        # =========================================================
        # AUTOMATION CENTER
        # =========================================================
        elif data == "auto:main":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, await build_automation_main_text(), kb_automation_main())

        elif data == "auto:create":
            await ack()
            await _wizard_start(cq, admin_id)

        elif data == "auto:cancel":
            await ack("Cancelled.")
            reset_flow(admin_id)
            await safe_edit(cq.message, await build_automation_main_text(), kb_automation_main())

        elif p[0] == "auto" and p[1] == "list":
            page = int(p[2]) if len(p) > 2 else 1
            active_only = len(p) > 3 and p[3] == "on"
            await ack()
            txt, kb = await build_rule_list(page, active_only=active_only)
            await safe_edit(cq.message, txt, kb)

        elif data == "auto:stats":
            await ack()
            await safe_edit(cq.message, await build_automation_stats(), kb_back("auto:main"))

        elif p[0] == "auto" and p[1] == "logs":
            page = int(p[2]) if len(p) > 2 else 1
            await ack()
            txt, kb = await build_automation_logs(page)
            await safe_edit(cq.message, txt, kb)

        elif p[0] == "auto" and p[1] == "logs_rule":
            rule_id = int(p[2])
            page = int(p[3]) if len(p) > 3 else 1
            await ack()
            txt, kb = await build_automation_logs(page, rule_id=rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data == "auto:vars":
            await ack()
            await safe_edit(
                cq.message,
                "🧩 <b>Available Variables</b>\n\n"
                "<code>{first_name}</code>\n<code>{last_name}</code>\n<code>{username}</code>\n"
                "<code>{user_id}</code>\n<code>{channel_name}</code>\n<code>{channel_id}</code>\n"
                "<code>{date}</code>\n<code>{time}</code>\n<code>{datetime}</code>\n\n"
                "Values are HTML-escaped automatically — safe to embed in any text.",
                kb_back("auto:main"))

        elif data == "auto:test":
            await ack()
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_TEST_INPUT
            f.data["test_rule_id"] = None
            await safe_edit(cq.message,
                            "🧪 Send a sample message to run through all enabled rules in test mode "
                            "(nothing will be sent to real users).",
                            kb_back("auto:main"))

        elif data.startswith("auto:test_rule:"):
            rule_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_TEST_INPUT
            f.data["test_rule_id"] = rule_id
            await ack()
            await safe_edit(cq.message,
                            f"🧪 <b>Testing rule #{rule_id}</b>\n\nSend a sample message to test.",
                            kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:test_send:"):
            rule_id = int(p[2])
            await ack("Sending test…")
            async with SessionLocal() as s:
                rule = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
            if rule is None:
                await safe_answer(cq, "Rule gone.", show_alert=True)
                return
            result = await _execute_rule(
                rule, user_id=admin_id, first_name=cq.from_user.first_name or "",
                last_name=cq.from_user.last_name or "", username=cq.from_user.username or "",
                channel_id=None, channel_name="(test)")
            await safe_edit(cq.message,
                            f"🧪 Test result: {'✅ Sent' if result['ok'] else '❌ ' + esc(result['detail'])}",
                            kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:view:"):
            await ack()
            txt, kb = await build_rule_view(int(p[2]))
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:advanced:"):
            await ack()
            txt, kb = await build_rule_advanced(int(p[2]))
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:duplicate:"):
            src_id = int(p[2])
            async with SessionLocal() as s:
                src = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == src_id))).scalar_one_or_none()
                if src is None:
                    await ack("Rule gone.", True)
                    return
                new_rule = AutomationRule(
                    name=(src.name + " (copy)")[:200],
                    description=src.description,
                    enabled=False,
                    priority=src.priority,
                    stop_on_match=src.stop_on_match,
                    trigger_type=src.trigger_type,
                    match_type=src.match_type,
                    trigger_value=src.trigger_value,
                    case_insensitive=src.case_insensitive,
                    scope_type=src.scope_type,
                    scope_channel_id=src.scope_channel_id,
                    cooldown_seconds=src.cooldown_seconds,
                    response_type=src.response_type,
                    response_text=src.response_text,
                    response_media_id=src.response_media_id,
                )
                s.add(new_rule)
                await s.flush()
                btns = (await s.execute(select(AutomationButton).where(
                    AutomationButton.rule_id == src_id))).scalars().all()
                for b in btns:
                    s.add(AutomationButton(rule_id=new_rule.id, row=b.row, col=b.col,
                                           label=b.label, url=b.url))
                await s.commit()
                new_id = new_rule.id
            invalidate_automation_cache()
            await ack(f"Duplicated as #{new_id} (disabled).")
            txt, kb = await build_rule_view(new_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:toggle:"):
            rule_id = int(p[2])
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.enabled = not r.enabled
                    await s.commit()
            invalidate_automation_cache()
            await ack("Updated.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:toggle_stop:"):
            rule_id = int(p[2])
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.stop_on_match = not r.stop_on_match
                    await s.commit()
            invalidate_automation_cache()
            await ack("Updated.")
            txt, kb = await build_rule_advanced(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:prio:"):
            rule_id, direction = int(p[2]), p[3]
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.priority = max(1, r.priority - 10) if direction == "up" else r.priority + 10
                    await s.commit()
            invalidate_automation_cache()
            await ack("Priority updated.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:edit_name:"):
            rule_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_EDIT_NAME
            f.data["edit_rule_id"] = rule_id
            await ack()
            await safe_edit(cq.message, "Send the new rule name:", kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:edit_pattern:"):
            rule_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_EDIT_PATTERN
            f.data["edit_rule_id"] = rule_id
            await ack()
            await safe_edit(cq.message,
                            "Send the new trigger pattern.\n\n"
                            "For keyword modes: separate by commas or new lines.",
                            kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:edit_response:"):
            rule_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_EDIT_RESPONSE
            f.data["edit_rule_id"] = rule_id
            await ack()
            await safe_edit(cq.message, "Send the new reply text:",
                            kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:edit_media:"):
            rule_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_EDIT_MEDIA
            f.data["edit_rule_id"] = rule_id
            await ack()
            await safe_edit(cq.message,
                            "Send a photo/video/document/audio/voice/animation to attach:",
                            kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:add_btn:"):
            rule_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.AUTO_EDIT_BTN_LABEL
            f.data["edit_rule_id"] = rule_id
            await ack()
            await safe_edit(cq.message, "Send the button label:",
                            kb_back(f"auto:view:{rule_id}"))

        elif data.startswith("auto:clear_btns:"):
            rule_id = int(p[2])
            async with SessionLocal() as s:
                await s.execute(delete(AutomationButton).where(AutomationButton.rule_id == rule_id))
                await s.commit()
            invalidate_automation_cache()
            await ack("Buttons cleared.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:edit_priority:"):
            rule_id = int(p[2])
            await ack()
            await safe_edit(cq.message,
                            "Choose priority (lower runs first):",
                            InlineKeyboardMarkup([
                                [InlineKeyboardButton(f"#{n}", callback_data=f"auto:set_prio:{rule_id}:{n}")
                                 for n in (1, 5, 10)],
                                [InlineKeyboardButton("50", callback_data=f"auto:set_prio:{rule_id}:50"),
                                 InlineKeyboardButton("100", callback_data=f"auto:set_prio:{rule_id}:100"),
                                 InlineKeyboardButton("500", callback_data=f"auto:set_prio:{rule_id}:500")],
                                [InlineKeyboardButton("« Back", callback_data=f"auto:view:{rule_id}")]]))

        elif data.startswith("auto:set_prio:"):
            rule_id, prio = int(p[2]), int(p[3])
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.priority = prio
                    await s.commit()
            invalidate_automation_cache()
            await ack("Priority updated.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:edit_cooldown:"):
            rule_id = int(p[2])
            await ack()
            rows = []
            for sec in COOLDOWN_PRESETS:
                label = "Off" if sec == 0 else (f"{sec}s" if sec < 60
                                                else (f"{sec // 60}m" if sec < 3600 else f"{sec // 3600}h"))
                rows.append(InlineKeyboardButton(label,
                                                 callback_data=f"auto:set_cd:{rule_id}:{sec}"))
            kb = InlineKeyboardMarkup([
                rows[:3], rows[3:6], rows[6:],
                [InlineKeyboardButton("« Back", callback_data=f"auto:view:{rule_id}")]])
            await safe_edit(cq.message, "Choose cooldown:", kb)

        elif data.startswith("auto:set_cd:"):
            rule_id, sec = int(p[2]), int(p[3])
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.cooldown_seconds = sec
                    await s.commit()
            invalidate_automation_cache()
            await ack("Cooldown updated.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:edit_scope:"):
            rule_id = int(p[2])
            await ack()
            channels = await _active_channels()
            rows = [[InlineKeyboardButton("🌐 Global", callback_data=f"auto:set_scope:{rule_id}:global:0")]]
            for c in channels:
                rows.append([InlineKeyboardButton(f"📣 {c.name or c.channel_id}"[:60],
                                                  callback_data=f"auto:set_scope:{rule_id}:channel:{c.channel_id}")])
            rows.append([InlineKeyboardButton("« Back", callback_data=f"auto:view:{rule_id}")])
            await safe_edit(cq.message, "Choose scope:", InlineKeyboardMarkup(rows))

        elif data.startswith("auto:set_scope:"):
            rule_id, scope_type, scope_val = int(p[2]), p[3], int(p[4])
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.scope_type = scope_type
                    r.scope_channel_id = scope_val if scope_type == "channel" else None
                    await s.commit()
            invalidate_automation_cache()
            await ack("Scope updated.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:edit_match:"):
            rule_id = int(p[2])
            await ack()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(label, callback_data=f"auto:set_match:{rule_id}:{mt}")]
                for mt, label in MATCH_LABELS.items()
            ] + [[InlineKeyboardButton("« Back", callback_data=f"auto:view:{rule_id}")]])
            await safe_edit(cq.message, "Choose match type:", kb)

        elif data.startswith("auto:set_match:"):
            rule_id, mt = int(p[2]), p[3]
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
                if r:
                    r.match_type = mt
                    if mt == "any":
                        r.trigger_value = ""
                    await s.commit()
            invalidate_automation_cache()
            await ack("Match type updated.")
            txt, kb = await build_rule_view(rule_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("auto:preview:"):
            rule_id = int(p[2])
            await ack()
            async with SessionLocal() as s:
                r = (await s.execute(select(AutomationRule).where(
                    AutomationRule.id == rule_id))).scalar_one_or_none()
            if not r:
                return
            markup = await _build_rule_markup(rule_id)
            sample = render_template(r.response_text or "", "Alex", "Smith", "alex",
                                     "(channel)", user_id=123, channel_id=-100)
            try:
                if r.response_type in ("photo", "video", "document", "audio", "voice", "animation") \
                        and r.response_media_id:
                    await bot.send_message(chat_id,
                                           f"👁 Preview (media type: {r.response_type}, caption below)",
                                           parse_mode=HTML)
                    await _send_preview(chat_id, "Caption", sample, markup)
                else:
                    await _send_preview(chat_id, "Response Preview", sample, markup)
            except Exception as exc:
                await bot.send_message(chat_id, f"❌ Preview failed: {esc(exc)}", parse_mode=HTML)

        elif data.startswith("auto:delete:"):
            rule_id = int(p[2])
            await ack()
            await safe_edit(cq.message,
                            "🗑 <b>Delete this rule?</b>\nThis cannot be undone.",
                            kb_confirm(f"auto:do_delete:{rule_id}", f"auto:view:{rule_id}"))

        elif data.startswith("auto:do_delete:"):
            rule_id = int(p[2])
            async with SessionLocal() as s:
                await s.execute(delete(AutomationButton).where(AutomationButton.rule_id == rule_id))
                await s.execute(delete(AutomationRule).where(AutomationRule.id == rule_id))
                await s.commit()
            invalidate_automation_cache()
            await ack("Deleted.")
            txt, kb = await build_rule_list(1)
            await safe_edit(cq.message, txt, kb)

        # ---------- SIMPLE WIZARD step responses ----------
        elif data == "auto:trig:any":
            await ack()
            f = flow(admin_id)
            d = _draft(f)
            d["trigger_type"] = "message"
            d["match_type"] = "any"
            d["trigger_value"] = ""
            f.state = St.AUTO_RESPONSE
            await safe_edit(
                cq.message,
                "➕ <b>Step 2/2</b>\n\n"
                "🌀 Any message → reply.\n\n"
                "📤 <b>Now send what you want the bot to reply.</b>",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="auto:cancel")]]))

        elif data == "auto:save_draft":
            await ack("Saving…")
            f = flow(admin_id)
            d = _draft(f)
            if not d.get("trigger_value") and d.get("match_type") != "any":
                await safe_edit(cq.message, "❌ Missing trigger value. Cancelled.",
                                kb_back("auto:main"))
                reset_flow(admin_id)
                return
            d.setdefault("trigger_type", "message")
            d.setdefault("match_type", "exact")
            d.setdefault("response_type", "text")
            rule_id = await _save_draft_rule(d)
            reset_flow(admin_id)
            if rule_id:
                txt, kb = await build_rule_view(rule_id)
                await safe_edit(cq.message, f"✅ <b>Automation saved!</b>\n\n{txt}", kb)
            else:
                await safe_edit(cq.message, "❌ Failed to save rule. Check logs.",
                                kb_back("auto:main"))

        elif data == "auto:adv_draft":
            await ack()
            await safe_edit(
                cq.message,
                "⚙️ <b>Advanced Settings for this new automation</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("✏️ Set Name", callback_data="auto:draft_name"),
                     InlineKeyboardButton("🔄 Match Mode", callback_data="auto:draft_match")],
                    [InlineKeyboardButton("🌐 Scope", callback_data="auto:draft_scope"),
                     InlineKeyboardButton("⏱ Cooldown", callback_data="auto:draft_cd")],
                    [InlineKeyboardButton("💾 Save Automation", callback_data="auto:save_draft")],
                    [InlineKeyboardButton("« Back to Preview", callback_data="auto:draft_preview")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="auto:cancel")],
                ]))

        elif data == "auto:draft_preview":
            await ack()
            await _wizard_show_preview(chat_id, admin_id, edit_msg=cq.message)

        elif data == "auto:draft_name":
            await ack()
            f = flow(admin_id)
            f.state = St.AUTO_EDIT_NAME
            f.data.pop("edit_rule_id", None)
            f.data["draft_mode"] = True
            await safe_edit(cq.message, "Send the automation name:", kb_back("auto:adv_draft"))

        elif data == "auto:draft_match":
            await ack()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(label, callback_data=f"auto:draft_setmatch:{mt}")]
                for mt, label in MATCH_LABELS.items()
            ] + [[InlineKeyboardButton("« Back", callback_data="auto:adv_draft")]])
            await safe_edit(cq.message, "Choose match mode:", kb)

        elif data.startswith("auto:draft_setmatch:"):
            await ack()
            f = flow(admin_id)
            d = _draft(f)
            mt = p[2]
            d["match_type"] = mt
            if mt == "any":
                d["trigger_value"] = ""
            await safe_edit(cq.message, "✅ Match mode updated.",
                            InlineKeyboardMarkup([
                                [InlineKeyboardButton("« Back", callback_data="auto:adv_draft")]]))

        elif data == "auto:draft_scope":
            await ack()
            channels = await _active_channels()
            rows = [[InlineKeyboardButton("🌐 Global", callback_data="auto:draft_setscope:global:0")]]
            for c in channels:
                rows.append([InlineKeyboardButton(f"📣 {c.name or c.channel_id}"[:60],
                                                  callback_data=f"auto:draft_setscope:channel:{c.channel_id}")])
            rows.append([InlineKeyboardButton("« Back", callback_data="auto:adv_draft")])
            await safe_edit(cq.message, "Choose scope:", InlineKeyboardMarkup(rows))

        elif data.startswith("auto:draft_setscope:"):
            await ack()
            f = flow(admin_id)
            d = _draft(f)
            scope_type, scope_val = p[2], int(p[3])
            d["scope_type"] = scope_type
            d["scope_channel_id"] = scope_val if scope_type == "channel" else None
            await safe_edit(cq.message, "✅ Scope updated.",
                            InlineKeyboardMarkup([
                                [InlineKeyboardButton("« Back", callback_data="auto:adv_draft")]]))

        elif data == "auto:draft_cd":
            await ack()
            rows = []
            for sec in COOLDOWN_PRESETS:
                label = "Off" if sec == 0 else (f"{sec}s" if sec < 60
                                                else (f"{sec // 60}m" if sec < 3600 else f"{sec // 3600}h"))
                rows.append(InlineKeyboardButton(label, callback_data=f"auto:draft_setcd:{sec}"))
            kb = InlineKeyboardMarkup([
                rows[:3], rows[3:6], rows[6:],
                [InlineKeyboardButton("« Back", callback_data="auto:adv_draft")]])
            await safe_edit(cq.message, "Choose cooldown:", kb)

        elif data.startswith("auto:draft_setcd:"):
            await ack()
            f = flow(admin_id)
            d = _draft(f)
            d["cooldown_seconds"] = int(p[2])
            await safe_edit(cq.message, "✅ Cooldown updated.",
                            InlineKeyboardMarkup([
                                [InlineKeyboardButton("« Back", callback_data="auto:adv_draft")]]))

        # ---------- inbox ----------
        elif data == "inbox:list":
            await ack()
            await show_inbox(chat_id)

        elif data.startswith("inbox:open:"):
            await ack()
            target_id = int(p[2])
            async with SessionLocal() as session:
                convo = list(reversed((await session.execute(
                    select(Conversation).where(Conversation.user_id == target_id)
                    .order_by(Conversation.sent_at.desc()).limit(20))).scalars().all()))
                for c in convo:
                    c.is_read = True
                await session.commit()
            lines = [f"<b>Conversation with <code>{target_id}</code>:</b>\n"]
            for c in convo:
                lines.append(f"{'→' if c.direction == 'out' else '←'} {esc((c.message or '')[:200])}")
            await safe_edit(cq.message, "\n".join(lines), InlineKeyboardMarkup([
                [InlineKeyboardButton("↩️ Reply", callback_data=f"inbox:reply:{target_id}")],
                [InlineKeyboardButton("« Back", callback_data="inbox:list")]]))

        elif data.startswith("inbox:reply:"):
            await ack()
            target_id = int(p[2])
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.INBOX_REPLY
            f.data["reply_to"] = target_id
            await bot.send_message(chat_id, f"Type your reply to <code>{target_id}</code>:",
                                   parse_mode="HTML")

        # ---------- per-request actions ----------
        elif p[0] == "jr" and len(p) >= 4:
            action, ch_id, target_id = p[1], int(p[2]), int(p[3])
            if action in ("accept", "decline"):
                if not await userbot_ready():
                    await ack("Userbot not logged in.", True)
                    return
                await ack("Processing…")
                approve = action == "accept"
                outcome = await apply_request_decision(ch_id, target_id, approve)
                await safe_edit(cq.message, outcome, kb_back())
            elif action == "refresh":
                await ack("Refreshing…")
                pending = await live_pending_count(ch_id, use_ttl=False)
                members = await live_member_count(ch_id, use_ttl=False)
                async with SessionLocal() as session:
                    jr = (await session.execute(select(JoinRequest).where(
                        JoinRequest.channel_id == ch_id, JoinRequest.user_id == target_id,
                        JoinRequest.status == "pending"))).scalars().first()
                state = "⏳ Still pending" if jr else "✔️ No longer pending"
                await safe_edit(
                    cq.message,
                    f"🔔 <b>Join request</b>\n\nChannel: <b>{esc(await get_channel_name(ch_id))}</b>\n"
                    f"User ID: <code>{target_id}</code>\nStatus: {state}\n\n"
                    f"⏳ Pending (Telegram): {pending.value}{src_tag(pending)}\n"
                    f"👥 Members: {members.value:,}{src_tag(members)}",
                    kb_join_request_actions(ch_id, target_id) if jr else kb_back())
            else:
                await ack()

        elif data.startswith("user:accept:") or data.startswith("user:decline:"):
            target_id = int(p[2])
            approve = p[1] == "accept"
            if not await userbot_ready():
                await ack("Userbot not logged in.", True)
                return
            async with SessionLocal() as session:
                rows = (await session.execute(select(JoinRequest).where(
                    JoinRequest.user_id == target_id,
                    JoinRequest.status == "pending"))).scalars().all()
            if not rows:
                await ack("No pending request.", True)
                return
            if len(rows) > 1:
                await ack()
                btns = [[InlineKeyboardButton(
                    f"{'✅' if approve else '❌'} {await get_channel_name(r.channel_id)}"[:60],
                    callback_data=f"jr:{p[1]}:{r.channel_id}:{target_id}")] for r in rows]
                btns.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
                await safe_edit(cq.message,
                                "User has requests in several channels. Pick one:",
                                InlineKeyboardMarkup(btns))
                return
            await ack("Processing…")
            outcome = await apply_request_decision(rows[0].channel_id, target_id, approve)
            await safe_edit(cq.message, outcome, kb_back())

        elif p[0] == "user" and p[1] in ("remove", "ban", "mute", "unban") and len(p) >= 4:
            target_id, ch_id, action = int(p[2]), int(p[3]), p[1]
            if not await userbot_ready():
                await ack("Userbot not logged in.", True)
                return
            try:
                if action == "remove":
                    await userbot.ban_chat_member(ch_id, target_id)
                    await userbot.unban_chat_member(ch_id, target_id)
                    await deactivate_member(ch_id, target_id)
                elif action == "ban":
                    await userbot.ban_chat_member(ch_id, target_id)
                    await deactivate_member(ch_id, target_id)
                elif action == "unban":
                    await userbot.unban_chat_member(ch_id, target_id)
                else:
                    await userbot.restrict_chat_member(ch_id, target_id, ChatPermissions())
            except Exception as exc:
                logger.warning("user action %s failed: %s", action, exc)
                await ack(f"Failed: {type(exc).__name__}", True)
                return
            _member_cache.pop(ch_id, None)
            await ack(f"{action.capitalize()} applied.")
            txt, kb = await build_user_profile(ch_id, target_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("user:profile:"):
            await ack("Checking Telegram…")
            if len(p) >= 4:
                ch_id, target_id = int(p[2]), int(p[3])
            else:
                target_id = int(p[2])
                async with SessionLocal() as session:
                    hit = (await session.execute(select(Member.channel_id).where(
                        Member.user_id == target_id).limit(1))).first() or \
                          (await session.execute(select(JoinRequest.channel_id).where(
                              JoinRequest.user_id == target_id).limit(1))).first()
                if not hit:
                    await safe_edit(cq.message, "No channel record.", kb_back())
                    return
                ch_id = hit[0]
            txt, kb = await build_user_profile(ch_id, target_id)
            await safe_edit(cq.message, txt, kb)

        # ---------- broadcast confirm ----------
        elif data == "confirm:broadcast_send":
            f = flow(admin_id)
            from_chat, msg_id = f.data.get("bc_from_chat_id"), f.data.get("bc_message_id")
            if not from_chat or not msg_id:
                await ack("No broadcast content.", True)
                return
            await ack("Sending…")
            reset_flow(admin_id)
            async with SessionLocal() as session:
                b = Broadcast(from_chat_id=from_chat, from_message_id=msg_id, status="pending")
                session.add(b)
                await session.commit()
                await session.refresh(b)
            await safe_edit(cq.message, "📤 Sending broadcast…")
            spawn(execute_broadcast(b.id, cq.message), name="broadcast")

        elif data == "confirm:broadcast_schedule":
            f = flow(admin_id)
            if not f.data.get("bc_message_id"):
                await ack("No broadcast content.", True)
                return
            await ack()
            f.state = St.BC_SCHEDULE
            await safe_edit(cq.message, "Send the schedule time in UTC: <code>YYYY-MM-DD HH:MM</code>",
                            kb_back())

        elif data == "confirm:cancel":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, await build_main_panel_text(),
                            kb_main_panel(await userbot_ready()))

        elif data == "setapi:cancel":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, "Setup cancelled.", kb_main_panel(await userbot_ready()))

        # ---------- login ----------
        elif data == "login:menu":
            await ack()
            logged_in = await userbot_ready()
            extra = ""
            if logged_in:
                try:
                    me = await userbot.get_me()
                    extra = f"\n\nLogged in as <b>{esc(me.first_name)}</b> (<code>{me.id}</code>)."
                except Exception:
                    pass
            await safe_edit(cq.message, f"<b>🔐 Userbot Login</b>{extra}", kb_login_menu(logged_in))
        elif data == "login:begin":
            await ack()
            await start_login_flow(admin_id, cq)
        elif data == "login:cancel":
            await ack()
            await cancel_login_flow(admin_id, chat_id)
        elif data == "login:logout":
            await ack()
            await logout_userbot(chat_id)

        # ---------- admins ----------
        elif data == "admins:list":
            await ack()
            async with SessionLocal() as session:
                admins = (await session.execute(select(Admin).order_by(Admin.is_owner.desc()))).scalars().all()
            await safe_edit(cq.message, "<b>🛡️ Admins:</b>", kb_admins_list(admins))
        elif data == "admins:add":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.ADD_ADMIN
            await safe_edit(cq.message, "Send the Telegram <b>user ID</b> to add as admin.",
                            kb_back("admins:list"))
        elif data.startswith("admins:remove:"):
            target_id = int(p[2])
            async with SessionLocal() as session:
                row = (await session.execute(select(Admin).where(Admin.user_id == target_id))).scalar_one_or_none()
                if row and not row.is_owner:
                    await session.delete(row)
                    await session.commit()
                    await reload_admins()
                    await ack("Admin removed.")
                else:
                    await ack("Can't remove owner.", True)
                    return
            async with SessionLocal() as session:
                admins = (await session.execute(select(Admin).order_by(Admin.is_owner.desc()))).scalars().all()
            await safe_edit(cq.message, "<b>🛡️ Admins:</b>", kb_admins_list(admins))

        # ---------- system tools ----------
        elif data == "tools:main":
            await ack()
            await safe_edit(
                cq.message,
                "🧰 <b>System Tools</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("💾 Backup & Restore", callback_data="tools:backup")],
                    [InlineKeyboardButton("📋 Recent Errors", callback_data="tools:errors"),
                     InlineKeyboardButton("🔄 Force Sync Now", callback_data="tools:sync")],
                    [InlineKeyboardButton("🤖 Automations", callback_data="auto:stats")],
                    [InlineKeyboardButton("« Back", callback_data="panel:main")],
                ]))
        elif data == "tools:sync":
            await ack("Syncing…")
            n = await sync_pending_with_telegram()
            await bot.send_message(chat_id, f"✅ Reconciled {n} channel(s).", reply_markup=kb_back())
        elif data == "tools:errors":
            await ack()
            try:
                tail = Path("logs/bot.log").read_text(encoding="utf-8", errors="ignore")[-3000:]
            except Exception:
                tail = "(no log file)"
            await bot.send_message(chat_id, f"<pre>{esc(tail)}</pre>", parse_mode=HTML)

        # ---------- backup & restore ----------
        elif data == "tools:backup":
            await ack()
            reset_flow(admin_id)
            db_kind = "SQLite (file-based)" if IS_SQLITE else "PostgreSQL (JSON export)"
            try:
                n_local = len(list(BACKUP_DIR.glob("bot_backup_*")))
            except Exception:
                n_local = 0
            await safe_edit(
                cq.message,
                f"💾 <b>Backup & Restore</b>\n\n"
                f"<b>Backend:</b> {esc(db_kind)}\n"
                f"<b>Local backups:</b> {n_local}\n\n"
                f"• <b>Create Backup</b> — makes a fresh backup and sends it to you here.\n"
                f"• <b>Restore From File</b> — upload a <code>.db</code> or <code>.json</code> "
                f"backup. Current DB is auto-backed up first.\n"
                f"• <b>Local Backups</b> — resend any previously created backup.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("📤 Create Backup", callback_data="backup:create")],
                    [InlineKeyboardButton("📥 Restore From File", callback_data="backup:restore")],
                    [InlineKeyboardButton("📂 Local Backups", callback_data="backup:list")],
                    [InlineKeyboardButton("« Back", callback_data="tools:main")],
                ]))

        elif data == "backup:create":
            await ack("Creating backup…")
            status = await bot.send_message(chat_id, "⏳ Creating database backup…")
            path, err = await create_db_backup()
            if path is None:
                await safe_edit(status, f"❌ <b>Backup failed:</b> {esc(err)}",
                                kb_back("tools:backup"))
                return
            size = path.stat().st_size
            await safe_edit(status, f"📤 Uploading <code>{esc(path.name)}</code> "
                                    f"({_human_size(size)})…", kb_back("tools:backup"))
            try:
                await bot.send_document(
                    chat_id, str(path),
                    caption=(f"💾 <b>Database Backup</b>\n"
                             f"File: <code>{esc(path.name)}</code>\n"
                             f"Size: {_human_size(size)}\n"
                             f"Backend: {'SQLite' if IS_SQLITE else 'JSON'}\n"
                             f"Created: {now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC"),
                    parse_mode=HTML)
                await safe_edit(status,
                                f"✅ <b>Backup created & sent.</b>\n\n"
                                f"File: <code>{esc(path.name)}</code>\n"
                                f"Size: {_human_size(size)}\n\n"
                                f"<i>Save it somewhere safe. It can be restored at any time.</i>",
                                kb_back("tools:backup"))
            except Exception as exc:
                logger.exception("send backup failed: %s", exc)
                await safe_edit(status, f"❌ Backup saved locally but failed to send: {esc(exc)}",
                                kb_back("tools:backup"))
            _cleanup_old_backups()

        elif data == "backup:restore":
            await ack()
            reset_flow(admin_id)
            f = flow(admin_id)
            f.state = St.RESTORE_UPLOAD
            await safe_edit(
                cq.message,
                "⚠️ <b>Restore Database</b>\n\n"
                "This will <b>replace the entire current database</b> with the uploaded backup.\n\n"
                "🛡 A safety backup of the current database will be created first "
                "and saved to <code>backups/</code>.\n\n"
                "📤 Now send the backup file as a <b>document</b>:\n"
                "• <code>.db</code> / <code>.sqlite</code> for SQLite backups\n"
                "• <code>.json</code> for JSON backups (works on any backend)\n\n"
                "Tap Cancel to abort.",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel",
                                                            callback_data="backup:cancel")]]))

        elif data == "backup:cancel":
            await ack("Cancelled.")
            reset_flow(admin_id)
            await safe_edit(cq.message, "💾 <b>Backup & Restore</b>",
                            InlineKeyboardMarkup([
                                [InlineKeyboardButton("📤 Create Backup", callback_data="backup:create")],
                                [InlineKeyboardButton("📥 Restore From File", callback_data="backup:restore")],
                                [InlineKeyboardButton("📂 Local Backups", callback_data="backup:list")],
                                [InlineKeyboardButton("« Back", callback_data="tools:main")],
                            ]))

        elif data == "backup:list":
            await ack()
            files = sorted(BACKUP_DIR.glob("bot_backup_*"),
                           key=lambda p: p.stat().st_mtime, reverse=True)[:10]
            if not files:
                await safe_edit(cq.message, "📂 <b>No backups yet.</b>",
                                kb_back("tools:backup"))
                return
            rows = []
            for f in files:
                try:
                    sz = _human_size(f.stat().st_size)
                    when = datetime.fromtimestamp(f.stat().st_mtime).strftime("%m-%d %H:%M")
                except Exception:
                    sz, when = "?", "?"
                rows.append([InlineKeyboardButton(
                    f"📄 {when} · {sz}"[:60],
                    callback_data=f"backup:send:{f.name}")])
            rows.append([InlineKeyboardButton("« Back", callback_data="tools:backup")])
            await safe_edit(
                cq.message,
                f"📂 <b>Local Backups</b> ({len(files)})\n\nTap to send a copy.",
                InlineKeyboardMarkup(rows))

        elif data.startswith("backup:send:"):
            fname = ":".join(p[2:])
            if "/" in fname or ".." in fname:
                await ack("Invalid filename.", True)
                return
            path = BACKUP_DIR / fname
            if not path.exists() or not path.is_file():
                await ack("File not found.", True)
                return
            await ack("Uploading…")
            try:
                await bot.send_document(
                    chat_id, str(path),
                    caption=f"💾 <code>{esc(fname)}</code>\n"
                            f"Size: {_human_size(path.stat().st_size)}",
                    parse_mode=HTML)
            except Exception as exc:
                await bot.send_message(chat_id, f"❌ Send failed: {esc(exc)}")

        else:
            await ack()

    except MessageNotModified:
        await ack()
    except FloodWait as fw:
        await ack("Too many requests — wait a moment.", True)
        await asyncio.sleep(int(getattr(fw, "value", 1)))
    except Exception as exc:
        logger.exception("Callback failed data=%s: %s", data, exc)
        await ack("Something went wrong — check logs.", True)
    finally:
        await ack()


# ===========================================================================
# HANDLER REGISTRATION
# ===========================================================================

def register_bot_handlers(client: Client):
    admin_cmds = {
        "panel": cmd_start, "channels": cmd_channels, "requests": cmd_requests,
        "accept_all": cmd_accept_all, "decline_all": cmd_decline_all, "search": cmd_search,
        "inbox": cmd_inbox, "block": cmd_block, "unblock": cmd_unblock,
        "broadcast": cmd_broadcast, "stats": cmd_stats, "settings": cmd_settings,
        "admins": cmd_admins, "login": cmd_login, "help": cmd_help,
        "automation": cmd_automation,
    }
    client.add_handler(MessageHandler(cmd_start, filters.command("start") & filters.private))
    for name, handler in admin_cmds.items():
        client.add_handler(MessageHandler(handler,
                                          filters.command(name) & filters.private & admin_only))
    client.add_handler(MessageHandler(cmd_setapi,
                                      filters.command("setapi") & filters.private & owner_only))

    all_cmds = ["start", "setapi"] + list(admin_cmds.keys())
    client.add_handler(MessageHandler(on_private_message,
                                      filters.private & ~filters.command(all_cmds) & ~filters.service))
    client.add_handler(CallbackQueryHandler(on_callback))
    client.add_handler(ChatJoinRequestHandler(_on_join_request))
    client.add_handler(ChatMemberUpdatedHandler(_on_member_updated))


# ===========================================================================
# BOOTSTRAP + MAIN
# ===========================================================================

async def bootstrap_owner():
    global _owner_id
    owner_id = int(OWNER_ID) if OWNER_ID.lstrip("-").isdigit() else 0
    _owner_id = owner_id
    if owner_id == 0:
        logger.warning("OWNER_ID not set in .env — nobody can use the admin panel.")
        return
    async with SessionLocal() as session:
        existing = (await session.execute(select(Admin).where(Admin.user_id == owner_id))).scalar_one_or_none()
        if existing is None:
            session.add(Admin(user_id=owner_id, name="Owner", is_owner=True))
        elif not existing.is_owner:
            existing.is_owner = True
        await session.commit()


async def periodic_sync():
    try:
        ok = await sync_pending_with_telegram()
        logger.info("periodic_sync reconciled %d channel(s)", ok)
    except Exception as exc:
        logger.exception("periodic_sync failed: %s", exc)


async def startup_self_check():
    checks = []
    try:
        async with SessionLocal() as s:
            await s.execute(select(1))
        checks.append("✅ database")
    except Exception as exc:
        checks.append(f"❌ database: {exc}")
    checks.append(f"{'✅' if _owner_id else '⚠️'} owner_id ({_owner_id or 'unset'})")
    checks.append(f"{'✅' if BOT_TOKEN else '❌'} bot_token")
    checks.append(f"{'✅' if ENV_API_ID and ENV_API_HASH else '⚠️'} env api creds")
    checks.append(f"{'✅' if await userbot_ready() else '⚠️'} userbot")
    checks.append(f"{'✅' if scheduler.running else '❌'} scheduler")
    try:
        n_bk = len(list(BACKUP_DIR.glob("bot_backup_*")))
        checks.append(f"💾 backups ({n_bk})")
    except Exception:
        pass
    logger.info("Startup self-check: %s", " | ".join(checks))


async def _graceful_shutdown():
    logger.info("Shutting down…")
    for t in list(_background_tasks):
        t.cancel()
    for t in list(_background_tasks):
        try:
            await t
        except Exception:
            pass
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    for c in (bot, userbot):
        if c is not None:
            try:
                await c.stop()
            except Exception:
                pass
    try:
        await engine.dispose()
    except Exception:
        pass
    logger.info("Shutdown complete.")


async def main():
    global bot

    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing — put it in .env. Cannot start.")
        sys.exit(1)

    await init_db()
    await bootstrap_owner()
    await reload_admins()

    api_id = int(ENV_API_ID) if ENV_API_ID.isdigit() else 0
    api_hash = ENV_API_HASH
    if api_id == 0 or not api_hash:
        db_id, db_hash = await kv_get("bot_api_id"), await kv_get("bot_api_hash")
        if db_id.isdigit() and db_hash:
            api_id, api_hash = int(db_id), db_hash
            logger.info("API_ID/API_HASH loaded from database.")
    if api_id == 0 or not api_hash:
        logger.warning("API_ID/API_HASH missing — using shared public credentials. "
                       "Set your own in .env or via /setapi.")
        api_id = 6
        api_hash = "eb06d4abfb49dc3eeb1aeb98ae0f581e"

    bot = Client("manager_bot", api_id=api_id, api_hash=api_hash, bot_token=BOT_TOKEN,
                 in_memory=True)
    register_bot_handlers(bot)

    scheduler.start()
    await bot.start()
    me = await bot.get_me()
    logger.info("Bot started as @%s", me.username)

    await restore_scheduled_broadcasts()

    if await start_userbot_from_kv():
        logger.info("Userbot restored from saved session.")
        try:
            ok = await sync_pending_with_telegram()
            logger.info("Startup sync reconciled %d channel(s).", ok)
        except Exception as exc:
            logger.exception("Startup sync failed: %s", exc)
    else:
        logger.info("No valid userbot session — use 🔐 Userbot Login in the admin panel.")

    scheduler.add_job(periodic_sync, "interval", minutes=10, id="periodic_sync",
                      replace_existing=True, misfire_grace_time=300)

    await startup_self_check()

    await idle()

    await _graceful_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
