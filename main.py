"""
Telegram Channel Manager Bot  (Phase 1: core bug fixes)
Pyrogram bot + userbot, SQLAlchemy async, APScheduler, FSM.

.env keys: BOT_TOKEN, OWNER_ID, API_ID, API_HASH, DATABASE_URL, LOG_LEVEL
Run:  pip install -r requirements.txt && python bot.py

Phase 1 changes (see CHANGELOG in the reply):
  * Telegram is the source of truth for pending requests / member counts; every number is
    labelled LIVE / CACHED / LOCAL and never mislabelled.
  * Safe per-channel reconciliation (never wipes DB rows on a failed listing).
  * Channel-scoped search (ID, @username, partial name) with live status verification.
  * Paginated join-request panel, channel-scoped accept/decline, honest bulk processing.
  * Member history tracking (first/last join, last leave), unique (channel_id,user_id).
  * Durable error handling in migrations; FSM flows expire; duplicate event handling fixed.
"""

import asyncio
import html as _html
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config as env
from pyrogram import Client, filters, idle, enums
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (
    ApiIdInvalid,
    AuthKeyUnregistered,
    FloodWait,
    MessageNotModified,
    PasswordHashInvalid,
    PeerIdInvalid,
    PhoneCodeEmpty,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberBanned,
    PhoneNumberFlood,
    PhoneNumberInvalid,
    SessionPasswordNeeded,
    UserDeactivated,
    UserDeactivatedBan,
    UserIsBlocked,
    UserPrivacyRestricted,
    UsernameNotOccupied,
    InputUserDeactivated,
)
from pyrogram.handlers import (
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberUpdatedHandler,
    MessageHandler,
)
from pyrogram.types import (
    CallbackQuery,
    ChatJoinRequest,
    ChatMemberUpdated,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    delete,
    func,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

# ===========================================================================
# CONFIG  (no secrets in code — everything from .env)
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

# Telegram allows ~30 msg/sec to different users. Stay safely below.
SEND_INTERVAL = 0.05          # 20 msg/sec
BULK_APPROVE_INTERVAL = 0.05
MAX_TEXT = 3800               # keep under 4096 with headroom

Path("logs").mkdir(exist_ok=True)
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


def now_utc() -> datetime:
    """Naive-UTC datetime (works identically on SQLite and Postgres)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def today_start() -> datetime:
    n = now_utc()
    return datetime(n.year, n.month, n.day)


def esc(v) -> str:
    """HTML-escape any value for safe use in parse_mode=HTML messages."""
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
    source = Column(String(20), default="event")   # event | sync


class Member(Base):
    __tablename__ = "members"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, index=True, nullable=False)
    user_id = Column(BigInteger, index=True, nullable=False)
    first_name = Column(String(255), default="")
    last_name = Column(String(255), default="")
    username = Column(String(255), default="")
    joined_at = Column(DateTime, default=now_utc)        # legacy: first join
    first_joined_at = Column(DateTime, nullable=True)
    last_joined_at = Column(DateTime, nullable=True)
    last_left_at = Column(DateTime, nullable=True)
    last_verified_at = Column(DateTime, nullable=True)   # last live Telegram check
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
    # notification toggles are GLOBAL (not per-channel)
    notif_join_request = Column(Boolean, default=True)
    notif_member_join = Column(Boolean, default=True)
    notif_member_leave = Column(Boolean, default=False)
    notif_auto_accept = Column(Boolean, default=True)


# ===========================================================================
# DATABASE SETUP + MIGRATION
# ===========================================================================

_engine_kwargs: dict = {"echo": False, "pool_pre_ping": True}
if IS_SQLITE:
    _engine_kwargs["connect_args"] = {"timeout": 30}

engine = create_async_engine(DATABASE_URL, **_engine_kwargs)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# bool literal differs between SQLite (0/1) and Postgres (TRUE/FALSE)
_T = "1" if IS_SQLITE else "TRUE"
_F = "0" if IS_SQLITE else "FALSE"


def _is_duplicate_column_error(exc: Exception) -> bool:
    """True only when the failure means 'column already exists' (safe to ignore)."""
    msg = str(exc).lower()
    return ("duplicate column" in msg or "already exists" in msg)


async def _add_column_if_missing(table: str, col: str, col_def: str):
    """Each ALTER runs in its OWN transaction so one failure never aborts the rest.
    Only an 'already exists' error is ignored; anything else is logged loudly."""
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
        logger.info("Migration: added %s.%s", table, col)
    except Exception as exc:
        if not _is_duplicate_column_error(exc):
            logger.error("Migration FAILED for %s.%s: %s", table, col, exc)


async def _dedupe_members():
    """Collapse duplicate (channel_id, user_id) rows so the unique index can be created.
    Keeps the row with the lowest id, ORs is_active, keeps earliest joined_at."""
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
    """At most one 'pending' row per (channel_id, user_id)."""
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
        ("settings", f"join_msg_enabled", f"BOOLEAN DEFAULT {_T}"),
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
        # Phase 1 additions
        ("members", "last_name", "VARCHAR(255) DEFAULT ''"),
        ("members", "first_joined_at", "TIMESTAMP"),
        ("members", "last_joined_at", "TIMESTAMP"),
        ("members", "last_left_at", "TIMESTAMP"),
        ("members", "last_verified_at", "TIMESTAMP"),
        ("join_requests", "source", "VARCHAR(20) DEFAULT 'event'"),
    ]
    for table, col, col_def in migrations:
        await _add_column_if_missing(table, col, col_def)

    # Backfill new timestamp columns from legacy joined_at (only where empty)
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

    async with SessionLocal() as s:
        gs = (await s.execute(select(GlobalSettings))).scalars().first()
        if gs is None:
            s.add(GlobalSettings())
            await s.commit()

    logger.info("Database ready: %s", DATABASE_URL.split("@")[-1])


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


FLOW_TTL_SECONDS = 15 * 60       # an abandoned prompt stops capturing messages after 15 min


@dataclass
class Flow:
    state: St = St.NONE
    data: dict = field(default_factory=dict)
    touched: float = 0.0          # monotonic time of last interaction


flows: dict = {}


def _mono() -> float:
    return asyncio.get_event_loop().time()


def flow(uid: int) -> Flow:
    """Per-admin state. A flow idle longer than FLOW_TTL_SECONDS is reset, so a forgotten
    prompt can never swallow a normal message later. Each admin has an isolated Flow."""
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
    """Run an awaitable-factory, sleeping on FloodWait and retrying."""
    for _ in range(retries):
        try:
            return await coro_factory()
        except FloodWait as fw:
            wait_s = int(getattr(fw, "value", 0) or 1)
            logger.warning("FloodWait %ss", wait_s)
            await asyncio.sleep(wait_s + 1)
    return await coro_factory()


async def safe_edit(msg: Message, txt: str, reply_markup=None):
    """edit_text that never raises MessageNotModified and falls back to a new message."""
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
            # HTML failure fallback: resend as plain text so the admin never misses it
            try:
                plain = re.sub(r"<[^>]+>", "", text_msg)
                await bot.send_message(admin_id, plain[:4090], reply_markup=reply_markup)
            except Exception:
                pass


# ===========================================================================
# KEYBOARDS
# ===========================================================================

def kb_main_panel(logged_in: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Join Requests", callback_data="reqs:overview")],
        [InlineKeyboardButton("✅ Accept All", callback_data="req:accept_all"),
         InlineKeyboardButton("❌ Decline All", callback_data="req:decline_all")],
        [InlineKeyboardButton("🔍 Search", callback_data="search:start"),
         InlineKeyboardButton("📣 Channels", callback_data="channels:list")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast:start"),
         InlineKeyboardButton("📊 Stats", callback_data="stats:show")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings:main"),
         InlineKeyboardButton("📥 Inbox", callback_data="inbox:list")],
        [InlineKeyboardButton("🛡️ Admins", callback_data="admins:list"),
         InlineKeyboardButton("🔐 Userbot ✅" if logged_in else "🔐 Userbot Login",
                              callback_data="login:menu")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="panel:refresh")],
    ])


def kb_settings_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📩 Join Message Settings", callback_data="settings:join_select")],
        [InlineKeyboardButton("🚪 Leave Message Settings", callback_data="settings:leave_select")],
        [InlineKeyboardButton("👋 Start Message Settings", callback_data="settings:start_msg")],
        [InlineKeyboardButton("🔁 Auto-Reply Settings", callback_data="settings:auto_reply")],
        [InlineKeyboardButton("🔔 Notification Settings", callback_data="settings:notifications")],
        [InlineKeyboardButton("« Back", callback_data="panel:main")],
    ])


def kb_join_msg_settings(ch_id: int, s: Settings) -> InlineKeyboardMarkup:
    status = "✅ Enabled" if s.join_msg_enabled else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Message", callback_data=f"join_msg:edit:{ch_id}")],
        [InlineKeyboardButton("🖼️ Set Media (Photo/Video)", callback_data=f"join_msg:media:{ch_id}")],
        [InlineKeyboardButton("🔗 Set Button", callback_data=f"join_msg:btn_set:{ch_id}")],
        [InlineKeyboardButton("🗑️ Remove Button", callback_data=f"join_msg:btn_remove:{ch_id}")],
        [InlineKeyboardButton(f"Join Message: {status}", callback_data=f"join_msg:toggle:{ch_id}")],
        [InlineKeyboardButton("👁️ Preview", callback_data=f"join_msg:preview:{ch_id}")],
        [InlineKeyboardButton("« Back", callback_data="settings:join_select")],
    ])


def kb_leave_msg_settings(ch_id: int, s: Settings) -> InlineKeyboardMarkup:
    status = "✅ Enabled" if s.leave_msg_enabled else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Leave Message", callback_data=f"leave_msg:edit:{ch_id}")],
        [InlineKeyboardButton("🖼️ Set Media", callback_data=f"leave_msg:media:{ch_id}")],
        [InlineKeyboardButton("🔗 Set Button", callback_data=f"leave_msg:btn_set:{ch_id}")],
        [InlineKeyboardButton("🗑️ Remove Button", callback_data=f"leave_msg:btn_remove:{ch_id}")],
        [InlineKeyboardButton(f"Leave Message: {status}", callback_data=f"leave_msg:toggle:{ch_id}")],
        [InlineKeyboardButton("👁️ Preview", callback_data=f"leave_msg:preview:{ch_id}")],
        [InlineKeyboardButton("« Back", callback_data="settings:leave_select")],
    ])


def kb_start_msg_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Start Message", callback_data="start_msg:edit")],
        [InlineKeyboardButton("🔗 Set Button", callback_data="start_msg:btn_set")],
        [InlineKeyboardButton("🗑️ Remove Button", callback_data="start_msg:btn_remove")],
        [InlineKeyboardButton("👁️ Preview", callback_data="start_msg:preview")],
        [InlineKeyboardButton("« Back", callback_data="settings:main")],
    ])


def kb_auto_reply_settings(gs: GlobalSettings) -> InlineKeyboardMarkup:
    status = "✅ Enabled" if gs.auto_reply_enabled else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Auto-Reply", callback_data="auto_reply:edit")],
        [InlineKeyboardButton("🔗 Set Button", callback_data="auto_reply:btn_set")],
        [InlineKeyboardButton("🗑️ Remove Button", callback_data="auto_reply:btn_remove")],
        [InlineKeyboardButton(f"Auto-Reply: {status}", callback_data="auto_reply:toggle")],
        [InlineKeyboardButton("👁️ Preview", callback_data="auto_reply:preview")],
        [InlineKeyboardButton("« Back", callback_data="settings:main")],
    ])


def kb_notifications(gs: GlobalSettings) -> InlineKeyboardMarkup:
    def t(v):
        return "✅" if v else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{t(gs.notif_join_request)} Join Request Notify",
                              callback_data="notif:toggle:join_request")],
        [InlineKeyboardButton(f"{t(gs.notif_member_join)} Member Joined Notify",
                              callback_data="notif:toggle:member_join")],
        [InlineKeyboardButton(f"{t(gs.notif_member_leave)} Member Left Notify",
                              callback_data="notif:toggle:member_leave")],
        [InlineKeyboardButton(f"{t(gs.notif_auto_accept)} Auto-Accept Notify",
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
        [InlineKeyboardButton("🔍 Search Member", callback_data="search:start"),
         InlineKeyboardButton("📊 Stats", callback_data="stats:show")],
    ])


def pager_row(prefix: str, page: int, pages: int) -> list:
    """[⬅️ Previous] [Page 2/8] [Next ➡️]. prefix is the callback base; page is appended."""
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"{prefix}:{page - 1}"))
    row.append(InlineKeyboardButton(f"Page {page}/{pages}", callback_data="noop"))
    if page < pages:
        row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}:{page + 1}"))
    return row


def kb_join_request_actions(channel_id: int, user_id: int) -> InlineKeyboardMarkup:
    """Per-request buttons. channel_id is embedded so we act on the RIGHT channel."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Accept", callback_data=f"jr:accept:{channel_id}:{user_id}"),
         InlineKeyboardButton("❌ Decline", callback_data=f"jr:decline:{channel_id}:{user_id}")],
        [InlineKeyboardButton("👁 Profile", callback_data=f"user:profile:{channel_id}:{user_id}"),
         InlineKeyboardButton("🔄 Refresh", callback_data=f"jr:refresh:{channel_id}:{user_id}")],
        [InlineKeyboardButton("✅ Accept All", callback_data=f"req:accept_all:{channel_id}"),
         InlineKeyboardButton("❌ Decline All", callback_data=f"req:decline_all:{channel_id}")],
    ])


def kb_user_actions(user_id: int, is_pending: bool, is_member: bool) -> InlineKeyboardMarkup:
    rows = []
    if is_pending:
        rows.append([
            InlineKeyboardButton("✅ Accept", callback_data=f"user:accept:{user_id}"),
            InlineKeyboardButton("❌ Decline", callback_data=f"user:decline:{user_id}"),
        ])
    if is_member:
        rows.append([
            InlineKeyboardButton("🚫 Remove", callback_data=f"user:remove:{user_id}"),
            InlineKeyboardButton("🔇 Mute", callback_data=f"user:mute:{user_id}"),
            InlineKeyboardButton("🔨 Ban", callback_data=f"user:ban:{user_id}"),
        ])
    rows.append([InlineKeyboardButton("👁️ Profile", callback_data=f"user:profile:{user_id}")])
    rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    return InlineKeyboardMarkup(rows)


# ===========================================================================
# TEMPLATE / SETTINGS HELPERS
# ===========================================================================

def render_template(template: str, first_name: str, last_name: str,
                    username: str, channel_name: str) -> str:
    """Substitute variables. Values are HTML-escaped because the template is sent as HTML."""
    return (template
            .replace("{first_name}", esc(first_name))
            .replace("{last_name}", esc(last_name))
            .replace("{username}", esc(f"@{username}") if username else "")
            .replace("{channel_name}", esc(channel_name))
            .replace("{date}", now_utc().strftime("%Y-%m-%d")))


def build_markup(label: str, url: str) -> Optional[InlineKeyboardMarkup]:
    if label and url and re.match(r"^(https?://|tg://)", url.strip()):
        return InlineKeyboardMarkup([[InlineKeyboardButton(label, url=url.strip())]])
    return None


def strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")


def short_preview(html_text: str, n: int = 100) -> str:
    """Plain-text, escaped, truncated preview — can never leave broken HTML."""
    plain = strip_tags(html_text or "")
    if len(plain) > n:
        plain = plain[:n] + "…"
    return esc(plain) if plain else "(not set)"


async def get_or_create_settings(session: AsyncSession, channel_id: int) -> Settings:
    row = (await session.execute(
        select(Settings).where(Settings.channel_id == channel_id)
    )).scalar_one_or_none()
    if row is None:
        row = Settings(channel_id=channel_id)
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return row


async def get_global_settings(session: AsyncSession) -> GlobalSettings:
    gs = (await session.execute(select(GlobalSettings))).scalars().first()
    if gs is None:
        gs = GlobalSettings()
        session.add(gs)
        await session.commit()
        await session.refresh(gs)
    return gs


async def get_channel_name(channel_id: int) -> str:
    async with SessionLocal() as s:
        ch = (await s.execute(select(Channel).where(Channel.channel_id == channel_id))).scalar_one_or_none()
    return ch.name if ch and ch.name else str(channel_id)


def html_of(message: Message) -> str:
    """Message text WITH formatting (bold/links/premium emoji) as HTML."""
    try:
        if message.text is not None:
            return message.text.html
        if message.caption is not None:
            return message.caption.html
    except Exception:
        pass
    return message.text or message.caption or ""


async def mark_user_blocked(user_id: int):
    try:
        async with SessionLocal() as s:
            ku = (await s.execute(select(KnownUser).where(KnownUser.user_id == user_id))).scalar_one_or_none()
            if ku:
                ku.bot_blocked = True
                await s.commit()
    except Exception as exc:
        logger.error("mark_user_blocked failed user_id=%s: %s: %s", user_id, type(exc).__name__, exc)


async def _send_configured(user_id: int, text_out: str, media_type: str, media_id: str, markup):
    """Send text / photo / video. Falls back to plain text if HTML parse fails."""
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
    """
    Returns: 'ok'  -> approved/declined
             'gone'-> request no longer exists on Telegram (already handled / user withdrew)
             'fail'-> real failure
    """
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
    """Upsert a member. Preserves history: first_joined_at is set once, last_joined_at
    updates on every (re)join, and a rejoin flips is_active back on."""
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
            if not m.is_active:                       # rejoin
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
            # A concurrent event inserted the same (channel,user) first -> unique index hit.
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
    """Mark a member as left. Returns True only if they WERE active (so callers can
    skip duplicate leave events / duplicate leave messages)."""
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
    """Accept/decline ONE request on the right channel. Only marks 'accepted' when
    Telegram confirmed it. Returns an HTML status line for the admin."""
    async with SessionLocal() as session:
        jr = (await session.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.user_id == user_id,
            JoinRequest.status == "pending"))).scalars().first()
    if jr is None:
        return "ℹ️ No pending request for that user in this channel (already handled?)."

    result = await process_join_request(channel_id, user_id, approve)
    if result == "fail":
        logger.warning("decision FAILED channel_id=%s user_id=%s approve=%s", channel_id, user_id, approve)
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
        return "ℹ️ That request was already handled on Telegram — cleared from pending."
    if approve:
        await record_member(channel_id, user_id, jr.first_name, jr.username, jr.last_name or "")
        await send_join_message(channel_id, await get_channel_name(channel_id), user_id,
                                jr.first_name, jr.last_name or "", jr.username)
    logger.info("decision OK channel_id=%s user_id=%s approve=%s", channel_id, user_id, approve)
    return f"{'✅ Accepted' if approve else '❌ Declined'} user <code>{user_id}</code>."


_bulk_running: set = set()      # channel keys currently being bulk-processed


async def bulk_process_requests(chat_id: int, channel_id: Optional[int], approve: bool):
    """Accept/decline all pending requests. Telegram is authoritative: we first reconcile
    the DB with Telegram's real list, then process, then report exactly what happened."""
    if not await userbot_ready():
        await bot.send_message(
            chat_id, "⚠️ Userbot isn't logged in yet. Open <b>🔐 Userbot Login</b> first.",
            parse_mode=HTML, reply_markup=kb_back())
        return

    key = channel_id if channel_id is not None else "all"
    if key in _bulk_running or (channel_id is not None and "all" in _bulk_running) \
            or (channel_id is None and _bulk_running):
        await bot.send_message(chat_id, "⏳ A bulk operation is already running. Wait for it to finish.",
                               reply_markup=kb_back())
        return
    _bulk_running.add(key)
    try:
        await _bulk_process_inner(chat_id, channel_id, approve)
    finally:
        _bulk_running.discard(key)


async def _bulk_process_inner(chat_id: int, channel_id: Optional[int], approve: bool):
    label = "Accepting" if approve else "Declining"
    progress = await bot.send_message(chat_id, f"🔄 Syncing pending requests from Telegram…")

    # 1) Reconcile with Telegram so we act on the REAL list, not stale DB rows.
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
            txt += "\n\n⚠️ Live sync problems (results may be incomplete):\n" + "\n".join(sync_warnings)
        await safe_edit(progress, txt, kb_back())
        return

    counters = {"ok": 0, "gone": 0, "fail": 0}
    ch_names: dict = {}
    processed = 0
    last_edit = 0.0
    sem = asyncio.Semaphore(3)               # bounded concurrency: fast but flood-safe
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
            now = asyncio.get_event_loop().time()
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
    logger.info("bulk_%s done total=%s ok=%s gone=%s fail=%s", "accept" if approve else "decline",
                total, counters["ok"], counters["gone"], counters["fail"])
    await safe_edit(progress, final, kb_back())


_sync_locks: dict = {}          # channel_id -> asyncio.Lock (no overlapping syncs)


def _sync_lock(channel_id: int) -> asyncio.Lock:
    lk = _sync_locks.get(channel_id)
    if lk is None:
        lk = _sync_locks[channel_id] = asyncio.Lock()
    return lk


async def sync_channel_requests(channel_id: int):
    """Reconcile ONE channel's pending requests against Telegram (source of truth).

    Returns (live_pending_count, error). error is None only when the FULL list was
    read successfully. On any listing failure we change NOTHING in the DB, so a
    network hiccup can never wipe real pending requests.
    """
    if not await userbot_ready():
        return 0, "userbot not connected"

    async with _sync_lock(channel_id):
        live: dict = {}
        try:
            async for r in userbot.get_chat_join_requests(channel_id):
                user = getattr(r, "user", None) or getattr(r, "from_user", None)
                if user is None:
                    continue
                live[user.id] = (user.first_name or "", user.last_name or "", user.username or "",
                                 getattr(r, "date", None))
        except FloodWait as fw:
            wait_s = int(getattr(fw, "value", 1))
            logger.warning("sync_requests channel=%s FloodWait %ss", channel_id, wait_s)
            return 0, f"FloodWait {wait_s}s"
        except Exception as exc:
            logger.warning("sync_requests channel=%s list failed: %s: %s",
                           channel_id, type(exc).__name__, exc)
            return 0, f"{type(exc).__name__}: {str(exc)[:80]}"

        # Full list read OK -> safe to reconcile.
        now = now_utc()
        try:
            async with SessionLocal() as s:
                rows = (await s.execute(select(JoinRequest).where(
                    JoinRequest.channel_id == channel_id,
                    JoinRequest.status == "pending"))).scalars().all()
                by_user = {}
                for row in rows:
                    if row.user_id in by_user:          # stray duplicate pending row
                        row.status = "expired"
                        row.processed_at = now
                    else:
                        by_user[row.user_id] = row

                for uid, (fn, ln, un, dt) in live.items():
                    row = by_user.get(uid)
                    if row is None:
                        req_time = dt.replace(tzinfo=None) if getattr(dt, "tzinfo", None) else (dt or now)
                        s.add(JoinRequest(channel_id=channel_id, user_id=uid, first_name=fn,
                                          last_name=ln, username=un, status="pending",
                                          requested_at=req_time, source="sync"))
                    else:
                        row.first_name = fn or row.first_name
                        row.last_name = ln or row.last_name
                        row.username = un or row.username

                for uid, row in by_user.items():
                    if uid not in live:
                        # No longer pending on Telegram: accepted/declined elsewhere or withdrawn.
                        row.status = "expired"
                        row.processed_at = now
                await s.commit()
        except Exception as exc:
            logger.exception("sync_requests channel=%s DB reconcile failed: %s", channel_id, exc)
            return 0, f"database error: {type(exc).__name__}"

        _pending_cache[channel_id] = (len(live), now)
        return len(live), None


async def sync_pending_with_telegram(channel_id: Optional[int] = None) -> int:
    """Back-compat wrapper (used at startup / periodic job). Returns how many
    channels were reconciled successfully."""
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
        await asyncio.sleep(0.5)         # gentle pacing between channels
    return ok


# ===========================================================================
# USERBOT LOGIN FLOW
# ===========================================================================

async def start_login_flow(admin_id: int, message_or_cq):
    reset_flow(admin_id)
    flow(admin_id).state = St.LOGIN_API_ID
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
            await bot.send_message(chat_id, "That doesn't look like a numeric API ID. Send just the number.")
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
            await bot.send_message(chat_id, "Invalid phone format. Use e.g. <code>+919876543210</code>.",
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
                await bot.send_message(chat_id, "❌ Invalid API ID / API Hash combination. Login cancelled.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except PhoneNumberInvalid:
                await bot.send_message(chat_id, "❌ Invalid phone number. Send it again "
                                                "(e.g. <code>+919876543210</code>).", parse_mode=HTML)
                await _drop_login_client()
                return
            except PhoneNumberBanned:
                await bot.send_message(chat_id, "❌ This phone number is banned from Telegram. Login cancelled.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except PhoneNumberFlood:
                await bot.send_message(chat_id, "❌ Too many login attempts on this number. Try later.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except Exception as exc:
                logger.exception("send_code failed: %s", exc)
                await bot.send_message(chat_id, f"❌ Failed to send OTP: {esc(exc)}\nLogin cancelled.",
                                       parse_mode=HTML)
                await _drop_login_client()
                reset_flow(admin_id)
                return

        f.data["phone"] = phone
        f.data["phone_code_hash"] = sent.phone_code_hash
        f.state = St.LOGIN_CODE
        await bot.send_message(
            chat_id,
            "<b>Step 4/4</b> — Enter the <b>OTP</b> Telegram just sent you.\n\n"
            "Send digits only (no dashes/spaces), e.g. <code>12345</code>.",
            reply_markup=kb_cancel_login(), parse_mode=HTML)
        return

    if f.state == St.LOGIN_CODE:
        code = re.sub(r"[^\d]", "", txt)
        if not code:
            await bot.send_message(chat_id, "Send the OTP as digits only, e.g. <code>12345</code>.",
                                   parse_mode=HTML)
            return
        async with login_lock:
            if login_client is None:
                await bot.send_message(chat_id, "Session expired. Start again from Userbot Login.")
                reset_flow(admin_id)
                return
            try:
                await login_client.sign_in(f.data["phone"], f.data["phone_code_hash"], code)
            except SessionPasswordNeeded:
                f.state = St.LOGIN_PASSWORD
                await bot.send_message(
                    chat_id, "🔒 Two-Step Verification is enabled. Send your <b>2FA password</b> now.",
                    reply_markup=kb_cancel_login(), parse_mode=HTML)
                return
            except (PhoneCodeInvalid, PhoneCodeEmpty):
                await bot.send_message(chat_id, "❌ Wrong OTP. Send the correct code again.")
                return
            except PhoneCodeExpired:
                await bot.send_message(chat_id, "❌ OTP expired. Login cancelled — start again.")
                await _drop_login_client()
                reset_flow(admin_id)
                return
            except Exception as exc:
                logger.exception("sign_in failed: %s", exc)
                await bot.send_message(chat_id, f"❌ Login failed: {esc(exc)}\nLogin cancelled.",
                                       parse_mode=HTML)
                await _drop_login_client()
                reset_flow(admin_id)
                return
        await finalize_login(admin_id, chat_id)
        return

    if f.state == St.LOGIN_PASSWORD:
        async with login_lock:
            if login_client is None:
                await bot.send_message(chat_id, "Session expired. Start again from Userbot Login.")
                reset_flow(admin_id)
                return
            try:
                await login_client.check_password(txt)
            except PasswordHashInvalid:
                await bot.send_message(chat_id, "❌ Wrong password. Try again.")
                return
            except Exception as exc:
                logger.exception("check_password failed: %s", exc)
                await bot.send_message(chat_id, f"❌ Login failed: {esc(exc)}\nLogin cancelled.",
                                       parse_mode=HTML)
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
                               f"✅ Userbot logged in as <b>{esc(me.first_name)}</b> and is now active.",
                               reply_markup=kb_main_panel(True), parse_mode=HTML)
    else:
        await bot.send_message(chat_id, "⚠️ Login saved, but the userbot failed to start. Check logs.",
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

    # Warm peer cache + verify admin status + import channels the userbot manages
    try:
        await import_userbot_channels(me.id)
    except Exception as exc:
        logger.warning("import_userbot_channels failed: %s", exc)
    return True


async def import_userbot_channels(me_id: int):
    """Register every channel where the userbot is admin (fixes 'userbot-only channel not shown')."""
    async for dialog in userbot.get_dialogs():
        chat = dialog.chat
        if chat.type not in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
            continue
        try:
            member = await userbot.get_chat_member(chat.id, me_id)
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
            continue
        except Exception as exc:
            logger.info("import_channels: skip %s (%s)", chat.id, type(exc).__name__)
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
    await bot.send_message(chat_id, "🚪 Userbot logged out and session cleared.",
                           reply_markup=kb_main_panel(False))


# ===========================================================================
# CHANNEL EVENT HANDLERS  (shared logic — attached to BOT and USERBOT)
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


_seen_events: dict = {}     # short-lived guard against the SAME event arriving twice
_BOT_ID: int = 0            # cached once; avoids bot.get_me() on every member update


def _dedup(key, ttl: float = 30.0) -> bool:
    """True if this exact event was already handled within ttl seconds.
    In-memory only: it guards near-simultaneous duplicates. Durable correctness
    comes from DB state checks (was_active / pending-row lookups) below."""
    now = asyncio.get_event_loop().time()
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
        await session.commit()
        settings = await get_or_create_settings(session, chat.id)
        gs = await get_global_settings(session)
        auto = settings.auto_accept
    _pending_cache.pop(chat.id, None)          # invalidate: count just changed

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
                logger.info("auto_accept OK channel_id=%s user_id=%s", chat.id, u.id)
                if gs.notif_auto_accept:
                    await notify_admins(f"✅ Auto-accepted: <b>{esc(u.first_name)}</b> "
                                        f"into <b>{esc(chat.title)}</b>")
            _pending_cache.pop(chat.id, None)
            return
        # Real failure: leave the request PENDING and tell admins the truth.
        auto_failed_reason = ("userbot not connected" if not await userbot_ready()
                              else "Telegram rejected the approval (check userbot admin rights)")
        logger.warning("auto_accept FAILED channel_id=%s user_id=%s reason=%s",
                       chat.id, u.id, auto_failed_reason)

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

    # Dedup on transition + Telegram's own event timestamp. True duplicates share the same
    # timestamp; a genuine leave -> rejoin -> leave has different timestamps, so it is kept.
    ev_ts = getattr(update, "date", None)
    ev_ts = int(ev_ts.timestamp()) if hasattr(ev_ts, "timestamp") else 0
    if _dedup(("mu", chat.id, u.id, str(old_status), str(new_status), ev_ts)):
        return
    _member_cache.pop(chat.id, None)             # count changed -> invalidate live cache

    async with SessionLocal() as session:
        gs = await get_global_settings(session)

    if new_status in left and (old_status in active or old_status is None):
        await _ensure_channel(chat)
        # deactivate_member returns False if we already knew they had left -> no duplicate DM.
        was_active = await deactivate_member(chat.id, u.id)
        if not was_active and old_status is None:
            logger.info("leave ignored (untracked, no prior state) ch=%s user=%s", chat.id, u.id)
            return
        async with SessionLocal() as session:
            session.add(MemberLeave(channel_id=chat.id, user_id=u.id,
                                    first_name=u.first_name or "", username=u.username or ""))
            await session.commit()
        logger.info("member_left channel_id=%s user_id=%s", chat.id, u.id)
        if was_active or old_status in active:
            await send_leave_message(chat.id, chat.title or "", u.id, u.first_name or "",
                                     u.last_name or "", u.username or "")
        if gs.notif_member_leave:
            await notify_admins(f"🚪 <b>{esc(u.first_name)}</b> "
                                f"({esc('@' + u.username) if u.username else 'no username'}) "
                                f"left <b>{esc(chat.title)}</b>")

    elif new_status in active and (old_status is None or old_status not in active):
        await _ensure_channel(chat)
        await record_member(chat.id, u.id, u.first_name or "", u.username or "", u.last_name or "")
        logger.info("member_joined channel_id=%s user_id=%s", chat.id, u.id)
        if gs.notif_member_join:
            await notify_admins(f"👤 <b>{esc(u.first_name)}</b> "
                                f"({esc('@' + u.username) if u.username else 'no username'}) "
                                f"joined <b>{esc(chat.title)}</b>")


async def _handle_bot_status(update: ChatMemberUpdated):
    new = update.new_chat_member
    chat = update.chat
    if new.status == ChatMemberStatus.ADMINISTRATOR:
        await _ensure_channel(chat)
        perms = new.privileges
        perm_lines = []
        if perms:
            for attr in ("can_invite_users", "can_delete_messages", "can_restrict_members",
                         "can_promote_members", "can_manage_chat", "can_pin_messages"):
                if getattr(perms, attr, False):
                    perm_lines.append(f"• {attr}")
        perms_txt = "\n".join(perm_lines) if perm_lines else "(none reported)"
        txt = (f"✅ <b>Bot added as admin</b>\n\nChannel: <b>{esc(chat.title)}</b>\n"
               f"ID: <code>{chat.id}</code>\n\n<b>Permissions granted:</b>\n{perms_txt}")
        if not await userbot_ready():
            txt += ("\n\n⚠️ The userbot also needs to be admin here with invite permissions. "
                    "Log it in via 🔐 Userbot Login.")
        await notify_admins(txt, kb_channel_notify(chat.id))
    elif new.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, ChatMemberStatus.MEMBER):
        # bot removed / demoted → hide channel unless userbot still manages it
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
            await notify_admins(f"⚠️ Bot removed from <b>{esc(chat.title)}</b>. Channel hidden from the list.")


def register_userbot_handlers(client: Client):
    """The BOT client already receives join-request and member updates for every channel
    it administers, so registering the same handlers on the userbot would run each event
    twice. The userbot only handles channels the bot is NOT part of."""
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


_bot_admin_cache: dict = {}     # channel_id -> (bool, ts)


async def _bot_manages(channel_id: int) -> bool:
    """True if the BOT is an admin of this channel (so it will get the events itself).
    Cached for 5 minutes to avoid a Telegram call per event."""
    now = asyncio.get_event_loop().time()
    hit = _bot_admin_cache.get(channel_id)
    if hit and now - hit[1] < 300:
        return hit[0]
    ok = False
    try:
        m = await bot.get_chat_member(channel_id, await _get_bot_id())
        ok = m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception as exc:
        logger.debug("bot admin check ch=%s failed: %s", channel_id, exc)
    _bot_admin_cache[channel_id] = (ok, now)
    return ok


# ===========================================================================
# LIVE COUNT HELPERS
# ===========================================================================

@dataclass
class CountResult:
    """A number plus WHERE it came from, so the UI never labels cached data as live."""
    value: int
    source: str                 # "LIVE" | "CACHED" | "LOCAL"
    reason: str = ""            # why we fell back (for logs / admin hint)
    fetched_at: Optional[datetime] = None

    @property
    def is_live(self) -> bool:
        return self.source == "LIVE"


# channel_id -> (count, fetched_at) : last SUCCESSFUL Telegram result
_member_cache: dict = {}
_pending_cache: dict = {}
CACHE_TTL_SECONDS = 20          # live results reused briefly to avoid API hammering


async def _client_member_count(client: Client, channel_id: int) -> int:
    """Works across pyrogram 2.0.x (get_chat_members_count) and newer forks
    (get_chat_member_count). Picks whichever this install actually provides."""
    fn = getattr(client, "get_chat_members_count", None) or getattr(client, "get_chat_member_count", None)
    if fn is None:
        raise RuntimeError("No member-count method on this Pyrogram version")
    return int(await fn(channel_id))


async def live_member_count(channel_id: int, use_ttl: bool = True) -> CountResult:
    """LIVE from Telegram when possible; else CACHED (last good Telegram value);
    else LOCAL (DB active-member rows). Never raises; logs the exact reason."""
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
            logger.warning("member_count channel=%s via %s failed: %s: %s",
                           channel_id, label, type(exc).__name__, exc)

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
    """Pending join requests. LIVE means we just listed them from Telegram AND
    reconciled the DB (sync_channel_requests). Otherwise LOCAL, clearly labelled."""
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
    """Short label appended to any dashboard number."""
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
# MAIN PANEL TEXT
# ===========================================================================

_tg_sem = asyncio.Semaphore(4)      # cap concurrent Telegram lookups (respect rate limits)


async def _gather_channel_stats(channels: list, refresh: bool):
    """Fetch member + pending counts for all channels with bounded concurrency."""
    async def one(ch):
        async with _tg_sem:
            members = await live_member_count(ch.channel_id, use_ttl=not refresh)
            pending = await live_pending_count(ch.channel_id, use_ttl=not refresh)
        return ch, members, pending
    return await asyncio.gather(*(one(c) for c in channels), return_exceptions=False)


async def build_main_panel_text(sync: bool = False) -> str:
    logged_in = await userbot_ready()
    userbot_status = "🟢 Connected" if logged_in else "🔴 Not logged in"
    userbot_info = ""
    if logged_in:
        try:
            me = await userbot.get_me()
            userbot_info = f" as {esc('@' + me.username) if me.username else esc(me.first_name)}"
        except Exception as exc:
            logger.warning("panel: userbot.get_me failed: %s", exc)

    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
        accepted_today = (await session.execute(
            select(func.count()).select_from(JoinRequest).where(
                JoinRequest.status == "accepted", JoinRequest.processed_at >= today_start()))).scalar()
        left_today = (await session.execute(
            select(func.count()).select_from(MemberLeave).where(
                MemberLeave.left_at >= today_start()))).scalar()

    results = await _gather_channel_stats(channels, refresh=sync)
    total_members = sum(m.value for _, m, _ in results)
    total_pending = sum(p.value for _, _, p in results)
    all_live_members = all(m.is_live for _, m, _ in results) if results else True
    all_live_pending = all(p.is_live for _, _, p in results) if results else True

    m_tag = "" if all_live_members else " ⚠️ some values cached/local"
    p_tag = "" if all_live_pending else " ⚠️ live unavailable — showing cached data"
    local_pending = await _local_pending_count()

    return (
        f"🤖 <b>Admin Panel</b>\n\n"
        f"🟢 Userbot: {userbot_status}{userbot_info}\n"
        f"📣 Channels: {len(channels)} managed\n"
        f"👥 Members (Telegram): {total_members:,}{m_tag}\n"
        f"⏳ Pending (Telegram): {total_pending}{p_tag}\n"
        f"🗄 Pending (Local DB): {local_pending}\n"
        f"✅ Accepted Today: {accepted_today}\n"
        f"🚪 Left Today: {left_today}\n"
        f"🚀 Bot Uptime: {fmt_uptime()}\n"
        f"🕒 Updated: {now_utc().strftime('%H:%M:%S')} UTC")


# ===========================================================================
# COMMAND HANDLERS
# ===========================================================================

async def cmd_start(client: Client, message: Message):
    u = message.from_user
    async with SessionLocal() as session:
        known = (await session.execute(select(KnownUser).where(KnownUser.user_id == u.id))).scalar_one_or_none()
        if known is None:
            session.add(KnownUser(user_id=u.id, first_name=u.first_name or "", username=u.username or ""))
        else:
            known.bot_blocked = False
        await session.commit()

    if is_admin(u.id):
        reset_flow(u.id)
        panel_text = await build_main_panel_text(sync=False)
        await message.reply_text(panel_text, reply_markup=kb_main_panel(await userbot_ready()),
                                 parse_mode=HTML)
    else:
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
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
    """Returns (text, markup). Each number is tagged when it is not LIVE."""
    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
    if not channels:
        return ("No channels yet. Add the bot (and userbot) as admin to a channel to begin.",
                kb_back())

    results = await _gather_channel_stats(channels, refresh=refresh)
    lines = ["📣 <b>Managed Channels</b>\n"]
    kb_rows = []
    for i, (ch, members, pending) in enumerate(results, 1):
        async with SessionLocal() as session:
            s = await get_or_create_settings(session, ch.channel_id)
            auto_str = "ON" if s.auto_accept else "OFF"
        lines.append(
            f"{i}. <b>{esc(ch.name)}</b>\n"
            f"   👥 Members: {members.value:,}{src_tag(members)}\n"
            f"   ⏳ Pending: {pending.value}{src_tag(pending)}\n"
            f"   ✅ Auto-Accept: {auto_str}")
        kb_rows.append([InlineKeyboardButton(f"⚙️ {ch.name}"[:60],
                                             callback_data=f"channels:settings:{ch.channel_id}")])
    lines.append(f"\n🕒 Updated: {now_utc().strftime('%H:%M:%S')} UTC")
    kb_rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="channels:refresh")])
    kb_rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(kb_rows)


async def show_channel_list(chat_id: int, edit_msg: Optional[Message] = None, refresh: bool = False):
    txt, kb = await build_channel_list(refresh=refresh)
    if edit_msg is not None:
        await safe_edit(edit_msg, txt, kb)
    else:
        await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode=HTML)


REQ_PAGE_SIZE = 8


async def build_requests_overview():
    """Per-channel LIVE pending/member counts + entry buttons."""
    channels = await _active_channels()
    if not channels:
        return "No channels yet.", kb_back()
    results = await _gather_channel_stats(channels, refresh=True)
    lines = ["⏳ <b>Join Requests</b>\n"]
    rows = []
    for ch, members, pending in results:
        lines.append(f"📣 <b>{esc(ch.name)}</b>\n"
                     f"   ⏳ Pending (Telegram): {pending.value}{src_tag(pending)}\n"
                     f"   👥 Members: {members.value:,}{src_tag(members)}\n")
        rows.append([InlineKeyboardButton(f"👁 View Requests — {ch.name}"[:60],
                                          callback_data=f"reqlist:{ch.channel_id}:1")])
        rows.append([InlineKeyboardButton("✅ Accept All", callback_data=f"req:accept_all:{ch.channel_id}"),
                     InlineKeyboardButton("❌ Decline All", callback_data=f"req:decline_all:{ch.channel_id}")])
    lines.append(f"🕒 {now_utc().strftime('%H:%M:%S')} UTC")
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="reqs:overview")])
    rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(rows)


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
                InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data=f"reqlist:{channel_id}:1")],
                                      [InlineKeyboardButton("« Back", callback_data="reqs:overview")]]))
    lines = [f"📣 <b>{esc(name)}</b> — pending {total}\n"]
    kb = []
    base = (page - 1) * REQ_PAGE_SIZE
    for i, r in enumerate(rows_, 1):
        uname = esc("@" + r.username) if r.username else "no username"
        ago = now_utc() - (r.requested_at or now_utc())
        mins = int(ago.total_seconds() // 60)
        when = f"{mins} min ago" if mins < 60 else (f"{mins // 60} h ago" if mins < 1440 else f"{mins // 1440} d ago")
        lines.append(f"<b>#{base + i}</b> 👤 {esc(r.first_name)} {esc(r.last_name or '')}\n"
                     f"   {uname}\n   ID: <code>{r.user_id}</code>\n   Requested: {when}\n")
        kb.append([InlineKeyboardButton(f"✅ #{base + i}", callback_data=f"jr:accept:{channel_id}:{r.user_id}"),
                   InlineKeyboardButton(f"❌ #{base + i}", callback_data=f"jr:decline:{channel_id}:{r.user_id}"),
                   InlineKeyboardButton(f"👁 #{base + i}", callback_data=f"user:profile:{channel_id}:{r.user_id}")])
    kb.append(pager_row(f"reqlist:{channel_id}", page, pages))
    kb.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"reqlist:{channel_id}:{page}:r"),
               InlineKeyboardButton("« Back", callback_data="reqs:overview")])
    return "\n".join(lines)[:4090], InlineKeyboardMarkup(kb)


async def cmd_requests(client: Client, message: Message):
    wait = await message.reply_text("🔄 Fetching live data from Telegram…")
    txt, kb = await build_requests_overview()
    await safe_edit(wait, txt, kb)


async def cmd_accept_all(client: Client, message: Message):
    await bulk_process_requests(message.chat.id, None, approve=True)


async def cmd_decline_all(client: Client, message: Message):
    await bulk_process_requests(message.chat.id, None, approve=False)


# ---------------------------------------------------------------------------
# USER SEARCH  (channel-scoped, multi-result, live-verified)
# ---------------------------------------------------------------------------

STATUS_LABEL = {
    "pending": "⏳ PENDING REQUEST", "member": "✅ MEMBER", "left": "🚪 LEFT",
    "banned": "🔨 BANNED", "declined": "❌ DECLINED", "unknown": "❔ UNKNOWN",
}


async def live_user_status(channel_id: int, user_id: int):
    """Ask Telegram for the user's CURRENT state in the channel.
    Returns (status_key or None, error_or_None). None means 'could not verify'."""
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
        # Telegram says the user simply is not in the chat -> that IS a definite answer.
        if "UserNotParticipant" in name or "USER_NOT_PARTICIPANT" in str(exc).upper():
            return "left", None
        return None, f"{name}"


async def resolve_user_status(channel_id: int, user_id: int, verify: bool = True):
    """Combine DB history with a live Telegram check. Telegram wins when it answers.
    Returns dict with status, source ('LIVE'|'LOCAL'), verify_error, and DB rows."""
    async with SessionLocal() as s:
        pending = (await s.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.user_id == user_id,
            JoinRequest.status == "pending"))).scalars().first()
        last_req = (await s.execute(select(JoinRequest).where(
            JoinRequest.channel_id == channel_id, JoinRequest.user_id == user_id
        ).order_by(JoinRequest.id.desc()))).scalars().first()
        mem = (await s.execute(select(Member).where(
            Member.channel_id == channel_id, Member.user_id == user_id))).scalars().first()

    # local guess from history
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
                if mem is None or not mem.is_active:      # reconcile DB with Telegram
                    await record_member(channel_id, user_id,
                                        (mem.first_name if mem else "") or (last_req.first_name if last_req else ""),
                                        (mem.username if mem else "") or (last_req.username if last_req else ""))
            elif live in ("banned", "left"):
                # A live 'left' with a genuine pending request stays PENDING (requesters aren't members).
                status = "pending" if (pending and live == "left") else live
                if mem and mem.is_active:
                    await deactivate_member(channel_id, user_id)
    return {"status": status, "source": source, "verify_error": verr,
            "pending": pending, "last_req": last_req, "member": mem}


def _fmt_dt(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M") + " UTC" if dt else "—"


async def find_users(query: str, channel_id: Optional[int], limit: int = 50):
    """Local search across members + join requests. Returns list of
    (channel_id, user_id, first_name, last_name, username), de-duplicated."""
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
    """Returns (html_text, markup) for a full profile in one channel, with live verification."""
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
        except Exception as exc:
            logger.info("profile: get_users(%s) failed: %s", user_id, type(exc).__name__)

    async with SessionLocal() as s:
        blocked = (await s.execute(select(BlockedUser).where(BlockedUser.user_id == user_id))).scalar_one_or_none()
        msgs = (await s.execute(select(func.count()).select_from(Conversation).where(
            Conversation.user_id == user_id))).scalar()
    chan = await get_channel_name(channel_id)
    if info["source"] == "LIVE":
        src_line = "🟢 Source: LIVE (verified with Telegram)"
    else:
        why = f" — {esc(info['verify_error'])}" if info["verify_error"] else ""
        src_line = f"⚠️ Source: LOCAL DB only, live check failed{why}"

    txt = (f"👤 <b>USER PROFILE</b>\n\n"
           f"Name: {esc((fn + ' ' + ln).strip() or '(unknown)')}\n"
           f"Username: {esc('@' + un) if un else '(none)'}\n"
           f"ID: <code>{user_id}</code>\n\n"
           f"📣 Channel: {esc(chan)}\n"
           f"Current Status: <b>{STATUS_LABEL[st]}</b>\n{src_line}\n\n"
           f"🕒 First Joined: {_fmt_dt(mem.first_joined_at or mem.joined_at) if mem else '—'}\n"
           f"🕒 Last Joined: {_fmt_dt(mem.last_joined_at) if mem else '—'}\n"
           f"🕒 Last Left: {_fmt_dt(mem.last_left_at) if mem else '—'}\n"
           f"⏳ Pending Request: {'Yes — since ' + _fmt_dt(pend.requested_at) if pend else 'No'}\n"
           f"📅 Last Request: {(last_req.status + ' ' + _fmt_dt(last_req.requested_at)) if last_req else '—'}\n\n"
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
    """Search by ID / @username / partial username / name, optionally within one channel."""
    query = query.strip()
    if not query:
        await bot.send_message(chat_id, "Send a user ID, @username or a name.", reply_markup=kb_back())
        return
    logger.info("search query_len=%d channel_id=%s", len(query), channel_id)
    results = await find_users(query, channel_id)

    # Nothing local -> try a live Telegram lookup so brand-new users are still found.
    if not results and await userbot_ready():
        try:
            lookup = int(query) if query.lstrip("-").isdigit() else query.lstrip("@")
            u = await userbot.get_users(lookup)
            targets = ([channel_id] if channel_id is not None else
                       [c.channel_id for c in await _active_channels()])
            for ch in targets:
                results.append((ch, u.id, u.first_name or "", u.last_name or "", u.username or ""))
        except (PeerIdInvalid, UsernameNotOccupied, KeyError, IndexError):
            pass
        except Exception as exc:
            logger.warning("search live lookup failed: %s: %s", type(exc).__name__, exc)
            await bot.send_message(chat_id, f"❌ Live lookup failed: {esc(type(exc).__name__)}",
                                   reply_markup=kb_back(), parse_mode=HTML)
            return

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
        await message.reply_text("Usage: <code>/search &lt;user_id or @username&gt;</code>", parse_mode=HTML)
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
    target_id = int(parts[1].strip())
    async with SessionLocal() as session:
        exists = (await session.execute(select(BlockedUser).where(BlockedUser.user_id == target_id))).scalar_one_or_none()
        if not exists:
            session.add(BlockedUser(user_id=target_id))
            await session.commit()
    await message.reply_text(f"🔇 User <code>{target_id}</code> blocked.", parse_mode=HTML)


async def cmd_unblock(client: Client, message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await message.reply_text("Usage: <code>/unblock &lt;user_id&gt;</code>", parse_mode=HTML)
        return
    target_id = int(parts[1].strip())
    async with SessionLocal() as session:
        row = (await session.execute(select(BlockedUser).where(BlockedUser.user_id == target_id))).scalar_one_or_none()
        if row:
            await session.delete(row)
            await session.commit()
    await message.reply_text(f"🔊 User <code>{target_id}</code> unblocked.", parse_mode=HTML)


async def cmd_broadcast(client: Client, message: Message):
    reset_flow(message.from_user.id)
    flow(message.from_user.id).state = St.BC_CONTENT
    await message.reply_text(
        "Send the broadcast content now (text, photo, video, voice or document).\n"
        "The exact message will be copied and sent to all reachable users.",
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

    logged_in = await userbot_ready()
    userbot_status_txt = "🟢 Connected" if logged_in else "🔴 Not logged in"
    userbot_info = ""
    if logged_in:
        try:
            me = await userbot.get_me()
            userbot_info = (f" as {esc('@' + me.username) if me.username else esc(me.first_name)} "
                            f"(ID: <code>{me.id}</code>)")
        except Exception as exc:
            logger.warning("stats: userbot.get_me failed: %s", exc)

    results = await _gather_channel_stats(channels, refresh=refresh)
    ts = today_start()
    blocks = []
    for ch, members, pending in results:
        async with SessionLocal() as session:
            # "Joined today" = members whose LATEST join is today (covers rejoins too)
            joined_today = (await session.execute(select(func.count()).select_from(Member).where(
                Member.channel_id == ch.channel_id,
                func.coalesce(Member.last_joined_at, Member.joined_at) >= ts))).scalar()
            left_today = (await session.execute(select(func.count()).select_from(MemberLeave).where(
                MemberLeave.channel_id == ch.channel_id, MemberLeave.left_at >= ts))).scalar()
            tracked = (await session.execute(select(func.count()).select_from(Member).where(
                Member.channel_id == ch.channel_id, Member.is_active == True))).scalar()  # noqa: E712
            local_pending = await _local_pending_count(ch.channel_id)
        blocks.append(
            f"📣 <b>{esc(ch.name)}</b>\n"
            f"  👥 Members (Telegram): {members.value:,}{src_tag(members)}\n"
            f"  ⏳ Pending (Telegram): {pending.value}{src_tag(pending)}\n"
            f"  🗄 Pending (Local DB): {local_pending}\n"
            f"  🧾 Tracked active members: {tracked}\n"
            f"  ✅ Joined Today: {joined_today}\n"
            f"  🚪 Left Today: {left_today}")
    ch_section = "\n\n".join(blocks) if blocks else "(no channels)"

    return (f"<b>📊 Statistics</b>\n\n🤖 Userbot: {userbot_status_txt}{userbot_info}\n"
            f"📣 Channels: {len(channels)}\n\n━━━ Per Channel ━━━\n{ch_section}\n\n"
            f"━━━ Bot Stats ━━━\n📨 Broadcasts Sent: {broadcasts_sent}\n"
            f"💬 Active Conversations: {active_convos}\n🛡️ Admins: {admin_count}\n"
            f"⏰ Bot Uptime: {fmt_uptime()}\n🕒 Updated: {now_utc().strftime('%H:%M:%S')} UTC")


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
        source = f"✅ Set via .env (API_ID: <code>{esc(ENV_API_ID)}</code>)"
    elif cur_id and cur_hash:
        source = f"✅ Saved in DB (API_ID: <code>{esc(cur_id)}</code>)"
    else:
        source = "❌ Not set — bot running on temp credentials"
    await message.reply_text(
        f"<b>⚙️ Bot API Credentials Setup</b>\n\n<b>Current status:</b> {source}\n\n"
        f"Get credentials: https://my.telegram.org → API Development Tools\n\n"
        f"<b>Step 1/2</b> — Send your <b>API ID</b> (numbers only):",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="setapi:cancel")]]),
        parse_mode=HTML, disable_web_page_preview=True)


async def cmd_help(client: Client, message: Message):
    await message.reply_text(
        "<b>Admin Commands</b>\n"
        "/start, /panel — main menu\n/channels — list managed channels\n"
        "/requests — show pending join requests\n/accept_all, /decline_all — bulk process requests\n"
        "/search &lt;id|@user&gt; — find a user\n/inbox — view user conversations\n"
        "/block &lt;id&gt;, /unblock &lt;id&gt; — manage messaging access\n"
        "/broadcast — start a broadcast\n/stats — statistics dashboard\n"
        "/settings — bot settings\n/admins — manage admins\n/login — log the userbot in\n"
        "/setapi — set bot API credentials (owner only)", parse_mode=HTML)


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

    # ---- Regular user -> auto-reply + relay ----
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

        if gs.auto_reply_enabled and not known.auto_reply_sent and gs.auto_reply_text:
            markup = build_markup(gs.auto_reply_btn_label, gs.auto_reply_btn_url)
            try:
                await bot.send_message(uid, gs.auto_reply_text, parse_mode=HTML, reply_markup=markup,
                                       disable_web_page_preview=True)
                known.auto_reply_sent = True
            except Exception as exc:
                logger.warning("Auto-reply failed for %s: %s", uid, exc)

        session.add(Conversation(user_id=uid, direction="in",
                                 message=(message.text or message.caption or media_label(message))))
        await session.commit()

    u = message.from_user
    header = (f"👤 <b>{esc(u.first_name)}</b> | {esc('@' + u.username) if u.username else 'no username'} "
              f"| ID: <code>{u.id}</code>")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Reply", callback_data=f"inbox:reply:{u.id}")]])

    if message.text:
        await notify_admins(f"{header}\n\n{esc(message.text)}", kb)
    else:
        # forward the actual media/voice/etc to every admin, plus a header
        for admin_id in list(_admin_ids):
            try:
                await flood_safe(lambda a=admin_id: bot.copy_message(
                    a, message.chat.id, message.id,
                    caption=(f"{header}\n\n{esc(message.caption)}" if message.caption else header)[:1024],
                    parse_mode=HTML, reply_markup=kb))
            except Exception as exc:
                logger.warning("copy to admin %s failed: %s", admin_id, exc)
                try:
                    await bot.send_message(admin_id, f"{header}\n\n{media_label(message)}",
                                           parse_mode=HTML, reply_markup=kb)
                except Exception:
                    pass


async def _settings_reply_join(message, ch_id):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
    name = await get_channel_name(ch_id)
    await message.reply_text(_join_settings_text(name, s), reply_markup=kb_join_msg_settings(ch_id, s),
                             parse_mode=HTML)


async def _settings_reply_leave(message, ch_id):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
    name = await get_channel_name(ch_id)
    await message.reply_text(_leave_settings_text(name, s), reply_markup=kb_leave_msg_settings(ch_id, s),
                             parse_mode=HTML)


def _valid_url(u: str) -> bool:
    return bool(re.match(r"^(https?://|tg://)\S+$", u.strip()))


async def handle_admin_message(client: Client, message: Message, uid: int):
    f = flow(uid)
    st = f.state
    chat_id = message.chat.id
    txt = message.text.strip() if message.text else ""

    if st == St.NONE:
        return

    # ---- login ----
    if st in (St.LOGIN_API_ID, St.LOGIN_API_HASH, St.LOGIN_PHONE, St.LOGIN_CODE, St.LOGIN_PASSWORD):
        if message.text:
            await handle_login_text(uid, chat_id, message.text)
        return

    # ---- bot API creds ----
    if st == St.SET_BOT_API_ID:
        if not txt.isdigit():
            await message.reply_text("❌ API ID must be numbers only. Try again:")
            return
        f.data["new_bot_api_id"] = txt
        f.state = St.SET_BOT_API_HASH
        await message.reply_text(
            "<b>Step 2/2</b> — Send your <b>API Hash</b>:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="setapi:cancel")]]),
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
            f"✅ <b>API Credentials saved!</b>\n\nAPI ID: <code>{esc(new_id)}</code>\n"
            f"API Hash: <code>{esc(txt[:6])}...</code> (hidden)\n\n⚠️ Restart the bot to load new credentials.",
            reply_markup=kb_main_panel(await userbot_ready()), parse_mode=HTML)
        return

    # ---- add admin ----
    if st == St.ADD_ADMIN:
        if not txt.lstrip("-").isdigit():
            await message.reply_text("Send a numeric Telegram user ID.")
            return
        target_id = int(txt)
        async with SessionLocal() as session:
            exists = (await session.execute(select(Admin).where(Admin.user_id == target_id))).scalar_one_or_none()
            if exists:
                await message.reply_text("Already an admin.")
            else:
                name = ""
                try:
                    u = await bot.get_users(target_id)
                    name = u.first_name or ""
                except Exception as exc:
                    logger.info("add_admin: could not resolve name for %s (%s)", target_id, type(exc).__name__)
                session.add(Admin(user_id=target_id, name=name))
                await session.commit()
                await reload_admins()
                await message.reply_text(f"✅ Added <code>{target_id}</code> as admin.", parse_mode=HTML)
                try:
                    await bot.send_message(target_id, "🛡️ You've been made an admin of this bot. Send /start.")
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

    # ---- broadcast content ----
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
                [InlineKeyboardButton("❌ Cancel", callback_data="confirm:cancel")],
            ]), parse_mode=HTML)
        return

    if st == St.BC_SCHEDULE:
        try:
            run_at = datetime.strptime(txt, "%Y-%m-%d %H:%M")
        except ValueError:
            await message.reply_text("Invalid format. Use <code>YYYY-MM-DD HH:MM</code> (UTC).", parse_mode=HTML)
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

    # ---- inbox reply ----
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

    # ---- join / leave message text (keeps formatting + premium emoji) ----
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
            f"✅ {kind.capitalize()} message text saved!\n\nAdd a button?",
            reply_markup=kb_btn_ask(f"btn_ask:yes:{kind}:{ch_id}", f"btn_ask:no:{kind}:{ch_id}"))
        return

    if st in (St.JOIN_MSG_BTN_LABEL, St.LEAVE_MSG_BTN_LABEL, St.START_MSG_BTN_LABEL, St.AUTO_REPLY_BTN_LABEL):
        if not txt:
            return
        f.data["btn_label"] = txt[:60]
        f.state = {St.JOIN_MSG_BTN_LABEL: St.JOIN_MSG_BTN_URL, St.LEAVE_MSG_BTN_LABEL: St.LEAVE_MSG_BTN_URL,
                   St.START_MSG_BTN_LABEL: St.START_MSG_BTN_URL,
                   St.AUTO_REPLY_BTN_LABEL: St.AUTO_REPLY_BTN_URL}[st]
        await message.reply_text("Now send the button URL (must start with https://):")
        return

    if st in (St.JOIN_MSG_BTN_URL, St.LEAVE_MSG_BTN_URL):
        if not _valid_url(txt):
            await message.reply_text("❌ Invalid URL. It must start with https:// — try again:")
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
        if st == St.JOIN_MSG_BTN_URL:
            await _settings_reply_join(message, ch_id)
        else:
            await _settings_reply_leave(message, ch_id)
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
        if st == St.JOIN_MSG_MEDIA:
            await _settings_reply_join(message, ch_id)
        else:
            await _settings_reply_leave(message, ch_id)
        return

    # ---- start message ----
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
            await message.reply_text("❌ Invalid URL. It must start with https:// — try again:")
            return
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            gs.start_btn_label, gs.start_btn_url = f.data.get("btn_label", ""), txt
            await session.commit()
        reset_flow(uid)
        await message.reply_text("✅ Start message button saved!", reply_markup=kb_start_msg_settings())
        return

    # ---- auto reply ----
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
            await message.reply_text("❌ Invalid URL. It must start with https:// — try again:")
            return
        async with SessionLocal() as session:
            gs = await get_global_settings(session)
            gs.auto_reply_btn_label, gs.auto_reply_btn_url = f.data.get("btn_label", ""), txt
            await session.commit()
            gs = await get_global_settings(session)
        reset_flow(uid)
        await message.reply_text("✅ Auto-reply button saved!", reply_markup=kb_auto_reply_settings(gs))
        return


# ===========================================================================
# BROADCAST
# ===========================================================================

async def _send_one_broadcast(target: int, from_chat: int, msg_id: int) -> str:
    """Returns 'ok' | 'blocked' | 'fail'."""
    for _ in range(3):
        try:
            await bot.copy_message(target, from_chat, msg_id)
            return "ok"
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, UserPrivacyRestricted):
            await mark_user_blocked(target)
            return "blocked"
        except Exception as exc:
            logger.debug("broadcast to %s failed: %s", target, exc)
            return "fail"
    return "fail"


async def broadcast_targets() -> list:
    async with SessionLocal() as session:
        rows = (await session.execute(select(KnownUser.user_id).where(
            KnownUser.bot_blocked == False))).all()  # noqa: E712
    ids = [r[0] for r in rows if r[0] not in _admin_ids]
    return ids


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
        now = asyncio.get_event_loop().time()
        if progress_msg is not None and now - last_edit > 2.5:
            last_edit = now
            try:
                await progress_msg.edit_text(f"📤 Sent: {sent}/{total} | Blocked: {blocked} | Failed: {failed}")
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
    scheduler.add_job(run_scheduled_broadcast, "date", run_date=run_at.replace(tzinfo=timezone.utc),
                      args=[broadcast_id], id=f"bc_{broadcast_id}", replace_existing=True,
                      misfire_grace_time=3600)


async def restore_scheduled_broadcasts():
    async with SessionLocal() as session:
        rows = (await session.execute(select(Broadcast).where(
            Broadcast.status == "pending", Broadcast.scheduled_at.isnot(None)))).scalars().all()
    for b in rows:
        run_at = b.scheduled_at if b.scheduled_at > now_utc() else now_utc() + timedelta(seconds=15)
        schedule_broadcast_job(b.id, run_at)
    if rows:
        logger.info("Restored %d scheduled broadcast(s)", len(rows))


# ===========================================================================
# SETTINGS TEXT BUILDERS
# ===========================================================================

def _msg_settings_text(title_emoji: str, title: str, ch_name: str, enabled: bool, text_html: str,
                       btn_label: str, btn_url: str, media_type: str) -> str:
    status = "✅ Enabled" if enabled else "❌ Disabled"
    btn_info = f"🔗 {esc(btn_label)} → {esc(btn_url)}" if btn_label else "(none)"
    media_info = f"📷 {esc(media_type.capitalize())}" if media_type else "(none)"
    return (f"{title_emoji} <b>{title}</b>\nChannel: <b>{esc(ch_name)}</b>\n\n"
            f"Status: {status}\nMessage: <i>{short_preview(text_html)}</i>\n"
            f"Button: {btn_info}\nMedia: {media_info}")


def _join_settings_text(ch_name: str, s: Settings) -> str:
    return _msg_settings_text("📩", "Join Message Settings", ch_name, s.join_msg_enabled, s.join_msg_text,
                              s.join_btn_label, s.join_btn_url, s.join_msg_media_type or "")


def _leave_settings_text(ch_name: str, s: Settings) -> str:
    return _msg_settings_text("🚪", "Leave Message Settings", ch_name, s.leave_msg_enabled, s.leave_msg_text,
                              s.leave_btn_label, s.leave_btn_url, s.leave_msg_media_type or "")


def _start_settings_text(gs: GlobalSettings) -> str:
    btn = f"🔗 {esc(gs.start_btn_label)} → {esc(gs.start_btn_url)}" if gs.start_btn_label else "(none)"
    return (f"👋 <b>Start Message Settings</b>\n\nMessage: <i>{short_preview(gs.start_msg_text, 200)}</i>\n"
            f"Button: {btn}")


def _auto_reply_settings_text(gs: GlobalSettings) -> str:
    status = "✅ Enabled" if gs.auto_reply_enabled else "❌ Disabled"
    btn = f"🔗 {esc(gs.auto_reply_btn_label)}" if gs.auto_reply_btn_label else "(none)"
    return (f"🔁 <b>Auto-Reply Settings</b>\n\nStatus: {status}\n"
            f"Message: <i>{short_preview(gs.auto_reply_text, 200)}</i>\nButton: {btn}")


async def _send_preview(chat_id: int, title: str, html_text: str):
    try:
        await bot.send_message(chat_id, f"<b>👁️ {title}:</b>\n\n{html_text or '(not set)'}",
                               parse_mode=HTML, disable_web_page_preview=True)
    except Exception:
        await bot.send_message(chat_id, f"{title}:\n\n{strip_tags(html_text) or '(not set)'}")


# ===========================================================================
# CALLBACK ROUTER
# ===========================================================================

async def show_join_settings(cq: CallbackQuery, ch_id: int):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
    await safe_edit(cq.message, _join_settings_text(await get_channel_name(ch_id), s),
                    kb_join_msg_settings(ch_id, s))


async def show_leave_settings(cq: CallbackQuery, ch_id: int):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
    await safe_edit(cq.message, _leave_settings_text(await get_channel_name(ch_id), s),
                    kb_leave_msg_settings(ch_id, s))


async def show_channel_settings(cq: CallbackQuery, ch_id: int):
    async with SessionLocal() as session:
        s = await get_or_create_settings(session, ch_id)
    name = await get_channel_name(ch_id)
    auto_str = "✅ ON" if s.auto_accept else "❌ OFF"
    await safe_edit(
        cq.message, f"⚙️ <b>Settings: {esc(name)}</b>\n\nAuto-Accept: {auto_str}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Join Message", callback_data=f"settings:join:{ch_id}")],
            [InlineKeyboardButton("🚪 Leave Message", callback_data=f"settings:leave:{ch_id}")],
            [InlineKeyboardButton(f"Auto-Accept: {auto_str}", callback_data=f"autoaccept:toggle:{ch_id}")],
            [InlineKeyboardButton("« Back", callback_data="channels:list")],
        ]))


async def on_callback(client: Client, cq: CallbackQuery):
    if cq.from_user is None or not is_admin(cq.from_user.id):
        try:
            await cq.answer("Not authorized.", show_alert=True)
        except Exception:
            pass
        return

    data = cq.data or ""
    admin_id = cq.from_user.id
    answered = False

    async def ack(text_: str = "", alert: bool = False):
        nonlocal answered
        if answered:
            return
        answered = True
        try:
            await cq.answer(text_, show_alert=alert)
        except Exception:
            pass

    try:
        chat_id = cq.message.chat.id
        p = data.split(":")

        if data == "noop":
            await ack()

        # ---- panel ----
        elif data in ("panel:main", "panel:refresh"):
            await ack("Refreshing…" if data == "panel:refresh" else "")
            reset_flow(admin_id)
            panel_text = await build_main_panel_text(sync=(data == "panel:refresh"))
            await safe_edit(cq.message, panel_text, kb_main_panel(await userbot_ready()))

        elif data == "reqs:overview":
            await ack("Refreshing…")
            txt, kb = await build_requests_overview()
            await safe_edit(cq.message, txt, kb)

        elif p[0] == "reqlist" and len(p) >= 3:
            ch_id, page = int(p[1]), int(p[2])
            force = len(p) >= 4 and p[3] == "r"
            await ack("Syncing…" if force else "")
            if force:
                _, err = await sync_channel_requests(ch_id)
                if err:
                    logger.info("reqlist refresh sync error ch=%s: %s", ch_id, err)
            txt, kb = await build_request_page(ch_id, page)
            await safe_edit(cq.message, txt, kb)

        # ---- accept / decline all ----
        elif data.startswith("req:accept_all") or data.startswith("req:decline_all"):
            approve = data.startswith("req:accept_all")
            ch_id = int(p[2]) if len(p) > 2 else None
            await ack("Processing…")
            await bulk_process_requests(chat_id, ch_id, approve)

        # ---- search ----
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
                f"🔍 Searching in {scope}.\n\nSend a <b>user ID</b>, <b>@username</b>, or part of a "
                f"<b>name</b>:", kb_back("search:start"))

        # ---- channels ----
        elif data in ("channels:list", "channels:refresh"):
            await ack("Refreshing…" if data == "channels:refresh" else "")
            await show_channel_list(chat_id, edit_msg=cq.message, refresh=(data == "channels:refresh"))

        elif data.startswith("channels:settings:"):
            await ack()
            await show_channel_settings(cq, int(p[2]))

        elif data.startswith("autoaccept:toggle:"):
            ch_id = int(p[2])
            async with SessionLocal() as session:
                s = await get_or_create_settings(session, ch_id)
                s.auto_accept = not s.auto_accept
                new_val = s.auto_accept
                await session.commit()
            await ack(f"Auto-accept {'ON' if new_val else 'OFF'}.")
            await show_channel_settings(cq, ch_id)

        # ---- broadcast ----
        elif data == "broadcast:start":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.BC_CONTENT
            await safe_edit(cq.message,
                            "Send the broadcast content (text, photo, video, voice or document).\n"
                            "The exact message will be copied to all reachable users.", kb_back())

        # ---- stats ----
        elif data in ("stats:show", "stats:refresh"):
            await ack("Loading…")
            txt = await build_stats_text(refresh=(data == "stats:refresh"))
            if data == "stats:refresh":
                await safe_edit(cq.message, txt, KB_STATS)
            else:
                await bot.send_message(chat_id, txt, reply_markup=KB_STATS, parse_mode=HTML)

        # ---- settings main ----
        elif data == "settings:main":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, "⚙️ <b>Settings</b>", kb_settings_main())

        # ---- join / leave channel-select ----
        elif data in ("settings:join_select", "settings:leave_select"):
            kind = "join" if data == "settings:join_select" else "leave"
            async with SessionLocal() as session:
                channels = (await session.execute(
                    select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
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
            await safe_edit(cq.message, _start_settings_text(gs), kb_start_msg_settings())

        elif data == "settings:auto_reply":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
            await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))

        elif data == "settings:notifications":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
            await safe_edit(cq.message, "🔔 <b>Notification Settings</b>\n<i>(applies to all channels)</i>",
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
            await ack("Updated.")
            await safe_edit(cq.message, "🔔 <b>Notification Settings</b>\n<i>(applies to all channels)</i>",
                            kb_notifications(gs))

        # ---- join_msg / leave_msg actions ----
        elif p[0] in ("join_msg", "leave_msg") and len(p) >= 3:
            kind = "join" if p[0] == "join_msg" else "leave"
            action, ch_id = p[1], int(p[2])
            show = show_join_settings if kind == "join" else show_leave_settings
            f = flow(admin_id)
            pre = "JOIN_MSG" if kind == "join" else "LEAVE_MSG"

            if action == "edit":
                await ack()
                reset_flow(admin_id)
                f = flow(admin_id)
                f.state, f.data["channel_id"] = St[f"{pre}_TEXT"], ch_id
                await safe_edit(
                    cq.message,
                    f"Send the {kind} message text.\nFormatting & premium emoji are kept.\n"
                    "Variables: <code>{first_name}</code> <code>{last_name}</code> "
                    "<code>{username}</code> <code>{channel_name}</code> <code>{date}</code>",
                    kb_back(f"settings:{kind}:{ch_id}"))
            elif action == "media":
                await ack()
                reset_flow(admin_id)
                f = flow(admin_id)
                f.state, f.data["channel_id"] = St[f"{pre}_MEDIA"], ch_id
                await safe_edit(
                    cq.message, f"Send a photo or video to attach to the {kind} message.",
                    InlineKeyboardMarkup([
                        [InlineKeyboardButton("🗑️ Remove Media", callback_data=f"{p[0]}:media_remove:{ch_id}")],
                        [InlineKeyboardButton("« Back", callback_data=f"settings:{kind}:{ch_id}")]]))
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
                await safe_edit(cq.message, "Send the button label (button text):",
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
                tpl = getattr(s, f"{kind}_msg_text") or "(no message set)"
                prev = render_template(tpl, cq.from_user.first_name or "Alex", "",
                                       cq.from_user.username or "alex", await get_channel_name(ch_id))
                await _send_preview(chat_id, f"{kind.capitalize()} Message Preview", prev)
            else:
                await ack()

        # ---- button ask yes/no ----
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
                    await safe_edit(cq.message, "Send the button label (button text):")
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
                    await safe_edit(cq.message, "Send the button label for the start message:")
                else:
                    reset_flow(admin_id)
                    async with SessionLocal() as session:
                        gs = await get_global_settings(session)
                    await safe_edit(cq.message, _start_settings_text(gs), kb_start_msg_settings())
            elif target == "auto_reply":
                if yn == "yes":
                    reset_flow(admin_id)
                    flow(admin_id).state = St.AUTO_REPLY_BTN_LABEL
                    await safe_edit(cq.message, "Send the button label for the auto-reply:")
                else:
                    reset_flow(admin_id)
                    async with SessionLocal() as session:
                        gs = await get_global_settings(session)
                    await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))

        # ---- start message ----
        elif data == "start_msg:edit":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.START_MSG_TEXT
            await safe_edit(cq.message, "Send the start message text (formatting + premium emoji kept):",
                            kb_back("settings:start_msg"))
        elif data == "start_msg:btn_set":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.START_MSG_BTN_LABEL
            await safe_edit(cq.message, "Send the button label for the start message:",
                            kb_back("settings:start_msg"))
        elif data == "start_msg:btn_remove":
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                gs.start_btn_label, gs.start_btn_url = "", ""
                await session.commit()
                gs = await get_global_settings(session)
            await ack("Button removed.")
            await safe_edit(cq.message, _start_settings_text(gs), kb_start_msg_settings())
        elif data == "start_msg:preview":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
            await _send_preview(chat_id, "Start Message Preview", gs.start_msg_text)

        # ---- auto reply ----
        elif data == "auto_reply:edit":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.AUTO_REPLY_TEXT
            await safe_edit(cq.message, "Send the auto-reply text (formatting + premium emoji kept):",
                            kb_back("settings:auto_reply"))
        elif data == "auto_reply:btn_set":
            await ack()
            reset_flow(admin_id)
            flow(admin_id).state = St.AUTO_REPLY_BTN_LABEL
            await safe_edit(cq.message, "Send the button label for the auto-reply:",
                            kb_back("settings:auto_reply"))
        elif data == "auto_reply:btn_remove":
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                gs.auto_reply_btn_label, gs.auto_reply_btn_url = "", ""
                await session.commit()
                gs = await get_global_settings(session)
            await ack("Button removed.")
            await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))
        elif data == "auto_reply:toggle":
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
                gs.auto_reply_enabled = not gs.auto_reply_enabled
                await session.commit()
                gs = await get_global_settings(session)
            await ack("Updated.")
            await safe_edit(cq.message, _auto_reply_settings_text(gs), kb_auto_reply_settings(gs))
        elif data == "auto_reply:preview":
            await ack()
            async with SessionLocal() as session:
                gs = await get_global_settings(session)
            await _send_preview(chat_id, "Auto-Reply Preview", gs.auto_reply_text)

        # ---- inbox ----
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
            await bot.send_message(chat_id, f"Type your reply to <code>{target_id}</code> "
                                            f"(text, photo, voice, etc.):", parse_mode=HTML)

        # ---- user actions ----
        # ---- per-request actions (channel-scoped: fixes acting on the wrong channel) ----
        elif p[0] == "jr" and len(p) >= 4:
            action, ch_id, target_id = p[1], int(p[2]), int(p[3])
            if action in ("accept", "decline"):
                approve = action == "accept"
                if not await userbot_ready():
                    await ack("Userbot not logged in.", True)
                    return
                await ack("Processing…")
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
                    f"👥 Members: {members.value:,}{src_tag(members)}\n"
                    f"🕒 {now_utc().strftime('%H:%M:%S')} UTC",
                    kb_join_request_actions(ch_id, target_id) if jr else kb_back())
            else:
                await ack()

        elif data.startswith("user:accept:") or data.startswith("user:decline:"):
            # Legacy 3-part callback (old messages still in chats). Resolve the channel from
            # the pending rows; if the user is pending in several, ask instead of guessing.
            target_id = int(p[2])
            approve = p[1] == "accept"
            if not await userbot_ready():
                await ack("Userbot not logged in.", True)
                return
            async with SessionLocal() as session:
                rows = (await session.execute(select(JoinRequest).where(
                    JoinRequest.user_id == target_id, JoinRequest.status == "pending"))).scalars().all()
            if not rows:
                await ack("No pending request found.", True)
                return
            if len(rows) > 1:
                await ack()
                btns = [[InlineKeyboardButton(
                    f"{'✅' if approve else '❌'} {await get_channel_name(r.channel_id)}"[:60],
                    callback_data=f"jr:{p[1]}:{r.channel_id}:{target_id}")] for r in rows]
                btns.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
                await safe_edit(cq.message, "This user has requests in several channels. Pick one:",
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
                logger.warning("user action %s failed ch=%s user=%s: %s: %s",
                               action, ch_id, target_id, type(exc).__name__, exc)
                await ack(f"Failed: {type(exc).__name__} — check userbot admin rights.", True)
                return
            _member_cache.pop(ch_id, None)
            logger.info("user_action %s channel_id=%s user_id=%s admin_id=%s",
                        action, ch_id, target_id, admin_id)
            await ack(f"{action.capitalize()} applied.")
            txt, kb = await build_user_profile(ch_id, target_id)
            await safe_edit(cq.message, txt, kb)

        elif data.startswith("user:profile:"):
            # new format user:profile:<channel_id>:<user_id>; legacy user:profile:<user_id>
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
                    await safe_edit(cq.message, "No channel record for that user.", kb_back())
                    return
                ch_id = hit[0]
            txt, kb = await build_user_profile(ch_id, target_id)
            await safe_edit(cq.message, txt, kb)

        # ---- broadcast confirm / schedule ----
        elif data == "confirm:broadcast_send":
            f = flow(admin_id)
            from_chat, msg_id = f.data.get("bc_from_chat_id"), f.data.get("bc_message_id")
            if not from_chat or not msg_id:
                await ack("No broadcast content found.", True)
                return
            await ack("Sending…")
            reset_flow(admin_id)
            async with SessionLocal() as session:
                b = Broadcast(from_chat_id=from_chat, from_message_id=msg_id, status="pending")
                session.add(b)
                await session.commit()
                await session.refresh(b)
            await safe_edit(cq.message, "📤 Sending broadcast…")
            asyncio.create_task(execute_broadcast(b.id, cq.message))

        elif data == "confirm:broadcast_schedule":
            f = flow(admin_id)
            if not f.data.get("bc_message_id"):
                await ack("No broadcast content found.", True)
                return
            await ack()
            f.state = St.BC_SCHEDULE
            await safe_edit(cq.message, "Send the schedule time in UTC: <code>YYYY-MM-DD HH:MM</code>",
                            kb_back())

        elif data == "confirm:cancel":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, await build_main_panel_text(), kb_main_panel(await userbot_ready()))

        elif data == "setapi:cancel":
            await ack()
            reset_flow(admin_id)
            await safe_edit(cq.message, "Setup cancelled.", kb_main_panel(await userbot_ready()))

        # ---- login ----
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

        # ---- admins ----
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
                    await ack("Can't remove the owner.", True)
                    return
            async with SessionLocal() as session:
                admins = (await session.execute(select(Admin).order_by(Admin.is_owner.desc()))).scalars().all()
            await safe_edit(cq.message, "<b>🛡️ Admins:</b>", kb_admins_list(admins))

        else:
            await ack()

    except MessageNotModified:
        await ack()
    except FloodWait as fw:
        await ack("Too many requests — wait a moment.", True)
        await asyncio.sleep(int(getattr(fw, "value", 1)))
    except Exception as exc:
        logger.exception("Callback failed for data=%s: %s", data, exc)
        await ack("Something went wrong — check logs.", True)
    finally:
        await ack()  # no-op if a branch already answered; otherwise stops the spinner


# ===========================================================================
# HANDLER REGISTRATION
# ===========================================================================

def register_bot_handlers(client: Client):
    admin_cmds = {
        "panel": cmd_start, "channels": cmd_channels, "requests": cmd_requests,
        "accept_all": cmd_accept_all, "decline_all": cmd_decline_all, "search": cmd_search,
        "inbox": cmd_inbox, "block": cmd_block, "unblock": cmd_unblock, "broadcast": cmd_broadcast,
        "stats": cmd_stats, "settings": cmd_settings, "admins": cmd_admins, "login": cmd_login,
        "help": cmd_help,
    }
    client.add_handler(MessageHandler(cmd_start, filters.command("start") & filters.private))
    for name, handler in admin_cmds.items():
        client.add_handler(MessageHandler(handler, filters.command(name) & filters.private & admin_only))
    client.add_handler(MessageHandler(cmd_setapi, filters.command("setapi") & filters.private & owner_only))

    all_cmds = ["start", "setapi"] + list(admin_cmds.keys())
    client.add_handler(MessageHandler(on_private_message,
                                      filters.private & ~filters.command(all_cmds) & ~filters.service))
    client.add_handler(CallbackQueryHandler(on_callback))
    # Bot also listens to join requests + member updates (so nothing depends on userbot alone)
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
        logger.warning("OWNER_ID not set in .env — nobody can use the admin panel until it is set.")
        return
    async with SessionLocal() as session:
        existing = (await session.execute(select(Admin).where(Admin.user_id == owner_id))).scalar_one_or_none()
        if existing is None:
            session.add(Admin(user_id=owner_id, name="Owner", is_owner=True))
        elif not existing.is_owner:
            existing.is_owner = True
        await session.commit()


async def periodic_sync():
    """Every 10 min reconcile DB-pending with Telegram so the panel stays truthful."""
    try:
        ok = await sync_pending_with_telegram()
        logger.info("periodic_sync reconciled %d channel(s)", ok)
    except Exception as exc:
        logger.exception("periodic_sync failed: %s", exc)


async def main():
    global bot

    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing — put it in .env (BOT_TOKEN=...). Cannot start.")
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
                       "Set your own in .env (API_ID / API_HASH) or via /setapi.")
        api_id = 6
        api_hash = "eb06d4abfb49dc3eeb1aeb98ae0f581e"

    bot = Client("manager_bot", api_id=api_id, api_hash=api_hash, bot_token=BOT_TOKEN, in_memory=True)
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

    scheduler.add_job(periodic_sync, "interval", minutes=10, id="periodic_sync", replace_existing=True)

    await idle()

    for c in (bot, userbot):
        if c is not None:
            try:
                await c.stop()
            except Exception:
                pass
    scheduler.shutdown(wait=False)
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    # Python 3.12+: asyncio.run creates a fresh loop; no get_event_loop() at import time.
    asyncio.run(main())
