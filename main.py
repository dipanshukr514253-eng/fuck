"""
Telegram Channel Manager Bot  (fixed rewrite)
Pyrogram bot + userbot, SQLAlchemy async, APScheduler, FSM.

.env keys: BOT_TOKEN, OWNER_ID, API_ID, API_HASH, DATABASE_URL, LOG_LEVEL
Run:  pip install -r requirements.txt && python bot.py
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
    select,
    text,
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


class Member(Base):
    __tablename__ = "members"
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, index=True, nullable=False)
    user_id = Column(BigInteger, index=True, nullable=False)
    first_name = Column(String(255), default="")
    username = Column(String(255), default="")
    joined_at = Column(DateTime, default=now_utc)
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


async def _add_column_if_missing(table: str, col: str, col_def: str):
    """Each ALTER runs in its OWN transaction so one failure never aborts the rest."""
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
    except Exception:
        pass  # already exists


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
    ]
    for table, col, col_def in migrations:
        await _add_column_if_missing(table, col, col_def)

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


@dataclass
class Flow:
    state: St = St.NONE
    data: dict = field(default_factory=dict)


flows: dict = {}


def flow(uid: int) -> Flow:
    if uid not in flows:
        flows[uid] = Flow()
    return flows[uid]


def reset_flow(uid: int):
    flows[uid] = Flow()


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
    except Exception:
        pass


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


async def record_member(channel_id: int, user_id: int, first_name: str, username: str):
    """Upsert into members table (so Remove/Mute/Ban work and counts are real)."""
    async with SessionLocal() as s:
        m = (await s.execute(select(Member).where(
            Member.channel_id == channel_id, Member.user_id == user_id))).scalars().first()
        if m is None:
            s.add(Member(channel_id=channel_id, user_id=user_id,
                         first_name=first_name or "", username=username or "", is_active=True))
        else:
            m.is_active = True
            m.first_name = first_name or m.first_name
            m.username = username or m.username
        await s.commit()


async def deactivate_member(channel_id: int, user_id: int):
    async with SessionLocal() as s:
        rows = (await s.execute(select(Member).where(
            Member.channel_id == channel_id, Member.user_id == user_id))).scalars().all()
        for m in rows:
            m.is_active = False
        await s.commit()


async def bulk_process_requests(chat_id: int, channel_id: Optional[int], approve: bool):
    if not await userbot_ready():
        await bot.send_message(
            chat_id, "⚠️ Userbot isn't logged in yet. Open <b>🔐 Userbot Login</b> first.",
            parse_mode=HTML, reply_markup=kb_back())
        return

    async with SessionLocal() as session:
        q = select(JoinRequest).where(JoinRequest.status == "pending")
        if channel_id is not None:
            q = q.where(JoinRequest.channel_id == channel_id)
        requests = (await session.execute(q)).scalars().all()

    total = len(requests)
    if total == 0:
        await bot.send_message(chat_id, "No pending requests found.", reply_markup=kb_back())
        return

    label = "Accepted" if approve else "Declined"
    progress = await bot.send_message(chat_id, f"⏳ {label} 0/{total} requests…")
    done = failed = stale = 0
    ch_names: dict = {}
    last_edit = 0.0

    for idx, req in enumerate(requests, 1):
        result = await process_join_request(req.channel_id, req.user_id, approve)
        if result in ("ok", "gone"):
            async with SessionLocal() as session:
                fresh = (await session.execute(
                    select(JoinRequest).where(JoinRequest.id == req.id))).scalar_one_or_none()
                if fresh:
                    if result == "ok":
                        fresh.status = "accepted" if approve else "declined"
                    else:
                        fresh.status = "expired"   # no longer pending on Telegram
                    fresh.processed_at = now_utc()
                    await session.commit()
            if result == "ok":
                done += 1
                if approve:
                    await record_member(req.channel_id, req.user_id, req.first_name, req.username)
                    if req.channel_id not in ch_names:
                        ch_names[req.channel_id] = await get_channel_name(req.channel_id)
                    await send_join_message(req.channel_id, ch_names[req.channel_id],
                                            req.user_id, req.first_name, req.last_name or "",
                                            req.username)
            else:
                stale += 1
        else:
            failed += 1

        await asyncio.sleep(BULK_APPROVE_INTERVAL)
        loop_now = asyncio.get_event_loop().time()
        if loop_now - last_edit > 2.0:
            last_edit = loop_now
            try:
                await progress.edit_text(
                    f"⏳ {label} {done}/{total} … (skipped {stale}, failed {failed})")
            except Exception:
                pass

    final = f"✅ {label} {done}/{total} requests complete."
    if stale:
        final += f"\nℹ️ {stale} were already handled on Telegram — cleared from pending."
    if failed:
        final += f"\n⚠️ {failed} failed (check that the userbot is admin with invite permission)."
    try:
        await progress.edit_text(final, reply_markup=kb_back())
    except Exception:
        await bot.send_message(chat_id, final, reply_markup=kb_back())


async def sync_pending_with_telegram(channel_id: Optional[int] = None) -> int:
    """
    Reconcile DB 'pending' against Telegram's real pending list (userbot).
    Fixes requests that were handled while bot was offline / manually in the Telegram app.
    Returns number of rows corrected.
    """
    if not await userbot_ready():
        return 0
    async with SessionLocal() as s:
        q = select(Channel).where(Channel.is_active == True)  # noqa: E712
        if channel_id is not None:
            q = q.where(Channel.channel_id == channel_id)
        channels = (await s.execute(q)).scalars().all()

    fixed = 0
    for ch in channels:
        live_ids = set()
        try:
            async for r in userbot.get_chat_join_requests(ch.channel_id):
                live_ids.add(r.user.id)
                # also add ones we missed while offline
                async with SessionLocal() as s:
                    ex = (await s.execute(select(JoinRequest).where(
                        JoinRequest.channel_id == ch.channel_id,
                        JoinRequest.user_id == r.user.id,
                        JoinRequest.status == "pending"))).scalars().first()
                    if ex is None:
                        s.add(JoinRequest(channel_id=ch.channel_id, user_id=r.user.id,
                                          first_name=r.user.first_name or "",
                                          last_name=r.user.last_name or "",
                                          username=r.user.username or "", status="pending"))
                        await s.commit()
                        fixed += 1
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 1)) + 1)
            continue
        except Exception as exc:
            logger.info("sync_pending: cannot list requests for %s: %s", ch.channel_id, exc)
            continue

        async with SessionLocal() as s:
            db_pending = (await s.execute(select(JoinRequest).where(
                JoinRequest.channel_id == ch.channel_id,
                JoinRequest.status == "pending"))).scalars().all()
            for jr in db_pending:
                if jr.user_id not in live_ids:
                    jr.status = "expired"
                    jr.processed_at = now_utc()
                    fixed += 1
            await s.commit()
    return fixed


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


_seen_requests: dict = {}   # (chat_id,user_id) -> ts  — dedup between bot & userbot handlers


def _dedup(key) -> bool:
    """True if this event was already processed in the last 30s."""
    now = asyncio.get_event_loop().time()
    for k in [k for k, t in _seen_requests.items() if now - t > 30]:
        _seen_requests.pop(k, None)
    if key in _seen_requests:
        return True
    _seen_requests[key] = now
    return False


async def _on_join_request(client: Client, request: ChatJoinRequest):
    chat = request.chat
    u = request.from_user
    if _dedup(("jr", chat.id, u.id)):
        return
    logger.info("Join request from %s in %s", u.id, chat.id)

    await _ensure_channel(chat)
    async with SessionLocal() as session:
        ex = (await session.execute(select(JoinRequest).where(
            JoinRequest.channel_id == chat.id, JoinRequest.user_id == u.id,
            JoinRequest.status == "pending"))).scalars().first()
        if ex is None:
            session.add(JoinRequest(
                channel_id=chat.id, user_id=u.id, first_name=u.first_name or "",
                last_name=u.last_name or "", username=u.username or "", status="pending"))
            await session.commit()
        settings = await get_or_create_settings(session, chat.id)
        gs = await get_global_settings(session)
        auto = settings.auto_accept

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
                await record_member(chat.id, u.id, u.first_name or "", u.username or "")
                await send_join_message(chat.id, chat.title or "", u.id, u.first_name or "",
                                        u.last_name or "", u.username or "")
                if gs.notif_auto_accept:
                    await notify_admins(f"✅ Auto-accepted: <b>{esc(u.first_name)}</b> "
                                        f"into <b>{esc(chat.title)}</b>")
            return
        # auto-accept failed (userbot down?) → fall through and notify admins as pending

    if gs.notif_join_request:
        async with SessionLocal() as session:
            pending_count = (await session.execute(
                select(func.count()).select_from(JoinRequest)
                .where(JoinRequest.channel_id == chat.id, JoinRequest.status == "pending")
            )).scalar()
        text_out = (
            f"🔔 <b>New join request</b>\n\n"
            f"Channel: <b>{esc(chat.title)}</b>\n"
            f"User: <b>{esc(u.first_name)}</b> ({esc('@' + u.username) if u.username else 'no username'})\n"
            f"ID: <code>{u.id}</code>\n\n"
            f"Pending in this channel: {pending_count}")
        await notify_admins(text_out, kb_channel_notify(chat.id))


async def _on_member_updated(client: Client, update: ChatMemberUpdated):
    old = update.old_chat_member
    new = update.new_chat_member
    if new is None:
        return
    chat = update.chat
    u = new.user
    if u is None:
        return
    if _dedup(("mu", chat.id, u.id, str(new.status))):
        return

    # Bot's own status change → handled separately (channel add/remove)
    try:
        me = await bot.get_me()
        if u.id == me.id:
            await _handle_bot_status(update)
            return
    except Exception:
        pass

    active = (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    left = (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    old_status = old.status if old is not None else None

    async with SessionLocal() as session:
        gs = await get_global_settings(session)

    if new.status in left and (old_status in active or old_status is None):
        await _ensure_channel(chat)
        await deactivate_member(chat.id, u.id)
        async with SessionLocal() as session:
            session.add(MemberLeave(channel_id=chat.id, user_id=u.id,
                                    first_name=u.first_name or "", username=u.username or ""))
            await session.commit()
        await send_leave_message(chat.id, chat.title or "", u.id, u.first_name or "",
                                 u.last_name or "", u.username or "")
        if gs.notif_member_leave:
            await notify_admins(f"🚪 <b>{esc(u.first_name)}</b> "
                                f"({esc('@' + u.username) if u.username else 'no username'}) "
                                f"left <b>{esc(chat.title)}</b>")

    elif new.status in active and (old_status is None or old_status not in active):
        await _ensure_channel(chat)
        await record_member(chat.id, u.id, u.first_name or "", u.username or "")
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
    client.add_handler(ChatJoinRequestHandler(_on_join_request))
    client.add_handler(ChatMemberUpdatedHandler(_on_member_updated))


# ===========================================================================
# LIVE COUNT HELPERS
# ===========================================================================

async def live_member_count(channel_id: int):
    """Returns (count, is_live). Tries userbot, then bot, then cached DB count."""
    for client in (userbot if await userbot_ready() else None, bot):
        if client is None:
            continue
        try:
            cnt = await flood_safe(lambda c=client: c.get_chat_members_count(channel_id))
            return int(cnt), True
        except Exception:
            continue
    async with SessionLocal() as s:
        cnt = (await s.execute(select(func.count()).select_from(Member).where(
            Member.channel_id == channel_id, Member.is_active == True))).scalar()  # noqa: E712
    return int(cnt or 0), False


async def live_pending_count(channel_id: Optional[int] = None) -> int:
    async with SessionLocal() as s:
        q = select(func.count()).select_from(JoinRequest).where(JoinRequest.status == "pending")
        if channel_id is not None:
            q = q.where(JoinRequest.channel_id == channel_id)
        return int((await s.execute(q)).scalar() or 0)


def fmt_uptime() -> str:
    total = int((datetime.now(timezone.utc) - BOT_START_TIME).total_seconds())
    return f"{total // 86400}d {(total % 86400) // 3600}h {(total % 3600) // 60}m"


# ===========================================================================
# MAIN PANEL TEXT
# ===========================================================================

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
        if sync:
            try:
                await sync_pending_with_telegram()
            except Exception as exc:
                logger.warning("sync_pending failed: %s", exc)

    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
        pending = (await session.execute(
            select(func.count()).select_from(JoinRequest).where(JoinRequest.status == "pending"))).scalar()
        accepted_today = (await session.execute(
            select(func.count()).select_from(JoinRequest).where(
                JoinRequest.status == "accepted", JoinRequest.processed_at >= today_start()))).scalar()

    live_total, any_cached = 0, False
    for ch in channels:
        cnt, live = await live_member_count(ch.channel_id)
        live_total += cnt
        if not live:
            any_cached = True
    cached_note = " (cached)" if any_cached else ""

    return (
        f"🤖 <b>Admin Panel</b>\n\n"
        f"🟢 Userbot: {userbot_status}{userbot_info}\n"
        f"📣 Channels: {len(channels)} managed\n"
        f"👥 Live Members: {live_total:,}{cached_note}\n"
        f"⏳ Pending Requests: {pending}\n"
        f"✅ Accepted Today: {accepted_today}\n"
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
        panel_text = await build_main_panel_text(sync=True)
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


async def show_channel_list(chat_id: int):
    async with SessionLocal() as session:
        channels = (await session.execute(
            select(Channel).where(Channel.is_active == True))).scalars().all()  # noqa: E712
    if not channels:
        await bot.send_message(
            chat_id, "No channels yet. Add the bot (and userbot) as admin to a channel to begin.",
            reply_markup=kb_back())
        return

    lines = ["📣 <b>Managed Channels</b>\n"]
    kb_rows = []
    for i, ch in enumerate(channels, 1):
        pending = await live_pending_count(ch.channel_id)
        async with SessionLocal() as session:
            s = await get_or_create_settings(session, ch.channel_id)
            auto_str = "ON" if s.auto_accept else "OFF"
        cnt, live = await live_member_count(ch.channel_id)
        note = "" if live else " (cached)"
        lines.append(f"{i}. <b>{esc(ch.name)}</b>\n"
                     f"   👥 Members: {cnt:,}{note}  ⏳ Pending: {pending}  ✅ Auto-Accept: {auto_str}")
        kb_rows.append([InlineKeyboardButton(f"⚙️ Settings: {ch.name}"[:60],
                                             callback_data=f"channels:settings:{ch.channel_id}")])
    kb_rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="channels:list")])
    kb_rows.append([InlineKeyboardButton("« Back", callback_data="panel:main")])
    await bot.send_message(chat_id, "\n".join(lines)[:4090],
                           reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=HTML)


async def cmd_requests(client: Client, message: Message):
    async with SessionLocal() as session:
        requests = (await session.execute(
            select(JoinRequest).where(JoinRequest.status == "pending").limit(20))).scalars().all()
    if not requests:
        await message.reply_text("No pending requests.")
        return
    lines = ["<b>Pending Requests (up to 20):</b>\n"]
    for r in requests:
        uname = f"@{r.username}" if r.username else "(no username)"
        lines.append(f"• {esc(r.first_name)} {esc(uname)} — <code>{r.user_id}</code> "
                     f"in <code>{r.channel_id}</code>")
    await message.reply_text("\n".join(lines), reply_markup=kb_main_panel(await userbot_ready()),
                             parse_mode=HTML)


async def cmd_accept_all(client: Client, message: Message):
    await bulk_process_requests(message.chat.id, None, approve=True)


async def cmd_decline_all(client: Client, message: Message):
    await bulk_process_requests(message.chat.id, None, approve=False)


async def run_search(chat_id: int, query: str):
    query = query.strip()
    target_id: Optional[int] = None
    target_username: Optional[str] = None
    if query.lstrip("-").isdigit():
        target_id = int(query)
    else:
        target_username = query.lstrip("@")

    async with SessionLocal() as session:
        if target_id is not None:
            jr = (await session.execute(select(JoinRequest).where(
                JoinRequest.user_id == target_id, JoinRequest.status == "pending"))).scalars().first()
            mem = (await session.execute(select(Member).where(
                Member.user_id == target_id, Member.is_active == True))).scalars().first()  # noqa: E712
        else:
            jr = (await session.execute(select(JoinRequest).where(
                func.lower(JoinRequest.username) == target_username.lower(),
                JoinRequest.status == "pending"))).scalars().first()
            mem = (await session.execute(select(Member).where(
                func.lower(Member.username) == target_username.lower(),
                Member.is_active == True))).scalars().first()  # noqa: E712

    if jr is None and mem is None:
        if not await userbot_ready():
            await bot.send_message(chat_id, "❌ No local record found, and userbot isn't logged in "
                                            "for a live lookup.", reply_markup=kb_back())
            return
        try:
            lookup = target_id if target_id is not None else target_username
            u = await userbot.get_users(lookup)
            txt = (f"<b>User found (live lookup)</b>\n"
                   f"Name: {esc(u.first_name)} {esc(u.last_name)}\n"
                   f"Username: {esc('@' + u.username) if u.username else '(none)'}\n"
                   f"ID: <code>{u.id}</code>\nStatus: Not tracked locally")
            await bot.send_message(chat_id, txt, reply_markup=kb_user_actions(u.id, False, False),
                                   parse_mode=HTML)
        except (PeerIdInvalid, UsernameNotOccupied, IndexError, KeyError):
            await bot.send_message(chat_id, "❌ No user found matching that ID or username.",
                                   reply_markup=kb_back())
        except Exception as exc:
            logger.warning("Search lookup failed: %s", exc)
            await bot.send_message(chat_id, f"❌ Lookup failed: {esc(exc)}", reply_markup=kb_back(),
                                   parse_mode=HTML)
        return

    uid = jr.user_id if jr else mem.user_id
    name = jr.first_name if jr else (mem.first_name or str(uid))
    uname_src = jr.username if jr else (mem.username if mem else "")
    uname = f"@{uname_src}" if uname_src else "(no username)"
    status = "Pending" if jr else "Member"
    txt = (f"<b>User Profile</b>\n\nName: {esc(name)}\nUsername: {esc(uname)}\n"
           f"ID: <code>{uid}</code>\nStatus: {status}")
    await bot.send_message(chat_id, txt, reply_markup=kb_user_actions(uid, jr is not None, mem is not None),
                           parse_mode=HTML)


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


async def build_stats_text() -> str:
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
        except Exception:
            pass
        try:
            await sync_pending_with_telegram()
        except Exception:
            pass

    ts = today_start()
    blocks = []
    for ch in channels:
        async with SessionLocal() as session:
            ch_pending = (await session.execute(select(func.count()).select_from(JoinRequest).where(
                JoinRequest.channel_id == ch.channel_id, JoinRequest.status == "pending"))).scalar()
            joined_today = (await session.execute(select(func.count()).select_from(Member).where(
                Member.channel_id == ch.channel_id, Member.joined_at >= ts))).scalar()
            left_today = (await session.execute(select(func.count()).select_from(MemberLeave).where(
                MemberLeave.channel_id == ch.channel_id, MemberLeave.left_at >= ts))).scalar()
        cnt, live = await live_member_count(ch.channel_id)
        note = "" if live else " (cached)"
        blocks.append(f"📣 <b>{esc(ch.name)}</b>\n  👥 Live Members: {cnt:,}{note}\n"
                      f"  ⏳ Pending Requests: {ch_pending}\n  ✅ Joined Today: {joined_today}\n"
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
                except Exception:
                    pass
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
        reset_flow(uid)
        await run_search(chat_id, txt)
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
            panel_text = await build_main_panel_text(sync=True)
            await safe_edit(cq.message, panel_text, kb_main_panel(await userbot_ready()))

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
            flow(admin_id).state = St.SEARCH
            await safe_edit(cq.message, "Send a User ID or @username to search.", kb_back())

        # ---- channels ----
        elif data == "channels:list":
            await ack()
            await show_channel_list(chat_id)

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
            txt = await build_stats_text()
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
        elif data.startswith("user:accept:") or data.startswith("user:decline:"):
            target_id = int(p[2])
            approve = p[1] == "accept"
            if not await userbot_ready():
                await ack("Userbot not logged in.", True)
                return
            async with SessionLocal() as session:
                jr = (await session.execute(select(JoinRequest).where(
                    JoinRequest.user_id == target_id, JoinRequest.status == "pending"))).scalars().first()
            if jr is None:
                await ack("No pending request found.", True)
                return
            result = await process_join_request(jr.channel_id, jr.user_id, approve)
            if result in ("ok", "gone"):
                async with SessionLocal() as session:
                    row = (await session.execute(select(JoinRequest).where(JoinRequest.id == jr.id))).scalar_one()
                    row.status = ("accepted" if approve else "declined") if result == "ok" else "expired"
                    row.processed_at = now_utc()
                    await session.commit()
                if approve and result == "ok":
                    await record_member(jr.channel_id, jr.user_id, jr.first_name, jr.username)
                    await send_join_message(jr.channel_id, await get_channel_name(jr.channel_id),
                                            jr.user_id, jr.first_name, jr.last_name or "", jr.username)
                await ack("Done.")
                await safe_edit(cq.message, f"{'✅ Accepted' if approve else '❌ Declined'} "
                                            f"user <code>{target_id}</code>.", kb_back())
            else:
                await ack("Failed — check the userbot is admin with invite permission.", True)

        elif p[0] == "user" and p[1] in ("remove", "ban", "mute"):
            target_id, action = int(p[2]), p[1]
            if not await userbot_ready():
                await ack("Userbot not logged in.", True)
                return
            async with SessionLocal() as session:
                mem = (await session.execute(select(Member).where(
                    Member.user_id == target_id, Member.is_active == True))).scalars().first()  # noqa: E712
            if mem is None:
                await ack("User not tracked as a member.", True)
                return
            try:
                if action == "remove":
                    await userbot.ban_chat_member(mem.channel_id, target_id)
                    await userbot.unban_chat_member(mem.channel_id, target_id)
                    await deactivate_member(mem.channel_id, target_id)
                elif action == "ban":
                    await userbot.ban_chat_member(mem.channel_id, target_id)
                    await deactivate_member(mem.channel_id, target_id)
                else:
                    await userbot.restrict_chat_member(mem.channel_id, target_id, ChatPermissions())
                await ack(f"{action.capitalize()} applied.")
                await safe_edit(cq.message, f"✅ {action.capitalize()} applied to <code>{target_id}</code>.",
                                kb_back())
            except Exception as exc:
                await ack(f"Failed: {str(exc)[:150]}", True)

        elif data.startswith("user:profile:"):
            target_id = int(p[2])
            try:
                u = await bot.get_users(target_id)
            except Exception:
                try:
                    u = await userbot.get_users(target_id) if await userbot_ready() else None
                except Exception:
                    u = None
            if u is None:
                await ack("Lookup failed.", True)
                return
            await ack()
            await safe_edit(cq.message,
                            f"<b>Profile</b>\nName: {esc(u.first_name)} {esc(u.last_name)}\n"
                            f"Username: {esc('@' + u.username) if u.username else '(none)'}\n"
                            f"ID: <code>{u.id}</code>", kb_back())

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
        await ack()  # guarantees the button spinner always stops


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
        n = await sync_pending_with_telegram()
        if n:
            logger.info("periodic_sync corrected %d row(s)", n)
    except Exception as exc:
        logger.warning("periodic_sync failed: %s", exc)


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
            fixed = await sync_pending_with_telegram()
            if fixed:
                logger.info("Startup sync corrected %d pending row(s).", fixed)
        except Exception as exc:
            logger.warning("Startup sync failed: %s", exc)
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
