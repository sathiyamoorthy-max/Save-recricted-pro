#!/usr/bin/env python3
"""
Save Restricted Pro V2
======================
A production-oriented Telegram utility/business bot built with Kurigram
(Pyrogram-compatible API), SQLite, APScheduler and encrypted user sessions.

Important: use this bot only with Telegram content/accounts/chats you are
allowed to access and process. Respect Telegram rules and applicable rights.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import re
import secrets
import shutil
import signal
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import aiosqlite
import pytz
import qrcode
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from cryptography.fernet import Fernet, InvalidToken
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (
    ChannelPrivate,
    FloodWait,
    PasswordHashInvalid,
    PeerIdInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    SessionPasswordNeeded,
)
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

load_dotenv()

APP_NAME = os.getenv("APP_NAME", "Save Restricted Pro V2").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
API_ID = int(API_ID_RAW) if API_ID_RAW.isdigit() else 0
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@").strip()

OWNER_ID_RAW = os.getenv("OWNER_ID", os.getenv("ADMIN_ID", "")).strip()
OWNER_ID = int(OWNER_ID_RAW) if OWNER_ID_RAW.isdigit() else 0
EXTRA_ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
if OWNER_ID:
    EXTRA_ADMIN_IDS.add(OWNER_ID)

TZ_NAME = os.getenv("TIMEZONE", "Asia/Kolkata")
TZ = pytz.timezone(TZ_NAME)
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "bot_data.db")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
TEMP_DIR = DATA_DIR / "tmp"
THUMB_DIR = DATA_DIR / "thumbnails"
LOG_DIR = DATA_DIR / "logs"

MAX_BATCH_SIZE = max(1, int(os.getenv("MAX_BATCH_SIZE", "250")))
MAX_QUEUE_PER_USER = max(1, int(os.getenv("MAX_QUEUE_PER_USER", "5")))
WORKER_COUNT = min(8, max(1, int(os.getenv("WORKER_COUNT", "2"))))
JOB_TIMEOUT_SECONDS = max(300, int(os.getenv("JOB_TIMEOUT_SECONDS", "7200")))
FREE_DAILY_LIMIT = max(0, int(os.getenv("FREE_DAILY_LIMIT", "2")))
TRIAL_DAILY_LIMIT = max(1, int(os.getenv("TRIAL_DAILY_LIMIT", "50")))
RATE_LIMIT_MESSAGES = max(3, int(os.getenv("RATE_LIMIT_MESSAGES", "12")))
RATE_LIMIT_WINDOW = max(5, int(os.getenv("RATE_LIMIT_WINDOW", "15")))
SUPPORT_USERNAME_ENV = os.getenv("SUPPORT_USERNAME", "admin").lstrip("@").strip()
UPDATES_CHANNEL_ENV = os.getenv("UPDATES_CHANNEL", "https://t.me/telegram").strip()
UPI_ID_ENV = os.getenv("UPI_ID", "").strip()
UPI_NAME_ENV = os.getenv("UPI_NAME", "").strip()
TERMS_URL = os.getenv("TERMS_URL", "").strip()

for d in (DATA_DIR, TEMP_DIR, THUMB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

START_TS = time.time()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("savepro")

if not API_ID or not API_HASH or not BOT_TOKEN:
    log.warning("API_ID/API_HASH/BOT_TOKEN are not fully configured yet.")

# Kurigram/Pyrogram binds each Client to the asyncio event loop that exists
# when the Client is created.  Create one explicit application loop up-front
# and use that same loop for the complete lifetime of the bot.  This avoids
# Python 3.12 / Render errors such as "Future attached to a different loop".
APP_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(APP_LOOP)

# Kurigram keeps the same `pyrogram` import namespace.
bot = Client(
    "savepro_master_bot",
    api_id=API_ID or 1,
    api_hash=API_HASH or "placeholder",
    bot_token=BOT_TOKEN or "123456:placeholder",
    in_memory=True,
)

# ---------------------------------------------------------------------------
# BUSINESS CONFIG
# ---------------------------------------------------------------------------

PLAN_LIMITS = {
    "Free": FREE_DAILY_LIMIT,
    "Trial": TRIAL_DAILY_LIMIT,
    "Basic": 10,
    "Standard": 50,
    "Premium": 100,
    "Ultimate": 9_999_999,
}

PLAN_PRIORITY = {
    "Ultimate": 0,
    "Premium": 1,
    "Standard": 2,
    "Basic": 3,
    "Trial": 3,
    "Free": 4,
}

PLAN_PRICING: dict[str, dict[str, Any]] = {
    "Basic": {
        "Weekly": 30,
        "Monthly": 100,
        "Yearly": 840,
        "access": "10 jobs/day",
    },
    "Standard": {
        "Weekly": 50,
        "Monthly": 180,
        "Yearly": 1500,
        "access": "50 jobs/day + batch",
    },
    "Premium": {
        "Weekly": 80,
        "Monthly": 280,
        "Yearly": 2350,
        "access": "100 jobs/day + priority batch",
    },
    "Ultimate": {
        "Weekly": 130,
        "Monthly": 500,
        "Yearly": 4200,
        "access": "Unlimited + highest priority + owned-channel transfer",
    },
}

DURATION_DAYS = {"Weekly": 7, "Monthly": 30, "Yearly": 365}
VALID_LANGS = {"ta", "en", "hi", "te", "ml", "kn", "bn"}
VALID_FILTERS = {"all", "video", "audio", "doc", "photo"}
ADMIN_ROLES = {"owner", "manager", "payments", "support", "marketing", "developer"}

# ---------------------------------------------------------------------------
# LOCALIZATION
# ---------------------------------------------------------------------------

LANG = {
    "ta": {
        "welcome": (
            "🌟 **வணக்கம் {name}! {app}-க்கு வரவேற்கிறோம்**\n\n"
            "🏷️ **Plan:** `{plan}`\n"
            "⏳ **Expiry:** `{expiry}`\n"
            "🔐 **Account:** {session}\n"
            "🎯 **Filter:** `{filter}`\n"
            "📥 **Queue:** `{queue}`\n\n"
            "உங்களுக்கு அனுமதி உள்ள Telegram content-ஐ மட்டும் பயன்படுத்தவும்."
        ),
        "connected": "✅ Connected",
        "not_connected": "❌ Not Connected",
        "choose": "கீழே உள்ள menu-ஐ பயன்படுத்தவும்.",
        "limit": "⛔ இன்றைய பயன்பாட்டு வரம்பு முடிந்தது.",
    },
    "en": {
        "welcome": (
            "🌟 **Welcome {name} to {app}**\n\n"
            "🏷️ **Plan:** `{plan}`\n"
            "⏳ **Expiry:** `{expiry}`\n"
            "🔐 **Account:** {session}\n"
            "🎯 **Filter:** `{filter}`\n"
            "📥 **Queue:** `{queue}`\n\n"
            "Use only Telegram content you are authorized to access and process."
        ),
        "connected": "✅ Connected",
        "not_connected": "❌ Not Connected",
        "choose": "Use the menu below.",
        "limit": "⛔ Your daily usage limit is exhausted.",
    },
}

MANUAL = (
    "📖 **Quick Guide**\n\n"
    "1️⃣ `/login` — connect your own Telegram account.\n"
    "2️⃣ Send a Telegram post link for a single authorized save.\n"
    "3️⃣ `/batch` — process a permitted range (plan limits apply).\n"
    "4️⃣ `/jobs` — see queued/running/recent jobs.\n"
    "5️⃣ `/plans` / `/myplan` — subscription and expiry.\n"
    "6️⃣ `/support` — open a support ticket.\n"
    "7️⃣ `/settings` — language, media filter and notifications.\n\n"
    "🔒 Never share your OTP, 2FA password or session string with anyone else."
)

# ---------------------------------------------------------------------------
# CRYPTO
# ---------------------------------------------------------------------------


def _fernet_key() -> bytes:
    configured = os.getenv("SESSION_ENCRYPTION_KEY", "").strip()
    if configured:
        try:
            raw = configured.encode()
            Fernet(raw)
            return raw
        except Exception:
            pass
        return base64.urlsafe_b64encode(hashlib.sha256(configured.encode()).digest())

    # Stable fallback so old sessions remain decryptable after restart.
    # Production deployments should always set SESSION_ENCRYPTION_KEY.
    seed = f"{BOT_TOKEN}|{API_HASH}|savepro-v2"
    log.warning(
        "SESSION_ENCRYPTION_KEY is not set. Using a derived fallback key. "
        "Set a dedicated secret in production."
    )
    return base64.urlsafe_b64encode(hashlib.sha256(seed.encode()).digest())


FERNET = Fernet(_fernet_key())


def encrypt_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return FERNET.encrypt(value.encode()).decode()


def decrypt_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return FERNET.decrypt(value.encode()).decode()
    except (InvalidToken, ValueError):
        return None

# ---------------------------------------------------------------------------
# STATE / QUEUES
# ---------------------------------------------------------------------------

DB_LOCK = asyncio.Lock()
LOGIN_STATES: dict[int, dict[str, Any]] = {}
RATE_BUCKETS: dict[int, deque[float]] = defaultdict(deque)
JOB_QUEUE: asyncio.PriorityQueue[tuple[int, int, str]] = asyncio.PriorityQueue()
JOB_SEQ = 0
SHUTDOWN_EVENT = asyncio.Event()
SCHEDULER: Optional[AsyncIOScheduler] = None

# ---------------------------------------------------------------------------
# GENERIC HELPERS
# ---------------------------------------------------------------------------


def now_dt() -> datetime:
    return datetime.now(TZ)


def now_iso() -> str:
    return now_dt().isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return TZ.localize(dt)
        return dt.astimezone(TZ)
    except Exception:
        return None


def human_dt(value: Optional[str]) -> str:
    dt = parse_dt(value)
    if not dt:
        return "No expiry"
    return dt.strftime("%d %b %Y, %I:%M %p")


def generate_id(prefix: str, digits: int = 6) -> str:
    stamp = now_dt().strftime("%y%m%d")
    return f"{prefix}-{stamp}-{secrets.token_hex(max(2, digits // 2)).upper()[:digits]}"


def safe_filename(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name or "file")
    return name[:180] or "file"


def get_valid_thumb(path: Optional[str]) -> Optional[str]:
    return path if path and os.path.exists(path) else None


def is_admin_id(user_id: int) -> bool:
    return user_id in EXTRA_ADMIN_IDS


def plan_days(duration: str) -> int:
    return DURATION_DAYS.get(duration, 30)


def create_qr_bytes(payload: str) -> io.BytesIO:
    qr = qrcode.QRCode(version=None, box_size=8, border=3)
    qr.add_data(payload)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = io.BytesIO()
    bio.name = "payment_qr.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def parse_tg_link(text: str) -> Optional[tuple[str, int]]:
    m = re.search(r"https?://t\.me/(?:c/)?([A-Za-z0-9_]+)/([0-9]+)", text)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def chat_ref_to_id(chat_raw: str) -> Any:
    return int(f"-100{chat_raw}") if str(chat_raw).isdigit() else chat_raw


def rate_limited(user_id: int) -> bool:
    if is_admin_id(user_id):
        return False
    now = time.time()
    bucket = RATE_BUCKETS[user_id]
    while bucket and now - bucket[0] > RATE_LIMIT_WINDOW:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_MESSAGES:
        return True
    bucket.append(now)
    return False


async def safe_answer(q: CallbackQuery, text: Optional[str] = None, alert: bool = False) -> None:
    try:
        await q.answer(text=text, show_alert=alert)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------


@asynccontextmanager
async def db_conn():
    db = await aiosqlite.connect(DB_PATH, timeout=30)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("PRAGMA busy_timeout=30000")
        yield db
    finally:
        await db.close()


async def table_columns(db: aiosqlite.Connection, table: str) -> set[str]:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    return {r[1] for r in rows}


async def ensure_column(db: aiosqlite.Connection, table: str, column_sql: str) -> None:
    col = column_sql.split()[0]
    cols = await table_columns(db, table)
    if col not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")


async def init_db() -> None:
    async with DB_LOCK:
        async with db_conn() as db:
            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    lang TEXT DEFAULT 'ta',
                    plan TEXT DEFAULT 'Free',
                    daily_limit INTEGER DEFAULT 2,
                    daily_used INTEGER DEFAULT 0,
                    bonus_credits INTEGER DEFAULT 0,
                    trial_used INTEGER DEFAULT 0,
                    plan_started_at TEXT,
                    plan_expires_at TEXT,
                    referred_by INTEGER,
                    ref_count INTEGER DEFAULT 0,
                    encrypted_session TEXT,
                    session TEXT,
                    custom_api_id INTEGER,
                    custom_api_hash TEXT,
                    doc_thumb TEXT,
                    vid_thumb TEXT,
                    custom_caption TEXT,
                    file_filter TEXT DEFAULT 'all',
                    is_banned INTEGER DEFAULT 0,
                    marketing_opt_in INTEGER DEFAULT 1,
                    notify_jobs INTEGER DEFAULT 1,
                    notify_expiry INTEGER DEFAULT 1,
                    created_at TEXT,
                    last_seen TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id TEXT UNIQUE,
                    user_id INTEGER NOT NULL,
                    plan TEXT,
                    duration TEXT,
                    amount INTEGER DEFAULT 0,
                    currency TEXT DEFAULT 'INR',
                    utr TEXT UNIQUE,
                    status TEXT DEFAULT 'awaiting_utr',
                    created_at TEXT,
                    submitted_at TEXT,
                    approved_at TEXT,
                    approved_by INTEGER,
                    rejected_at TEXT,
                    rejected_by INTEGER,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    plan TEXT,
                    days INTEGER DEFAULT 0,
                    uses_left INTEGER DEFAULT 0,
                    expires_at TEXT,
                    first_purchase_only INTEGER DEFAULT 0,
                    max_per_user INTEGER DEFAULT 1,
                    discount_percent INTEGER DEFAULT 0,
                    discount_flat INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS promo_redemptions (
                    code TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    redeemed_at TEXT,
                    PRIMARY KEY (code, user_id)
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT UNIQUE,
                    user_id INTEGER NOT NULL,
                    job_type TEXT NOT NULL,
                    source TEXT,
                    start_id INTEGER,
                    end_id INTEGER,
                    destination TEXT,
                    status TEXT DEFAULT 'queued',
                    priority INTEGER DEFAULT 4,
                    progress_total INTEGER DEFAULT 0,
                    progress_done INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    error_code TEXT,
                    payload_json TEXT,
                    status_message_id INTEGER,
                    created_at TEXT,
                    started_at TEXT,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS support_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT UNIQUE,
                    user_id INTEGER NOT NULL,
                    category TEXT,
                    message TEXT,
                    status TEXT DEFAULT 'open',
                    admin_note TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    role TEXT DEFAULT 'manager',
                    added_by INTEGER,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_id INTEGER,
                    action TEXT,
                    target_id TEXT,
                    details TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_code TEXT UNIQUE,
                    user_id INTEGER,
                    feature TEXT,
                    message TEXT,
                    traceback TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS subscription_reminders (
                    user_id INTEGER,
                    expiry TEXT,
                    reminder_type TEXT,
                    sent_at TEXT,
                    PRIMARY KEY (user_id, expiry, reminder_type)
                );
                """
            )

            # Migration support for older databases.
            user_cols = [
                "username TEXT",
                "trial_used INTEGER DEFAULT 0",
                "plan_started_at TEXT",
                "plan_expires_at TEXT",
                "encrypted_session TEXT",
                "marketing_opt_in INTEGER DEFAULT 1",
                "notify_jobs INTEGER DEFAULT 1",
                "notify_expiry INTEGER DEFAULT 1",
                "created_at TEXT",
                "last_seen TEXT",
                "vid_thumb TEXT",
            ]
            for col in user_cols:
                await ensure_column(db, "users", col)

            payment_cols = [
                "invoice_id TEXT",
                "amount INTEGER DEFAULT 0",
                "currency TEXT DEFAULT 'INR'",
                "submitted_at TEXT",
                "approved_at TEXT",
                "approved_by INTEGER",
                "rejected_at TEXT",
                "rejected_by INTEGER",
                "note TEXT",
            ]
            for col in payment_cols:
                await ensure_column(db, "payments", col)

            promo_cols = [
                "expires_at TEXT",
                "first_purchase_only INTEGER DEFAULT 0",
                "max_per_user INTEGER DEFAULT 1",
                "discount_percent INTEGER DEFAULT 0",
                "discount_flat INTEGER DEFAULT 0",
            ]
            for col in promo_cols:
                await ensure_column(db, "promo_codes", col)

            # Helpful indexes. Partial unique invoice index works with legacy NULL rows.
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id) WHERE invoice_id IS NOT NULL")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority, id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_plan ON users(plan)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen)")

            defaults = {
                "owner_id": str(OWNER_ID),
                "upi_id": UPI_ID_ENV,
                "upi_name": UPI_NAME_ENV,
                "updates_channel": UPDATES_CHANNEL_ENV,
                "support_username": SUPPORT_USERNAME_ENV,
                "maintenance_mode": "0",
                "maintenance_message": "🛠 Service maintenance is in progress. Please try again later.",
            }
            for key, value in defaults.items():
                await db.execute(
                    "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                    (key, value),
                )

            if OWNER_ID:
                await db.execute(
                    "INSERT OR IGNORE INTO admins(user_id, role, added_by, created_at) VALUES (?, 'owner', ?, ?)",
                    (OWNER_ID, OWNER_ID, now_iso()),
                )
            for aid in EXTRA_ADMIN_IDS:
                await db.execute(
                    "INSERT OR IGNORE INTO admins(user_id, role, added_by, created_at) VALUES (?, 'manager', ?, ?)",
                    (aid, OWNER_ID or aid, now_iso()),
                )

            # Old V1 trials did not have an expiry timestamp. Do not allow those
            # legacy trial labels to become accidental lifetime access.
            await db.execute(
                "UPDATE users SET plan='Free', daily_limit=?, daily_used=0, trial_used=1 "
                "WHERE plan LIKE 'Trial%' AND (plan_expires_at IS NULL OR plan_expires_at='')",
                (FREE_DAILY_LIMIT,),
            )

            # Encrypt legacy plaintext sessions once.
            cols = await table_columns(db, "users")
            if "session" in cols and "encrypted_session" in cols:
                cur = await db.execute(
                    "SELECT user_id, session FROM users WHERE session IS NOT NULL AND session != '' "
                    "AND (encrypted_session IS NULL OR encrypted_session = '')"
                )
                for row in await cur.fetchall():
                    await db.execute(
                        "UPDATE users SET encrypted_session = ?, session = NULL WHERE user_id = ?",
                        (encrypt_text(row[1]), row[0]),
                    )

            await db.commit()


async def get_setting(key: str, default: str = "") -> str:
    async with db_conn() as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return str(row[0]) if row and row[0] is not None else default


async def set_setting(key: str, value: Any) -> None:
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            await db.commit()


async def audit(actor_id: int, action: str, target_id: Any = None, details: Any = None) -> None:
    async with db_conn() as db:
        await db.execute(
            "INSERT INTO audit_logs(actor_id, action, target_id, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                actor_id,
                action,
                str(target_id) if target_id is not None else None,
                json.dumps(details, ensure_ascii=False, default=str) if details is not None else None,
                now_iso(),
            ),
        )
        await db.commit()


async def log_error(user_id: Optional[int], feature: str, exc: Exception) -> str:
    code = generate_id("ERR", 6)
    tb = traceback.format_exc(limit=20)
    msg = f"{type(exc).__name__}: {exc}"
    log.error("%s | %s | user=%s | %s\n%s", code, feature, user_id, msg, tb)
    try:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO errors(error_code, user_id, feature, message, traceback, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (code, user_id, feature, msg[:1000], tb[:8000], now_iso()),
            )
            await db.commit()
    except Exception:
        pass
    return code


async def get_admin_role(user_id: int) -> Optional[str]:
    if user_id == OWNER_ID and OWNER_ID:
        return "owner"
    async with db_conn() as db:
        cur = await db.execute("SELECT role FROM admins WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else None


async def has_admin_role(user_id: int, *roles: str) -> bool:
    role = await get_admin_role(user_id)
    if role == "owner":
        return True
    return bool(role and role in roles)


async def get_user(user_id: int, name: str = "User", username: Optional[str] = None) -> dict[str, Any]:
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            if not row:
                plan = "Ultimate" if is_admin_id(user_id) else "Free"
                limit = PLAN_LIMITS[plan]
                await db.execute(
                    """
                    INSERT INTO users(
                        user_id, name, username, plan, daily_limit, daily_used,
                        lang, created_at, last_seen
                    ) VALUES (?, ?, ?, ?, ?, 0, 'ta', ?, ?)
                    """,
                    (user_id, name, username, plan, limit, now_iso(), now_iso()),
                )
                await db.commit()
            else:
                await db.execute(
                    "UPDATE users SET name = COALESCE(?, name), username = COALESCE(?, username), last_seen = ? WHERE user_id = ?",
                    (name or None, username, now_iso(), user_id),
                )
                await db.commit()

    await expire_user_if_needed(user_id)
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else {}


async def update_user(user_id: int, **kwargs: Any) -> None:
    if not kwargs:
        return
    allowed = await _user_columns()
    safe = {k: v for k, v in kwargs.items() if k in allowed}
    if not safe:
        return
    parts = ", ".join(f"{k} = ?" for k in safe)
    values = list(safe.values()) + [user_id]
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(f"UPDATE users SET {parts} WHERE user_id = ?", values)
            await db.commit()


_USER_COL_CACHE: Optional[set[str]] = None


async def _user_columns() -> set[str]:
    global _USER_COL_CACHE
    if _USER_COL_CACHE is None:
        async with db_conn() as db:
            _USER_COL_CACHE = await table_columns(db, "users")
    return _USER_COL_CACHE


async def expire_user_if_needed(user_id: int) -> None:
    if is_admin_id(user_id):
        return
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute(
                "SELECT plan, plan_expires_at FROM users WHERE user_id = ?",
                (user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return
            expiry = parse_dt(row["plan_expires_at"])
            if row["plan"] != "Free" and expiry and now_dt() >= expiry:
                await db.execute(
                    "UPDATE users SET plan='Free', daily_limit=?, daily_used=0, plan_started_at=NULL, plan_expires_at=NULL WHERE user_id=?",
                    (FREE_DAILY_LIMIT, user_id),
                )
                await db.commit()


async def activate_plan(
    user_id: int,
    plan: str,
    days: int,
    actor_id: int = 0,
    reason: str = "manual",
) -> str:
    if plan not in PLAN_LIMITS or plan == "Free":
        raise ValueError("Invalid paid plan")
    days = max(1, int(days))
    user = await get_user(user_id)
    current_expiry = parse_dt(user.get("plan_expires_at"))
    base = current_expiry if current_expiry and current_expiry > now_dt() else now_dt()
    expiry = base + timedelta(days=days)
    await update_user(
        user_id,
        plan=plan,
        daily_limit=PLAN_LIMITS[plan],
        daily_used=0,
        plan_started_at=now_iso(),
        plan_expires_at=expiry.isoformat(),
    )
    await audit(actor_id or user_id, "plan_activated", user_id, {"plan": plan, "days": days, "reason": reason})
    return expiry.isoformat()


async def remaining_usage(user: dict[str, Any]) -> str:
    if user.get("plan") == "Ultimate" or is_admin_id(int(user["user_id"])):
        return "Unlimited"
    remaining = max(0, int(user.get("daily_limit") or 0) - int(user.get("daily_used") or 0))
    return str(remaining + int(user.get("bonus_credits") or 0))


async def can_consume(user_id: int) -> bool:
    await expire_user_if_needed(user_id)
    user = await get_user(user_id)
    if user.get("plan") == "Ultimate" or is_admin_id(user_id):
        return True
    return int(user.get("daily_used") or 0) < int(user.get("daily_limit") or 0) or int(user.get("bonus_credits") or 0) > 0


async def consume_usage(user_id: int) -> bool:
    if is_admin_id(user_id):
        return True
    await expire_user_if_needed(user_id)
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute(
                "SELECT plan, daily_limit, daily_used, bonus_credits FROM users WHERE user_id = ?",
                (user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return False
            if row["plan"] == "Ultimate":
                return True
            if row["daily_used"] < row["daily_limit"]:
                await db.execute("UPDATE users SET daily_used=daily_used+1 WHERE user_id=?", (user_id,))
                await db.commit()
                return True
            if row["bonus_credits"] > 0:
                await db.execute("UPDATE users SET bonus_credits=bonus_credits-1 WHERE user_id=?", (user_id,))
                await db.commit()
                return True
            return False


async def daily_reset_job() -> None:
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute("UPDATE users SET daily_used = 0")
            await db.commit()
    log.info("Daily usage counters reset")

# ---------------------------------------------------------------------------
# PAYMENTS / PROMOS / SUPPORT
# ---------------------------------------------------------------------------


async def create_invoice(user_id: int, plan: str, duration: str) -> dict[str, Any]:
    if plan not in PLAN_PRICING or duration not in DURATION_DAYS:
        raise ValueError("Invalid plan or duration")
    invoice_id = generate_id("INV", 6)
    amount = int(PLAN_PRICING[plan][duration])
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                """
                INSERT INTO payments(invoice_id, user_id, plan, duration, amount, currency, status, created_at)
                VALUES (?, ?, ?, ?, ?, 'INR', 'awaiting_utr', ?)
                """,
                (invoice_id, user_id, plan, duration, amount, now_iso()),
            )
            await db.commit()
    return {
        "invoice_id": invoice_id,
        "user_id": user_id,
        "plan": plan,
        "duration": duration,
        "amount": amount,
        "currency": "INR",
        "status": "awaiting_utr",
    }


async def get_payment(invoice_id: str) -> Optional[dict[str, Any]]:
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM payments WHERE invoice_id = ?", (invoice_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def latest_awaiting_payment(user_id: int) -> Optional[dict[str, Any]]:
    async with db_conn() as db:
        cur = await db.execute(
            """
            SELECT * FROM payments
            WHERE user_id=? AND status='awaiting_utr'
            ORDER BY id DESC LIMIT 1
            """,
            (user_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def submit_utr(user_id: int, invoice_id: str, utr: str) -> tuple[bool, str]:
    utr = utr.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{10,22}", utr):
        return False, "UTR / transaction reference must be 10–22 letters/numbers."
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM payments WHERE invoice_id=? AND user_id=?", (invoice_id, user_id))
            row = await cur.fetchone()
            if not row:
                return False, "Invoice not found."
            if row["status"] not in {"awaiting_utr", "pending"}:
                return False, f"Invoice is already `{row['status']}`."
            dup = await db.execute("SELECT invoice_id FROM payments WHERE utr=? AND invoice_id != ?", (utr, invoice_id))
            if await dup.fetchone():
                return False, "This UTR has already been submitted for another invoice."
            await db.execute(
                "UPDATE payments SET utr=?, status='pending', submitted_at=? WHERE invoice_id=?",
                (utr, now_iso(), invoice_id),
            )
            await db.commit()
    await audit(user_id, "payment_submitted", invoice_id, {"utr": utr})
    return True, "Payment submitted for verification."


async def approve_payment(invoice_id: str, admin_id: int) -> tuple[bool, str, Optional[dict[str, Any]]]:
    if not await has_admin_role(admin_id, "payments", "manager"):
        return False, "Not authorized", None
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM payments WHERE invoice_id=?", (invoice_id,))
            row = await cur.fetchone()
            if not row:
                return False, "Invoice not found", None
            payment = dict(row)
            if payment["status"] == "approved":
                return False, "Already approved", payment
            if payment["status"] != "pending":
                return False, f"Cannot approve status {payment['status']}", payment
            await db.execute(
                "UPDATE payments SET status='approved', approved_at=?, approved_by=? WHERE invoice_id=?",
                (now_iso(), admin_id, invoice_id),
            )
            await db.commit()
    expiry = await activate_plan(
        int(payment["user_id"]),
        str(payment["plan"]),
        plan_days(str(payment["duration"])),
        actor_id=admin_id,
        reason=f"payment:{invoice_id}",
    )
    payment["plan_expires_at"] = expiry
    await audit(admin_id, "payment_approved", invoice_id, payment)
    return True, "Approved", payment


async def reject_payment(invoice_id: str, admin_id: int, note: str = "") -> tuple[bool, str, Optional[dict[str, Any]]]:
    if not await has_admin_role(admin_id, "payments", "manager"):
        return False, "Not authorized", None
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM payments WHERE invoice_id=?", (invoice_id,))
            row = await cur.fetchone()
            if not row:
                return False, "Invoice not found", None
            payment = dict(row)
            if payment["status"] == "approved":
                return False, "Approved payments cannot be rejected from the bot.", payment
            await db.execute(
                "UPDATE payments SET status='rejected', rejected_at=?, rejected_by=?, note=? WHERE invoice_id=?",
                (now_iso(), admin_id, note[:500], invoice_id),
            )
            await db.commit()
    await audit(admin_id, "payment_rejected", invoice_id, {"note": note})
    return True, "Rejected", payment


async def redeem_promo(user_id: int, code: str) -> tuple[bool, str]:
    code = code.strip().upper()
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
            row = await cur.fetchone()
            if not row:
                return False, "Invalid promo code."
            promo = dict(row)
            expiry = parse_dt(promo.get("expires_at"))
            if expiry and now_dt() >= expiry:
                return False, "This promo code has expired."
            if int(promo.get("uses_left") or 0) <= 0:
                return False, "This promo code has no uses left."
            cur2 = await db.execute(
                "SELECT COUNT(*) FROM promo_redemptions WHERE code=? AND user_id=?",
                (code, user_id),
            )
            redeemed = int((await cur2.fetchone())[0])
            if redeemed >= int(promo.get("max_per_user") or 1):
                return False, "You already used this promo code."
            if int(promo.get("first_purchase_only") or 0):
                pc = await db.execute("SELECT COUNT(*) FROM payments WHERE user_id=? AND status='approved'", (user_id,))
                if int((await pc.fetchone())[0]) > 0:
                    return False, "This promo is only for first-time customers."
            plan = str(promo.get("plan") or "")
            days = int(promo.get("days") or 0)
            if plan not in PLAN_LIMITS or plan == "Free" or days <= 0:
                return False, "This promo is not configured for direct activation."
            await db.execute("UPDATE promo_codes SET uses_left=uses_left-1 WHERE code=?", (code,))
            await db.execute(
                "INSERT INTO promo_redemptions(code, user_id, redeemed_at) VALUES (?, ?, ?)",
                (code, user_id, now_iso()),
            )
            await db.commit()
    expiry_iso = await activate_plan(user_id, plan, days, actor_id=user_id, reason=f"promo:{code}")
    return True, f"🎉 `{plan}` activated until **{human_dt(expiry_iso)}**."


async def create_ticket(user_id: int, category: str, message: str) -> str:
    ticket_id = generate_id("SUP", 6)
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                """
                INSERT INTO support_tickets(ticket_id, user_id, category, message, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'open', ?, ?)
                """,
                (ticket_id, user_id, category[:50], message[:3500], now_iso(), now_iso()),
            )
            await db.commit()
    return ticket_id


async def close_ticket(ticket_id: str, admin_id: int, note: str = "") -> bool:
    if not await has_admin_role(admin_id, "support", "manager"):
        return False
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT user_id FROM support_tickets WHERE ticket_id=?", (ticket_id,))
            row = await cur.fetchone()
            if not row:
                return False
            await db.execute(
                "UPDATE support_tickets SET status='resolved', admin_note=?, updated_at=? WHERE ticket_id=?",
                (note[:1000], now_iso(), ticket_id),
            )
            await db.commit()
    await audit(admin_id, "ticket_resolved", ticket_id, {"note": note})
    return True

# ---------------------------------------------------------------------------
# JOB QUEUE
# ---------------------------------------------------------------------------


async def user_queue_count(user_id: int) -> int:
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM jobs WHERE user_id=? AND status IN ('queued','running')",
            (user_id,),
        )
        return int((await cur.fetchone())[0])


async def enqueue_job(
    user_id: int,
    job_type: str,
    source: str,
    start_id: Optional[int] = None,
    end_id: Optional[int] = None,
    destination: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> str:
    global JOB_SEQ
    if await user_queue_count(user_id) >= MAX_QUEUE_PER_USER and not is_admin_id(user_id):
        raise RuntimeError(f"Maximum {MAX_QUEUE_PER_USER} queued/running jobs allowed per user.")
    user = await get_user(user_id)
    priority = PLAN_PRIORITY.get(str(user.get("plan") or "Free"), 4)
    job_id = generate_id("JOB", 6)
    total = max(0, (end_id or 0) - (start_id or 0) + 1) if start_id and end_id else 0
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                """
                INSERT INTO jobs(
                    job_id,user_id,job_type,source,start_id,end_id,destination,status,priority,
                    progress_total,payload_json,created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
                """,
                (
                    job_id,
                    user_id,
                    job_type,
                    source,
                    start_id,
                    end_id,
                    destination,
                    priority,
                    total,
                    json.dumps(payload or {}, ensure_ascii=False),
                    now_iso(),
                ),
            )
            await db.commit()
    JOB_SEQ += 1
    await JOB_QUEUE.put((priority, JOB_SEQ, job_id))
    await audit(user_id, "job_queued", job_id, {"type": job_type, "source": source, "start": start_id, "end": end_id})
    return job_id


async def get_job(job_id: str) -> Optional[dict[str, Any]]:
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def update_job(job_id: str, **kwargs: Any) -> None:
    if not kwargs:
        return
    allowed = {
        "status", "progress_total", "progress_done", "success_count", "failed_count",
        "error_code", "status_message_id", "started_at", "finished_at", "payload_json",
    }
    safe = {k: v for k, v in kwargs.items() if k in allowed}
    if not safe:
        return
    parts = ", ".join(f"{k}=?" for k in safe)
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(f"UPDATE jobs SET {parts} WHERE job_id=?", list(safe.values()) + [job_id])
            await db.commit()


async def cancel_job(job_id: str, user_id: int) -> bool:
    job = await get_job(job_id)
    if not job or (job["user_id"] != user_id and not is_admin_id(user_id)):
        return False
    if job["status"] not in {"queued", "running"}:
        return False
    await update_job(job_id, status="cancelled", finished_at=now_iso())
    await audit(user_id, "job_cancelled", job_id)
    return True


async def recover_jobs() -> None:
    global JOB_SEQ
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute("UPDATE jobs SET status='queued', started_at=NULL WHERE status='running'")
            cur = await db.execute("SELECT job_id, priority FROM jobs WHERE status='queued' ORDER BY id ASC")
            rows = await cur.fetchall()
            await db.commit()
    for row in rows:
        JOB_SEQ += 1
        await JOB_QUEUE.put((int(row["priority"]), JOB_SEQ, str(row["job_id"])))
    if rows:
        log.info("Recovered %d queued jobs", len(rows))


async def connect_user_client(user: dict[str, Any], tag: str) -> Client:
    encrypted = user.get("encrypted_session")
    session_string = decrypt_text(encrypted)
    if not session_string:
        raise RuntimeError("Telegram account is not connected or session cannot be decrypted.")
    api_id = int(user.get("custom_api_id") or API_ID)
    api_hash = str(user.get("custom_api_hash") or API_HASH)
    client = Client(
        f"user_{user['user_id']}_{tag}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
    )
    await client.connect()
    return client


async def resolve_target_peer(client: Client, chat_id: Any) -> Any:
    try:
        chat = await client.get_chat(chat_id)
        return chat.id
    except Exception:
        pass
    try:
        async for dialog in client.get_dialogs(limit=200):
            if str(dialog.chat.id) == str(chat_id) or str(dialog.chat.username or "").lower() == str(chat_id).lower().lstrip("@"):
                return dialog.chat.id
    except Exception:
        pass
    return chat_id


async def is_chat_admin(client: Client, chat_id: Any) -> bool:
    try:
        me = await client.get_me()
        member = await client.get_chat_member(chat_id, me.id)
        return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
    except Exception:
        return False


async def progress_tracker(current: int, total: int, status_msg: Message, label: str, last: list[float]) -> None:
    now = time.time()
    if now - last[0] < 3 and current != total:
        return
    last[0] = now
    pct = (current / total * 100) if total else 0
    filled = min(10, int(pct // 10))
    bar = "█" * filled + "░" * (10 - filled)
    try:
        await status_msg.edit_text(
            f"⚙️ **{label}**\n\n[{bar}] {pct:.1f}%\n"
            f"📦 {current / 1048576:.1f} MB / {total / 1048576:.1f} MB"
        )
    except Exception:
        pass


async def deliver_message_media(
    user_client: Client,
    target: Message,
    user: dict[str, Any],
    status_msg: Message,
    job_dir: Path,
    item_label: str,
) -> bool:
    user_id = int(user["user_id"])
    filter_type = str(user.get("file_filter") or "all")

    if not target or getattr(target, "empty", False):
        return False
    if target.service:
        return False
    if not target.media and target.text:
        if filter_type not in {"all", "doc"}:
            return False
        await bot.send_message(user_id, target.text[:4096])
        return True
    if filter_type == "video" and not target.video:
        return False
    if filter_type == "doc" and not target.document:
        return False
    if filter_type == "audio" and not (target.audio or target.voice):
        return False
    if filter_type == "photo" and not target.photo:
        return False

    caption = (user.get("custom_caption") or target.caption or "")[:1024]
    thumb_doc = get_valid_thumb(user.get("doc_thumb"))
    thumb_vid = get_valid_thumb(user.get("vid_thumb"))
    last_d = [0.0]
    f_path = await target.download(
        file_name=str(job_dir) + os.sep,
        progress=progress_tracker,
        progress_args=(status_msg, f"Downloading {item_label}", last_d),
    )
    if not f_path or not os.path.exists(f_path):
        return False

    last_u = [0.0]
    try:
        if target.document:
            await bot.send_document(user_id, f_path, caption=caption, thumb=thumb_doc, progress=progress_tracker, progress_args=(status_msg, f"Uploading {item_label}", last_u))
        elif target.video:
            await bot.send_video(user_id, f_path, caption=caption, thumb=thumb_vid, supports_streaming=True, progress=progress_tracker, progress_args=(status_msg, f"Uploading {item_label}", last_u))
        elif target.audio:
            await bot.send_audio(user_id, f_path, caption=caption, thumb=thumb_doc, progress=progress_tracker, progress_args=(status_msg, f"Uploading {item_label}", last_u))
        elif target.voice:
            await bot.send_voice(user_id, f_path, caption=caption)
        elif target.photo:
            await bot.send_photo(user_id, f_path, caption=caption)
        else:
            return False
        return True
    finally:
        try:
            os.remove(f_path)
        except OSError:
            pass


async def execute_save_job(job: dict[str, Any]) -> None:
    user_id = int(job["user_id"])
    user = await get_user(user_id)
    if user.get("is_banned"):
        raise RuntimeError("User is banned")
    chat_id = chat_ref_to_id(str(job["source"]))
    start_id = int(job["start_id"])
    end_id = int(job["end_id"])
    total = end_id - start_id + 1
    job_id = str(job["job_id"])
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    status_msg = await bot.send_message(
        user_id,
        f"⏳ **{job_id} started**\nItems: {total}\nPriority: {PLAN_PRIORITY.get(user.get('plan'), 4)}",
    )
    await update_job(job_id, status_message_id=status_msg.id, progress_total=total)
    u_client: Optional[Client] = None
    success = failed = 0
    try:
        u_client = await connect_user_client(user, job_id[-6:])
        peer = await resolve_target_peer(u_client, chat_id)
        for idx, cur_id in enumerate(range(start_id, end_id + 1), start=1):
            latest = await get_job(job_id)
            if not latest or latest["status"] == "cancelled":
                await status_msg.edit_text(f"🚫 **{job_id} cancelled**\nCompleted: {success} | Failed: {failed}")
                return
            if not await can_consume(user_id):
                await status_msg.edit_text(f"⛔ Daily limit reached.\nJob: `{job_id}`\nCompleted: {success}")
                break
            try:
                target = await u_client.get_messages(peer, cur_id)
                sent = await deliver_message_media(u_client, target, user, status_msg, job_dir, f"#{cur_id}")
                if sent:
                    await consume_usage(user_id)
                    success += 1
                else:
                    failed += 1
            except FloodWait as fw:
                await asyncio.sleep(int(fw.value) + 1)
                try:
                    target = await u_client.get_messages(peer, cur_id)
                    sent = await deliver_message_media(u_client, target, user, status_msg, job_dir, f"#{cur_id}")
                    if sent:
                        await consume_usage(user_id)
                        success += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
            except (PeerIdInvalid, ChannelPrivate):
                failed += 1
            except Exception as exc:
                failed += 1
                await log_error(user_id, f"job_item:{job_id}:{cur_id}", exc)
            await update_job(job_id, progress_done=idx, success_count=success, failed_count=failed)
            if idx % 5 == 0 or idx == total:
                try:
                    await status_msg.edit_text(
                        f"⚙️ **{job_id}**\n"
                        f"Progress: {idx}/{total}\n✅ {success} | ❌ {failed}"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.35)

        latest = await get_job(job_id)
        if latest and latest["status"] != "cancelled":
            await status_msg.edit_text(
                f"✅ **Job completed**\n\nJob: `{job_id}`\n"
                f"Success: **{success}**\nFailed/Skipped: **{failed}**"
            )
    finally:
        if u_client and u_client.is_connected:
            await u_client.disconnect()
        shutil.rmtree(job_dir, ignore_errors=True)


async def execute_transfer_job(job: dict[str, Any]) -> None:
    """Transfer history only when the connected user is admin/owner in BOTH chats."""
    user_id = int(job["user_id"])
    user = await get_user(user_id)
    if user.get("plan") != "Ultimate" and not is_admin_id(user_id):
        raise RuntimeError("Owned-channel transfer requires Ultimate plan")
    src = chat_ref_to_id(str(job["source"]))
    dest = chat_ref_to_id(str(job["destination"]))
    job_id = str(job["job_id"])
    status_msg = await bot.send_message(user_id, f"🔄 **{job_id}** validating channel ownership/admin rights…")
    u_client: Optional[Client] = None
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    success = failed = 0
    try:
        u_client = await connect_user_client(user, f"transfer_{job_id[-6:]}")
        src = await resolve_target_peer(u_client, src)
        dest = await resolve_target_peer(u_client, dest)
        if not await is_chat_admin(u_client, src) or not await is_chat_admin(u_client, dest):
            raise PermissionError("For safety, you must be owner/admin in both source and destination chats.")
        await status_msg.edit_text(f"🚀 **{job_id}** transfer started")
        limit = int((json.loads(job.get("payload_json") or "{}") or {}).get("limit", 500))
        limit = max(1, min(limit, 5000))
        async for post in u_client.get_chat_history(src, limit=limit):
            latest = await get_job(job_id)
            if not latest or latest["status"] == "cancelled":
                break
            try:
                caption = (user.get("custom_caption") or post.caption or "")[:1024]
                if post.media:
                    f = await post.download(file_name=str(job_dir) + os.sep)
                    if not f:
                        failed += 1
                        continue
                    try:
                        if post.document:
                            await u_client.send_document(dest, f, caption=caption, thumb=get_valid_thumb(user.get("doc_thumb")))
                        elif post.video:
                            await u_client.send_video(dest, f, caption=caption, thumb=get_valid_thumb(user.get("vid_thumb")), supports_streaming=True)
                        elif post.audio:
                            await u_client.send_audio(dest, f, caption=caption, thumb=get_valid_thumb(user.get("doc_thumb")))
                        elif post.voice:
                            await u_client.send_voice(dest, f, caption=caption)
                        elif post.photo:
                            await u_client.send_photo(dest, f, caption=caption)
                        success += 1
                    finally:
                        try:
                            os.remove(f)
                        except OSError:
                            pass
                elif post.text:
                    await u_client.send_message(dest, post.text[:4096])
                    success += 1
                else:
                    failed += 1
                await update_job(job_id, progress_done=success + failed, success_count=success, failed_count=failed)
                if (success + failed) % 10 == 0:
                    await status_msg.edit_text(f"🔄 **{job_id}**\n✅ {success} | ❌ {failed}")
                await asyncio.sleep(0.6)
            except FloodWait as fw:
                await asyncio.sleep(int(fw.value) + 1)
            except Exception as exc:
                failed += 1
                await log_error(user_id, f"transfer_item:{job_id}", exc)
        await status_msg.edit_text(f"✅ **Transfer complete**\nJob: `{job_id}`\n✅ {success} | ❌ {failed}")
    finally:
        if u_client and u_client.is_connected:
            await u_client.disconnect()
        shutil.rmtree(job_dir, ignore_errors=True)


async def job_worker(worker_no: int) -> None:
    log.info("Worker %d started", worker_no)
    while not SHUTDOWN_EVENT.is_set():
        try:
            priority, seq, job_id = await asyncio.wait_for(JOB_QUEUE.get(), timeout=2)
        except asyncio.TimeoutError:
            continue
        try:
            job = await get_job(job_id)
            if not job or job["status"] != "queued":
                continue
            await update_job(job_id, status="running", started_at=now_iso())
            try:
                if job["job_type"] in {"single", "batch"}:
                    await asyncio.wait_for(execute_save_job(job), timeout=JOB_TIMEOUT_SECONDS)
                elif job["job_type"] == "transfer":
                    await asyncio.wait_for(execute_transfer_job(job), timeout=JOB_TIMEOUT_SECONDS)
                else:
                    raise RuntimeError(f"Unknown job type: {job['job_type']}")
                latest = await get_job(job_id)
                if latest and latest["status"] == "running":
                    await update_job(job_id, status="completed", finished_at=now_iso())
            except asyncio.TimeoutError as exc:
                code = await log_error(int(job["user_id"]), f"job_timeout:{job_id}", exc)
                await update_job(job_id, status="failed", error_code=code, finished_at=now_iso())
                try:
                    await bot.send_message(int(job["user_id"]), f"❌ Job `{job_id}` timed out. Error ID: `{code}`")
                except Exception:
                    pass
            except Exception as exc:
                code = await log_error(int(job["user_id"]), f"job:{job_id}", exc)
                await update_job(job_id, status="failed", error_code=code, finished_at=now_iso())
                try:
                    await bot.send_message(int(job["user_id"]), f"❌ Job `{job_id}` failed. Error ID: `{code}`")
                except Exception:
                    pass
        finally:
            JOB_QUEUE.task_done()

# ---------------------------------------------------------------------------
# UI BUILDERS
# ---------------------------------------------------------------------------


def home_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📥 New Save", callback_data="home_save"), InlineKeyboardButton("📋 My Jobs", callback_data="home_jobs")],
        [InlineKeyboardButton("💎 Plans", callback_data="home_plans"), InlineKeyboardButton("📊 My Plan", callback_data="home_myplan")],
        [InlineKeyboardButton("🔐 Connect", callback_data="home_login"), InlineKeyboardButton("🚪 Logout", callback_data="home_logout")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="home_settings"), InlineKeyboardButton("🎁 Rewards", callback_data="home_rewards")],
        [InlineKeyboardButton("🆘 Support", callback_data="home_support"), InlineKeyboardButton("📖 Help", callback_data="home_help")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("👑 Admin Dashboard", callback_data="admin_home")])
    return InlineKeyboardMarkup(rows)


def settings_keyboard(user: dict[str, Any]) -> InlineKeyboardMarkup:
    marketing = "ON" if int(user.get("marketing_opt_in") or 0) else "OFF"
    notify = "ON" if int(user.get("notify_jobs") or 0) else "OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Language", callback_data="settings_lang"), InlineKeyboardButton("🎯 Media Filter", callback_data="settings_filter")],
        [InlineKeyboardButton("🖼 Doc Thumb", callback_data="settings_doc_thumb"), InlineKeyboardButton("🎬 Video Thumb", callback_data="settings_vid_thumb")],
        [InlineKeyboardButton(f"🔔 Job Alerts: {notify}", callback_data="toggle_job_notify")],
        [InlineKeyboardButton(f"📢 Marketing: {marketing}", callback_data="toggle_marketing")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Overview", callback_data="adm_overview"), InlineKeyboardButton("💳 Payments", callback_data="adm_payments")],
        [InlineKeyboardButton("👥 Customers", callback_data="adm_customers"), InlineKeyboardButton("🆘 Support", callback_data="adm_support")],
        [InlineKeyboardButton("💰 Revenue", callback_data="adm_revenue"), InlineKeyboardButton("📢 Marketing", callback_data="adm_marketing")],
        [InlineKeyboardButton("🛠 System", callback_data="adm_system"), InlineKeyboardButton("📜 Audit", callback_data="adm_audit")],
        [InlineKeyboardButton("⬅️ User Home", callback_data="home")],
    ])


async def maintenance_blocked(user_id: int) -> Optional[str]:
    if await has_admin_role(user_id, *ADMIN_ROLES):
        return None
    if await get_setting("maintenance_mode", "0") == "1":
        return await get_setting("maintenance_message", "🛠 Maintenance in progress.")
    return None


async def render_home(user_id: int, message: Message, edit: bool = False, display_name: str = "User") -> None:
    user = await get_user(user_id, display_name)
    lang = str(user.get("lang") or "ta")
    t = LANG.get(lang, LANG["en"])
    expiry = human_dt(user.get("plan_expires_at"))
    qcount = await user_queue_count(user_id)
    session_ok = bool(decrypt_text(user.get("encrypted_session")))
    text = t["welcome"].format(
        name=display_name,
        app=APP_NAME,
        plan=user.get("plan", "Free"),
        expiry=expiry,
        session=t["connected"] if session_ok else t["not_connected"],
        filter=str(user.get("file_filter") or "all").upper(),
        queue=qcount,
    )
    markup = home_keyboard(bool(await get_admin_role(user_id)))
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_plans(message: Message, edit: bool = False) -> None:
    text = (
        "💎 **PLANS & PRICING**\n\n"
        "⚡ **Basic** — 10 jobs/day\nWeekly ₹30 | Monthly ₹100 | Yearly ₹840\n\n"
        "🥈 **Standard** — 50 jobs/day + batch\nWeekly ₹50 | Monthly ₹180 | Yearly ₹1500\n\n"
        "🥇 **Premium** — 100 jobs/day + priority batch\nWeekly ₹80 | Monthly ₹280 | Yearly ₹2350\n\n"
        "🔷 **Ultimate** — Unlimited + highest priority + owned-channel transfer\n"
        "Weekly ₹130 | Monthly ₹500 | Yearly ₹4200\n\n"
        "✅ Every paid plan has a real expiry date."
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Basic", callback_data="plan:Basic"), InlineKeyboardButton("🥈 Standard", callback_data="plan:Standard")],
        [InlineKeyboardButton("🥇 Premium", callback_data="plan:Premium"), InlineKeyboardButton("🔷 Ultimate", callback_data="plan:Ultimate")],
        [InlineKeyboardButton("🎁 24h Free Trial", callback_data="trial_activate")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_myplan(user_id: int, message: Message, edit: bool = False) -> None:
    user = await get_user(user_id)
    remaining = await remaining_usage(user)
    text = (
        "📊 **MY SUBSCRIPTION**\n\n"
        f"🏷 Plan: **{user.get('plan', 'Free')}**\n"
        f"⏳ Expiry: **{human_dt(user.get('plan_expires_at'))}**\n"
        f"📥 Used today: **{user.get('daily_used', 0)}**\n"
        f"🎯 Remaining: **{remaining}**\n"
        f"🎁 Bonus credits: **{user.get('bonus_credits', 0)}**\n"
        f"🧪 Trial used: **{'Yes' if user.get('trial_used') else 'No'}**"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬆️ Upgrade / Renew", callback_data="home_plans")],
        [InlineKeyboardButton("💳 Payment History", callback_data="payment_history")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_jobs(user_id: int, message: Message, edit: bool = False) -> None:
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT job_id, job_type, status, progress_done, progress_total, success_count, failed_count, created_at "
            "FROM jobs WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (user_id,),
        )
        rows = await cur.fetchall()
    if not rows:
        text = "📋 **MY JOBS**\n\nNo jobs yet. Send a Telegram post link to start."
    else:
        lines = ["📋 **MY RECENT JOBS**", ""]
        for r in rows:
            progress = f"{r['progress_done']}/{r['progress_total']}" if r["progress_total"] else str(r["progress_done"])
            lines.append(f"`{r['job_id']}` • {r['job_type']} • **{r['status']}** • {progress} • ✅{r['success_count']} ❌{r['failed_count']}")
        text = "\n".join(lines)
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="home_jobs")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_settings(user_id: int, message: Message, edit: bool = False) -> None:
    user = await get_user(user_id)
    text = (
        "⚙️ **SETTINGS**\n\n"
        f"🌐 Language: `{user.get('lang', 'ta')}`\n"
        f"🎯 Media filter: `{str(user.get('file_filter') or 'all').upper()}`\n"
        f"🖼 Document thumb: {'✅' if get_valid_thumb(user.get('doc_thumb')) else '❌'}\n"
        f"🎬 Video thumb: {'✅' if get_valid_thumb(user.get('vid_thumb')) else '❌'}\n"
        f"📝 Custom caption: {'✅' if user.get('custom_caption') else '❌'}"
    )
    if edit:
        try:
            await message.edit_text(text, reply_markup=settings_keyboard(user))
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=settings_keyboard(user))


async def render_admin(message: Message, edit: bool = False) -> None:
    text = "👑 **ADMIN BUSINESS DASHBOARD**\n\nPayments • Customers • Support • Revenue • Marketing • System • Audit"
    if edit:
        try:
            await message.edit_text(text, reply_markup=admin_keyboard())
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=admin_keyboard())

# ---------------------------------------------------------------------------
# COMMAND HANDLERS
# ---------------------------------------------------------------------------


@bot.on_message(filters.command("start") & filters.private)
async def start_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user_id = msg.from_user.id
    if rate_limited(user_id):
        await msg.reply_text("⏳ Too many requests. Please retry in a few seconds.")
        return
    user = await get_user(user_id, msg.from_user.first_name or "User", msg.from_user.username)
    if user.get("is_banned"):
        await msg.reply_text("⛔ Your access to this bot has been disabled. Contact support if you believe this is an error.")
        return
    args = msg.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("ref_") and not user.get("referred_by"):
        raw = args[1][4:]
        if raw.isdigit() and int(raw) != user_id:
            ref_id = int(raw)
            try:
                ref_user = await get_user(ref_id)
                await update_user(user_id, referred_by=ref_id)
                async with DB_LOCK:
                    async with db_conn() as db:
                        await db.execute("UPDATE users SET ref_count=ref_count+1, bonus_credits=bonus_credits+2 WHERE user_id=?", (ref_id,))
                        await db.commit()
                await audit(user_id, "referral_join", ref_id)
                try:
                    await bot.send_message(ref_id, "🎉 New referral! +2 bonus credits added.")
                except Exception:
                    pass
            except Exception:
                pass
    await render_home(user_id, msg, display_name=msg.from_user.first_name or "User")


@bot.on_message(filters.command("help") & filters.private)
async def help_handler(_: Client, msg: Message) -> None:
    await msg.reply_text(MANUAL)


@bot.on_message(filters.command("login") & filters.private)
async def login_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    block = await maintenance_blocked(msg.from_user.id)
    if block:
        await msg.reply_text(block)
        return
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Session String", callback_data="login:session"), InlineKeyboardButton("📲 Phone + OTP", callback_data="login:phone")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    await msg.reply_text(
        "🔐 **Connect your Telegram account**\n\n"
        "Your session is encrypted before storage. For best security, use a dedicated SESSION_ENCRYPTION_KEY on the server.\n\n"
        "Choose a method:",
        reply_markup=markup,
    )


@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    await update_user(msg.from_user.id, encrypted_session=None, session=None)
    state = LOGIN_STATES.pop(msg.from_user.id, None)
    cli = state.get("client") if state else None
    if cli:
        try:
            if cli.is_connected:
                await cli.disconnect()
        except Exception:
            pass
    await audit(msg.from_user.id, "logout")
    await msg.reply_text("🚪 Account disconnected and stored session removed.")


@bot.on_message(filters.command("plans") & filters.private)
async def plans_handler(_: Client, msg: Message) -> None:
    await render_plans(msg)


@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await render_myplan(msg.from_user.id, msg)


@bot.on_message(filters.command("trial") & filters.private)
async def trial_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    if user.get("trial_used"):
        await msg.reply_text("🎁 Your one-time free trial has already been used.")
        return
    if user.get("plan") not in {"Free"}:
        await msg.reply_text("You already have an active plan. Use the trial later only if eligible.")
        return
    expiry = now_dt() + timedelta(hours=24)
    await update_user(
        msg.from_user.id,
        plan="Trial",
        daily_limit=TRIAL_DAILY_LIMIT,
        daily_used=0,
        trial_used=1,
        plan_started_at=now_iso(),
        plan_expires_at=expiry.isoformat(),
    )
    await audit(msg.from_user.id, "trial_activated", msg.from_user.id)
    await msg.reply_text(f"🎁 **24-hour Trial activated**\nExpires: **{human_dt(expiry.isoformat())}**")


@bot.on_message(filters.command("redeem") & filters.private)
async def redeem_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.reply_text("Usage: `/redeem YOUR_CODE`")
        return
    ok, text = await redeem_promo(msg.from_user.id, args[1])
    await msg.reply_text(("✅ " if ok else "❌ ") + text)


@bot.on_message(filters.command("paymenthistory") & filters.private)
async def payment_history_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT invoice_id, plan, duration, amount, status, created_at FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (msg.from_user.id,),
        )
        rows = await cur.fetchall()
    if not rows:
        await msg.reply_text("💳 No payment history yet.")
        return
    lines = ["💳 **PAYMENT HISTORY**", ""]
    for r in rows:
        lines.append(f"`{r['invoice_id']}` • {r['plan']} {r['duration']} • ₹{r['amount']} • **{r['status']}**")
    await msg.reply_text("\n".join(lines))


@bot.on_message(filters.command("jobs") & filters.private)
async def jobs_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await render_jobs(msg.from_user.id, msg)


@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) > 1 and args[1].strip().upper().startswith("JOB-"):
        ok = await cancel_job(args[1].strip().upper(), msg.from_user.id)
        await msg.reply_text("🚫 Job cancelled." if ok else "❌ Job not found or cannot be cancelled.")
        return
    state = LOGIN_STATES.pop(msg.from_user.id, None)
    if state and state.get("client"):
        try:
            if state["client"].is_connected:
                await state["client"].disconnect()
        except Exception:
            pass
    await msg.reply_text("✅ Current input flow cancelled. To cancel a job: `/cancel JOB-xxxxxx-xxxxxx`")


@bot.on_message(filters.command("settings") & filters.private)
async def settings_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await render_settings(msg.from_user.id, msg)


@bot.on_message(filters.command("setcaption") & filters.private)
async def setcaption_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    caption = msg.text.partition(" ")[2].strip()
    if not caption:
        await msg.reply_text("Usage: `/setcaption Your caption`")
        return
    await update_user(msg.from_user.id, custom_caption=caption[:1024])
    await msg.reply_text("✅ Custom caption saved.")


@bot.on_message(filters.command("delcaption") & filters.private)
async def delcaption_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await update_user(msg.from_user.id, custom_caption=None)
        await msg.reply_text("🗑 Custom caption removed.")


@bot.on_message(filters.command("setthumb") & filters.private)
async def setthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with `/setthumb`.")
        return
    path = THUMB_DIR / f"doc_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=str(path))
    await update_user(msg.from_user.id, doc_thumb=str(path))
    await msg.reply_text("✅ Document/audio thumbnail saved.")


@bot.on_message(filters.command("setvthumb") & filters.private)
async def setvthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with `/setvthumb`.")
        return
    path = THUMB_DIR / f"video_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=str(path))
    await update_user(msg.from_user.id, vid_thumb=str(path))
    await msg.reply_text("✅ Video thumbnail saved.")


@bot.on_message(filters.command("delthumb") & filters.private)
async def delthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    path = user.get("doc_thumb")
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    await update_user(msg.from_user.id, doc_thumb=None)
    await msg.reply_text("🗑 Document/audio thumbnail removed.")


@bot.on_message(filters.command("delvthumb") & filters.private)
async def delvthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    path = user.get("vid_thumb")
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    await update_user(msg.from_user.id, vid_thumb=None)
    await msg.reply_text("🗑 Video thumbnail removed.")


@bot.on_message(filters.command("batch") & filters.private)
async def batch_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    block = await maintenance_blocked(msg.from_user.id)
    if block:
        await msg.reply_text(block)
        return
    user = await get_user(msg.from_user.id)
    if user.get("plan") not in {"Standard", "Premium", "Ultimate"} and not is_admin_id(msg.from_user.id):
        await msg.reply_text("🔒 Batch is available on Standard, Premium and Ultimate plans.")
        return
    links = re.findall(r"https?://t\.me/(?:c/)?[A-Za-z0-9_]+/[0-9]+", msg.text)
    if len(links) >= 2:
        a = parse_tg_link(links[0])
        b = parse_tg_link(links[1])
        if a and b and a[0] == b[0]:
            start_id, end_id = sorted((a[1], b[1]))
            if end_id - start_id + 1 > MAX_BATCH_SIZE:
                await msg.reply_text(f"❌ Maximum batch size is {MAX_BATCH_SIZE} items.")
                return
            job_id = await enqueue_job(msg.from_user.id, "batch", a[0], start_id, end_id)
            await msg.reply_text(f"✅ Batch queued: `{job_id}`\nItems: {end_id-start_id+1}")
            return
    LOGIN_STATES[msg.from_user.id] = {"step": "BATCH_START", "updated_at": time.time()}
    await msg.reply_text("📦 Send the first Telegram post link. Use `/cancel` to exit.")


@bot.on_message(filters.command(["transfer", "clone"]) & filters.private)
async def transfer_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    if user.get("plan") != "Ultimate" and not is_admin_id(msg.from_user.id):
        await msg.reply_text("🔒 Owned-channel transfer is available on Ultimate plan.")
        return
    parts = msg.text.split()
    if len(parts) < 3:
        await msg.reply_text(
            "Usage: `/transfer <source_chat_id_or_username> <destination_chat_id_or_username> [limit]`\n\n"
            "For safety, your connected Telegram account must be admin/owner in both chats."
        )
        return
    src, dest = parts[1], parts[2]
    limit = 500
    if len(parts) >= 4 and parts[3].isdigit():
        limit = max(1, min(int(parts[3]), 5000))
    job_id = await enqueue_job(msg.from_user.id, "transfer", src, destination=dest, payload={"limit": limit})
    await msg.reply_text(f"✅ Owned-channel transfer queued: `{job_id}`")


@bot.on_message(filters.command("support") & filters.private)
async def support_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Payment", callback_data="support:payment"), InlineKeyboardButton("🔐 Login", callback_data="support:login")],
        [InlineKeyboardButton("📥 Job", callback_data="support:job"), InlineKeyboardButton("💎 Subscription", callback_data="support:subscription")],
        [InlineKeyboardButton("🐞 Bug", callback_data="support:bug"), InlineKeyboardButton("💡 Feature", callback_data="support:feature")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    await msg.reply_text("🆘 Choose a support category:", reply_markup=markup)

# ---------------------------------------------------------------------------
# ADMIN COMMANDS
# ---------------------------------------------------------------------------


@bot.on_message(filters.command(["admin", "adminsetting"]) & filters.private)
async def admin_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await get_admin_role(msg.from_user.id):
        return
    await render_admin(msg)


@bot.on_message(filters.command("stats") & filters.private)
async def stats_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager", "developer", "marketing", "payments", "support"):
        return
    async with db_conn() as db:
        total = int((await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0])
        paid = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE plan NOT IN ('Free','Trial')")).fetchone())[0])
        active = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE last_seen >= ?", ((now_dt()-timedelta(days=7)).isoformat(),))).fetchone())[0])
        open_tickets = int((await (await db.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'")).fetchone())[0])
        pending = int((await (await db.execute("SELECT COUNT(*) FROM payments WHERE status='pending'")).fetchone())[0])
        queued = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running')")).fetchone())[0])
    await msg.reply_text(
        f"📊 **BOT OVERVIEW**\n\nUsers: `{total}`\n7-day active: `{active}`\nPaid: `{paid}`\nPending payments: `{pending}`\nOpen tickets: `{open_tickets}`\nQueued/running jobs: `{queued}`"
    )


@bot.on_message(filters.command("revenue") & filters.private)
async def revenue_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager", "payments"):
        return
    today = now_dt().date().isoformat()
    month = now_dt().strftime("%Y-%m")
    async with db_conn() as db:
        total = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved'")).fetchone())[0])
        today_rev = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved' AND substr(approved_at,1,10)=?", (today,))).fetchone())[0])
        month_rev = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved' AND substr(approved_at,1,7)=?", (month,))).fetchone())[0])
    await msg.reply_text(f"💰 **REVENUE**\n\nToday: **₹{today_rev}**\nThis month: **₹{month_rev}**\nAll time: **₹{total}**")


@bot.on_message(filters.command("user") & filters.private)
async def user_lookup_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager", "payments", "support"):
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.reply_text("Usage: `/user <telegram_user_id>`")
        return
    uid = int(args[1])
    user = await get_user(uid)
    async with db_conn() as db:
        pcount = int((await (await db.execute("SELECT COUNT(*) FROM payments WHERE user_id=? AND status='approved'", (uid,))).fetchone())[0])
        spent = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE user_id=? AND status='approved'", (uid,))).fetchone())[0])
        jcount = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE user_id=?", (uid,))).fetchone())[0])
    await msg.reply_text(
        "👤 **CUSTOMER PROFILE**\n\n"
        f"Name: {user.get('name')}\nID: `{uid}`\nUsername: @{user.get('username') or '-'}\n"
        f"Plan: **{user.get('plan')}**\nExpiry: **{human_dt(user.get('plan_expires_at'))}**\n"
        f"Paid orders: `{pcount}`\nLifetime revenue: `₹{spent}`\nJobs: `{jcount}`\n"
        f"Referrals: `{user.get('ref_count', 0)}`\nBanned: `{bool(user.get('is_banned'))}`"
    )


@bot.on_message(filters.command("ap") & filters.private)
async def admin_plan_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 3 or not args[1].isdigit():
        await msg.reply_text("Usage: `/ap <user_id> <Basic|Standard|Premium|Ultimate> [days]`")
        return
    uid = int(args[1])
    plan = args[2].capitalize()
    days = int(args[3]) if len(args) >= 4 and args[3].isdigit() else 30
    if plan not in PLAN_PRICING:
        await msg.reply_text("Invalid plan.")
        return
    expiry = await activate_plan(uid, plan, days, msg.from_user.id, "admin_manual")
    await msg.reply_text(f"✅ `{uid}` → **{plan}** until **{human_dt(expiry)}**")
    try:
        await bot.send_message(uid, f"🎉 Your **{plan}** plan is active until **{human_dt(expiry)}**.")
    except Exception:
        pass


@bot.on_message(filters.command("rp") & filters.private)
async def reset_plan_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.reply_text("Usage: `/rp <user_id>`")
        return
    uid = int(args[1])
    await get_user(uid)
    await update_user(uid, plan="Free", daily_limit=FREE_DAILY_LIMIT, daily_used=0, plan_started_at=None, plan_expires_at=None)
    await audit(msg.from_user.id, "plan_reset", uid)
    await msg.reply_text(f"✅ `{uid}` reset to Free.")


@bot.on_message(filters.command(["ban", "unban"]) & filters.private)
async def ban_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.reply_text(f"Usage: `/{msg.command[0]} <user_id>`")
        return
    uid = int(args[1])
    if uid == OWNER_ID:
        await msg.reply_text("Owner cannot be banned.")
        return
    value = 1 if msg.command[0] == "ban" else 0
    await get_user(uid)
    await update_user(uid, is_banned=value)
    await audit(msg.from_user.id, msg.command[0], uid)
    await msg.reply_text(f"✅ User `{uid}` {'banned' if value else 'unbanned'}.")


@bot.on_message(filters.command("addcredits") & filters.private)
async def addcredits_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 3 or not args[1].isdigit() or not args[2].lstrip("-").isdigit():
        await msg.reply_text("Usage: `/addcredits <user_id> <amount>`")
        return
    uid, amount = int(args[1]), int(args[2])
    await get_user(uid)
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute("UPDATE users SET bonus_credits=MAX(0, bonus_credits + ?) WHERE user_id=?", (amount, uid))
            await db.commit()
    await audit(msg.from_user.id, "credits_changed", uid, {"amount": amount})
    await msg.reply_text(f"✅ Credits changed by {amount} for `{uid}`.")


@bot.on_message(filters.command("addpromo") & filters.private)
async def addpromo_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "marketing", "manager"):
        return
    args = msg.text.split()
    if len(args) < 5:
        await msg.reply_text("Usage: `/addpromo <CODE> <Plan> <Days> <Uses> [expiry_days]`\nExample: `/addpromo VIP30 Premium 30 25 14`")
        return
    code = args[1].upper()
    plan = args[2].capitalize()
    if plan not in PLAN_PRICING or not args[3].isdigit() or not args[4].isdigit():
        await msg.reply_text("Invalid plan/days/uses.")
        return
    days, uses = int(args[3]), int(args[4])
    expiry = None
    if len(args) >= 6 and args[5].isdigit():
        expiry = (now_dt() + timedelta(days=int(args[5]))).isoformat()
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO promo_codes(code,plan,days,uses_left,expires_at,max_per_user) VALUES (?,?,?,?,?,1) "
                "ON CONFLICT(code) DO UPDATE SET plan=excluded.plan, days=excluded.days, uses_left=excluded.uses_left, expires_at=excluded.expires_at",
                (code, plan, days, uses, expiry),
            )
            await db.commit()
    await audit(msg.from_user.id, "promo_created", code, {"plan": plan, "days": days, "uses": uses})
    await msg.reply_text(f"✅ Promo `{code}` created: {plan} / {days} days / {uses} uses.")


@bot.on_message(filters.command("tickets") & filters.private)
async def tickets_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "support", "manager"):
        return
    async with db_conn() as db:
        cur = await db.execute("SELECT ticket_id,user_id,category,message,created_at FROM support_tickets WHERE status='open' ORDER BY id ASC LIMIT 15")
        rows = await cur.fetchall()
    if not rows:
        await msg.reply_text("🆘 No open support tickets.")
        return
    for r in rows:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Resolve", callback_data=f"ticket:close:{r['ticket_id']}")]])
        await msg.reply_text(
            f"🆘 `{r['ticket_id']}`\nUser: `{r['user_id']}`\nCategory: **{r['category']}**\n\n{r['message'][:2500]}",
            reply_markup=markup,
        )


@bot.on_message(filters.command("broadcast") & filters.private)
async def broadcast_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "marketing", "manager"):
        return
    if not msg.reply_to_message:
        await msg.reply_text("Reply to a message with `/broadcast [all|free|paid|marketing|lang:ta]`.")
        return
    args = msg.text.split(maxsplit=1)
    segment = args[1].strip().lower() if len(args) > 1 else "all"
    query = "SELECT user_id FROM users WHERE is_banned=0"
    params: list[Any] = []
    if segment == "free":
        query += " AND plan IN ('Free','Trial')"
    elif segment == "paid":
        query += " AND plan NOT IN ('Free','Trial')"
    elif segment == "marketing":
        query += " AND marketing_opt_in=1"
    elif segment.startswith("lang:"):
        query += " AND lang=?"
        params.append(segment.split(":", 1)[1][:5])
    async with db_conn() as db:
        rows = await (await db.execute(query, params)).fetchall()
    sent = blocked = failed = 0
    status = await msg.reply_text(f"📢 Broadcasting to {len(rows)} users…")
    for idx, r in enumerate(rows, start=1):
        try:
            await msg.reply_to_message.copy(int(r["user_id"]))
            sent += 1
        except Exception as exc:
            text = str(exc).lower()
            if "blocked" in text or "deactivated" in text:
                blocked += 1
            else:
                failed += 1
        if idx % 40 == 0:
            try:
                await status.edit_text(f"📢 Broadcast progress {idx}/{len(rows)}\n✅ {sent} | 🚫 {blocked} | ❌ {failed}")
            except Exception:
                pass
        await asyncio.sleep(0.04)
    await audit(msg.from_user.id, "broadcast", segment, {"sent": sent, "blocked": blocked, "failed": failed})
    await status.edit_text(f"✅ **Broadcast complete**\nSent: {sent}\nBlocked: {blocked}\nFailed: {failed}")


@bot.on_message(filters.command("maintenance") & filters.private)
async def maintenance_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "developer", "manager"):
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2 or args[1].lower() not in {"on", "off"}:
        current = await get_setting("maintenance_mode", "0")
        await msg.reply_text(f"Maintenance: {'ON' if current == '1' else 'OFF'}\nUsage: `/maintenance on|off`")
        return
    value = "1" if args[1].lower() == "on" else "0"
    await set_setting("maintenance_mode", value)
    await audit(msg.from_user.id, "maintenance", value)
    await msg.reply_text(f"🛠 Maintenance {'enabled' if value == '1' else 'disabled'}.")


@bot.on_message(filters.command("addadmin") & filters.private)
async def addadmin_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or await get_admin_role(msg.from_user.id) != "owner":
        return
    args = msg.text.split()
    if len(args) < 3 or not args[1].isdigit() or args[2].lower() not in ADMIN_ROLES - {"owner"}:
        await msg.reply_text("Usage: `/addadmin <user_id> <manager|payments|support|marketing|developer>`")
        return
    uid, role = int(args[1]), args[2].lower()
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO admins(user_id,role,added_by,created_at) VALUES (?,?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET role=excluded.role",
                (uid, role, msg.from_user.id, now_iso()),
            )
            await db.commit()
    await audit(msg.from_user.id, "admin_added", uid, {"role": role})
    await msg.reply_text(f"✅ `{uid}` added as **{role}**.")

# ---------------------------------------------------------------------------
# CALLBACK ROUTER
# ---------------------------------------------------------------------------


@bot.on_callback_query()
async def callback_router(_: Client, q: CallbackQuery) -> None:
    if not q.from_user or not q.message:
        return
    user_id = q.from_user.id
    data = q.data or ""
    if rate_limited(user_id):
        await safe_answer(q, "Too many requests. Try again shortly.", True)
        return

    try:
        user = await get_user(user_id, q.from_user.first_name or "User", q.from_user.username)
        if user.get("is_banned"):
            await safe_answer(q, "Access disabled.", True)
            return

        if data == "home":
            await render_home(user_id, q.message, edit=True, display_name=q.from_user.first_name or "User")

        elif data == "home_save":
            await q.message.edit_text(
                "📥 **NEW SAVE**\n\nSend a Telegram post link that your connected account is authorized to access.\n\n"
                "For permitted ranges use `/batch`."
            )

        elif data == "home_jobs":
            await render_jobs(user_id, q.message, edit=True)

        elif data in {"home_plans", "plan_back"}:
            await render_plans(q.message, edit=True)

        elif data == "home_myplan":
            await render_myplan(user_id, q.message, edit=True)

        elif data == "home_login":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ Session String", callback_data="login:session"), InlineKeyboardButton("📲 Phone + OTP", callback_data="login:phone")],
                [InlineKeyboardButton("⬅️ Home", callback_data="home")],
            ])
            await q.message.edit_text("🔐 Choose a secure account connection method:", reply_markup=markup)

        elif data == "home_logout":
            await update_user(user_id, encrypted_session=None, session=None)
            state = LOGIN_STATES.pop(user_id, None)
            cli = state.get("client") if state else None
            if cli:
                try:
                    if cli.is_connected:
                        await cli.disconnect()
                except Exception:
                    pass
            await audit(user_id, "logout")
            await q.message.edit_text("🚪 Account disconnected.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]))

        elif data == "home_settings":
            await render_settings(user_id, q.message, edit=True)

        elif data == "home_rewards":
            bot_name = BOT_USERNAME or (await bot.get_me()).username or "YourBot"
            link = f"https://t.me/{bot_name}?start=ref_{user_id}"
            text = (
                "🎁 **REWARDS & REFERRALS**\n\n"
                f"Referrals: **{user.get('ref_count', 0)}**\n"
                f"Bonus credits: **{user.get('bonus_credits', 0)}**\n\n"
                f"Your referral link:\n`{link}`\n\n"
                "Current reward: +2 bonus credits for a valid new referral."
            )
            await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]))

        elif data == "home_support":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Payment", callback_data="support:payment"), InlineKeyboardButton("🔐 Login", callback_data="support:login")],
                [InlineKeyboardButton("📥 Job", callback_data="support:job"), InlineKeyboardButton("💎 Subscription", callback_data="support:subscription")],
                [InlineKeyboardButton("🐞 Bug", callback_data="support:bug"), InlineKeyboardButton("💡 Feature", callback_data="support:feature")],
                [InlineKeyboardButton("⬅️ Home", callback_data="home")],
            ])
            await q.message.edit_text("🆘 Choose a support category:", reply_markup=markup)

        elif data == "home_help":
            await q.message.edit_text(MANUAL, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]))

        elif data == "trial_activate":
            if user.get("trial_used"):
                await q.message.reply_text("🎁 Your one-time trial has already been used.")
            elif user.get("plan") != "Free":
                await q.message.reply_text("You already have an active plan.")
            else:
                expiry = now_dt() + timedelta(hours=24)
                await update_user(
                    user_id,
                    plan="Trial",
                    daily_limit=TRIAL_DAILY_LIMIT,
                    daily_used=0,
                    trial_used=1,
                    plan_started_at=now_iso(),
                    plan_expires_at=expiry.isoformat(),
                )
                await audit(user_id, "trial_activated", user_id)
                await q.message.edit_text(
                    f"🎁 **24-hour Trial activated**\nExpires: **{human_dt(expiry.isoformat())}**",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]),
                )

        elif data.startswith("plan:"):
            plan = data.split(":", 1)[1]
            if plan not in PLAN_PRICING:
                await safe_answer(q, "Invalid plan", True)
                return
            p = PLAN_PRICING[plan]
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"Weekly — ₹{p['Weekly']}", callback_data=f"buy:{plan}:Weekly")],
                [InlineKeyboardButton(f"Monthly — ₹{p['Monthly']}", callback_data=f"buy:{plan}:Monthly")],
                [InlineKeyboardButton(f"Yearly — ₹{p['Yearly']}", callback_data=f"buy:{plan}:Yearly")],
                [InlineKeyboardButton("⬅️ Back", callback_data="plan_back")],
            ])
            await q.message.edit_text(f"💎 **{plan}**\n{p['access']}\n\nChoose a duration:", reply_markup=markup)

        elif data.startswith("buy:"):
            _, plan, duration = data.split(":", 2)
            invoice = await create_invoice(user_id, plan, duration)
            upi_id = await get_setting("upi_id", UPI_ID_ENV)
            upi_name = await get_setting("upi_name", UPI_NAME_ENV)
            if not upi_id or not upi_name:
                await q.message.reply_text("⚠️ Payment configuration is not ready. Please contact support.")
                return
            payload = (
                f"upi://pay?pa={quote(upi_id)}&pn={quote(upi_name)}&am={invoice['amount']}"
                f"&cu=INR&tn={quote(invoice['invoice_id'])}"
            )
            qr = create_qr_bytes(payload)
            caption = (
                "💳 **PAYMENT INVOICE**\n\n"
                f"Invoice: `{invoice['invoice_id']}`\n"
                f"Plan: **{plan}**\nDuration: **{duration}**\nAmount: **₹{invoice['amount']}**\n"
                f"UPI ID: `{upi_id}`\nPayee: **{upi_name}**\n\n"
                "After payment, send the UTR / transaction reference in this chat.\n"
                "The selected plan, duration and amount are locked to this invoice."
            )
            LOGIN_STATES[user_id] = {
                "step": "PAYMENT_UTR",
                "invoice_id": invoice["invoice_id"],
                "updated_at": time.time(),
            }
            try:
                await q.message.reply_photo(qr, caption=caption)
            except Exception:
                await q.message.reply_text(caption)

        elif data == "payment_history":
            async with db_conn() as db:
                rows = await (await db.execute(
                    "SELECT invoice_id,plan,duration,amount,status FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 10",
                    (user_id,),
                )).fetchall()
            lines = ["💳 **PAYMENT HISTORY**", ""]
            if rows:
                for r in rows:
                    lines.append(f"`{r['invoice_id']}` • {r['plan']} {r['duration']} • ₹{r['amount']} • **{r['status']}**")
            else:
                lines.append("No payment history yet.")
            await q.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ My Plan", callback_data="home_myplan")]]))

        elif data == "login:session":
            LOGIN_STATES[user_id] = {"step": "SESSION_STRING", "updated_at": time.time()}
            await q.message.edit_text(
                "⚡ Send your **Kurigram/Pyrogram session string** now.\n\n"
                "It will be validated and encrypted before database storage.\n"
                "Use `/cancel` to stop."
            )

        elif data == "login:phone":
            LOGIN_STATES[user_id] = {"step": "PHONE", "updated_at": time.time()}
            await q.message.edit_text("📲 Send your phone number with country code, e.g. `+919876543210`.\nUse `/cancel` to stop.")

        elif data == "settings_lang":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("தமிழ்", callback_data="lang:ta"), InlineKeyboardButton("English", callback_data="lang:en")],
                [InlineKeyboardButton("हिन्दी", callback_data="lang:hi"), InlineKeyboardButton("తెలుగు", callback_data="lang:te")],
                [InlineKeyboardButton("മലയാളം", callback_data="lang:ml"), InlineKeyboardButton("ಕನ್ನಡ", callback_data="lang:kn")],
                [InlineKeyboardButton("বাংলা", callback_data="lang:bn")],
                [InlineKeyboardButton("⬅️ Settings", callback_data="home_settings")],
            ])
            await q.message.edit_text("🌐 Choose language:", reply_markup=markup)

        elif data.startswith("lang:"):
            lang = data.split(":", 1)[1]
            if lang in VALID_LANGS:
                await update_user(user_id, lang=lang)
            await render_settings(user_id, q.message, edit=True)

        elif data == "settings_filter":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("All", callback_data="filter:all"), InlineKeyboardButton("Video", callback_data="filter:video")],
                [InlineKeyboardButton("Audio", callback_data="filter:audio"), InlineKeyboardButton("Document", callback_data="filter:doc")],
                [InlineKeyboardButton("Photo", callback_data="filter:photo")],
                [InlineKeyboardButton("⬅️ Settings", callback_data="home_settings")],
            ])
            await q.message.edit_text("🎯 Choose media filter:", reply_markup=markup)

        elif data.startswith("filter:"):
            flt = data.split(":", 1)[1]
            if flt in VALID_FILTERS:
                await update_user(user_id, file_filter=flt)
            await render_settings(user_id, q.message, edit=True)

        elif data == "toggle_job_notify":
            new = 0 if int(user.get("notify_jobs") or 0) else 1
            await update_user(user_id, notify_jobs=new)
            await render_settings(user_id, q.message, edit=True)

        elif data == "toggle_marketing":
            new = 0 if int(user.get("marketing_opt_in") or 0) else 1
            await update_user(user_id, marketing_opt_in=new)
            await render_settings(user_id, q.message, edit=True)

        elif data in {"settings_doc_thumb", "settings_vid_thumb"}:
            cmd = "/setthumb" if data.endswith("doc_thumb") else "/setvthumb"
            await q.message.reply_text(f"Reply to a photo with `{cmd}`. Use `/delthumb` or `/delvthumb` to remove it.")

        elif data.startswith("support:"):
            category = data.split(":", 1)[1]
            LOGIN_STATES[user_id] = {"step": "SUPPORT_MESSAGE", "category": category, "updated_at": time.time()}
            await q.message.edit_text(f"🆘 Category: **{category.title()}**\n\nDescribe the issue in one message. Use `/cancel` to stop.")

        elif data == "admin_home":
            if await get_admin_role(user_id):
                await render_admin(q.message, edit=True)

        elif data == "adm_overview":
            if not await get_admin_role(user_id):
                return
            async with db_conn() as db:
                total = int((await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0])
                paid = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE plan NOT IN ('Free','Trial')")).fetchone())[0])
                pending = int((await (await db.execute("SELECT COUNT(*) FROM payments WHERE status='pending'")).fetchone())[0])
                open_t = int((await (await db.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'")).fetchone())[0])
                jobs = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running')")).fetchone())[0])
            await q.message.edit_text(
                f"📊 **OVERVIEW**\n\nUsers: `{total}`\nPaid: `{paid}`\nPending payments: `{pending}`\nOpen tickets: `{open_t}`\nActive jobs: `{jobs}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_payments":
            if not await has_admin_role(user_id, "payments", "manager"):
                await safe_answer(q, "Not authorized", True)
                return
            async with db_conn() as db:
                rows = await (await db.execute(
                    "SELECT invoice_id,user_id,plan,duration,amount,utr FROM payments WHERE status='pending' ORDER BY id ASC LIMIT 10"
                )).fetchall()
            if not rows:
                await q.message.edit_text("💳 No pending payments.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))
            else:
                await q.message.edit_text("💳 **Pending payments are listed below.**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))
                for r in rows:
                    markup = InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅ Approve", callback_data=f"pay:ok:{r['invoice_id']}"),
                        InlineKeyboardButton("❌ Reject", callback_data=f"pay:no:{r['invoice_id']}"),
                    ]])
                    await q.message.reply_text(
                        f"💳 `{r['invoice_id']}`\nUser: `{r['user_id']}`\n{r['plan']} • {r['duration']} • ₹{r['amount']}\nUTR: `{r['utr']}`",
                        reply_markup=markup,
                    )

        elif data.startswith("pay:ok:"):
            invoice_id = data.split(":", 2)[2]
            ok, text, payment = await approve_payment(invoice_id, user_id)
            if ok and payment:
                await q.message.edit_text(f"✅ `{invoice_id}` approved for user `{payment['user_id']}`.")
                try:
                    await bot.send_message(
                        int(payment["user_id"]),
                        f"🎉 **SUBSCRIPTION ACTIVATED**\n\nInvoice: `{invoice_id}`\n"
                        f"Plan: **{payment['plan']}**\nDuration: **{payment['duration']}**\n"
                        f"Expires: **{human_dt(payment.get('plan_expires_at'))}**",
                    )
                except Exception:
                    pass
            else:
                await safe_answer(q, text, True)

        elif data.startswith("pay:no:"):
            invoice_id = data.split(":", 2)[2]
            ok, text, payment = await reject_payment(invoice_id, user_id, "Rejected by admin")
            if ok and payment:
                await q.message.edit_text(f"❌ `{invoice_id}` rejected.")
                try:
                    await bot.send_message(int(payment["user_id"]), f"❌ Payment `{invoice_id}` was not approved. Please contact support if needed.")
                except Exception:
                    pass
            else:
                await safe_answer(q, text, True)

        elif data == "adm_customers":
            if not await has_admin_role(user_id, "manager", "payments", "support"):
                return
            await q.message.edit_text(
                "👥 **CUSTOMER CRM**\n\nUse:\n`/user <id>` — profile\n`/ap <id> <plan> [days]` — activate\n`/rp <id>` — reset\n`/addcredits <id> <amount>`\n`/ban <id>` / `/unban <id>`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_support":
            if not await has_admin_role(user_id, "support", "manager"):
                return
            await q.message.edit_text("🆘 Use `/tickets` to open the support queue.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))

        elif data == "adm_revenue":
            if not await has_admin_role(user_id, "payments", "manager"):
                return
            month = now_dt().strftime("%Y-%m")
            async with db_conn() as db:
                total = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved'")).fetchone())[0])
                monthly = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved' AND substr(approved_at,1,7)=?", (month,))).fetchone())[0])
            await q.message.edit_text(f"💰 **REVENUE**\n\nThis month: **₹{monthly}**\nAll time: **₹{total}**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))

        elif data == "adm_marketing":
            if not await has_admin_role(user_id, "marketing", "manager"):
                return
            await q.message.edit_text(
                "📢 **MARKETING**\n\n`/addpromo CODE Plan Days Uses [ExpiryDays]`\n"
                "Reply to a message: `/broadcast all|free|paid|marketing|lang:ta`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_system":
            if not await has_admin_role(user_id, "developer", "manager"):
                return
            maintenance = await get_setting("maintenance_mode", "0")
            uptime = int(time.time() - START_TS)
            await q.message.edit_text(
                f"🛠 **SYSTEM**\n\nMaintenance: **{'ON' if maintenance == '1' else 'OFF'}**\n"
                f"Workers: `{WORKER_COUNT}`\nQueue: `{JOB_QUEUE.qsize()}`\nUptime: `{uptime//3600}h {(uptime%3600)//60}m`\n\n"
                "Use `/maintenance on|off`.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_audit":
            if not await has_admin_role(user_id, "developer", "manager"):
                return
            async with db_conn() as db:
                rows = await (await db.execute("SELECT actor_id,action,target_id,created_at FROM audit_logs ORDER BY id DESC LIMIT 12")).fetchall()
            lines = ["📜 **RECENT AUDIT LOG**", ""]
            for r in rows:
                lines.append(f"`{str(r['created_at'])[:16]}` • `{r['actor_id']}` • **{r['action']}** • `{r['target_id'] or '-'}`")
            await q.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))

        elif data.startswith("ticket:close:"):
            ticket_id = data.split(":", 2)[2]
            if await close_ticket(ticket_id, user_id, "Resolved from admin panel"):
                await q.message.edit_text(f"✅ `{ticket_id}` resolved.")
            else:
                await safe_answer(q, "Unable to resolve ticket", True)

        await safe_answer(q)

    except Exception as exc:
        code = await log_error(user_id, f"callback:{data}", exc)
        await safe_answer(q, f"Error ID: {code}", True)

# ---------------------------------------------------------------------------
# TEXT / STATE HANDLER
# ---------------------------------------------------------------------------


@bot.on_message(filters.text & filters.private)
async def text_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user_id = msg.from_user.id
    text = (msg.text or "").strip()
    if text.startswith("/"):
        return
    if rate_limited(user_id):
        await msg.reply_text("⏳ Too many requests. Please retry shortly.")
        return
    user = await get_user(user_id, msg.from_user.first_name or "User", msg.from_user.username)
    if user.get("is_banned"):
        return

    # Expire stale interactive states after 15 minutes.
    state = LOGIN_STATES.get(user_id)
    if state and time.time() - float(state.get("updated_at", time.time())) > 900:
        cli = state.get("client")
        if cli:
            try:
                if cli.is_connected:
                    await cli.disconnect()
            except Exception:
                pass
        LOGIN_STATES.pop(user_id, None)
        state = None

    if state:
        step = state.get("step")
        state["updated_at"] = time.time()

        if step == "SESSION_STRING":
            status = await msg.reply_text("🔎 Validating session…")
            test_client: Optional[Client] = None
            try:
                test_client = Client(
                    f"validate_{user_id}_{secrets.token_hex(2)}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=text,
                    in_memory=True,
                    no_updates=True,
                )
                await test_client.connect()
                me = await test_client.get_me()
                await update_user(user_id, encrypted_session=encrypt_text(text), session=None)
                LOGIN_STATES.pop(user_id, None)
                await audit(user_id, "login_session", me.id)
                await status.edit_text(f"✅ **Account connected**\n{me.first_name} (`{me.id}`)\nSession encrypted at rest.")
            except Exception as exc:
                code = await log_error(user_id, "login_session", exc)
                await status.edit_text(f"❌ Session validation failed. Error ID: `{code}`")
            finally:
                if test_client and test_client.is_connected:
                    await test_client.disconnect()
            return

        if step == "PHONE":
            phone = re.sub(r"[\s-]+", "", text)
            if not re.fullmatch(r"\+[1-9][0-9]{7,14}", phone):
                await msg.reply_text("❌ Enter a valid international phone number, e.g. `+919876543210`.")
                return
            u_client = Client(
                f"otp_{user_id}_{secrets.token_hex(2)}",
                api_id=API_ID,
                api_hash=API_HASH,
                in_memory=True,
                no_updates=True,
            )
            try:
                await u_client.connect()
                sent = await u_client.send_code(phone)
                LOGIN_STATES[user_id] = {
                    "step": "OTP",
                    "client": u_client,
                    "phone": phone,
                    "phone_code_hash": sent.phone_code_hash,
                    "updated_at": time.time(),
                }
                await msg.reply_text("📩 Send the OTP digits. Spaces are allowed. Never share OTP with anyone else.")
            except Exception as exc:
                try:
                    if u_client.is_connected:
                        await u_client.disconnect()
                except Exception:
                    pass
                LOGIN_STATES.pop(user_id, None)
                code = await log_error(user_id, "login_phone", exc)
                await msg.reply_text(f"❌ Could not send OTP. Error ID: `{code}`")
            return

        if step == "OTP":
            otp = re.sub(r"\D", "", text)
            u_client: Client = state["client"]
            try:
                await u_client.sign_in(state["phone"], state["phone_code_hash"], otp)
                session_string = await u_client.export_session_string()
                me = await u_client.get_me()
                await update_user(user_id, encrypted_session=encrypt_text(session_string), session=None)
                LOGIN_STATES.pop(user_id, None)
                await audit(user_id, "login_otp", me.id)
                await msg.reply_text(f"✅ **Account connected:** {me.first_name}\nSession encrypted at rest.")
                if u_client.is_connected:
                    await u_client.disconnect()
            except SessionPasswordNeeded:
                LOGIN_STATES[user_id]["step"] = "2FA"
                await msg.reply_text("🔐 2-step verification is enabled. Send your Telegram 2FA password now.")
            except (PhoneCodeInvalid, PhoneCodeExpired) as exc:
                await msg.reply_text(f"❌ OTP failed: {type(exc).__name__}. Use `/cancel` and `/login` to retry.")
            except Exception as exc:
                code = await log_error(user_id, "login_otp", exc)
                await msg.reply_text(f"❌ Login failed. Error ID: `{code}`")
            return

        if step == "2FA":
            u_client: Client = state["client"]
            try:
                await u_client.check_password(text)
                session_string = await u_client.export_session_string()
                me = await u_client.get_me()
                await update_user(user_id, encrypted_session=encrypt_text(session_string), session=None)
                LOGIN_STATES.pop(user_id, None)
                await audit(user_id, "login_2fa", me.id)
                await msg.reply_text(f"✅ **Account connected:** {me.first_name}\nSession encrypted at rest.")
                if u_client.is_connected:
                    await u_client.disconnect()
            except PasswordHashInvalid:
                await msg.reply_text("❌ Incorrect 2FA password. Try again or `/cancel`.")
            except Exception as exc:
                code = await log_error(user_id, "login_2fa", exc)
                await msg.reply_text(f"❌ 2FA login failed. Error ID: `{code}`")
            return

        if step == "BATCH_START":
            parsed = parse_tg_link(text)
            if not parsed:
                await msg.reply_text("❌ Send a valid Telegram post link.")
                return
            LOGIN_STATES[user_id] = {
                "step": "BATCH_COUNT",
                "chat_raw": parsed[0],
                "start_id": parsed[1],
                "updated_at": time.time(),
            }
            await msg.reply_text(f"✅ Start saved: `#{parsed[1]}`\nNow send item count (1–{MAX_BATCH_SIZE}).")
            return

        if step == "BATCH_COUNT":
            if not text.isdigit() or not (1 <= int(text) <= MAX_BATCH_SIZE):
                await msg.reply_text(f"❌ Enter a number from 1 to {MAX_BATCH_SIZE}.")
                return
            count = int(text)
            chat_raw = state["chat_raw"]
            start_id = int(state["start_id"])
            end_id = start_id + count - 1
            LOGIN_STATES.pop(user_id, None)
            try:
                job_id = await enqueue_job(user_id, "batch", chat_raw, start_id, end_id)
                await msg.reply_text(f"✅ Batch queued: `{job_id}`\nRange: `#{start_id}` → `#{end_id}`")
            except Exception as exc:
                await msg.reply_text(f"❌ Could not queue batch: {exc}")
            return

        if step == "PAYMENT_UTR":
            invoice_id = str(state["invoice_id"])
            utr = re.sub(r"\s+", "", text).upper()
            ok, result = await submit_utr(user_id, invoice_id, utr)
            if not ok:
                await msg.reply_text(f"❌ {result}\nPlease send the correct transaction reference or `/cancel`.")
                return
            LOGIN_STATES.pop(user_id, None)
            payment = await get_payment(invoice_id)
            await msg.reply_text(f"✅ `{invoice_id}` submitted. Payment is pending admin verification.")
            if payment:
                async with db_conn() as db:
                    admins = await (await db.execute("SELECT user_id FROM admins WHERE role IN ('owner','manager','payments')")).fetchall()
                markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Approve", callback_data=f"pay:ok:{invoice_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"pay:no:{invoice_id}"),
                ]])
                alert = (
                    f"💳 **NEW PAYMENT**\n\nInvoice: `{invoice_id}`\nUser: `{user_id}`\n"
                    f"Plan: **{payment['plan']}**\nDuration: **{payment['duration']}**\n"
                    f"Amount: **₹{payment['amount']}**\nUTR: `{payment['utr']}`"
                )
                for a in {int(r["user_id"]) for r in admins}:
                    try:
                        await bot.send_message(a, alert, reply_markup=markup)
                    except Exception:
                        pass
            return

        if step == "SUPPORT_MESSAGE":
            ticket_id = await create_ticket(user_id, str(state.get("category") or "general"), text)
            LOGIN_STATES.pop(user_id, None)
            await msg.reply_text(f"✅ Support ticket created: `{ticket_id}`\nOur team can track this ticket until resolution.")
            async with db_conn() as db:
                admins = await (await db.execute("SELECT user_id FROM admins WHERE role IN ('owner','manager','support')")).fetchall()
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Resolve", callback_data=f"ticket:close:{ticket_id}")]])
            for a in {int(r["user_id"]) for r in admins}:
                try:
                    await bot.send_message(a, f"🆘 **NEW TICKET** `{ticket_id}`\nUser: `{user_id}`\nCategory: **{state.get('category')}**\n\n{text[:2500]}", reply_markup=markup)
                except Exception:
                    pass
            return

    # Direct single/range link flow.
    if "t.me/" in text:
        block = await maintenance_blocked(user_id)
        if block:
            await msg.reply_text(block)
            return
        if not decrypt_text(user.get("encrypted_session")):
            await msg.reply_text("🔐 Connect your Telegram account first with `/login`.")
            return
        # Optional compact range syntax: https://t.me/channel/100-120
        rm = re.search(r"https?://t\.me/(?:c/)?([A-Za-z0-9_]+)/([0-9]+)-([0-9]+)", text)
        try:
            if rm:
                if user.get("plan") not in {"Standard", "Premium", "Ultimate"} and not is_admin_id(user_id):
                    await msg.reply_text("🔒 Range processing requires Standard or above.")
                    return
                start_id, end_id = sorted((int(rm.group(2)), int(rm.group(3))))
                if end_id - start_id + 1 > MAX_BATCH_SIZE:
                    await msg.reply_text(f"❌ Maximum range size is {MAX_BATCH_SIZE}.")
                    return
                job_id = await enqueue_job(user_id, "batch", rm.group(1), start_id, end_id)
                await msg.reply_text(f"✅ Range queued: `{job_id}`")
                return
            parsed = parse_tg_link(text)
            if parsed:
                job_id = await enqueue_job(user_id, "single", parsed[0], parsed[1], parsed[1])
                await msg.reply_text(f"✅ Save queued: `{job_id}`")
                return
        except Exception as exc:
            await msg.reply_text(f"❌ Could not queue job: {exc}")
            return

# ---------------------------------------------------------------------------
# SCHEDULED JOBS
# ---------------------------------------------------------------------------


async def subscription_reminder_job() -> None:
    now = now_dt()
    async with db_conn() as db:
        rows = await (await db.execute(
            "SELECT user_id,plan,plan_expires_at,notify_expiry FROM users WHERE plan NOT IN ('Free') AND plan_expires_at IS NOT NULL"
        )).fetchall()
    for row in rows:
        if not int(row["notify_expiry"] or 0):
            continue
        expiry = parse_dt(row["plan_expires_at"])
        if not expiry:
            continue
        delta = expiry - now
        reminder_type: Optional[str] = None
        if timedelta(hours=23) <= delta <= timedelta(hours=25):
            reminder_type = "1day"
        elif timedelta(days=2, hours=23) <= delta <= timedelta(days=3, hours=1):
            reminder_type = "3day"
        elif -timedelta(hours=2) <= delta <= timedelta(minutes=0):
            reminder_type = "expired"
        if not reminder_type:
            continue
        async with DB_LOCK:
            async with db_conn() as db:
                cur = await db.execute(
                    "SELECT 1 FROM subscription_reminders WHERE user_id=? AND expiry=? AND reminder_type=?",
                    (row["user_id"], row["plan_expires_at"], reminder_type),
                )
                if await cur.fetchone():
                    continue
                await db.execute(
                    "INSERT INTO subscription_reminders(user_id,expiry,reminder_type,sent_at) VALUES (?,?,?,?)",
                    (row["user_id"], row["plan_expires_at"], reminder_type, now_iso()),
                )
                await db.commit()
        try:
            if reminder_type == "expired":
                await expire_user_if_needed(int(row["user_id"]))
                await bot.send_message(int(row["user_id"]), "⏰ Your subscription has expired. Use `/plans` to renew.")
            else:
                days = 1 if reminder_type == "1day" else 3
                await bot.send_message(int(row["user_id"]), f"⏰ Your **{row['plan']}** plan expires in about {days} day(s). Use `/plans` to renew.")
        except Exception:
            pass


async def cleanup_temp_job() -> None:
    cutoff = time.time() - 24 * 3600
    for child in TEMP_DIR.glob("*"):
        try:
            if child.stat().st_mtime < cutoff:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
        except OSError:
            pass

# ---------------------------------------------------------------------------
# HEALTH SERVER
# ---------------------------------------------------------------------------


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/", "/health", "/healthz"}:
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps({
            "status": "ok",
            "app": APP_NAME,
            "uptime_seconds": int(time.time() - START_TS),
            "queue_size": JOB_QUEUE.qsize(),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def run_health_server() -> None:
    try:
        server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
        log.info("Health server listening on port %s", PORT)
        server.serve_forever()
    except Exception as exc:
        log.warning("Health server failed: %s", exc)

# ---------------------------------------------------------------------------
# BOOTSTRAP / SHUTDOWN
# ---------------------------------------------------------------------------


async def register_commands() -> None:
    commands = [
        BotCommand("start", "Home dashboard"),
        BotCommand("login", "Connect Telegram account"),
        BotCommand("logout", "Disconnect account"),
        BotCommand("help", "Quick user guide"),
        BotCommand("batch", "Process an authorized range"),
        BotCommand("transfer", "Owned-channel transfer (Ultimate)"),
        BotCommand("jobs", "My recent jobs"),
        BotCommand("cancel", "Cancel input or a job"),
        BotCommand("settings", "User settings"),
        BotCommand("plans", "Plans and pricing"),
        BotCommand("myplan", "Subscription status"),
        BotCommand("trial", "One-time 24h trial"),
        BotCommand("redeem", "Redeem promo code"),
        BotCommand("paymenthistory", "Payment history"),
        BotCommand("support", "Open support ticket"),
        BotCommand("admin", "Admin dashboard"),
    ]
    await bot.set_bot_commands(commands)


async def start_services() -> None:
    global SCHEDULER
    await init_db()
    await recover_jobs()

    workers = [asyncio.create_task(job_worker(i + 1), name=f"worker-{i+1}") for i in range(WORKER_COUNT)]

    SCHEDULER = AsyncIOScheduler(timezone=TZ)
    SCHEDULER.add_job(daily_reset_job, "cron", hour=0, minute=0, id="daily_reset", replace_existing=True)
    SCHEDULER.add_job(subscription_reminder_job, "cron", hour=9, minute=0, id="subscription_reminders", replace_existing=True)
    SCHEDULER.add_job(cleanup_temp_job, "interval", hours=6, id="temp_cleanup", replace_existing=True)
    SCHEDULER.start()

    await bot.start()
    me = await bot.get_me()
    log.info("Bot started: @%s (%s)", me.username, me.id)
    try:
        await register_commands()
    except Exception as exc:
        log.warning("Could not register commands: %s", exc)

    try:
        await idle()
    finally:
        SHUTDOWN_EVENT.set()
        if SCHEDULER:
            SCHEDULER.shutdown(wait=False)
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await bot.stop()


def main() -> None:
    if not API_ID or not API_HASH or not BOT_TOKEN or not OWNER_ID:
        print("FATAL: API_ID, API_HASH, BOT_TOKEN and OWNER_ID must be configured.")
        raise SystemExit(2)

    thread = threading.Thread(target=run_health_server, daemon=True, name="health-server")
    thread.start()

    try:
        # Run all async services on the exact loop that existed when `bot` was
        # created.  Do not use asyncio.run() here (it creates another loop), and
        # do not pass a coroutine to Client.run() because Kurigram 2.2.26's
        # public run() API is intended as `bot.run()` in this release.
        asyncio.set_event_loop(APP_LOOP)
        APP_LOOP.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    finally:
        if not APP_LOOP.is_closed():
            pending = asyncio.all_tasks(APP_LOOP)
            for task in pending:
                task.cancel()
            if pending:
                APP_LOOP.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            APP_LOOP.run_until_complete(APP_LOOP.shutdown_asyncgens())
            APP_LOOP.close()


if __name__ == "__main__":
    main()
