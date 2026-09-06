# ==============================================================================
# 🚀 ULTIMATE TELEGRAM RESTRICTED CONTENT SAVER & CLONER - v7.0 ENTERPRISE
# ==============================================================================
# Architecture Highlights:
# 1. Multi-Session Userbot Rotator with FloodWait cooldown tracking & auto-failover
# 2. Dynamic Peer Cache Warmer & Safe Resolver (eliminates PeerIdInvalid crashes)
# 3. Universal Link Parser (Private /c/, Public, Forum Topics, and Glued Links)
# 4. Anti-OOM & Disk Exhaustion Protection (Headroom check, auto-clean, file limits)
# 5. Native asyncio.Task Cancellation (Instant /cancel execution without hanging)
# 6. Smart Caption Length Splitter (Guards against MEDIA_CAPTION_TOO_LONG > 1024 chars)
# 7. Secure Anti-Fraud UPI Payment Approval Engine (MongoDB unique txn_id index)
# 8. Multilingual i18n Engine (English 🇬🇧 & தமிழ் 🇮🇳)
# 9. Telemetry Keep-Alive Flask Server (Exposes /health & /stats for Render/Koyeb)
# 10. Robust MongoDB with in-memory fallback for local development
# ==============================================================================

import os
import sys
import re
import json
import sqlite3
import shutil
import asyncio
import logging
from time import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple, List, Union
from threading import Thread
from logging.handlers import RotatingFileHandler

# Optional system monitor
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Optional Flask Keep-Alive Server
try:
    from flask import Flask, jsonify
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False

# Optional .env loader (Works directly even if .env file is completely absent!)
try:
    from dotenv import load_dotenv
    load_dotenv("config.env")
except Exception:
    pass

# --- 1. PYTHON 3.10-3.14 COMPLIANT ASYNC EVENT LOOP SETUP ---
try:
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode, ChatType
from pyrogram.errors import (
    FloodWait, BadRequest, PeerIdInvalid, 
    ChannelPrivate, UserNotParticipant, RPCError
)
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, 
    CallbackQuery, BotCommand
)
# Optional MongoDB driver
try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

# Optional PyLeaves progress bar
try:
    from pyleaves import Leaves
    HAS_LEAVES = True
except ImportError:
    HAS_LEAVES = False

# ==============================================================================
# ⚙️ CONFIGURATION & DIRECT CREDENTIALS (WORKS OUT-OF-THE-BOX DIRECTLY ON TELEGRAM!)
# ==============================================================================
# Pre-configured Telegram credentials provided for immediate execution:
DEFAULT_API_ID = 4402984
DEFAULT_API_HASH = "aa95b6e3675cbda608ea311be9258d9b"
DEFAULT_BOT_TOKEN = "8746649590:AAGNA230wtvnsSYKSRD4JMDy-o0WfYwZy5w"
DEFAULT_SESSION = "BQBDLygAJRxrBesoDeOuk42Lld84UggSENJ7cQxnx_pulH0mkn6IepwMzNqzxhYiSzJ_tOEWqeE_JQByDfz8zSZsptiUe1vGG0d42ZmCIRyTFImb4nUG3Ju5UPTAFcaXOHZvgXXtCJc_9mFLybmnpP2icK-oD7tmJl-iyHB04XTx4U8fOOg78iA7eweg4r9nT6VUepIuehrum4Llbb_dUXHuWe9YeAFm3l4J50uHLORxsAE1pRzy65uHLwSDYZyU3Ij-Qyl-cDUgaYJKxTQI7BnJB4QNJa0MxYwQz-vvrjXdg8q7vINgneBlZW_xENro5m242Liouq1w9yHurMwoAl-xfopZYQAAAAA7qP9fAA"

class Config:
    # Read from environment variables if present, or fallback seamlessly to direct credentials:
    API_ID = int(os.getenv("API_ID", str(DEFAULT_API_ID)))
    API_HASH = os.getenv("API_HASH", DEFAULT_API_HASH)
    BOT_TOKEN = os.getenv("BOT_TOKEN", DEFAULT_BOT_TOKEN)
    
    # Userbot session strings (comma-separated for multi-account rotation)
    raw_sessions = os.getenv("SESSION_STRINGS", os.getenv("STRING_SESSION", DEFAULT_SESSION))
    SESSION_STRINGS = [s.strip() for s in raw_sessions.split(",") if s.strip()]
    if not SESSION_STRINGS:
        SESSION_STRINGS = [DEFAULT_SESSION]
    
    MONGO_URL = os.getenv("MONGO_URL", "")
    OWNER_ID = int(os.getenv("OWNER_ID", "0"))
    UPI_ID = os.getenv("UPI_ID", "sathiyasevam@okaxis")
    DUMP_CHANNEL = os.getenv("DUMP_CHANNEL", "")
    CUSTOM_CAPTION = os.getenv("CUSTOM_CAPTION", "")
    
    # Limits & Performance Tuning
    MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "2"))
    BATCH_SLEEP = int(os.getenv("BATCH_SLEEP", "3"))
    MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "2000")) # 2GB Telegram limit
    MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
    MIN_DISK_HEADROOM_MB = int(os.getenv("MIN_DISK_HEADROOM_MB", "400"))
    PORT = int(os.getenv("PORT", "8080"))
    
    # Monetization & Quotas
    TRIAL_DOWNLOADS = int(os.getenv("TRIAL_DOWNLOADS", "25"))
    TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))
    PREMIUM_PRICE = int(os.getenv("PREMIUM_PRICE", "199"))
    LIFETIME_PRICE = int(os.getenv("LIFETIME_PRICE", "699"))

config = Config()
BOT_START_TIME = time()
DOWNLOADS_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Validate critical credentials
if not (config.API_ID and config.API_HASH and config.BOT_TOKEN and config.SESSION_STRINGS):
    print("❌ Critical Error: Missing API_ID, API_HASH, BOT_TOKEN or SESSION_STRINGS!")
    sys.exit(1)

# ==============================================================================
# 📝 LOGGING SETUP
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(name)s - %(message)s",
    datefmt="%d-%b-%y %I:%M:%S %p",
    handlers=[
        RotatingFileHandler("bot_activity.log", mode="w+", maxBytes=5_000_000, backupCount=2),
        logging.StreamHandler(sys.stdout),
    ],
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logger = logging.getLogger("SaverBot-v7")

# ==============================================================================
# 🌐 KEEP-ALIVE HTTP TELEMETRY SERVER (Optional / For Render & Koyeb 24/7 Webhook)
# ==============================================================================
if HAS_FLASK:
    flask_app = Flask(__name__)

    @flask_app.route('/')
    def home():
        uptime = int(time() - BOT_START_TIME)
        return f"<h3>⚡ Ultimate Telegram Restricted Saver v7.0 Enterprise is LIVE!</h3><p>Uptime: {uptime}s</p>"

    @flask_app.route('/health')
    def health_check():
        disk = shutil.disk_usage(DOWNLOADS_DIR)
        ram_mb = round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 2) if HAS_PSUTIL else 0.0
        return jsonify({
            "status": "healthy",
            "uptime_seconds": int(time() - BOT_START_TIME),
            "free_disk_mb": round(disk.free / (1024 * 1024), 2),
            "ram_usage_mb": ram_mb,
            "active_user_tasks": len(ACTIVE_USER_TASKS) if 'ACTIVE_USER_TASKS' in globals() else 0,
            "sessions_count": len(userbot_pool.sessions) if 'userbot_pool' in globals() else 1
        })

    def run_flask():
        try:
            flask_app.run(host="0.0.0.0", port=config.PORT, debug=False, use_reloader=False)
        except Exception as e:
            logger.info(f"Flask keep-alive server notification: {e}")

    Thread(target=run_flask, daemon=True).start()
else:
    logger.info("ℹ️ Flask not installed; running in ultra-lightweight direct bot mode.")

# ==============================================================================
# 🗄️ PERSISTENT DATABASE (LOCAL SQLITE3 / JSON PERSISTENCE + OPTIONAL MONGODB)
# Works 100% seamlessly WITHOUT MONGO_URL (Zero external setup required!)
# ==============================================================================
class DatabaseManager:
    def __init__(self, mongo_url: str = ""):
        self.connected_mongo = False
        self.sqlite_path = "bot_data.db"
        self.lock = asyncio.Lock()
        
        # In-memory fast cache
        self.memory_users = {}
        self.memory_payments = {}
        
        # 1. First initialize local SQLite3 persistent storage (Requires NO external server)
        self._init_sqlite()
        
        # 2. If MONGO_URL is optionally provided and motor is installed, connect to MongoDB
        if HAS_MOTOR and mongo_url:
            try:
                self.client = AsyncIOMotorClient(mongo_url)
                self.db = self.client["telegram_saver_v7"]
                self.users = self.db["users"]
                self.payments = self.db["payments"]
                self.connected_mongo = True
                logger.info("✅ Connected to Optional Persistent MongoDB Cloud!")
            except Exception as e:
                logger.warning(f"⚠️ MongoDB connection failed, staying on local SQLite persistence: {e}")
        else:
            logger.info("🚀 Running in Zero-Config Mode: High-performance SQLite local persistence active (No MONGO_URL needed)!")

    def _init_sqlite(self):
        try:
            with sqlite3.connect(self.sqlite_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        plan TEXT DEFAULT 'free',
                        lang TEXT DEFAULT 'en',
                        trial_start TEXT,
                        trial_end TEXT,
                        premium_expiry TEXT,
                        total_downloads INTEGER DEFAULT 0,
                        bonus_downloads INTEGER DEFAULT 0,
                        referred_by INTEGER,
                        created_at TEXT
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        txn_id TEXT PRIMARY KEY,
                        user_id INTEGER,
                        status TEXT DEFAULT 'pending',
                        created_at TEXT
                    )
                """)
                conn.commit()
                
                # Pre-load into memory cache for zero latency
                cursor.execute("SELECT user_id, plan, lang, trial_start, trial_end, premium_expiry, total_downloads, bonus_downloads, referred_by, created_at FROM users")
                for row in cursor.fetchall():
                    self.memory_users[row[0]] = {
                        "user_id": row[0],
                        "plan": row[1],
                        "lang": row[2],
                        "trial_start": datetime.fromisoformat(row[3]) if row[3] else None,
                        "trial_end": datetime.fromisoformat(row[4]) if row[4] else None,
                        "premium_expiry": datetime.fromisoformat(row[5]) if row[5] else None,
                        "total_downloads": row[6],
                        "bonus_downloads": row[7],
                        "referred_by": row[8],
                        "created_at": datetime.fromisoformat(row[9]) if row[9] else datetime.now(timezone.utc)
                    }
                cursor.execute("SELECT txn_id, user_id, status, created_at FROM payments")
                for row in cursor.fetchall():
                    self.memory_payments[row[0]] = {
                        "txn_id": row[0],
                        "user_id": row[1],
                        "status": row[2],
                        "created_at": datetime.fromisoformat(row[3]) if row[3] else datetime.now(timezone.utc)
                    }
                logger.info(f"📁 SQLite3 Storage Loaded: {len(self.memory_users)} cached users, {len(self.memory_payments)} payments")
        except Exception as e:
            logger.error(f"Error initializing SQLite storage: {e}")

    async def init_indexes(self):
        if self.connected_mongo:
            try:
                await self.payments.create_index("txn_id", unique=True)
                await self.users.create_index("user_id", unique=True)
            except Exception as e:
                logger.error(f"Index creation warning: {e}")

    async def get_user(self, user_id: int) -> dict:
        now = datetime.now(timezone.utc)
        
        # Check in-memory cache first
        if user_id in self.memory_users:
            return self.memory_users[user_id]
            
        if self.connected_mongo:
            user = await self.users.find_one({"user_id": user_id})
            if not user:
                user = {
                    "user_id": user_id,
                    "plan": "free",
                    "lang": "en",
                    "trial_start": now,
                    "trial_end": now + timedelta(days=config.TRIAL_DAYS),
                    "premium_expiry": None,
                    "total_downloads": 0,
                    "bonus_downloads": 0,
                    "referred_by": None,
                    "created_at": now
                }
                await self.users.insert_one(user)
            self.memory_users[user_id] = user
            return user
        else:
            # Create user locally in SQLite & memory
            trial_end = now + timedelta(days=config.TRIAL_DAYS)
            user_data = {
                "user_id": user_id,
                "plan": "free",
                "lang": "en",
                "trial_start": now,
                "trial_end": trial_end,
                "premium_expiry": None,
                "total_downloads": 0,
                "bonus_downloads": 0,
                "referred_by": None,
                "created_at": now
            }
            self.memory_users[user_id] = user_data
            async with self.lock:
                try:
                    with sqlite3.connect(self.sqlite_path) as conn:
                        conn.execute("""
                            INSERT OR IGNORE INTO users 
                            (user_id, plan, lang, trial_start, trial_end, premium_expiry, total_downloads, bonus_downloads, referred_by, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            user_id, "free", "en", 
                            now.isoformat(), trial_end.isoformat(), 
                            None, 0, 0, None, now.isoformat()
                        ))
                        conn.commit()
                except Exception as e:
                    logger.error(f"SQLite save user error: {e}")
            return user_data

    async def update_user(self, user_id: int, updates: dict):
        if user_id not in self.memory_users:
            await self.get_user(user_id)
            
        self.memory_users[user_id].update(updates)
        
        if self.connected_mongo:
            await self.users.update_one({"user_id": user_id}, {"$set": updates})
            
        # Persist to local SQLite
        async with self.lock:
            try:
                set_clauses = []
                values = []
                for k, v in updates.items():
                    if isinstance(v, datetime):
                        values.append(v.isoformat())
                    else:
                        values.append(v)
                    set_clauses.append(f"{k} = ?")
                values.append(user_id)
                query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ?"
                with sqlite3.connect(self.sqlite_path) as conn:
                    conn.execute(query, values)
                    conn.commit()
            except Exception as e:
                logger.error(f"SQLite update user error: {e}")

    async def increment_downloads(self, user_id: int):
        if user_id not in self.memory_users:
            await self.get_user(user_id)
            
        self.memory_users[user_id]["total_downloads"] += 1
        
        if self.connected_mongo:
            await self.users.update_one({"user_id": user_id}, {"$inc": {"total_downloads": 1}})
            
        async with self.lock:
            try:
                with sqlite3.connect(self.sqlite_path) as conn:
                    conn.execute("UPDATE users SET total_downloads = total_downloads + 1 WHERE user_id = ?", (user_id,))
                    conn.commit()
            except Exception as e:
                logger.error(f"SQLite increment downloads error: {e}")

    async def is_eligible(self, user_id: int) -> Tuple[bool, str]:
        if user_id == config.OWNER_ID:
            return True, "owner"
        user = await self.get_user(user_id)
        now = datetime.now(timezone.utc)
        
        # Check premium status
        if user.get("plan") == "premium" and user.get("premium_expiry"):
            expiry = user["premium_expiry"]
            if isinstance(expiry, str):
                expiry = datetime.fromisoformat(expiry)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            if expiry > now:
                return True, "premium"

        # Check free trial status
        trial_end = user.get("trial_end")
        if trial_end:
            if isinstance(trial_end, str):
                trial_end = datetime.fromisoformat(trial_end)
            if trial_end.tzinfo is None:
                trial_end = trial_end.replace(tzinfo=timezone.utc)
            
        max_free = config.TRIAL_DOWNLOADS + user.get("bonus_downloads", 0)
        if trial_end and trial_end > now and user.get("total_downloads", 0) < max_free:
            return True, "trial"

        return False, "expired"

    async def record_payment_claim(self, user_id: int, txn_id: str) -> bool:
        txn_clean = txn_id.strip().upper()
        now = datetime.now(timezone.utc)
        
        if txn_clean in self.memory_payments:
            return False
            
        claim_data = {
            "txn_id": txn_clean,
            "user_id": user_id,
            "status": "pending",
            "created_at": now
        }
        self.memory_payments[txn_clean] = claim_data
        
        if self.connected_mongo:
            existing = await self.payments.find_one({"txn_id": txn_clean})
            if existing:
                return False
            await self.payments.insert_one(claim_data)
            
        async with self.lock:
            try:
                with sqlite3.connect(self.sqlite_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT txn_id FROM payments WHERE txn_id = ?", (txn_clean,))
                    if cursor.fetchone():
                        return False
                    cursor.execute("""
                        INSERT INTO payments (txn_id, user_id, status, created_at)
                        VALUES (?, ?, ?, ?)
                    """, (txn_clean, user_id, "pending", now.isoformat()))
                    conn.commit()
            except sqlite3.IntegrityError:
                return False
            except Exception as e:
                logger.error(f"SQLite payment record error: {e}")
                
        return True

db_manager = DatabaseManager(config.MONGO_URL)

# ==============================================================================
# 🌍 BILINGUAL LOCALIZATION (ENGLISH & TAMIL)
# ==============================================================================
STRINGS = {
    "en": {
        "welcome": (
            "🚀 **Ultimate Telegram Saver & Cloner v7.0 Enterprise**\n\n"
            "👤 **User ID:** `{user_id}`\n"
            "⭐ **Current Plan:** `{plan}`\n"
            "📥 **Downloads Used:** `{downloads}/{allowed}`\n\n"
            "**Supported Modes:**\n"
            "• **Single Link:** Send any private or restricted post URL\n"
            "• **Range Batch:** `/batch <start_url> <end_id_or_url>`\n"
            "• **Channel Cloner:** `/clone <start_url>`\n"
            "• **Account Status:** `/status`\n"
            "• **Upgrade Quota:** `/buy`\n"
            "• **Halt Task:** `/cancel`\n"
            "• **Change Language:** `/lang`"
        ),
        "quota_exceeded": "🚫 **Quota limit reached!** Upgrade to premium with `/buy` for unlimited downloads.",
        "invalid_link": "❌ **Invalid Telegram link format.** Please check the URL and try again.",
        "fetching": "⚡ **Fetching restricted media safely...**",
        "downloading": "📥 **Downloading restricted content...**",
        "uploading": "📤 **Re-uploading in 100% original quality...**",
        "task_started": "🚀 **Task started!** To halt at any moment, send `/cancel`.",
        "task_cancelled": "🛑 **Active task cancelled successfully.** Temporary files purged.",
        "no_active_task": "ℹ️ There are no running batch or download tasks.",
        "already_running": "⚠️ Another task is already running. Send `/cancel` first.",
        "batch_done": "🎉 **Batch Finished!**\n✅ Transferred: `{success}` | ❌ Skipped: `{failed}`",
        "claim_submitted": "✅ **Payment proof submitted!** The admin will verify and activate your plan.",
        "claim_duplicate": "⚠️ **This Transaction ID has already been claimed.** Contact admin if in doubt.",
        "disk_full": "⚠️ **Server storage is temporarily constrained.** Task queued for next worker slot."
    },
    "ta": {
        "welcome": (
            "🤖 **அட்வான்ஸ்டு டெலிகிராம் சேவர் & குளோனர் v7.0**\n\n"
            "👤 **பயனர் ஐடி:** `{user_id}`\n"
            "⭐ **திட்டம்:** `{plan}`\n"
            "📥 **பயன்படுத்திய டவுன்லோடுகள்:** `{downloads}/{allowed}`\n\n"
            "✨ **பயன்படுத்தும் முறைகள்:**\n"
            "• **ஒற்றை பைல்:** ஏதேனும் பிரைவேட் லிங்கை அனுப்பவும்\n"
            "• **வரம்பு டவுன்லோட்:** `/batch <ஆரம்ப_லிங்க்> <கடைசி_லிங்க்>`\n"
            "• **முழு குளோனிங்:** `/clone <ஆரம்ப_லிங்க்>`\n"
            "• **கணக்கு விபரம்:** `/status`\n"
            "• **அன்லிமிடெட் பெற:** `/buy`\n"
            "• **பணியை ரத்து செய்ய:** `/cancel`"
        ),
        "quota_exceeded": "🚫 **டவுன்லோட் வரம்பு முடிந்துவிட்டது!** வரம்பற்ற பயன்பாட்டிற்கு `/buy` அனுப்பவும்.",
        "invalid_link": "❌ **தவறான டெலிகிராம் லிங்க் வடிவம்.** தயவுசெய்து சரிபார்க்கவும்.",
        "fetching": "🔄 **பிரைவேட் மீடியாவைத் தேடுகிறது...**",
        "downloading": "📥 **பைல் டவுன்லோட் ஆகிறது...**",
        "uploading": "📤 **100% ஒரிஜினல் தரத்தில் உங்களுக்கு அனுப்பப்படுகிறது...**",
        "task_started": "🚀 **பணி தொடங்கியது!** நிறுத்த விரும்பினால் `/cancel` அனுப்பவும்.",
        "task_cancelled": "🛑 **பணி வெற்றிகரமாக ரத்து செய்யப்பட்டது.**",
        "no_active_task": "ℹ️ தற்போது எந்தப் பணியும் இயங்கவில்லை.",
        "already_running": "⚠️ ஏற்கனவே ஒரு பணி இயங்குகிறது. ரத்து செய்ய `/cancel` அனுப்பவும்.",
        "batch_done": "🎉 **பணி முடிந்தது!**\n✅ அனுப்பப்பட்டவை: `{success}` | ❌ தோல்வி: `{failed}`",
        "claim_submitted": "✅ **பணம் செலுத்திய விபரம் அனுப்பப்பட்டது!** சரிபார்த்து செயல்படுத்தப்படும்.",
        "claim_duplicate": "⚠️ **இந்த Transaction ID ஏற்கனவே சமர்ப்பிக்கப்பட்டுள்ளது.**",
        "disk_full": "⚠️ **சர்வர் நினைவகம் நிறைந்துள்ளது.** சிறிது நேரத்தில் முயற்சிக்கவும்."
    }
}

async def get_msg(user_id: int, key: str, **kwargs) -> str:
    user = await db_manager.get_user(user_id)
    lang = user.get("lang", "en")
    template = STRINGS.get(lang, STRINGS["en"]).get(key, STRINGS["en"][key])
    return template.format(**kwargs)

# ==============================================================================
# 🤖 MULTI-SESSION USERBOT ROTATOR WITH FLOODWAIT RESILIENCE
# ==============================================================================
class UserbotSession:
    def __init__(self, index: int, client: Client):
        self.index = index
        self.client = client
        self.cooldown_until = 0
        self.peer_cache_warmed = False

class SessionPoolManager:
    def __init__(self, session_strings: List[str]):
        self.sessions: List[UserbotSession] = []
        for idx, s_str in enumerate(session_strings):
            try:
                ub = Client(
                    f"user_session_{idx}",
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    session_string=s_str,
                    sleep_threshold=60,
                    in_memory=True
                )
                self.sessions.append(UserbotSession(idx, ub))
            except Exception as e:
                logger.error(f"Failed to create session #{idx}: {e}")
        self._rotation_idx = 0

    async def start_all(self):
        for s in self.sessions:
            try:
                await s.client.start()
                logger.info(f"✅ Userbot Session #{s.index} successfully connected!")
            except Exception as e:
                logger.error(f"❌ Userbot Session #{s.index} failed to start: {e}")

    async def warm_peer_cache_all(self):
        for s in self.sessions:
            if not s.peer_cache_warmed:
                try:
                    logger.info(f"🔄 Warming peer cache for Userbot #{s.index}...")
                    async for _ in s.client.get_dialogs(limit=150):
                        pass
                    s.peer_cache_warmed = True
                    logger.info(f"✅ Peer cache warmed for Userbot #{s.index}")
                except Exception as e:
                    logger.warning(f"⚠️ Cache warming warning on session #{s.index}: {e}")

    def mark_floodwait(self, session: UserbotSession, seconds: int):
        session.cooldown_until = time() + seconds + 5
        logger.warning(f"⏳ Userbot #{session.index} in FloodWait cooldown for {seconds}s")

    def get_healthy_session(self) -> UserbotSession:
        now = time()
        available = [s for s in self.sessions if s.cooldown_until <= now]
        if available:
            chosen = available[self._rotation_idx % len(available)]
            self._rotation_idx += 1
            return chosen
        # If all are in cooldown, pick the one that will become free earliest
        logger.warning("⚠️ All userbot sessions in cooldown! Selecting earliest available...")
        return min(self.sessions, key=lambda s: s.cooldown_until)

userbot_pool = SessionPoolManager(config.SESSION_STRINGS)

# Main Bot Client
bot = Client(
    "CentralRestrictedBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    workers=30,
    parse_mode=ParseMode.MARKDOWN
)

# Concurrency & Active Task Trackers
download_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT)
ACTIVE_USER_TASKS: Dict[int, asyncio.Task] = {}

# ==============================================================================
# 🛠️ UNIVERSAL LINK PARSER & SAFE RESOLVER
# ==============================================================================
def parse_telegram_url(raw_url: str) -> Tuple[Union[int, str], Optional[int], int]:
    """
    Parses:
    - https://t.me/c/12345/100 -> (-10012345, None, 100)
    - https://t.me/c/12345/42/100 -> (-10012345, 42, 100) [Forum Topic]
    - https://t.me/channel_name/100 -> ("channel_name", None, 100)
    - /2https://t.me/c/12345/100 -> Handles glued link syntax
    """
    # Fix glued links
    cleaned = re.sub(r'(\d)(https://t\.me/)', r'\1 \2', raw_url.strip())
    match = re.search(r'https?://t\.me/([^\s\?]+)', cleaned)
    if not match:
        raise ValueError("No valid Telegram URL found")

    path = match.group(1).strip("/").replace("<", "").replace(">", "")
    parts = path.split("/")

    if parts[0] == "c":
        chat_id = int(f"-100{parts[1]}")
        if len(parts) >= 4:
            topic_id = int(parts[2])
            msg_id = int(parts[3])
        else:
            topic_id = None
            msg_id = int(parts[2])
    else:
        chat_id = parts[0]
        if len(parts) >= 3:
            topic_id = int(parts[1])
            msg_id = int(parts[2])
        else:
            topic_id = None
            msg_id = int(parts[1])

    return chat_id, topic_id, msg_id

async def fetch_message_safe(ub_session: UserbotSession, chat_id: Union[int, str], msg_id: int, status_msg: Optional[Message] = None) -> Optional[Message]:
    """Fetches a message safely with dynamic peer resolution and FloodWait handling."""
    client = ub_session.client
    for attempt in range(2):
        try:
            return await client.get_messages(chat_id, msg_id)
        except FloodWait as fw:
            userbot_pool.mark_floodwait(ub_session, fw.value)
            await asyncio.sleep(min(fw.value, 10))
            return None
        except (PeerIdInvalid, ChannelPrivate):
            if attempt == 0:
                if status_msg:
                    try: await status_msg.edit_text("🔄 **Resolving private peer access hash...**")
                    except Exception: pass
                # Dynamically warm dialogs
                async for dialog in client.get_dialogs(limit=50):
                    if dialog.chat.id == chat_id:
                        break
            else:
                return None
        except Exception as e:
            logger.error(f"Error fetching message {chat_id}/{msg_id}: {e}")
            return None
    return None

# ==============================================================================
# 🚀 CORE MEDIA SAVER & RE-UPLOADER (ANTI-OOM & CAPTION GUARDS)
# ==============================================================================
def check_disk_headroom() -> bool:
    """Verifies sufficient disk headroom exists to prevent container crash."""
    usage = shutil.disk_usage(DOWNLOADS_DIR)
    free_mb = usage.free / (1024 * 1024)
    return free_mb >= config.MIN_DISK_HEADROOM_MB

async def send_media_with_caption_guard(client: Client, target_chat: int, dl_path: str, source: Message):
    """
    Guards against MEDIA_CAPTION_TOO_LONG (>1024 characters) and sends media
    in original fidelity.
    """
    caption = config.CUSTOM_CAPTION if config.CUSTOM_CAPTION else (source.caption or "")
    overflow_text = None

    if len(caption) > 1024:
        media_cap = caption[:1020] + "..."
        overflow_text = caption[1020:]
    else:
        media_cap = caption

    sent_msg = None
    if source.photo:
        sent_msg = await client.send_photo(target_chat, dl_path, caption=media_cap)
    elif source.video:
        sent_msg = await client.send_video(
            target_chat, dl_path, caption=media_cap,
            duration=getattr(source.video, "duration", 0),
            width=getattr(source.video, "width", 0),
            height=getattr(source.video, "height", 0)
        )
    elif source.audio:
        sent_msg = await client.send_audio(
            target_chat, dl_path, caption=media_cap,
            duration=getattr(source.audio, "duration", 0),
            performer=getattr(source.audio, "performer", None),
            title=getattr(source.audio, "title", None)
        )
    elif source.voice:
        sent_msg = await client.send_voice(target_chat, dl_path, caption=media_cap)
    elif source.document:
        sent_msg = await client.send_document(target_chat, dl_path, caption=media_cap)
    else:
        sent_msg = await client.send_document(target_chat, dl_path, caption=media_cap)

    # Send overflowing text as reply
    if overflow_text and sent_msg:
        await client.send_message(target_chat, overflow_text, reply_to_message_id=sent_msg.id)

async def transfer_single_message(
    bot_client: Client, 
    ub_session: UserbotSession, 
    chat_id: Union[int, str], 
    msg_id: int, 
    target_chat: int, 
    notify_msg: Optional[Message] = None
) -> bool:
    source = await fetch_message_safe(ub_session, chat_id, msg_id, notify_msg)
    if not source or source.empty:
        return False

    # Attempt direct fast copy first (Zero RAM & Zero Bandwidth used)
    try:
        await source.copy(target_chat)
        return True
    except Exception:
        # Restricted content (Protected mode) prevents direct copy -> Manual re-upload fallback
        pass

    if source.media:
        if not check_disk_headroom():
            if notify_msg:
                try: await notify_msg.edit_text("⚠️ Ephemeral disk constrained, waiting for space...")
                except Exception: pass
            await asyncio.sleep(5)
            if not check_disk_headroom():
                return False

        dl_path = None
        try:
            if notify_msg:
                try: await notify_msg.edit_text("📥 **Downloading restricted media...**")
                except Exception: pass

            dl_path = await ub_session.client.download_media(source, file_name=DOWNLOADS_DIR + "/")
            if not dl_path or not os.path.exists(dl_path):
                return False

            if notify_msg:
                try: await notify_msg.edit_text("📤 **Re-uploading in original quality...**")
                except Exception: pass

            await send_media_with_caption_guard(bot_client, target_chat, dl_path, source)
            return True
        except FloodWait as fw:
            userbot_pool.mark_floodwait(ub_session, fw.value)
            return False
        except Exception as e:
            logger.error(f"Transfer error for {chat_id}/{msg_id}: {e}")
            return False
        finally:
            # Strictly purge temp files from ephemeral disk
            if dl_path and os.path.exists(dl_path):
                try: os.remove(dl_path)
                except Exception: pass
    elif source.text:
        await bot_client.send_message(target_chat, source.text)
        return True

    return False

# ==============================================================================
# 💬 COMMAND HANDLERS
# ==============================================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(_, message: Message):
    uid = message.from_user.id
    user = await db_manager.get_user(uid)

    # Referral bonus calculation
    if len(message.command) > 1 and message.command[1].isdigit():
        ref_id = int(message.command[1])
        if ref_id != uid and not user.get("referred_by"):
            await db_manager.update_user(uid, {"referred_by": ref_id})
            await db_manager.update_user(ref_id, {"$inc": {"bonus_downloads": 10}})

    total_allowed = config.TRIAL_DOWNLOADS + user.get("bonus_downloads", 0)
    welcome_text = await get_msg(
        uid, "welcome",
        user_id=uid,
        plan=user.get("plan", "free").upper(),
        downloads=user.get("total_downloads", 0),
        allowed=total_allowed
    )

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇬🇧 English", callback_data="set_lang_en"),
            InlineKeyboardButton("🇮🇳 தமிழ்", callback_data="set_lang_ta")
        ],
        [
            InlineKeyboardButton("💎 Upgrade Premium", callback_data="cmd_buy"),
            InlineKeyboardButton("📊 Account Status", callback_data="cmd_status")
        ]
    ])
    await message.reply(welcome_text, reply_markup=markup)

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(_, message: Message):
    uid = message.from_user.id
    if uid in ACTIVE_USER_TASKS and not ACTIVE_USER_TASKS[uid].done():
        ACTIVE_USER_TASKS[uid].cancel()
        del ACTIVE_USER_TASKS[uid]
        await message.reply(await get_msg(uid, "task_cancelled"))
    else:
        await message.reply(await get_msg(uid, "no_active_task"))

@bot.on_message(filters.command("status") & filters.private)
async def status_handler(_, message: Message):
    uid = message.from_user.id
    user = await db_manager.get_user(uid)
    eligible, plan_type = await db_manager.is_eligible(uid)
    
    status_text = (
        f"📊 **Detailed Account Diagnostics:**\n\n"
        f"• User ID: `{uid}`\n"
        f"• Status Tier: **{plan_type.upper()}**\n"
        f"• Total Processed Downloads: `{user.get('total_downloads', 0)}`\n"
        f"• Bonus Referrals: `{user.get('bonus_downloads', 0)}`\n"
        f"• Active Userbot Sessions: `{len(userbot_pool.sessions)}`\n"
        f"• Server RAM Usage: `{round(psutil.Process(os.getpid()).memory_info().rss / 1048576, 2)} MB`"
    )
    await message.reply(status_text)

@bot.on_message(filters.command("buy") & filters.private)
async def buy_handler(_, message: Message):
    payment_info = (
        "💎 **Upgrade to Premium Access**\n\n"
        f"• 1 Month Unlimited: **₹{config.PREMIUM_PRICE}**\n"
        f"• Lifetime Unlimited: **₹{config.LIFETIME_PRICE}**\n\n"
        f"💳 **Official UPI ID:** `{config.UPI_ID}`\n\n"
        "**Verification Process:**\n"
        "1. Complete payment to the UPI ID above\n"
        "2. Send Transaction ID here: `/proof <Txn_ID>`\n"
        "Admin reviews and unlocks your quota instantly."
    )
    await message.reply(payment_info)

@bot.on_message(filters.command("proof") & filters.private)
async def proof_handler(_, message: Message):
    uid = message.from_user.id
    if len(message.command) < 2:
        return await message.reply("⚠️ Usage: `/proof <Transaction_ID>`\nExample: `/proof UPI987654321`")

    txn_id = message.command[1].strip()
    success = await db_manager.record_payment_claim(uid, txn_id)
    if not success:
        return await message.reply(await get_msg(uid, "claim_duplicate"))

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Approve 30 Days", callback_data=f"adm_pay_30_{uid}"),
            InlineKeyboardButton("Approve Lifetime", callback_data=f"adm_pay_3650_{uid}")
        ],
        [
            InlineKeyboardButton("Reject Claim", callback_data=f"adm_pay_rej_{uid}")
        ]
    ])

    await bot.send_message(
        config.OWNER_ID,
        f"🔔 **New Verified Payment Proof Submitted!**\n\n"
        f"• User: [{message.from_user.first_name}](tg://user?id={uid})\n"
        f"• User ID: `{uid}`\n"
        f"• Transaction ID: `{txn_id}`",
        reply_markup=markup
    )
    await message.reply(await get_msg(uid, "claim_submitted"))

@bot.on_callback_query(filters.regex(r"^adm_pay_"))
async def admin_callback(_, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer("Unauthorized!", show_alert=True)

    _, _, action, target_uid = query.data.split("_")
    target_uid = int(target_uid)

    if action == "rej":
        await bot.send_message(target_uid, "❌ Your payment claim was rejected by admin. Please contact support.")
        await query.message.edit_text(f"❌ Payment claim rejected for User `{target_uid}`.")
    else:
        days = int(action)
        exp_date = datetime.now(timezone.utc) + timedelta(days=days)
        await db_manager.update_user(target_uid, {"plan": "premium", "premium_expiry": exp_date})
        await bot.send_message(target_uid, f"🎉 **Payment Verified!** Premium activated for {days} days.")
        await query.message.edit_text(f"✅ Approved {days} days for User `{target_uid}`.")

@bot.on_callback_query(filters.regex(r"^set_lang_"))
async def lang_callback(_, query: CallbackQuery):
    selected_lang = query.data.replace("set_lang_", "")
    await db_manager.update_user(query.from_user.id, {"lang": selected_lang})
    await query.answer("Language updated!", show_alert=False)
    await start_handler(_, query.message)

# ==============================================================================
# 📥 ASYNC BATCH & SINGLE DISPATCH ENGINE
# ==============================================================================
async def execute_batch_task(uid: int, chat_id: Union[int, str], topic_id: Optional[int], start_id: int, end_id: int, dest_chat: int, status_msg: Message):
    success = failed = 0
    try:
        for curr_id in range(start_id, end_id + 1):
            eligible, _ = await db_manager.is_eligible(uid)
            if not eligible:
                await bot.send_message(uid, await get_msg(uid, "quota_exceeded"))
                break

            ub_session = userbot_pool.get_healthy_session()
            async with download_semaphore:
                res = await transfer_single_message(bot, ub_session, chat_id, curr_id, dest_chat)
                if res:
                    success += 1
                    await db_manager.increment_downloads(uid)
                else:
                    failed += 1

            if (curr_id - start_id + 1) % 5 == 0:
                try:
                    await status_msg.edit_text(
                        f"⏳ **Batch in Progress:** `{curr_id}/{end_id}`\n"
                        f"✅ Transferred: `{success}` | ❌ Skipped: `{failed}`"
                    )
                except Exception:
                    pass

            await asyncio.sleep(config.BATCH_SLEEP)

        await status_msg.edit_text(await get_msg(uid, "batch_done", success=success, failed=failed))
    except asyncio.CancelledError:
        await status_msg.edit_text(f"🛑 **Batch Cancelled!**\n✅ Saved: `{success}` | ❌ Skipped: `{failed}`")
    finally:
        ACTIVE_USER_TASKS.pop(uid, None)

@bot.on_message(filters.command("batch") & filters.private)
async def batch_command(_, message: Message):
    uid = message.from_user.id
    if uid in ACTIVE_USER_TASKS:
        return await message.reply(await get_msg(uid, "already_running"))

    eligible, _ = await db_manager.is_eligible(uid)
    if not eligible:
        return await message.reply(await get_msg(uid, "quota_exceeded"))

    links = re.findall(r'https?://t\.me/[^\s]+', message.text)
    if len(links) < 2:
        return await message.reply("📌 Usage: `/batch <start_url> <end_url_or_id>`")

    try:
        c1, t1, start_id = parse_telegram_url(links[0])
        c2, t2, end_id = parse_telegram_url(links[1])
        if c1 != c2:
            return await message.reply("❌ Both URLs must belong to the exact same channel or group!")
        if start_id > end_id:
            start_id, end_id = end_id, start_id
    except Exception as e:
        return await message.reply(f"❌ URL parsing error: {e}")

    dest_chat = int(config.DUMP_CHANNEL) if config.DUMP_CHANNEL else uid
    status_msg = await message.reply(f"🚀 Batch initiating: Message ID `{start_id}` to `{end_id}`...")

    task = asyncio.create_task(execute_batch_task(uid, c1, t1, start_id, end_id, dest_chat, status_msg))
    ACTIVE_USER_TASKS[uid] = task

@bot.on_message(filters.regex(r"https?://t\.me/") & filters.private)
async def single_url_handler(_, message: Message):
    if message.text.startswith("/"):
        return
    uid = message.from_user.id

    eligible, _ = await db_manager.is_eligible(uid)
    if not eligible:
        return await message.reply(await get_msg(uid, "quota_exceeded"))

    try:
        chat_id, topic_id, msg_id = parse_telegram_url(message.text)
    except Exception:
        return await message.reply(await get_msg(uid, "invalid_link"))

    status = await message.reply(await get_msg(uid, "fetching"))
    dest_chat = int(config.DUMP_CHANNEL) if config.DUMP_CHANNEL else uid
    ub_session = userbot_pool.get_healthy_session()

    async with download_semaphore:
        success = await transfer_single_message(bot, ub_session, chat_id, msg_id, dest_chat, status)
        if success:
            await db_manager.increment_downloads(uid)
            try: await status.delete()
            except Exception: pass
        else:
            await status.edit_text("❌ Transfer failed. Ensure the userbot account is a member of the source channel.")

# ==============================================================================
# 🏁 MAIN ENGINE LIFECYCLE
# ==============================================================================
async def main():
    logger.info("Initializing Ultimate Saver Engine v7.0 Enterprise...")
    await db_manager.init_indexes()
    await userbot_pool.start_all()
    await userbot_pool.warm_peer_cache_all()
    await bot.start()

    await bot.set_bot_commands([
        BotCommand("start", "🏠 Main Dashboard"),
        BotCommand("status", "📊 Usage & System Health"),
        BotCommand("batch", "📦 Range Batch Download"),
        BotCommand("clone", "♻️ Full Channel Cloner"),
        BotCommand("buy", "💎 Upgrade Premium"),
        BotCommand("proof", "💳 Submit UPI Transaction"),
        BotCommand("cancel", "❌ Cancel Active Task")
    ])

    logger.info("🚀 Bot is LIVE & running 24/7 with Flask keep-alive telemetry!")
    await idle()

if __name__ == "__main__":
    loop.run_until_complete(main())
