import aiosqlite
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

DB_PATH = "bot_database.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Users table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                plan TEXT DEFAULT 'free',
                trial_start DATETIME,
                trial_end DATETIME,
                premium_expiry DATETIME,
                total_downloads INTEGER DEFAULT 0,
                daily_downloads INTEGER DEFAULT 0,
                last_download_date DATE
            )
        """)
        # Clone progress table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS clone_progress (
                user_id INTEGER,
                chat_id TEXT,
                last_msg_id INTEGER,
                total_msgs INTEGER,
                status TEXT DEFAULT 'running',
                updated_at DATETIME,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        # Payments table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                order_id TEXT,
                amount INTEGER,
                status TEXT,
                created_at DATETIME
            )
        """)
        await db.commit()

# ---- User Functions ----
async def get_user(user_id: int) -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        if row:
            return dict(row)
        return None

async def create_user(user_id: int, username: str = ""):
    trial_end = datetime.now() + timedelta(days=config.TRIAL_DAYS)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, plan, trial_start, trial_end) VALUES (?, ?, 'free', ?, ?)",
            (user_id, username, datetime.now(), trial_end)
        )
        await db.commit()

async def update_user(user_id: int, data: dict):
    keys = ", ".join([f"{k} = ?" for k in data.keys()])
    values = list(data.values()) + [user_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE users SET {keys} WHERE user_id = ?", values)
        await db.commit()

async def increment_downloads(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET total_downloads = total_downloads + 1, daily_downloads = daily_downloads + 1, last_download_date = date('now') WHERE user_id = ?",
            (user_id,)
        )
        await db.commit()

# ---- Clone Progress ----
async def get_clone_progress(user_id: int, chat_id: str) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM clone_progress WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        row = await cur.fetchone()
        return dict(row) if row else None

async def save_clone_progress(user_id: int, chat_id: str, last_id: int, total: int, status: str = "running"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "REPLACE INTO clone_progress (user_id, chat_id, last_msg_id, total_msgs, status, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, chat_id, last_id, total, status, datetime.now())
        )
        await db.commit()
