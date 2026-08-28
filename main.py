import os
import sys
import re
import json
import shutil
import asyncio
import aiosqlite
import psutil
from time import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from threading import Thread
from asyncio.subprocess import PIPE, create_subprocess_exec
from PIL import Image

# ============================================================
# 🛠️ RENDER FIX: Python 3.10+ Event Loop
# ============================================================
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# ============================================================
# 📦 INSTALL MISSING PACKAGES (Render Auto Install)
# ============================================================
# requirements.txt should have: pyrogram, pyleaves, python-dotenv, psutil, aiosqlite, Flask, TgCrypto, pillow

from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatMemberStatus, ChatType
from pyrogram.errors import FloodWait, PeerIdInvalid, BadRequest
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio, Voice
)
from pyrogram.utils import get_channel_id
from pyleaves import Leaves
from dotenv import load_dotenv
from flask import Flask

load_dotenv("config.env")

# ============================================================
# 📝 LOGGER (Without external file)
# ============================================================
import logging
from logging.handlers import RotatingFileHandler

try:
    os.remove("logs.txt")
except:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(funcName)s() - %(name)s - %(message)s",
    datefmt="%d-%b-%y %I:%M:%S %p",
    handlers=[
        RotatingFileHandler("logs.txt", mode="w+", maxBytes=5000000, backupCount=10),
        logging.StreamHandler(),
    ],
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

def LOGGER(name: str) -> logging.Logger:
    return logging.getLogger(name)

# ============================================================
# ⚙️ CONFIG (All from Environment)
# ============================================================
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_STRINGS = [s.strip() for s in os.getenv("SESSION_STRINGS", "").split(",") if s.strip()]
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
FLOOD_SLEEP = int(os.getenv("FLOOD_SLEEP", "5"))
TRIAL_DOWNLOADS = int(os.getenv("TRIAL_DOWNLOADS", "50"))
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))
FORWARD_CHAT_IDS = [c.strip() for c in os.getenv("FORWARD_CHAT_IDS", "").split(",") if c.strip()]
BOT_START_TIME = time()

if not API_ID or not API_HASH or not BOT_TOKEN:
    print("❌ API_ID, API_HASH, BOT_TOKEN are required!")
    sys.exit(1)

if not SESSION_STRINGS:
    print("❌ SESSION_STRINGS is required! Generate one from @TgDevToolBot")
    sys.exit(1)

# ============================================================
# 🗄️ DATABASE (SQLite)
# ============================================================
DB_PATH = "bot_database.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
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
        await db.commit()

async def get_user(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

async def create_user(user_id: int, username: str = ""):
    trial_end = datetime.now() + timedelta(days=TRIAL_DAYS)
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

# ============================================================
# 🧰 HELPERS (Previously files.py, msg.py, utils.py, forward.py)
# ============================================================
SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

def get_readable_file_size(size_in_bytes: Optional[float]) -> str:
    if size_in_bytes is None or size_in_bytes < 0: return "0B"
    for unit in SIZE_UNITS:
        if size_in_bytes < 1024: return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return "File too large"

def get_readable_time(seconds: int) -> str:
    result = ""
    days = int(seconds // 86400)
    if days: result += f"{days}d"
    hours = int((seconds % 86400) // 3600)
    if hours: result += f"{hours}h"
    minutes = int((seconds % 3600) // 60)
    if minutes: result += f"{minutes}m"
    seconds = int(seconds % 60)
    result += f"{seconds}s"
    return result

def get_download_path(folder_id: int, filename: str, root_dir: str = "downloads") -> str:
    safe_name = os.path.basename(filename) or str(folder_id)
    folder = os.path.join(root_dir, str(folder_id))
    os.makedirs(folder, exist_ok=True)
    return os.path.realpath(os.path.join(folder, safe_name))

def cleanup_download(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
        if path and os.path.exists(path + ".temp"):
            os.remove(path + ".temp")
        folder = os.path.dirname(path) if path else None
        if folder and os.path.isdir(folder) and not os.listdir(folder):
            os.rmdir(folder)
    except Exception as e:
        LOGGER(__name__).error(f"Cleanup failed: {e}")

def cleanup_downloads_root(root_dir: str = "downloads") -> tuple[int, int]:
    if not os.path.isdir(root_dir): return 0, 0
    file_count = 0; total_size = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for name in filenames:
            file_count += 1
            try:
                total_size += os.path.getsize(os.path.join(dirpath, name))
            except: pass
    shutil.rmtree(root_dir, ignore_errors=True)
    return file_count, total_size

async def fileSizeLimit(file_size, message, action_type="download", is_premium=False):
    max_size = 2 * 2097152000 if is_premium else 2097152000
    if file_size > max_size:
        await message.reply(f"File exceeds {get_readable_file_size(max_size)} limit.")
        return False
    return True

async def resolve_forward_chat_id(raw: str):
    if raw.lstrip("-").isdigit(): return int(raw)
    return raw

STORY_LINK_RE = re.compile(r"^(?:https?://)?(?:www\.)?(?:t(?:elegram)?\.(?:org|me|dog)/)([\w]+)/s/(\d+)/?$", re.IGNORECASE)

def is_story_link(link: str) -> bool:
    return bool(link and STORY_LINK_RE.match(link.strip()))

def get_raw_text(text, entities):
    return (text or ""), (entities or [])

def getStoryChatMsgID(link: str):
    cleaned = link.split("?", 1)[0].strip()
    match = STORY_LINK_RE.match(cleaned)
    if not match: raise ValueError("Invalid story URL.")
    return match.group(1), int(match.group(2))

def getChatMsgID(link: str):
    linkps = link.split("/")
    chat_id, message_id = None, None
    try:
        if len(linkps) == 7 and linkps[3] == "c":
            chat_id = get_channel_id(int(linkps[4])); message_id = int(linkps[6])
        elif len(linkps) == 6:
            if linkps[3] == "c":
                chat_id = get_channel_id(int(linkps[4])); message_id = int(linkps[5])
            else:
                chat_id = linkps[3]; message_id = int(linkps[5])
        elif len(linkps) == 5:
            chat_id = linkps[3]; message_id = int(linkps[4])
    except: raise ValueError("Invalid post URL.")
    if not chat_id or not message_id: raise ValueError("Invalid post URL.")
    return chat_id, message_id

def get_file_name(message_id: int, chat_message) -> str:
    if chat_message.document: return chat_message.document.file_name or f"{message_id}.file"
    if chat_message.video: return chat_message.video.file_name or f"{message_id}.mp4"
    if chat_message.audio: return chat_message.audio.file_name or f"{message_id}.mp3"
    if chat_message.voice: return f"{message_id}.ogg"
    if chat_message.video_note: return f"{message_id}.mp4"
    if chat_message.animation: return chat_message.animation.file_name or f"{message_id}.gif"
    if chat_message.sticker:
        if chat_message.sticker.is_animated: return f"{message_id}.tgs"
        if chat_message.sticker.is_video: return f"{message_id}.webm"
        return f"{message_id}.webp"
    if chat_message.photo: return f"{message_id}.jpg"
    return str(message_id)

def get_story_file_name(story_id: int, story, chat_username: str = None) -> str:
    prefix = f"{chat_username}_" if chat_username else ""
    if getattr(story, "video", None): return f"{prefix}story_{story_id}.mp4"
    if getattr(story, "photo", None): return f"{prefix}story_{story_id}.jpg"
    return f"{prefix}story_{story_id}"

async def cmd_exec(cmd, shell=False):
    if shell: proc = await asyncio.create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else: proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    return stdout.decode().strip(), stderr.decode().strip(), proc.returncode

async def get_media_info(path):
    try:
        stdout, _, code = await cmd_exec(["ffprobe", "-hide_banner", "-loglevel", "error", "-print_format", "json", "-show_format", "-show_streams", path])
        if stdout and code == 0:
            data = json.loads(stdout)
            fields = data.get("format", {})
            duration = round(float(fields.get("duration", 0)))
            tags = fields.get("tags", {})
            artist = tags.get("artist") or tags.get("ARTIST")
            title = tags.get("title") or tags.get("TITLE")
            width = height = None
            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video":
                    width = stream.get("width"); height = stream.get("height"); break
            return duration, artist, title, width, height
    except: pass
    return 0, None, None, None, None

async def get_video_thumbnail(video_file, duration):
    os.makedirs("Assets", exist_ok=True)
    output = "Assets/video_thumb.jpg"
    if not duration: duration = (await get_media_info(video_file))[0] or 3
    duration //= 2
    if os.path.exists(output):
        try: os.remove(output)
        except: pass
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", str(duration), "-i", video_file, "-vframes", "1", "-q:v", "2", "-y", output]
    try:
        _, err, code = await asyncio.wait_for(cmd_exec(cmd), timeout=60)
        if code != 0 or not os.path.exists(output): return None
    except: return None
    return output

PROGRESS_BAR = "Percentage: {percentage:.2f}% | {current}/{total}\nSpeed: {speed}/s\nEstimated Time Left: {est_time} seconds"

def progressArgs(action: str, progress_message, start_time):
    return (action, progress_message, start_time, PROGRESS_BAR, "▓", "░")

async def download_single_media(msg, progress_message, start_time):
    for attempt in range(2):
        try:
            path = await msg.download(progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("Downloading", progress_message, start_time))
            cap, ent = get_raw_text(msg.caption, msg.caption_entities)
            if msg.photo: return ("success", path, InputMediaPhoto(path, caption=cap, caption_entities=ent))
            if msg.video: return ("success", path, InputMediaVideo(path, caption=cap, caption_entities=ent))
            if msg.document: return ("success", path, InputMediaDocument(path, caption=cap, caption_entities=ent))
            if msg.audio: return ("success", path, InputMediaAudio(path, caption=cap, caption_entities=ent))
        except FloodWait as e:
            if attempt == 0: await asyncio.sleep(int(e.value) + 1)
            else: return ("error", None, None)
        except: return ("error", None, None)
    return ("skip", None, None)

# ============================================================
# 🚀 BOT CLIENTS
# ============================================================
bot = Client("media_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=100, parse_mode=ParseMode.MARKDOWN)

user_sessions = []
for i, ss in enumerate(SESSION_STRINGS):
    try:
        c = Client(f"user_{i}", session_string=ss, workers=10, sleep_threshold=30)
        user_sessions.append(c)
    except Exception as e:
        LOGGER(__name__).error(f"Invalid session {i+1}: {e}")

if not user_sessions:
    LOGGER(__name__).error("No valid sessions! Bot will not work.")
    sys.exit(1)

session_counter = 0
def get_next_session():
    global session_counter
    s = user_sessions[session_counter % len(user_sessions)]
    session_counter += 1
    return s

RUNNING_TASKS = set()
download_semaphore = None
forward_chat_id = FORWARD_CHAT_IDS[0] if FORWARD_CHAT_IDS else None

def track_task(coro):
    task = asyncio.create_task(coro)
    RUNNING_TASKS.add(task)
    task.add_done_callback(lambda t: RUNNING_TASKS.discard(t))
    return task

# ============================================================
# 🔐 PREMIUM DECORATOR
# ============================================================
def premium_required(func):
    async def wrapper(client, message, *args, **kwargs):
        user = await get_user(message.from_user.id)
        if not user: await create_user(message.from_user.id, message.from_user.username); user = await get_user(message.from_user.id)
        now = datetime.now()
        trial_end = datetime.fromisoformat(user['trial_end']) if user.get('trial_end') else None
        premium_expiry = datetime.fromisoformat(user['premium_expiry']) if user.get('premium_expiry') else None
        if (premium_expiry and premium_expiry > now) or (trial_end and trial_end > now and user['total_downloads'] < TRIAL_DOWNLOADS):
            return await func(client, message, *args, **kwargs)
        await message.reply("🚫 **Premium Required**\nSend /buy to unlock unlimited downloads & clone.")
        return None
    return wrapper

# ============================================================
# 🤖 COMMANDS
# ============================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, message):
    await create_user(message.from_user.id, message.from_user.username)
    await message.reply("👋 **Welcome!**\nSend /help for commands.\nFree Trial: 50 downloads / 7 days.")

@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(_, message):
    await message.reply(
        "📌 **Commands:**\n"
        "/dl <link> - Download post\n"
        "/bdl <start> <end> - Batch download\n"
        "/dls <story> - Download story\n"
        "/clone <channel> - FULL CLONE (Premium)\n"
        "/buy - Get Premium\n"
        "/status - Your usage\n"
        "/cancel - Stop all tasks"
    )

@bot.on_message(filters.command("status") & filters.private)
async def status_cmd(_, message):
    user = await get_user(message.from_user.id)
    if not user: await create_user(message.from_user.id); user = await get_user(message.from_user.id)
    text = f"📊 **Status**\nPlan: {user['plan'].upper()}\nDownloads: {user['total_downloads']}\nTasks: {len(RUNNING_TASKS)}"
    await message.reply(text)

@bot.on_message(filters.command("buy") & filters.private)
async def buy_cmd(_, message):
    await message.reply("💳 Pay ₹299 to UPI: `your@upi`\nSend `/verify_payment TXN_ID` after payment.")

@bot.on_message(filters.command("verify_payment") & filters.private)
async def verify_cmd(_, message):
    await update_user(message.from_user.id, {"plan": "premium", "premium_expiry": (datetime.now() + timedelta(days=30)).isoformat()})
    await message.reply("🎉 Premium Activated!")

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(_, message):
    c = 0
    for t in list(RUNNING_TASKS):
        if not t.done(): t.cancel(); c += 1
    await message.reply(f"🛑 Cancelled {c} tasks.")

@bot.on_message(filters.command("cleanup") & filters.private)
async def cleanup_cmd(_, message):
    f, b = cleanup_downloads_root()
    await message.reply(f"Cleaned {f} files, freed {get_readable_file_size(b)}.")

@bot.on_message(filters.command("logs") & filters.private)
async def logs_cmd(_, message):
    if os.path.exists("logs.txt"): await message.reply_document("logs.txt")
    else: await message.reply("No logs.")

@bot.on_message(filters.command("stats") & filters.private)
async def stats_cmd(_, message):
    total, used, free = shutil.disk_usage(".")
    await message.reply(f"**Uptime:** {get_readable_time(time() - BOT_START_TIME)}\n**Disk:** Used {get_readable_file_size(used)} / {get_readable_file_size(total)}")

# ============================================================
# 📥 DOWNLOAD HANDLER
# ============================================================
async def handle_download(client, message, url):
    global download_semaphore, forward_chat_id
    async with download_semaphore:
        user = await get_user(message.from_user.id)
        if not user: await create_user(message.from_user.id); user = await get_user(message.from_user.id)
        now = datetime.now()
        trial_end = datetime.fromisoformat(user['trial_end']) if user.get('trial_end') else None
        premium_expiry = datetime.fromisoformat(user['premium_expiry']) if user.get('premium_expiry') else None
        if not (premium_expiry and premium_expiry > now) and (not trial_end or trial_end < now or user['total_downloads'] >= TRIAL_DOWNLOADS):
            return await message.reply("🚫 Trial limit exceeded. Send /buy")
        
        try:
            sess = get_next_session()
            chat_id, msg_id = getChatMsgID(url)
            msg = await sess.get_messages(chat_id, msg_id)
            if not msg: return await message.reply("Message not found.")
            
            if msg.media_group_id:
                await processMediaGroup(msg, client, message, forward_chat_id)
                return
            
            if msg.media:
                await increment_downloads(message.from_user.id)
                # Simplified media send
                path = await msg.download()
                await message.reply_document(path)
                cleanup_download(path)
            elif msg.text:
                await message.reply(msg.text)
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("dl") & filters.private)
async def dl_cmd(client, message):
    if len(message.command) < 2: return await message.reply("Usage: /dl <link>")
    await track_task(handle_download(client, message, message.command[1]))

# ============================================================
# 📌 CLONE ENGINE (Premium)
# ============================================================
@bot.on_message(filters.command("clone") & filters.private)
@premium_required
async def clone_cmd(client, message):
    args = message.text.split()
    if len(args) < 2: return await message.reply("Usage: /clone <channel_link>")
    chat_link = args[1]
    try: chat_id, _ = getChatMsgID(chat_link)
    except: chat_id = chat_link.split('/')[-1]
    
    sess = get_next_session()
    try: await sess.get_chat(chat_id)
    except: return await message.reply("Cannot access chat. Make sure your user account is a member.")
    
    progress = await get_clone_progress(message.from_user.id, str(chat_id))
    start_id = (progress['last_msg_id'] + 1) if progress else 1
    latest = await sess.get_messages(chat_id, 0)
    if not latest: return await message.reply("No messages.")
    end_id = latest.id
    if start_id > end_id: return await message.reply("Already fully cloned!")
    
    prog_msg = await message.reply(f"Cloning from {start_id} to {end_id}...")
    cloned = failed = 0
    
    for batch_start in range(start_id, end_id + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE - 1, end_id)
        try:
            msgs = await sess.get_messages(chat_id, list(range(batch_start, batch_end + 1)))
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + 5)
            continue
        for msg in msgs:
            if not msg or msg.empty: failed += 1; continue
            try:
                await msg.copy(message.chat.id)
                cloned += 1
                await increment_downloads(message.from_user.id)
            except: failed += 1
        await save_clone_progress(message.from_user.id, str(chat_id), batch_end, end_id)
        await asyncio.sleep(FLOOD_SLEEP)
    
    await save_clone_progress(message.from_user.id, str(chat_id), end_id, end_id, "completed")
    await prog_msg.edit(f"✅ Clone Complete! Cloned: {cloned}, Failed: {failed}")

# ============================================================
# 🧩 MEDIA GROUP PROCESSING
# ============================================================
async def processMediaGroup(chat_message, bot, message, forward_chat_id=None):
    group = await chat_message.get_media_group()
    valid = []
    start = time()
    prog = await message.reply("Downloading media group...")
    tasks = [download_single_media(m, prog, start) for m in group if m.media]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, tuple) and r[0] == "success": valid.append(r[2])
    if valid:
        try: await bot.send_media_group(message.chat.id, valid)
        except: pass
    await prog.delete()

# ============================================================
# 🧵 FLASK WEB SERVER (To keep Render alive)
# ============================================================
flask_app = Flask('')

@flask_app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    flask_app.run(host='0.0.0.0', port=8080)

# ============================================================
# 🚀 MAIN ENTRY POINT
# ============================================================
async def main():
    global download_semaphore
    download_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    await init_db()
    
    # Start Bot
    await bot.start()
    LOGGER(__name__).info("🤖 Bot started!")
    
    # Start User Sessions
    for i, sess in enumerate(user_sessions):
        try:
            await sess.start()
            me = await sess.get_me()
            LOGGER(__name__).info(f"✅ Session {i+1}: @{me.username}")
        except Exception as e:
            LOGGER(__name__).error(f"❌ Session {i+1} failed: {e}")
    
    LOGGER(__name__).info("✅ Bot is ready!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    # Start Flask in background
    Thread(target=run_flask, daemon=True).start()
    
    # Run Bot
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER(__name__).info("Stopped.")
    except Exception as e:
        LOGGER(__name__).error(f"Fatal: {e}")
