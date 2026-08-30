import os
import sys
import re
import json
import shutil
import asyncio
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

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from pyrogram.utils import get_channel_id
from pyleaves import Leaves
from dotenv import load_dotenv
from flask import Flask

# MongoDB Async Driver
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv("config.env")

# ============================================================
# 📝 LOGGER
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
# ⚙️ CONFIG
# ============================================================
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_STRINGS = [s.strip() for s in os.getenv("SESSION_STRINGS", "").split(",") if s.strip()]
MONGO_URL = os.getenv("MONGO_URL", "") # NEW REQUIREMENT
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
FLOOD_SLEEP = int(os.getenv("FLOOD_SLEEP", "5"))
TRIAL_DOWNLOADS = int(os.getenv("TRIAL_DOWNLOADS", "50"))
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))
FORWARD_CHAT_IDS = [c.strip() for c in os.getenv("FORWARD_CHAT_IDS", "").split(",") if c.strip()]
BOT_START_TIME = time()

if not API_ID or not API_HASH or not BOT_TOKEN or not MONGO_URL:
    print("❌ API_ID, API_HASH, BOT_TOKEN, and MONGO_URL are required!")
    sys.exit(1)

if not SESSION_STRINGS:
    print("❌ SESSION_STRINGS is required!")
    sys.exit(1)

# ============================================================
# 🗄️ DATABASE (MongoDB - Cloud Persistent)
# ============================================================
db_client = AsyncIOMotorClient(MONGO_URL)
db = db_client["media_bot_db"]
users_col = db["users"]
clone_col = db["clone_progress"]

async def get_user(user_id: int) -> Optional[Dict]:
    return await users_col.find_one({"user_id": user_id})

async def create_user(user_id: int, username: str = ""):
    exists = await get_user(user_id)
    if not exists:
        trial_end = datetime.now() + timedelta(days=TRIAL_DAYS)
        await users_col.insert_one({
            "user_id": user_id,
            "username": username,
            "plan": "free",
            "trial_start": datetime.now(),
            "trial_end": trial_end,
            "premium_expiry": None,
            "total_downloads": 0,
            "daily_downloads": 0,
            "last_download_date": datetime.now().strftime("%Y-%m-%d")
        })

async def update_user(user_id: int, data: dict):
    await users_col.update_one({"user_id": user_id}, {"$set": data})

async def increment_downloads(user_id: int):
    await users_col.update_one(
        {"user_id": user_id},
        {
            "$inc": {"total_downloads": 1, "daily_downloads": 1},
            "$set": {"last_download_date": datetime.now().strftime("%Y-%m-%d")}
        }
    )

async def get_clone_progress(user_id: int, chat_id: str) -> Optional[Dict]:
    return await clone_col.find_one({"user_id": user_id, "chat_id": chat_id})

async def save_clone_progress(user_id: int, chat_id: str, last_id: int, total: int, status: str = "running"):
    await clone_col.update_one(
        {"user_id": user_id, "chat_id": chat_id},
        {"$set": {
            "last_msg_id": last_id,
            "total_msgs": total,
            "status": status,
            "updated_at": datetime.now()
        }},
        upsert=True
    )

# ============================================================
# 🧰 HELPERS 
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

def cleanup_download(path: str) -> None:
    try:
        if path and os.path.exists(path): os.remove(path)
        if path and os.path.exists(path + ".temp"): os.remove(path + ".temp")
        folder = os.path.dirname(path) if path else None
        if folder and os.path.isdir(folder) and not os.listdir(folder):
            os.rmdir(folder)
    except Exception as e:
        LOGGER(__name__).error(f"Cleanup failed: {e}")

def get_raw_text(text, entities):
    return (text or ""), (entities or [])

STORY_LINK_RE = re.compile(r"^(?:https?://)?(?:www\.)?(?:t(?:elegram)?\.(?:org|me|dog)/)([\w]+)/s/(\d+)/?$", re.IGNORECASE)

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

def parse_date(date_val):
    if isinstance(date_val, str):
        try: return datetime.fromisoformat(date_val)
        except: return None
    return date_val

# ============================================================
# 🔐 PREMIUM DECORATOR
# ============================================================
def premium_required(func):
    async def wrapper(client, message, *args, **kwargs):
        user = await get_user(message.from_user.id)
        if not user: await create_user(message.from_user.id, message.from_user.username); user = await get_user(message.from_user.id)
        now = datetime.now()
        
        trial_end = parse_date(user.get('trial_end'))
        premium_expiry = parse_date(user.get('premium_expiry'))

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
        "/clone <channel> - FULL CLONE (Premium)\n"
        "/buy - Get Premium\n"
        "/status - Your usage\n"
        "/cancel - Stop all tasks"
    )

@bot.on_message(filters.command("status") & filters.private)
async def status_cmd(_, message):
    user = await get_user(message.from_user.id)
    if not user: await create_user(message.from_user.id); user = await get_user(message.from_user.id)
    text = f"📊 **Status**\nPlan: {user.get('plan', 'free').upper()}\nDownloads: {user.get('total_downloads', 0)}\nTasks: {len(RUNNING_TASKS)}"
    await message.reply(text)

@bot.on_message(filters.command("buy") & filters.private)
async def buy_cmd(_, message):
    await message.reply("💳 Pay ₹299 to UPI: `your@upi`\nSend `/verify_payment TXN_ID` after payment.")

@bot.on_message(filters.command("verify_payment") & filters.private)
async def verify_cmd(_, message):
    await update_user(message.from_user.id, {"plan": "premium", "premium_expiry": datetime.now() + timedelta(days=30)})
    await message.reply("🎉 Premium Activated!")

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(_, message):
    c = 0
    for t in list(RUNNING_TASKS):
        if not t.done(): t.cancel(); c += 1
    await message.reply(f"🛑 Cancelled {c} tasks.")

# ============================================================
# 📥 DOWNLOAD HANDLER
# ============================================================
async def handle_download(client, message, url):
    global download_semaphore, forward_chat_id
    async with download_semaphore:
        user = await get_user(message.from_user.id)
        if not user: await create_user(message.from_user.id); user = await get_user(message.from_user.id)
        now = datetime.now()
        
        trial_end = parse_date(user.get('trial_end'))
        premium_expiry = parse_date(user.get('premium_expiry'))

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
# 🧵 FLASK WEB SERVER (Dynamic Port Fix)
# ============================================================
flask_app = Flask('')

@flask_app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 8080)) # Fix: dynamic port handling
    flask_app.run(host='0.0.0.0', port=port)

# ============================================================
# 🚀 MAIN ENTRY POINT
# ============================================================
async def main():
    global download_semaphore
    download_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    await bot.start()
    LOGGER(__name__).info("🤖 Bot started!")
    
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
    Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER(__name__).info("Stopped.")
    except Exception as e:
        LOGGER(__name__).error(f"Fatal: {e}")
