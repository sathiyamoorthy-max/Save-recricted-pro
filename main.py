"""
⚡ ULTIMATE TELEGRAM SAVER & CLONER BOT v7.0
👑 Owner: @sathiyamoorthy-max
💰 ₹199/month | ₹699/lifetime
🔓 Owner gets UNLIMITED access (based on OWNER_ID in config)
"""

import os, sys, re, asyncio, shutil, logging, sqlite3, psutil
from time import time
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from threading import Thread
from asyncio import Queue, Semaphore
from logging.handlers import RotatingFileHandler

# ============================================================
# 📦 AUTO-INSTALL MISSING PACKAGES (RENDER COMPATIBLE)
# ============================================================
try:
    import pyrogram, aiosqlite
    from flask import Flask, jsonify
    from dotenv import load_dotenv
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    from pyleaves import Leaves
except ImportError as e:
    print(f"⚠️ Installing missing packages: {e}")
    os.system(f"{sys.executable} -m pip install pyrogram pyleaves python-dotenv psutil aiosqlite Flask tenacity TgCrypto pillow")
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ============================================================
# 🔧 LOAD ENVIRONMENT VARIABLES FROM config.env
# ============================================================
load_dotenv("config.env")

class Config:
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    SESSION_STRINGS = [s.strip() for s in os.getenv("SESSION_STRINGS", "").split(",") if s.strip()]
    
    # 👑 OWNER ID (from config.env) – gets UNLIMITED access
    OWNER_ID = int(os.getenv("OWNER_ID", 0))  # <-- YOUR TELEGRAM ID
    
    # Pricing (from config.env)
    PREMIUM_PRICE = int(os.getenv("PREMIUM_PRICE", 199))
    LIFETIME_PRICE = int(os.getenv("LIFETIME_PRICE", 699))
    UPI_ID = os.getenv("UPI_ID", "sathiyasevam@okaxis")
    
    # Trial limits
    TRIAL_DOWNLOADS = int(os.getenv("TRIAL_DOWNLOADS", 25))
    TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", 3))
    
    # Performance
    MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 2))
    BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", 30))
    FLOOD_SLEEP_THRESHOLD = int(os.getenv("FLOOD_SLEEP_THRESHOLD", 3))
    
    # Validate
    if not API_ID or not API_HASH or not BOT_TOKEN or not SESSION_STRINGS:
        print("❌ Missing API_ID / API_HASH / BOT_TOKEN / SESSION_STRINGS")
        sys.exit(1)
    if OWNER_ID == 0:
        print("⚠️ WARNING: OWNER_ID not set! Set your Telegram numeric ID in config.env")

config = Config()
BOT_START_TIME = time()
LOGGER = logging.getLogger("UltimateBot")

# ============================================================
# 📝 LOGGING
# ============================================================
try: os.remove("logs.txt")
except: pass
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(message)s",
    handlers=[
        RotatingFileHandler("logs.txt", maxBytes=10_000_000, backupCount=5),
        logging.StreamHandler(),
    ],
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

# ============================================================
# 🗄️ SQLite DATABASE (local, no MongoDB needed)
# ============================================================
DB_PATH = "bot_database.db"

async def db_execute(query, params=(), fetchone=False, fetchall=False):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        if fetchone:
            row = await cur.fetchone(); await db.commit(); return dict(row) if row else None
        if fetchall:
            rows = await cur.fetchall(); await db.commit(); return [dict(r) for r in rows]
        await db.commit()

async def init_db():
    await db_execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, username TEXT, plan TEXT DEFAULT 'free',
        trial_start TEXT, trial_end TEXT, premium_expiry TEXT,
        total_downloads INTEGER DEFAULT 0, created_at TEXT)""")
    await db_execute("""CREATE TABLE IF NOT EXISTS clone_progress (
        user_id INTEGER, chat_id TEXT, last_msg_id INTEGER, total_msgs INTEGER,
        status TEXT, updated_at TEXT, PRIMARY KEY (user_id, chat_id))""")
    await db_execute("""CREATE TABLE IF NOT EXISTS download_cache (
        chat_id TEXT, msg_id INTEGER, status TEXT, PRIMARY KEY (chat_id, msg_id))""")

async def get_user(user_id): return await db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)
async def create_user(user_id, username=""):
    if await get_user(user_id): return
    trial_end = (datetime.now() + timedelta(days=config.TRIAL_DAYS)).isoformat()
    await db_execute("INSERT INTO users (user_id, username, plan, trial_start, trial_end, created_at) VALUES (?, ?, 'free', ?, ?, ?)",
                     (user_id, username, datetime.now().isoformat(), trial_end, datetime.now().isoformat()))
async def update_user(user_id, data):
    set_clause = ", ".join([f"{k} = ?" for k in data])
    await db_execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", [data[k] for k in data] + [user_id])
async def increment_downloads(user_id): await db_execute("UPDATE users SET total_downloads = total_downloads + 1 WHERE user_id = ?", (user_id,))
async def is_cached(chat_id, msg_id):
    return bool(await db_execute("SELECT 1 FROM download_cache WHERE chat_id = ? AND msg_id = ?", (str(chat_id), msg_id), fetchone=True))
async def mark_cached(chat_id, msg_id):
    await db_execute("REPLACE INTO download_cache (chat_id, msg_id, status) VALUES (?, ?, 'done')", (str(chat_id), msg_id))
async def get_clone_progress(user_id, chat_id):
    return await db_execute("SELECT * FROM clone_progress WHERE user_id = ? AND chat_id = ?", (user_id, str(chat_id)), fetchone=True)
async def save_clone_progress(user_id, chat_id, last_id, total):
    await db_execute("REPLACE INTO clone_progress (user_id, chat_id, last_msg_id, total_msgs, status, updated_at) VALUES (?, ?, ?, ?, 'running', ?)",
                     (user_id, str(chat_id), last_id, total, datetime.now().isoformat()))

# ============================================================
# 🧰 UTILITY FUNCTIONS
# ============================================================
def get_readable_size(size):
    if not size: return "0B"
    for unit in ["B","KB","MB","GB","TB"]:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024
    return "Very Large"
def get_readable_time(sec):
    d, rem = divmod(sec, 86400); h, rem = divmod(rem, 3600); m, s = divmod(rem, 60)
    return f"{int(d)}d {int(h)}h {int(m)}m {int(s)}s" if d>0 else f"{int(h)}h {int(m)}m"
def cleanup_download(path):
    try:
        if path and os.path.exists(path): os.remove(path)
        folder = os.path.dirname(path) if path else None
        if folder and os.path.isdir(folder) and not os.listdir(folder): os.rmdir(folder)
    except: pass
def get_raw_text(text, entities): return (text or ""), (entities or [])

# --- Link Parsers ---
STORY_LINK_RE = re.compile(r"t\.me/([\w]+)/s/(\d+)", re.IGNORECASE)
def getChatMsgID(link):
    parts = link.split("/")
    if len(parts) >= 5 and parts[3] == "c":
        from pyrogram.utils import get_channel_id
        return get_channel_id(int(parts[4])), int(parts[5])
    elif len(parts) >= 4:
        return parts[3], int(parts[4])
    raise ValueError("Invalid link")
def is_story_link(link): return bool(STORY_LINK_RE.search(link))

# ============================================================
# 🚀 CLIENTS, QUEUE, SEMAPHORES
# ============================================================
PROGRESS_BAR = "📥 {action}\n{percentage:.2f}% | {current}/{total}\n⚡ {speed}/s | ⏳ {est_time}s"
def progress_args(action, msg, start): return (action, msg, start, PROGRESS_BAR, "▓", "░")

from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatType
from pyrogram.errors import FloodWait, UsernameNotOccupied
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.utils import get_channel_id
from pyleaves import Leaves

bot = Client("bot", api_id=config.API_ID, api_hash=config.API_HASH,
             bot_token=config.BOT_TOKEN, workers=50)

user_sessions = []
for i, ss in enumerate(config.SESSION_STRINGS):
    try:
        c = Client(f"u{i}", session_string=ss, workers=10)
        user_sessions.append(c)
    except: pass
if not user_sessions:
    LOGGER.error("❌ No sessions! Exiting.")
    sys.exit(1)

session_counter = 0
def get_next_session():
    global session_counter
    s = user_sessions[session_counter % len(user_sessions)]
    session_counter += 1
    return s

download_queue = Queue()
semaphore = Semaphore(config.MAX_CONCURRENT_DOWNLOADS)
RUNNING_TASKS = set()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30), retry=retry_if_exception_type(FloodWait))
async def safe_download(msg, prog, start):
    return await msg.download(progress=Leaves.progress_for_pyrogram,
                              progress_args=progress_args("Downloading", prog, start))

def track_task(coro):
    task = asyncio.create_task(coro); RUNNING_TASKS.add(task); task.add_done_callback(lambda _: RUNNING_TASKS.discard(task)); return task

# ============================================================
# 🔐 ACCESS GUARD (OWNER UNLIMITED, OTHERS HAVE TRIAL/PREMIUM)
# ============================================================
def access_required(func):
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        
        # 👑 OWNER – UNLIMITED (bypass everything)
        if user_id == config.OWNER_ID:
            return await func(client, message, *args, **kwargs)
        
        user = await get_user(user_id)
        if not user: await create_user(user_id, message.from_user.username); user = await get_user(user_id)
        
        now = datetime.now()
        premium_expiry = datetime.fromisoformat(user["premium_expiry"]) if user.get("premium_expiry") else None
        trial_end = datetime.fromisoformat(user["trial_end"]) if user.get("trial_end") else None
        
        # Premium or valid trial
        if premium_expiry and premium_expiry > now:
            return await func(client, message, *args, **kwargs)
        if trial_end and trial_end > now and user.get("total_downloads", 0) < config.TRIAL_DOWNLOADS:
            return await func(client, message, *args, **kwargs)
        
        # Blocked – show payment prompt
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Buy Premium", callback_data="buy")],
            [InlineKeyboardButton("🔗 Referral", callback_data="refer")]
        ])
        await message.reply(
            "🚫 **Access Denied!**\n\n"
            f"📊 You've used {user.get('total_downloads', 0)} of {config.TRIAL_DOWNLOADS} trial downloads.\n"
            "Upgrade to **Premium** for unlimited access.",
            reply_markup=keyboard
        )
        return None
    return wrapper

# ============================================================
# 🤖 COMMANDS: /start, /status, /buy, /proof
# ============================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, message):
    await create_user(message.from_user.id, message.from_user.username)
    await message.reply(
        "👋 **Welcome to Pro Save Restricted Bot!**\n\n"
        "✅ Download ANY restricted content\n"
        "✅ Clone full channels (public/private)\n"
        "✅ Batch & Story download\n\n"
        "📌 **Commands:**\n"
        "/dl <link> – Single\n"
        "/batch <start_link> <end_id> – Unlimited Batch\n"
        "/clone <channel_link> – Full Clone (Premium)\n"
        "/dls <story_link> – Story\n"
        "/status – Your usage\n"
        "/buy – Get Premium\n\n"
        f"🎁 Trial: {config.TRIAL_DOWNLOADS} downloads / {config.TRIAL_DAYS} days"
    )

@bot.on_message(filters.command("status") & filters.private)
async def status_cmd(_, message):
    user = await get_user(message.from_user.id)
    if not user: await create_user(message.from_user.id); user = await get_user(message.from_user.id)
    text = f"📊 **Status**\nPlan: {user['plan'].upper()}\nDownloads: {user['total_downloads']}/{config.TRIAL_DOWNLOADS}\nQueue: {download_queue.qsize()}"
    if message.from_user.id == config.OWNER_ID:
        text += "\n\n👑 **OWNER MODE: UNLIMITED**"
    await message.reply(text)

@bot.on_message(filters.command("buy") & filters.private)
async def buy_cmd(_, message):
    await message.reply(
        f"💎 **Upgrade to Premium Access**\n\n"
        f"• 1 Month Unlimited: ₹{config.PREMIUM_PRICE}\n"
        f"• Lifetime Unlimited: ₹{config.LIFETIME_PRICE}\n\n"
        f"**Official UPI ID:**\n`{config.UPI_ID}`\n\n"
        "**Verification Process:**\n"
        "1. Pay to UPI ID above\n"
        "2. Send /proof <Transaction_ID>\n"
        "Admin reviews and unlocks instantly."
    )

@bot.on_message(filters.command("proof") & filters.private)
async def proof_cmd(_, message):
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("Usage: `/proof <Transaction_ID>`\nExample: `/proof UPI987654321`")
    txn = args[1]
    # Auto‑activate for 30 days (you can later extend to 30 or 365 based on amount)
    await update_user(message.from_user.id, {"plan": "premium", "premium_expiry": (datetime.now() + timedelta(days=30)).isoformat()})
    await message.reply(f"✅ **Transaction {txn} verified!**\nYour Premium is active for 30 Days.\nEnjoy unlimited access! 🎉")

# ============================================================
# 📥 CORE DOWNLOAD / BATCH / CLONE / STORY
# ============================================================
async def process_media_group(src, client, msg):
    from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
    group = await src.get_media_group()
    valid, temps = [], []
    prog = await msg.reply("📥 Downloading album...")
    start = time()
    for m in group:
        if m.media:
            try:
                p = await safe_download(m, prog, start)
                cap, ent = get_raw_text(m.caption, m.caption_entities)
                temps.append(p)
                if m.photo: valid.append(InputMediaPhoto(p, caption=cap, caption_entities=ent))
                elif m.video: valid.append(InputMediaVideo(p, caption=cap, caption_entities=ent))
                elif m.document: valid.append(InputMediaDocument(p, caption=cap, caption_entities=ent))
                elif m.audio: valid.append(InputMediaAudio(p, caption=cap, caption_entities=ent))
            except: pass
    if valid:
        try: await client.send_media_group(msg.chat.id, valid); await increment_downloads(msg.from_user.id)
        except: pass
    for p in temps: cleanup_download(p)
    await prog.delete()

async def execute_download(client, message, url: str):
    async with semaphore:
        sess = get_next_session()
        try:
            if is_story_link(url): return await story_download(client, message, url, sess)
            chat_id, msg_id = getChatMsgID(url)
        except Exception as e: return await message.reply(f"❌ Invalid URL: {e}")
        if await is_cached(str(chat_id), msg_id):
            return await message.reply("✅ Already downloaded.")
        prog = await message.reply("⚡ Fetching...")
        try:
            src = await sess.get_messages(chat_id, msg_id)
            if not src or src.empty: await prog.delete(); return await message.reply("❌ Not found.")
            if src.media_group_id: await prog.delete(); return await process_media_group(src, client, message)
            if src.media:
                start = time()
                await prog.edit_text("📥 Downloading...")
                path = await safe_download(src, prog, start)
                await prog.edit_text("📤 Uploading...")
                cap, ent = get_raw_text(src.caption, src.caption_entities)
                if src.photo: await message.reply_photo(path, caption=cap, caption_entities=ent)
                elif src.video: await message.reply_video(path, caption=cap, caption_entities=ent)
                elif src.document: await message.reply_document(path, caption=cap, caption_entities=ent)
                elif src.audio: await message.reply_audio(path, caption=cap, caption_entities=ent)
                else: await message.reply_document(path, caption=cap)
                await increment_downloads(message.from_user.id)
                await mark_cached(str(chat_id), msg_id)
                cleanup_download(path)
                await prog.delete()
            elif src.text: await prog.delete(); await message.reply(src.text)
            else: await prog.delete(); await message.reply("⚠️ No media.")
        except FloodWait as e:
            await prog.edit_text(f"⏳ FloodWait: {e.value}s..."); await asyncio.sleep(e.value+5)
        except Exception as e: await prog.delete(); await message.reply(f"❌ Error: {e}")

async def story_download(client, message, url, sess):
    try:
        match = STORY_LINK_RE.search(url)
        username, sid = match.group(1), int(match.group(2))
        story = await sess.get_stories(username, sid)
        if not story: return await message.reply("❌ Story expired.")
        prog = await message.reply("📥 Downloading story...")
        start = time()
        path = await safe_download(story, prog, start)
        cap, ent = get_raw_text(story.caption, story.caption_entities)
        if story.photo: await message.reply_photo(path, caption=cap, caption_entities=ent)
        elif story.video: await message.reply_video(path, caption=cap, caption_entities=ent)
        await increment_downloads(message.from_user.id)
        cleanup_download(path)
        await prog.delete()
    except Exception as e: await message.reply(f"❌ Story error: {e}")

# ------------------------------------------------------------
# /dl (single)
@bot.on_message(filters.command("dl") & filters.private)
@access_required
async def dl_cmd(client, message):
    if len(message.command) < 2: return await message.reply("Usage: /dl <link>")
    await download_queue.put((execute_download, client, message, message.command[1]))
    await message.reply(f"⏳ Queued. Pos: {download_queue.qsize()}")

# ------------------------------------------------------------
# /dls (story)
@bot.on_message(filters.command("dls") & filters.private)
@access_required
async def dls_cmd(client, message):
    if len(message.command) < 2: return await message.reply("Usage: /dls <story_link>")
    await download_queue.put((execute_download, client, message, message.command[1]))
    await message.reply("⏳ Story queued.")

# ------------------------------------------------------------
# /batch (unlimited)
@bot.on_message(filters.command("batch") & filters.private)
@access_required
async def batch_cmd(client, message):
    args = message.text.split()
    if len(args) < 3:
        return await message.reply("Usage: `/batch <start_url> <end_id>`\nExample: `/batch https://t.me/yourchannel/1 5000`")
    try:
        start_link, end_id = args[1], int(args[2])
        chat_id, start_id = getChatMsgID(start_link)
        if end_id < start_id: return await message.reply("❌ End ID >= Start ID required.")
        count = end_id - start_id + 1
        prefix = start_link.rsplit('/', 1)[0]
        prog = await message.reply(f"⏳ Queuing **{count}** posts...")
        queued = 0
        for mid in range(start_id, end_id + 1):
            await download_queue.put((execute_download, client, message, f"{prefix}/{mid}"))
            queued += 1
            if queued % 50 == 0:
                await prog.edit_text(f"⏳ Queuing {queued}/{count}...")
        await prog.edit_text(f"✅ **{queued} posts queued!**\n🔄 Queue Size: {download_queue.qsize()}")
    except Exception as e: await message.reply(f"❌ Error: {e}")

# ------------------------------------------------------------
# /clone (public/private, resume)
@bot.on_message(filters.command("clone") & filters.private)
@access_required
async def clone_cmd(client, message):
    # Clone is also premium, but owner bypass already done by @access_required
    if len(message.command) < 2:
        return await message.reply("Usage: `/clone <channel_link>`\nExample: `/clone https://t.me/yourchannel`")
    
    chat_input = message.command[1]
    # Extract chat ID from any link
    try:
        # if it's a post link like t.me/username/123, extract the username
        parts = chat_input.split("/")
        if len(parts) >= 4 and parts[3] != "c":
            chat_id = parts[3]  # username
        else:
            # use the whole link as ID (for private numeric IDs)
            chat_id = chat_input.split("/")[-1] if "/" in chat_input else chat_input
    except: chat_id = chat_input

    sess = get_next_session()
    prog = await message.reply(f"🔄 Cloning `{chat_id}`...")
    try:
        # Try to get latest message
        latest = await sess.get_messages(chat_id, 0)
        if not latest:
            return await prog.edit_text("❌ No messages or access denied. Make sure your session account is a member.")
        end_id = latest.id
    except Exception as e:
        return await prog.edit_text(f"❌ Cannot access chat. Error: {e}")

    saved = await get_clone_progress(message.from_user.id, str(chat_id))
    start_id = saved["last_msg_id"] + 1 if saved else 1
    if start_id > end_id:
        return await prog.edit_text("✅ Channel already fully cloned!")

    cloned, failed = 0, 0
    total = end_id - start_id + 1
    await prog.edit_text(f"📥 Cloning {start_id} to {end_id} (Resume)...")

    for batch_start in range(start_id, end_id + 1, config.BATCH_CHUNK_SIZE):
        batch_end = min(batch_start + config.BATCH_CHUNK_SIZE - 1, end_id)
        try:
            msgs = await sess.get_messages(chat_id, list(range(batch_start, batch_end + 1)))
        except FloodWait as e:
            await asyncio.sleep(e.value + 5)
            continue
        for m in msgs:
            if not m or m.empty:
                failed += 1
                continue
            try:
                await m.copy(message.chat.id)
                cloned += 1
                await increment_downloads(message.from_user.id)
            except Exception as e:
                failed += 1
                LOGGER.error(f"Clone copy error: {e}")
        await save_clone_progress(message.from_user.id, str(chat_id), batch_end, end_id)
        await prog.edit_text(f"📥 {batch_end}/{end_id} | ✅ {cloned} | ❌ {failed}")
        await asyncio.sleep(config.FLOOD_SLEEP_THRESHOLD)

    await save_clone_progress(message.from_user.id, str(chat_id), end_id, end_id)
    await prog.edit_text(f"✅ **Clone Complete!**\nCloned: {cloned} | Failed: {failed}")

# ============================================================
# 🧹 CANCEL / CLEANUP / LOGS / STATS
# ============================================================
@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(_, message):
    c = sum(1 for t in list(RUNNING_TASKS) if not t.done() and t.cancel())
    await message.reply(f"🛑 Cancelled {c} tasks.")

@bot.on_message(filters.command("cleanup") & filters.private)
async def cleanup_cmd(_, message):
    c, s = 0, 0
    if os.path.isdir("downloads"):
        for root, _, files in os.walk("downloads"):
            for f in files:
                c += 1
                try: s += os.path.getsize(os.path.join(root, f))
                except: pass
        shutil.rmtree("downloads", ignore_errors=True)
    await message.reply(f"🧹 Cleaned {c} files, freed {get_readable_size(s)}.")

@bot.on_message(filters.command("logs") & filters.private)
async def logs_cmd(_, message):
    if os.path.exists("logs.txt"): await message.reply_document("logs.txt")
    else: await message.reply("No logs.")

@bot.on_message(filters.command("stats") & filters.private)
async def stats_cmd(_, message):
    disk = shutil.disk_usage("/")
    await message.reply(
        f"📊 **Bot Stats**\n"
        f"Queue: {download_queue.qsize()}\n"
        f"Disk: {get_readable_size(disk.used)}/{get_readable_size(disk.total)}\n"
        f"Uptime: {get_readable_time(int(time()-BOT_START_TIME))}"
    )

# ============================================================
# 🧵 QUEUE WORKER & FLASK KEEP-ALIVE
# ============================================================
async def queue_worker():
    while True:
        task = await download_queue.get()
        try:
            handler, client, msg, url = task
            await handler(client, msg, url)
        except Exception as e: LOGGER.error(f"Worker error: {e}")
        finally: download_queue.task_done()

flask_app = Flask("")
@flask_app.route("/")
def health(): return jsonify({"status": "alive", "uptime": get_readable_time(int(time()-BOT_START_TIME))})
def run_flask():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port)

# ============================================================
# 🚀 MAIN
# ============================================================
async def main():
    await init_db()
    await bot.start()
    LOGGER.info("🤖 Bot started.")
    for i, s in enumerate(user_sessions):
        try: await s.start(); LOGGER.info(f"✅ Session {i+1} started")
        except: pass
    for _ in range(config.MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(queue_worker())
    LOGGER.info("🚀 Bot is ready!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: LOGGER.info("Shutdown.")
    except Exception as e: LOGGER.error(f"Fatal error: {e}")
