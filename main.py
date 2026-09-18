import os
import re
import asyncio
import threading
import hashlib
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from datetime import datetime, timedelta
import pytz
import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    InlineQueryResultArticle,
    InputTextMessageContent
)
from pyrogram.errors import (
    SessionPasswordNeeded,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    PasswordHashInvalid,
    FloodWait,
    PeerIdInvalid,
    ChannelPrivate,
    UserAlreadyParticipant
)

# ----------------- CONFIGURATION -----------------
API_ID_RAW = os.environ.get("API_ID", "").strip()
DEFAULT_API_ID = int(API_ID_RAW) if API_ID_RAW.isdigit() else 0
DEFAULT_API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else 0

BOT_USERNAME = os.environ.get("BOT_USERNAME", "pro_saver_bot").strip()
PORT = int(os.environ.get("PORT", "8080").strip())

UPI_ID = "sathiyamoorthy8020-2@okhdfcbank"
UPI_NAME = "Sathiyamoorthy"

IST = pytz.timezone("Asia/Kolkata")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("SaveRestrictedBot")

# Main Pyrogram Bot Client
bot = Client(
    "pro_saver_master_session",
    api_id=DEFAULT_API_ID,
    api_hash=DEFAULT_API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

db_lock = asyncio.Lock()
login_states = {}
task_queue = asyncio.Queue()

PLANS = {
    "Standard": {"week": 50, "month": 180, "limit": 50},
    "Premium": {"week": 80, "month": 280, "limit": 100},
    "Ultimate": {"week": 130, "month": 450, "limit": 999999}
}

# ----------------- THREADED HTTP KEEP-ALIVE SERVER -----------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Save Restricted Bot 100% Active 24/7!")

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        logger.info(f"24/7 Health Server on port {PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health Server: {e}")

# ----------------- UPI QR GENERATOR -----------------
def generate_upi_qr(amount, note="VIP Plan"):
    try:
        import qrcode
        upi_url = f"upi://pay?pa={UPI_ID}&pn={UPI_NAME}&am={amount}&cu=INR&tn={note}"
        qr = qrcode.make(upi_url)
        buf = BytesIO()
        qr.save(buf, format="PNG")
        buf.seek(0)
        return buf
    except Exception as e:
        logger.error(f"QR Error: {e}")
        return None

# ----------------- DATABASE MANAGEMENT -----------------
async def init_db():
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    plan TEXT DEFAULT 'Free',
                    plan_expiry TEXT,
                    daily_limit INTEGER DEFAULT 5,
                    daily_used INTEGER DEFAULT 0,
                    bonus_credits INTEGER DEFAULT 0,
                    streak_count INTEGER DEFAULT 0,
                    last_bonus_date TEXT,
                    referred_by INTEGER,
                    ref_count INTEGER DEFAULT 0,
                    session TEXT,
                    custom_api_id INTEGER,
                    custom_api_hash TEXT,
                    doc_thumb TEXT,
                    custom_caption TEXT,
                    file_filter TEXT DEFAULT 'all',
                    rate_limit_until TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS file_hashes (
                    hash TEXT PRIMARY KEY,
                    user_id INTEGER,
                    file_name TEXT,
                    created_at TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS clone_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    source TEXT,
                    target TEXT,
                    total INTEGER,
                    done INTEGER,
                    status TEXT,
                    started_at TEXT
                )
            """)
            await db.commit()

async def get_user(user_id, name="பயனர்"):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            user = await cursor.fetchone()
            if not user:
                plan = "Admin" if user_id == ADMIN_ID else "Free"
                limit = 999999 if user_id == ADMIN_ID else 5
                await db.execute(
                    "INSERT INTO users (user_id, name, plan, daily_limit, daily_used) VALUES (?, ?, ?, ?, 0)",
                    (user_id, name, plan, limit)
                )
                await db.commit()
                cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
                user = await cursor.fetchone()
            return dict(user)

async def update_user(user_id, **kwargs):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            for k, v in kwargs.items():
                await db.execute(f"UPDATE users SET {k} = ? WHERE user_id = ?", (v, user_id))
            await db.commit()

async def deduct_usage(user_id):
    if user_id == ADMIN_ID:
        return True
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT plan, daily_limit, daily_used, bonus_credits FROM users WHERE user_id = ?", (user_id,))
            u = await cursor.fetchone()
            if not u:
                return False
            if "Unlimited" in u["plan"] or u["plan"] in ["Ultimate", "Admin"]:
                return True
            if u["daily_used"] < u["daily_limit"]:
                await db.execute("UPDATE users SET daily_used = daily_used + 1 WHERE user_id = ?", (user_id,))
                await db.commit()
                return True
            elif u["bonus_credits"] > 0:
                await db.execute("UPDATE users SET bonus_credits = bonus_credits - 1 WHERE user_id = ?", (user_id,))
                await db.commit()
                return True
            return False

async def is_duplicate(user_id, file_path):
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    file_hash = h.hexdigest()
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            cursor = await db.execute("SELECT 1 FROM file_hashes WHERE hash = ? AND user_id = ?", (file_hash, user_id))
            exists = await cursor.fetchone()
            if not exists:
                await db.execute(
                    "INSERT INTO file_hashes (hash, user_id, file_name, created_at) VALUES (?, ?, ?, ?)",
                    (file_hash, user_id, os.path.basename(file_path), datetime.now(IST).isoformat())
                )
                await db.commit()
                return False
            return True

async def check_rate_limit(user_id):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            cursor = await db.execute("SELECT rate_limit_until FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            if row and row[0]:
                try:
                    until = datetime.fromisoformat(row[0])
                    if datetime.now(IST) < until:
                        return False
                except Exception:
                    pass
            until = datetime.now(IST) + timedelta(seconds=5)
            await db.execute("UPDATE users SET rate_limit_until = ? WHERE user_id = ?", (until.isoformat(), user_id))
            await db.commit()
            return True

async def daily_reset_job():
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            await db.execute("UPDATE users SET daily_used = 0")
            await db.execute("DELETE FROM file_hashes WHERE created_at < ?",
                             ((datetime.now(IST) - timedelta(days=7)).isoformat(),))
            await db.commit()
    logger.info("Daily reset completed.")

# ----------------- QUEUE WORKER -----------------
async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            handler, args, kwargs = task
            await asyncio.wait_for(handler(*args, **kwargs), timeout=1800)
        except Exception as e:
            logger.error(f"Queue Error: {e}")
        finally:
            task_queue.task_done()

# ----------------- PROGRESS HELPER -----------------
async def progress_bar(current, total, client, message, start_time, action="📥 Downloading"):
    now = datetime.now()
    diff = (now - start_time).total_seconds()
    if round(diff % 4.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff if diff > 0 else 0
        progress = "[{0}{1}]".format(
            ''.join(["▰" for _ in range(int(percentage / 10))]),
            ''.join(["▱" for _ in range(10 - int(percentage / 10))])
        )
        text = (
            f"**{action}...**\n\n{progress} `{percentage:.1f}%`\n"
            f"⚡ **வேகம்:** `{speed / 1024 / 1024:.2f} MB/s`\n"
            f"📦 **அளவு:** `{current / 1024 / 1024:.2f} MB` / `{total / 1024 / 1024:.2f} MB`"
        )
        try:
            await message.edit_text(text)
        except Exception:
            pass

# ----------------- START HANDLER -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    user = await get_user(user_id, msg.from_user.first_name)

    args = msg.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            ref_id = int(args[1].replace("ref_", ""))
            if ref_id != user_id and not user.get("referred_by"):
                await update_user(user_id, referred_by=ref_id)
                async with db_lock:
                    async with aiosqlite.connect("bot_data.db") as db:
                        await db.execute("UPDATE users SET bonus_credits = bonus_credits + 2, ref_count = ref_count + 1 WHERE user_id = ?", (ref_id,))
                        await db.commit()
                try:
                    await bot.send_message(ref_id, f"🎉 **புதிய ரெஃபரல்!** {msg.from_user.first_name} உங்கள் லிங்க் மூலம் இணைந்தார். **+2 போனஸ்** கிடைத்தன!")
                except Exception:
                    pass
        except Exception:
            pass

    session_status = "✅ இணைக்கப்பட்டுள்ளது" if user.get("session") else "❌ இணைக்கப்படவில்லை"
    plan_display = "👑 உரிமையாளர் (Unlimited)" if user_id == ADMIN_ID else user["plan"]

    text = (
        f"👋 **வணக்கம் {msg.from_user.mention}!**\n\n"
        f"இது உலகின் நம்பர் 1 **Save Restricted Content & Cloner Bot**.\n\n"
        f"🏷️ **திட்டம்:** `{plan_display}`\n"
        f"🔑 **கணக்கு நிலை:** {session_status}\n"
        f"🎯 **மீடியா ஃபில்டர்:** `{user.get('file_filter', 'all').upper()}`\n\n"
        f"Restricted சேனலின் போஸ்ட் லிங்கை இங்கு அனுப்புங்கள் அல்லது கணக்கை இணைக்க கீழே உள்ள பட்டன்களைப் பயன்படுத்தவும்."
    )

    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ ஸ்ட்ரிங் செஷன் லாகின்", callback_data="login_session"), InlineKeyboardButton("📲 OTP லாகின்", callback_data="login_otp")],
        [InlineKeyboardButton("🔑 சொந்த API ID/Hash", callback_data="login_custom_api")],
        [InlineKeyboardButton("📦 விஐபி திட்டங்கள்", callback_data="btn_plans"), InlineKeyboardButton("📊 எனது திட்டம்", callback_data="btn_myplan")],
        [InlineKeyboardButton("🎁 தினசரி போனஸ்", callback_data="btn_bonus"), InlineKeyboardButton("🎁 இலவச ட்ரையல்", callback_data="btn_trial")],
        [InlineKeyboardButton("👥 ரெஃபர் & சம்பாதி", callback_data="btn_ref"), InlineKeyboardButton("❓ உதவி", callback_data="btn_help")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

# ----------------- LOGIN & USER COMMANDS -----------------
@bot.on_message(filters.command("login") & filters.private)
async def login_menu_cmd(_, msg: Message):
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ String Session Login", callback_data="login_session")],
        [InlineKeyboardButton("📲 Mobile Number + OTP", callback_data="login_otp")],
        [InlineKeyboardButton("🔑 Custom API ID/Hash", callback_data="login_custom_api")]
    ])
    await msg.reply_text("🔑 **கணக்கு இணைக்கும் முறையைத் தேர்ந்தெடுக்கவும்:**", reply_markup=buttons)

@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_, msg: Message):
    await update_user(msg.from_user.id, session=None)
    login_states.pop(msg.from_user.id, None)
    await msg.reply_text("🚪 கணக்கு துண்டிக்கப்பட்டது.")

@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_handler(_, msg: Message):
    user = await get_user(msg.from_user.id, msg.from_user.first_name)
    is_unlimited = (user["plan"] in ["Ultimate", "Admin"] or msg.from_user.id == ADMIN_ID)
    limit = "வரம்பற்றது" if is_unlimited else str(user["daily_limit"])
    used = user["daily_used"]
    bonus = user.get("bonus_credits", 0)
    left = "வரம்பற்றது" if is_unlimited else str(max(0, user["daily_limit"] - used) + bonus)
    reset_str = datetime.now(IST).strftime("%d-%m-%Y நள்ளிரவு 12:00 AM IST")
    text = (
        "📋 **உங்கள் திட்ட விவரங்கள்:**\n\n"
        f"👤 **பயனர்:** {user['name']}\n"
        f"⚡ **ஐடி:** `{user['user_id']}`\n"
        f"🏷️ **திட்டம்:** {'👑 Admin' if msg.from_user.id == ADMIN_ID else user['plan']}\n"
        f"📊 **இன்றைய பயன்பாடு:** {used}/{limit}\n"
        f"🎁 **போனஸ்:** {bonus}\n"
        f"🎯 **மீதம்:** {left}\n\n"
        f"🔄 **அடுத்த ரீசெட்:** {reset_str}"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬆️ Upgrade", callback_data="btn_plans"), InlineKeyboardButton("🎁 Trial", callback_data="btn_trial")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("plans") & filters.private)
async def plans_handler(_, msg: Message):
    text = (
        "💎 **விஐபி திட்டங்கள்** 💎\n\n"
        "🥈 **Standard:** 50 கோப்புகள்/நாள்\n"
        "   • ₹50/வாரம் | ₹180/மாதம்\n\n"
        "🥇 **Premium:** 100 கோப்புகள்/நாள்\n"
        "   • ₹80/வாரம் | ₹280/மாதம்\n\n"
        "🔷 **Ultimate:** ♾️ Unlimited + Channel Cloner\n"
        "   • ₹130/வாரம் | ₹450/மாதம்\n\n"
        "💳 **UPI:** `sathiyamoorthy8020-2@okhdfcbank`\n"
        "பணம் செலுத்திய screenshot-ஐ @admin-க்கு அனுப்பவும்."
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Standard (₹180/மாதம்)", callback_data="buy_Standard_month")],
        [InlineKeyboardButton("🥇 Premium (₹280/மாதம்)", callback_data="buy_Premium_month")],
        [InlineKeyboardButton("🔷 Ultimate (₹450/மாதம்)", callback_data="buy_Ultimate_month")],
        [InlineKeyboardButton("💬 அட்மின்", url=f"tg://user?id={ADMIN_ID}")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("trial") & filters.private)
async def trial_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Free" and msg.from_user.id != ADMIN_ID:
        await msg.reply_text("⚠️ நீங்கள் ஏற்கனவே விஐபி / ட்ரையல் திட்டத்தில் உள்ளீர்கள்.")
        return
    await update_user(msg.from_user.id, plan="Trial (Standard)", daily_limit=50)
    await msg.reply_text("🎁 **வாழ்த்துகள்!** 1-நாள் Standard Trial activated!")

@bot.on_message(filters.command("bonus") & filters.private)
async def bonus_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    today = datetime.now(IST).date()
    last_date_str = user.get("last_bonus_date")
    streak = user.get("streak_count", 0) or 0
    if last_date_str:
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        if last_date == today:
            await msg.reply_text("⚠️ இன்றைய போனஸ் ஏற்கனவே பெறப்பட்டது.")
            return
        elif last_date == today - timedelta(days=1):
            streak += 1
        else:
            streak = 1
    else:
        streak = 1
    await update_user(
        msg.from_user.id,
        last_bonus_date=today.strftime("%Y-%m-%d"),
        streak_count=streak,
        bonus_credits=(user.get("bonus_credits", 0) or 0) + 1
    )
    reward_text = f"🎉 **போனஸ்!**\n🔥 Streak: `{streak}`\n🎁 +1 Bonus"
    if streak >= 30 and user["plan"] == "Free":
        await update_user(msg.from_user.id, plan="Standard", daily_limit=50)
        reward_text += "\n\n🏆 30 நாள்! இலவச Standard!"
    await msg.reply_text(reward_text)

@bot.on_message(filters.command("referral") & filters.private)
async def referral_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{msg.from_user.id}"
    text = (
        "👥 **ரெஃபர் & சம்பாதி**\n\n"
        f"`{ref_link}`\n\n"
        f"• அழைத்தவர்கள்: `{user.get('ref_count', 0)}`\n"
        f"• போனஸ்: `{user.get('bonus_credits', 0)}`\n\n"
        "💡 +2 கூடுதல் download!"
    )
    await msg.reply_text(text)

@bot.on_message(filters.command("filter") & filters.private)
async def filter_handler(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2 or args[1].lower() not in ["all", "video", "doc", "audio", "photo"]:
        await msg.reply_text("பயன்பாடு: `/filter <all|video|doc|audio|photo>`")
        return
    await update_user(msg.from_user.id, file_filter=args[1].lower())
    await msg.reply_text(f"🎯 Filter: `{args[1].upper()}`")

@bot.on_message(filters.command("setcaption") & filters.private)
async def setcaption_handler(_, msg: Message):
    caption = msg.text.partition(" ")[2].strip()
    if not caption:
        await msg.reply_text("பயன்பாடு: `/setcaption your caption`")
        return
    await update_user(msg.from_user.id, custom_caption=caption)
    await msg.reply_text(f"✅ Caption: `{caption}`")

@bot.on_message(filters.command("delcaption") & filters.private)
async def delcaption_handler(_, msg: Message):
    await update_user(msg.from_user.id, custom_caption=None)
    await msg.reply_text("🗑️ Caption நீக்கப்பட்டது.")

@bot.on_message(filters.command("setthumb") & filters.private)
async def setthumb_handler(_, msg: Message):
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("படத்திற்கு ரிப்ளை செய்யவும்.")
        return
    os.makedirs("thumbnails", exist_ok=True)
    path = f"thumbnails/thumb_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=path)
    await update_user(msg.from_user.id, doc_thumb=path)
    await msg.reply_text("✅ Thumbnail saved!")

@bot.on_message(filters.command("delthumb") & filters.private)
async def delthumb_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]):
        try:
            os.remove(user["doc_thumb"])
        except Exception:
            pass
    await update_user(msg.from_user.id, doc_thumb=None)
    await msg.reply_text("🗑️ Thumbnail deleted.")

@bot.on_message(filters.command("autojoin") & filters.private)
async def autojoin_handler(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("பயன்பாடு: `/autojoin <channel>`")
        return
    user = await get_user(msg.from_user.id)
    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login`.")
        return
    target = args[1].replace("https://t.me/", "").replace("@", "")
    user_client = Client(f"join_{msg.from_user.id}", session_string=user["session"], in_memory=True)
    await user_client.start()
    try:
        await user_client.join_chat(target)
        await msg.reply_text(f"✅ `{target}`-ல் join ஆனோம்!")
    except UserAlreadyParticipant:
        await msg.reply_text(f"ℹ️ ஏற்கனவே join ஆகியுள்ளீர்கள்.")
    except Exception as e:
        await msg.reply_text(f"❌ Error: {e}")
    finally:
        await user_client.stop()

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(_, msg: Message):
    if msg.from_user.id in login_states:
        login_states.pop(msg.from_user.id, None)
        await msg.reply_text("❌ ரத்து செய்யப்பட்டது.")
    else:
        await msg.reply_text("எதுவும் இயங்கவில்லை.")

# ----------------- ADMIN COMMANDS -----------------
@bot.on_message(filters.command("ap") & filters.user(ADMIN_ID))
async def admin_ap(_, msg: Message):
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("Usage: `/ap <uid> <Standard|Premium|Ultimate>`")
        return
    t_uid = int(args[1])
    t_plan = args[2].capitalize()
    if t_plan not in PLANS:
        await msg.reply_text("Invalid plan.")
        return
    await update_user(t_uid, plan=t_plan, daily_limit=PLANS[t_plan]["limit"])
    await msg.reply_text(f"✅ `{t_uid}` → `{t_plan}`")

@bot.on_message(filters.command("rp") & filters.user(ADMIN_ID))
async def admin_rp(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("Usage: `/rp <uid>`")
        return
    await update_user(int(args[1]), plan="Free", daily_limit=5, daily_used=0)
    await msg.reply_text(f"✅ Reset done.")

@bot.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def admin_stats(_, msg: Message):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            c1 = await db.execute("SELECT COUNT(*) FROM users")
            total = (await c1.fetchone())[0]
            c2 = await db.execute("SELECT COUNT(*) FROM users WHERE plan != 'Free'")
            prem = (await c2.fetchone())[0]
    await msg.reply_text(f"📊 Total: `{total}`\nVIP: `{prem}`\nQueue: `{task_queue.qsize()}`")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def admin_broadcast(_, msg: Message):
    if not msg.reply_to_message:
        await msg.reply_text("Reply to a message with /broadcast")
        return
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            c = await db.execute("SELECT user_id FROM users")
            rows = await c.fetchall()
    sent = 0
    for (uid,) in rows:
        try:
            await msg.reply_to_message.copy(uid)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await msg.reply_text(f"✅ Broadcast to {sent} users.")

@bot.on_message(filters.command("activate") & filters.user(ADMIN_ID))
async def admin_activate(_, msg: Message):
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("Usage: `/activate <uid> <Standard|Premium|Ultimate>`")
        return
    t_uid = int(args[1])
    t_plan = args[2].capitalize()
    if t_plan not in PLANS:
        await msg.reply_text("Invalid plan.")
        return
    expiry = (datetime.now(IST) + timedelta(days=30)).isoformat()
    await update_user(t_uid, plan=t_plan, daily_limit=PLANS[t_plan]["limit"], plan_expiry=expiry)
    await msg.reply_text(f"✅ `{t_uid}` → `{t_plan}` (30 days)")
    try:
        await bot.send_message(t_uid, f"🎉 உங்கள் `{t_plan}` plan activated! 30 நாட்கள் செல்லும்.")
    except Exception:
        pass

# ----------------- CALLBACK HANDLER -----------------
@bot.on_callback_query()
async def callback_router(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data
    try:
        if data == "login_session":
            login_states[user_id] = {"step": "SESSION_STRING"}
            await query.message.reply_text("⚡ **Pyrogram String Session-ஐ paste செய்யவும்:**")
        elif data == "login_otp":
            login_states[user_id] = {"step": "PHONE", "api_id": DEFAULT_API_ID, "api_hash": DEFAULT_API_HASH}
            await query.message.reply_text("📲 **Phone number (+91...):**")
        elif data == "login_custom_api":
            login_states[user_id] = {"step": "API_ID"}
            await query.message.reply_text("🔑 **API ID:**")
        elif data == "btn_myplan":
            await myplan_handler(client, query.message)
        elif data == "btn_plans":
            await plans_handler(client, query.message)
        elif data == "btn_trial":
            await trial_handler(client, query.message)
        elif data == "btn_bonus":
            await bonus_handler(client, query.message)
        elif data == "btn_ref":
            await referral_handler(client, query.message)
        elif data == "btn_help":
            await query.message.reply_text(
                "❓ **உதவி:**\n\n"
                "1. `/login` - Account connect\n"
                "2. `/myplan` - Plan details\n"
                "3. `/plans` - VIP plans\n"
                "4. `/trial` - Free trial\n"
                "5. `/bonus` - Daily bonus\n"
                "6. `/referral` - Refer & earn\n"
                "7. `/filter` - Media filter\n"
                "8. `/setcaption` - Custom caption\n"
                "9. `/setthumb` - Thumbnail\n"
                "10. `/autojoin` - Auto join channel\n"
                "11. `/logout` - Disconnect"
            )
        elif data.startswith("buy_"):
            parts = data.split("_")
            plan = parts[1]
            duration = parts[2]
            amount = PLANS[plan][duration]
            qr_buf = generate_upi_qr(amount, f"{plan} {duration}")
            text = (
                f"💳 **{plan} - {duration}**\n\n"
                f"💰 **தொகை:** ₹{amount}\n"
                f"📱 **UPI ID:** `{UPI_ID}`\n\n"
                "1. கீழே உள்ள QR scan செய்யவும்\n"
                "2. பணம் செலுத்தவும்\n"
                "3. Screenshot @admin-க்கு அனுப்பவும்\n"
                "4. Admin activate செய்வார் ✅"
            )
            if qr_buf:
                await query.message.reply_photo(qr_buf, caption=text)
            else:
                await query.message.reply_text(text)
    except Exception as e:
        logger.error(f"Callback Error: {e}")
        try:
            await query.message.reply_text(f"❌ Error: {e}")
        except Exception:
            pass
    finally:
        try:
            await query.answer()
        except Exception:
            pass

# ----------------- TEXT DISPATCHER -----------------
@bot.on_message(filters.private & filters.text & ~filters.command(["start", "login", "logout", "myplan", "plans", "trial", "bonus", "referral", "filter", "setcaption", "delcaption", "setthumb", "delthumb", "cancel", "autojoin"]))
async def message_dispatcher(client: Client, msg: Message):
    user_id = msg.from_user.id

    # Login Flow
    if user_id in login_states:
        state = login_states[user_id]
        step = state.get("step")

        if step == "SESSION_STRING":
            try:
                test_client = Client("test_session", api_id=DEFAULT_API_ID, api_hash=DEFAULT_API_HASH, session_string=msg.text.strip(), in_memory=True)
                await test_client.connect()
                me = await test_client.get_me()
                await test_client.disconnect()
                await update_user(user_id, session=msg.text.strip())
                login_states.pop(user_id, None)
                await msg.reply_text(f"✅ Connected: **{me.first_name}** (`{me.id}`)")
            except Exception as e:
                await msg.reply_text(f"❌ Session Error: `{e}`")
            return

        if step == "API_ID":
            if not msg.text.strip().isdigit():
                await msg.reply_text("⚠️ Numbers மட்டும்:")
                return
            state["api_id"] = int(msg.text.strip())
            state["step"] = "API_HASH"
            await msg.reply_text("🔑 **API HASH:**")
            return

        if step == "API_HASH":
            state["api_hash"] = msg.text.strip()
            state["step"] = "PHONE"
            await msg.reply_text("📲 **Phone (+91...):**")
            return

        if step == "PHONE":
            phone = msg.text.strip().replace(" ", "")
            state["phone"] = phone
            try:
                temp_client = Client(f"auth_{user_id}", api_id=state.get("api_id", DEFAULT_API_ID), api_hash=state.get("api_hash", DEFAULT_API_HASH), in_memory=True)
                await temp_client.connect()
                code_obj = await temp_client.send_code(phone)
                state["client"] = temp_client
                state["phone_code_hash"] = code_obj.phone_code_hash
                state["step"] = "OTP"
                await msg.reply_text("📩 **OTP:** (உதா: `1 2 3 4 5`)")
            except Exception as e:
                login_states.pop(user_id, None)
                await msg.reply_text(f"❌ {e}")
            return

        if step == "OTP":
            try:
                temp_client = state["client"]
                await temp_client.sign_in(state["phone"], state["phone_code_hash"], msg.text.strip().replace(" ", ""))
                session_str = await temp_client.export_session_string()
                await temp_client.disconnect()
                await update_user(user_id, session=session_str)
                login_states.pop(user_id, None)
                await msg.reply_text("✅ Login successful!")
            except SessionPasswordNeeded:
                state["step"] = "2FA"
                await msg.reply_text("🔐 **2FA password:**")
            except Exception as e:
                await msg.reply_text(f"❌ {e}")
            return

        if step == "2FA":
            try:
                temp_client = state["client"]
                await temp_client.check_password(msg.text.strip())
                session_str = await temp_client.export_session_string()
                await temp_client.disconnect()
                await update_user(user_id, session=session_str)
                login_states.pop(user_id, None)
                await msg.reply_text("✅ Login successful with 2FA!")
            except Exception as e:
                await msg.reply_text(f"❌ {e}")
            return

    # Download Link Handler
    link = msg.text.strip()
    if "t.me/" in link:
        user = await get_user(user_id)
        if not user.get("session"):
            await msg.reply_text("⚠️ `/login` செய்யவும்.")
            return

        if not await check_rate_limit(user_id):
            await msg.reply_text("⏳ 5 வினாடிகள் காத்திருக்கவும்.")
            return

        if not await deduct_usage(user_id):
            await msg.reply_text("⛔ வரம்பு முடிந்தது. `/plans` பார்க்கவும்.")
            return

        status_msg = await msg.reply_text("⏳ வரிசையில்...")
        await task_queue.put((process_download_link, (user_id, link, status_msg.id), {}))

# ----------------- FORWARDED MESSAGE HANDLER -----------------
@bot.on_message(filters.forwarded & filters.private)
async def forwarded_handler(client: Client, msg: Message):
    if not msg.media and not msg.text:
        await msg.reply_text("❌ Media இல்லை.")
        return
    user_id = msg.from_user.id
    user = await get_user(user_id)
    if not user.get("session"):
        await msg.reply_text("⚠️ `/login` செய்யவும்.")
        return
    if not await deduct_usage(user_id):
        await msg.reply_text("⛔ வரம்பு முடிந்தது.")
        return
    status_msg = await msg.reply_text("⏳ Forwarded media வரிசையில்...")
    await task_queue.put((process_forwarded_media, (msg, user, status_msg.id), {}))

async def process_forwarded_media(orig_msg, user, status_msg_id):
    user_id = user["user_id"]
    try:
        file_path = await orig_msg.download()
        caption = user.get("custom_caption") or orig_msg.caption or ""
        thumb = user.get("doc_thumb") if (user.get("doc_thumb") and os.path.exists(user.get("doc_thumb"))) else None
        if orig_msg.document:
            await bot.send_document(user_id, file_path, caption=caption, thumb=thumb)
        elif orig_msg.video:
            await bot.send_video(user_id, file_path, caption=caption, thumb=thumb, supports_streaming=True)
        elif orig_msg.audio:
            await bot.send_audio(user_id, file_path, caption=caption, thumb=thumb)
        elif orig_msg.photo:
            await bot.send_photo(user_id, file_path, caption=caption)
        elif orig_msg.text:
            await bot.send_message(user_id, orig_msg.text)
        if os.path.exists(file_path):
            os.remove(file_path)
        await bot.delete_messages(user_id, status_msg_id)
    except Exception as e:
        await bot.edit_message_text(user_id, status_msg_id, f"❌ {e}")

# ----------------- DOWNLOAD ENGINE -----------------
async def process_download_link(user_id, link, status_msg_id):
    user = await get_user(user_id)
    custom_caption = user.get("custom_caption")
    file_filter = user.get("file_filter", "all")

    user_client = Client(
        f"worker_{user_id}",
        api_id=user.get("custom_api_id") or DEFAULT_API_ID,
        api_hash=user.get("custom_api_hash") or DEFAULT_API_HASH,
        session_string=user["session"],
        in_memory=True
    )
    try:
        await user_client.start()
    except Exception as e:
        await bot.edit_message_text(user_id, status_msg_id, f"❌ Session Error: `{e}`")
        return

    try:
        private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)

        if private_match:
            chat_id = int(f"-100{private_match.group(1)}")
            msg_id = int(private_match.group(2))
        elif public_match:
            chat_id = public_match.group(1)
            msg_id = int(public_match.group(2))
        else:
            await bot.edit_message_text(user_id, status_msg_id, "❌ Invalid link.")
            return

        await bot.edit_message_text(user_id, status_msg_id, f"🔍 Accessing channel...")
        try:
            chat_obj = await user_client.get_chat(chat_id)
            chat_title = getattr(chat_obj, "title", str(chat_id))
        except PeerIdInvalid:
            await bot.edit_message_text(user_id, status_msg_id, "❌ **சேனலில் join ஆகவும்!**")
            return
        except ChannelPrivate:
            await bot.edit_message_text(user_id, status_msg_id, "❌ **Private channel.**")
            return
        except Exception as e:
            await bot.edit_message_text(user_id, status_msg_id, f"❌ {e}")
            return

        await bot.edit_message_text(user_id, status_msg_id, f"📥 **{chat_title}** - ID: `{msg_id}`")
        source_msg = await user_client.get_messages(chat_id, msg_id)
        if not source_msg or source_msg.empty:
            await bot.edit_message_text(user_id, status_msg_id, "❌ Message not found.")
            return

        if file_filter == "video" and not source_msg.video:
            await bot.edit_message_text(user_id, status_msg_id, "⏭️ Video filter")
            return
        elif file_filter == "doc" and not source_msg.document:
            await bot.edit_message_text(user_id, status_msg_id, "⏭️ Doc filter")
            return
        elif file_filter == "photo" and not source_msg.photo:
            await bot.edit_message_text(user_id, status_msg_id, "⏭️ Photo filter")
            return

        caption = custom_caption or source_msg.caption or ""

        if source_msg.media:
            start_time = datetime.now()
            file_path = await user_client.download_media(
                source_msg,
                progress=progress_bar,
                progress_args=(bot, await bot.get_messages(user_id, status_msg_id), start_time, "📥 Downloading")
            )
            if not file_path or not os.path.exists(file_path):
                await bot.edit_message_text(user_id, status_msg_id, "❌ Download failed.")
                return

            if await is_duplicate(user_id, file_path):
                await bot.edit_message_text(user_id, status_msg_id, "⚠️ Duplicate file. Skipped.")
                os.remove(file_path)
                return

            await bot.edit_message_text(user_id, status_msg_id, "📤 Uploading...")
            thumb = user.get("doc_thumb") if (user.get("doc_thumb") and os.path.exists(user.get("doc_thumb"))) else None

            up_start = datetime.now()
            if source_msg.video:
                await bot.send_video(user_id, video=file_path, caption=caption, thumb=thumb, supports_streaming=True,
                                     progress=progress_bar, progress_args=(bot, await bot.get_messages(user_id, status_msg_id), up_start, "📤 Uploading"))
            elif source_msg.document:
                await bot.send_document(user_id, document=file_path, caption=caption, thumb=thumb,
                                        progress=progress_bar, progress_args=(bot, await bot.get_messages(user_id, status_msg_id), up_start, "📤 Uploading"))
            elif source_msg.photo:
                await bot.send_photo(user_id, photo=file_path, caption=caption)
            elif source_msg.audio:
                await bot.send_audio(user_id, audio=file_path, caption=caption, thumb=thumb)
            elif source_msg.voice:
                await bot.send_voice(user_id, voice=file_path, caption=caption)
            if os.path.exists(file_path):
                os.remove(file_path)
        elif source_msg.text:
            await bot.send_message(user_id, source_msg.text)
        else:
            await bot.edit_message_text(user_id, status_msg_id, "⚠️ No media.")
            return

        await bot.delete_messages(user_id, status_msg_id)
    except FloodWait as fw:
        await bot.edit_message_text(user_id, status_msg_id, f"⏳ FloodWait: {fw.value}s")
    except Exception as e:
        logger.error(f"Download Error: {e}")
        await bot.edit_message_text(user_id, status_msg_id, f"❌ {e}")
    finally:
        await user_client.stop()

# ----------------- INLINE MODE -----------------
@bot.on_inline_query()
async def inline_handler(_, query):
    text = query.query.strip()
    if "t.me/" in text:
        results = [
            InlineQueryResultArticle(
                id="1",
                title="📥 Download this link",
                description="Tap to send to bot and download",
                input_message_content=InputTextMessageContent(f"📥 {text}")
            )
        ]
        await query.answer(results, cache_time=1)

# ----------------- MAIN BOOTSTRAPPER -----------------
async def main():
    if not DEFAULT_API_ID or not DEFAULT_API_HASH or not BOT_TOKEN:
        logger.error("API_ID, API_HASH, BOT_TOKEN missing!")
        return

    await init_db()
    threading.Thread(target=run_health_server, daemon=True).start()

    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    asyncio.create_task(queue_worker())

    logger.info("Starting bot...")
    await bot.start()
    logger.info("Bot is online and polling!")
    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
