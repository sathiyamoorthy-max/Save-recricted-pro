import os
import re
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
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
    BotCommand
)
from pyrogram.errors import (
    SessionPasswordNeeded,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    PasswordHashInvalid,
    FloodWait,
    PeerIdInvalid,
    ChannelPrivate,
    AuthKeyUnregistered,
    UserAlreadyParticipant
)

# ----------------- CONFIGURATION -----------------
API_ID_RAW = os.environ.get("API_ID", "").strip()
DEFAULT_API_ID = int(API_ID_RAW) if API_ID_RAW.isdigit() else 0
DEFAULT_API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "").strip()
DEFAULT_ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else 0

BOT_USERNAME = os.environ.get("BOT_USERNAME", "pro_saver_bot").strip()
PORT = int(os.environ.get("PORT", "8080").strip())
IST = pytz.timezone("Asia/Kolkata")

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

def get_valid_thumb(path):
    if path and os.path.exists(path):
        return path
    return None

# ----------------- THREADED HTTP KEEP-ALIVE SERVER (RENDER 24/7) -----------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Save Restricted Bot is 100% Online!")

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        server.serve_forever()
    except Exception as e:
        print(f"[Health Server Warning] {e}")

# ----------------- DATABASE ENGINE -----------------
async def init_db():
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    plan TEXT DEFAULT 'Free',
                    daily_limit INTEGER DEFAULT 2,
                    daily_used INTEGER DEFAULT 0,
                    bonus_credits INTEGER DEFAULT 0,
                    streak_count INTEGER DEFAULT 0,
                    last_bonus_date TEXT,
                    referred_by INTEGER,
                    ref_count INTEGER DEFAULT 0,
                    lang TEXT DEFAULT 'ta',
                    session TEXT,
                    custom_api_id INTEGER,
                    custom_api_hash TEXT,
                    doc_thumb TEXT,
                    vid_thumb TEXT,
                    custom_caption TEXT,
                    file_filter TEXT DEFAULT 'all'
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    utr TEXT UNIQUE,
                    plan TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT
                )
            """)
            default_settings = [
                ("admin_id", str(DEFAULT_ADMIN_ID)),
                ("upi_id", "sathiyamoorthy8020-2@okhdfcbank"),
                ("upi_name", "Sathiya Moorthy (Indian Overseas Bank)"),
                ("price_standard", "50/wk | 180/mo"),
                ("price_premium", "80/wk | 280/mo"),
                ("price_ultimate", "130/wk | 450/mo")
            ]
            for k, v in default_settings:
                await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
            await db.commit()

async def get_setting(key, default=""):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cursor.fetchone()
            return row[0] if row else default

async def set_setting(key, value):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
            await db.commit()

async def get_admin_id():
    val = await get_setting("admin_id", str(DEFAULT_ADMIN_ID))
    return int(val) if val.isdigit() else DEFAULT_ADMIN_ID

async def get_user(user_id, name="User"):
    admin_id = await get_admin_id()
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            user = await cursor.fetchone()
            if not user:
                plan = "Ultimate" if user_id == admin_id else "Free"
                limit = 9999999 if user_id == admin_id else 2
                await db.execute(
                    "INSERT INTO users (user_id, name, plan, daily_limit, daily_used, lang) VALUES (?, ?, ?, ?, 0, 'ta')",
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
    admin_id = await get_admin_id()
    if user_id == admin_id:
        return True
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT plan, daily_limit, daily_used, bonus_credits FROM users WHERE user_id = ?", (user_id,))
            u = await cursor.fetchone()
            if not u:
                return False
            if u["plan"] == "Ultimate":
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

async def daily_reset_job():
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            await db.execute("UPDATE users SET daily_used = 0")
            await db.commit()
    print("[RESET] Daily usage reset completed at 12:00 AM IST.")

async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            handler, args, kwargs = task
            await asyncio.wait_for(handler(*args, **kwargs), timeout=2400)
        except Exception as e:
            print(f"[Queue Worker Error] {e}")
        finally:
            task_queue.task_done()

# ----------------- PEER RESOLVER ENGINE -----------------
async def resolve_target_peer(client: Client, chat_id):
    try:
        return await client.get_chat(chat_id)
    except Exception:
        pass
    try:
        async for dialog in client.get_dialogs(limit=100):
            if dialog.chat.id == chat_id:
                return dialog.chat
    except Exception:
        pass
    return None

# ----------------- START COMMAND -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    admin_id = await get_admin_id()
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
                    await bot.send_message(ref_id, f"🎉 **புதிய ரெஃபரல்!** {msg.from_user.first_name} உங்கள் லிங்க் மூலம் இணைந்தார். **+2 போனஸ் டவுன்லோட்கள்** கிடைத்தன!")
                except Exception:
                    pass
        except Exception:
            pass

    session_status = "✅ இணைக்கப்பட்டுள்ளது (Connected)" if user.get("session") else "❌ இணைக்கப்படவில்லை (Not Connected)"
    plan_display = "👑 Admin / Owner (Unlimited)" if user_id == admin_id else user["plan"]

    text = (
        f"👋 **வணக்கம் {msg.from_user.mention}!**\n\n"
        "இது உலகின் நம்பர் 1 **Save Restricted, Batch & Channel Cloner Bot**.\n\n"
        f"🏷️ **திட்டம் (Plan):** `{plan_display}`\n"
        f"🔑 **கணக்கு நிலை:** {session_status}\n"
        f"🎯 **மீடியா ஃபில்டர்:** `{user.get('file_filter', 'all').upper()}`\n\n"
        "Restricted சேனலின் போஸ்ட் லிங்கை இங்கு அனுப்புங்கள் அல்லது கீழே உள்ள பட்டன்களைப் பயன்படுத்தவும்."
    )
    buttons = [
        [InlineKeyboardButton("⚡ String Session Login", callback_data="login_session"), InlineKeyboardButton("📲 Mobile + OTP Login", callback_data="login_otp")],
        [InlineKeyboardButton("📦 VIP திட்டங்கள் & கட்டணம்", callback_data="btn_plans"), InlineKeyboardButton("📊 எனது திட்டம்", callback_data="btn_myplan")],
        [InlineKeyboardButton("🎁 இலவச ட்ரையல்", callback_data="btn_trial"), InlineKeyboardButton("🎁 தினசரி போனஸ்", callback_data="btn_bonus")],
        [InlineKeyboardButton("👥 ரெஃபர் & சம்பாதி", callback_data="btn_ref"), InlineKeyboardButton("💳 UPI பேமெண்ட்", callback_data="btn_buy")],
        [InlineKeyboardButton("🌐 மொழியை மாற்ற / Language", callback_data="btn_lang")]
    ]
    if user_id == admin_id:
        buttons.append([InlineKeyboardButton("⚙️ அட்மின் கண்ட்ரோல் பேனல்", callback_data="admin_panel")])

    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ----------------- LOGIN / LOGOUT -----------------
@bot.on_message(filters.command("login") & filters.private)
async def login_menu_cmd(_, msg: Message):
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ String Session Login (மிகவும் எளிதானது)", callback_data="login_session")],
        [InlineKeyboardButton("📲 Mobile Number + OTP Login", callback_data="login_otp")],
        [InlineKeyboardButton("🔑 Custom API ID + API Hash Login", callback_data="login_custom_api")]
    ])
    await msg.reply_text("🔑 **கணக்கு இணைக்கும் முறையைத் தேர்வுசெய்யவும்:**", reply_markup=buttons)

@bot.on_message(filters.command("skip") & filters.private)
async def skip_command_handler(_, msg: Message):
    login_states[msg.from_user.id] = {"step": "PHONE", "api_id": DEFAULT_API_ID, "api_hash": DEFAULT_API_HASH}
    await msg.reply_text("📲 நாட்டின் குறியீட்டுடன் (+91...) உங்கள் மொபைல் எண்ணை அனுப்பவும்:\nஉதாரணம்: `+919876543210`\nரத்து செய்ய: `/cancel`")

@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_, msg: Message):
    await update_user(msg.from_user.id, session=None)
    login_states.pop(msg.from_user.id, None)
    await msg.reply_text("🚪 உங்கள் கணக்கு வெற்றிகரமாக துண்டிக்கப்பட்டது.")

@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(_, msg: Message):
    btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("தமிழ் 🇮🇳", callback_data="set_ta"), InlineKeyboardButton("English 🌐", callback_data="set_en")],
        [InlineKeyboardButton("हिन्दी 🇮🇳", callback_data="set_hi")]
    ])
    await msg.reply_text("மொழியைத் தேர்ந்தெடுக்கவும் / Select Language:", reply_markup=btn)

@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_handler(_, msg: Message):
    admin_id = await get_admin_id()
    user = await get_user(msg.from_user.id, msg.from_user.first_name)
    is_unlimited = (user["plan"] == "Ultimate" or msg.from_user.id == admin_id)
    limit = "Unlimited" if is_unlimited else str(user["daily_limit"])
    used = user["daily_used"]
    bonus = user.get("bonus_credits", 0)
    left = "Unlimited" if is_unlimited else str(max(0, user["daily_limit"] - used) + bonus)

    reset_str = datetime.now(IST).strftime("%d-%m-%Y at 12:00 AM IST")
    text = (
        "📋 **பயனர் திட்ட விவரங்கள் (Plan Details):**\n\n"
        f"👤 **பெயர்:** {user['name']}\n"
        f"⚡ **பயனர் ID:** `{user['user_id']}`\n"
        f"🏷️ **திட்டம்:** {'👑 Admin (Unlimited)' if msg.from_user.id == admin_id else user['plan']}\n"
        f"📊 **இன்றைய பயன்பாடு:** {used}/{limit}\n"
        f"🎁 **போனஸ் கிரெடிட்:** {bonus}\n"
        f"🎯 **மீதமுள்ள வரம்பு:** {left}\n"
        f"🔄 **ரீசெட் நேரம்:** {reset_str}"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬆️ திட்டத்தை மாற்ற", callback_data="btn_plans"), InlineKeyboardButton("🎁 இலவச ட்ரையல்", callback_data="btn_trial")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("plans") & filters.private)
async def plans_handler(_, msg: Message):
    admin_id = await get_admin_id()
    upi_id = await get_setting("upi_id")
    upi_name = await get_setting("upi_name")
    p_std = await get_setting("price_standard")
    p_prm = await get_setting("price_premium")
    p_ult = await get_setting("price_ultimate")

    text = (
        "💎 **VIP கட்டணத் திட்டங்கள் (Premium Plans)** 💎\n\n"
        f"🥈 **Standard Plan:** 50 Files/Day\n• கட்டணம்: **₹{p_std}**\n\n"
        f"🥇 **Premium Plan:** 100 Files/Day\n• கட்டணம்: **₹{p_prm}**\n\n"
        f"🔷 **Ultimate Plan:** ♾️ Unlimited + Channel Cloner\n• கட்டணம்: **₹{p_ult}**\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💳 **பணம் செலுத்தும் விவரம் (UPI):**\n"
        f"• **UPI ID:** `{upi_id}`\n"
        f"• **பெயர்:** `{upi_name}`\n\n"
        "📌 பணம் செலுத்திய பிறகு **12-இலக்க UTR / Transaction ID-ஐ** இந்த பாட்டிற்கு மெசேஜாக அனுப்பவும். தானாக சரிபார்க்கப்பட்டு 2 நிமிடத்தில் செயல்படுத்தப்படும்!"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Buy Plan (UPI)", callback_data="btn_buy")],
        [InlineKeyboardButton("💬 அட்மின் தொடர்பு", url=f"tg://user?id={admin_id}")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("trial") & filters.private)
async def trial_handler(_, msg: Message):
    admin_id = await get_admin_id()
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Free" and msg.from_user.id != admin_id:
        await msg.reply_text("நீங்கள் ஏற்கனவே கட்டண அல்லது ட்ரையல் திட்டத்தில் உள்ளீர்கள்.")
        return
    await update_user(msg.from_user.id, plan="Trial (Standard)", daily_limit=50)
    await msg.reply_text("🎁 1-நாள் Standard இலவச ட்ரையல் செயல்படுத்தப்பட்டது!")

@bot.on_message(filters.command("bonus") & filters.private)
async def bonus_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    today = datetime.now(IST).date()
    last_date_str = user.get("last_bonus_date")
    streak = user.get("streak_count", 0) or 0

    if last_date_str:
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        if last_date == today:
            await msg.reply_text("⚠️ இன்றைய போனஸை ஏற்கனவே பெற்றுவிட்டீர்கள்! நாளை மீண்டும் வரவும்.")
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
    reward_text = f"🎉 தினசரி போனஸ் பெறப்பட்டது!\n🔥 ஸ்ட்ரீக்: {streak} நாட்கள்\n🎁 +1 போனஸ் டவுன்லோட் சேர்க்கப்பட்டது."
    if streak >= 30 and user["plan"] == "Free":
        await update_user(msg.from_user.id, plan="Standard", daily_limit=50)
        reward_text += "\n\n🏆 30-நாள் ஸ்ட்ரீக்! உங்களுக்கு இலவச Standard திட்டம் வழங்கப்பட்டது!"
    await msg.reply_text(reward_text)

@bot.on_message(filters.command("referral") & filters.private)
async def referral_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{msg.from_user.id}"
    text = (
        "👥 **ரெஃபரல் திட்டம் (Refer & Earn)**\n\n"
        f"உங்கள் ரெஃபரல் லிங்க்:\n`{ref_link}`\n\n"
        f"• அழைத்தவர்கள்: `{user.get('ref_count', 0)}`\n"
        f"• போனஸ் கிரெடிட்: `{user.get('bonus_credits', 0)}`\n\n"
        "💡 ஒவ்வொரு ரெஃபரலுக்கும் +2 கூடுதல் பதிவிறக்கங்கள் கிடைக்கும்!"
    )
    await msg.reply_text(text)

@bot.on_message(filters.command("filter") & filters.private)
async def filter_handler(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2 or args[1].lower() not in ["all", "video", "doc", "audio", "photo"]:
        await msg.reply_text("பயன்பாடு: `/filter <all|video|doc|audio|photo>`")
        return
    chosen = args[1].lower()
    await update_user(msg.from_user.id, file_filter=chosen)
    await msg.reply_text(f"🎯 மீடியா ஃபில்டர் மாற்றப்பட்டது: `{chosen.upper()}`")

@bot.on_message(filters.command("setcaption") & filters.private)
async def setcaption_handler(_, msg: Message):
    caption = msg.text.partition(" ")[2].strip()
    if not caption:
        await msg.reply_text("பயன்பாடு: `/setcaption உங்கள் தனிப்பட்ட கேப்ஷன்`")
        return
    await update_user(msg.from_user.id, custom_caption=caption)
    await msg.reply_text(f"✅ தனிப்பட்ட கேப்ஷன் சேமிக்கப்பட்டது:\n`{caption}`")

@bot.on_message(filters.command("delcaption") & filters.private)
async def delcaption_handler(_, msg: Message):
    await update_user(msg.from_user.id, custom_caption=None)
    await msg.reply_text("🗑️ தனிப்பட்ட கேப்ஷன் நீக்கப்பட்டது.")

@bot.on_message(filters.command("id") & filters.private)
async def id_handler(_, msg: Message):
    await msg.reply_text(f"🆔 உங்கள் Telegram ID: `{msg.from_user.id}`")

# ----------------- THUMBNAILS -----------------
@bot.on_message(filters.command("setthumb") & filters.private)
async def setthumb_handler(_, msg: Message):
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("படத்திற்கு ரிப்ளை செய்து `/setthumb` அனுப்பவும்.")
        return
    os.makedirs("thumbnails", exist_ok=True)
    path = f"thumbnails/thumb_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=path)
    await update_user(msg.from_user.id, doc_thumb=path)
    await msg.reply_text("✅ Document Thumbnail சேமிக்கப்பட்டது!")

@bot.on_message(filters.command("delthumb") & filters.private)
async def delthumb_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]):
        os.remove(user["doc_thumb"])
    await update_user(msg.from_user.id, doc_thumb=None)
    await msg.reply_text("🗑️ Document Thumbnail நீக்கப்பட்டது.")

@bot.on_message(filters.command("mythumb") & filters.private)
async def mythumb_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]):
        await msg.reply_photo(user["doc_thumb"], caption="உங்கள் Document Thumbnail")
    else:
        await msg.reply_text("❌ Document Thumbnail எதுவும் அமைக்கப்படவில்லை.")

@bot.on_message(filters.command("setvthumb") & filters.private)
async def setvthumb_handler(_, msg: Message):
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("படத்திற்கு ரிப்ளை செய்து `/setvthumb` அனுப்பவும்.")
        return
    os.makedirs("thumbnails", exist_ok=True)
    path = f"thumbnails/vthumb_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=path)
    await update_user(msg.from_user.id, vid_thumb=path)
    await msg.reply_text("✅ Video Thumbnail சேமிக்கப்பட்டது!")

@bot.on_message(filters.command("delvthumb") & filters.private)
async def delvthumb_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]):
        os.remove(user["vid_thumb"])
    await update_user(msg.from_user.id, vid_thumb=None)
    await msg.reply_text("🗑️ Video Thumbnail நீக்கப்பட்டது.")

@bot.on_message(filters.command("myvthumb") & filters.private)
async def myvthumb_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]):
        await msg.reply_photo(user["vid_thumb"], caption="உங்கள் Video Thumbnail")
    else:
        await msg.reply_text("❌ Video Thumbnail எதுவும் அமைக்கப்படவில்லை.")

# ----------------- BATCH & UNLIMITED CLONE ENGINE -----------------
@bot.on_message(filters.command("batch") & filters.private)
async def batch_handler(client: Client, msg: Message):
    admin_id = await get_admin_id()
    user = await get_user(msg.from_user.id)
    if user["plan"] == "Free" and msg.from_user.id != admin_id:
        await msg.reply_text("🔒 Batch வசதி Standard மற்றும் அதற்கு மேற்பட்ட பிளான்களில் மட்டுமே கிடைக்கும்.")
        return
    await msg.reply_text("📦 Batch Mode: தொடக்கம் மற்றும் முடிவு லிங்கை அனுப்பவும்:\n`https://t.me/c/1234567890/10-30`")

@bot.on_message(filters.command("clone") & filters.private)
async def clone_handler(client: Client, msg: Message):
    admin_id = await get_admin_id()
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Ultimate" and msg.from_user.id != admin_id:
        await msg.reply_text("⭐ சேனல் குளோனிங் வசதி Ultimate Plan அல்லது Admin-க்கு மட்டுமே கிடைக்கும்.")
        return
    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login` செய்யவும்.")
        return
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text(
            "📋 **பயன்பாடு (Usage):**\n"
            "`/clone <source_channel_id_or_link> <target_channel_id>`\n\n"
            "👉 **உதாரணம்:**\n"
            "`/clone -1004211421898 -1004430912409`"
        )
        return
    raw_src = args[1]
    dest_raw = args[2]

    match = re.search(r"t\.me/c/(\d+)", raw_src)
    src_id = int(f"-100{match.group(1)}") if match else (int(raw_src) if raw_src.lstrip("-").isdigit() else raw_src)
    dest_id = int(dest_raw) if dest_raw.lstrip("-").isdigit() else dest_raw

    await task_queue.put((execute_unlimited_clone, (client, msg, user, src_id, dest_id), {}))
    await msg.reply_text("🚀 **Unlimited Channel Cloning** வரிசையில் சேர்க்கப்பட்டது! பணி பின்னணியில் தொடங்குகிறது...")

async def execute_unlimited_clone(bot_client: Client, msg: Message, user: dict, src, dest):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "🚀 சேனல் குளோனிங் தொடங்குகிறது... (Unlimited Mode)")
    u_client = Client(f"ub_clone_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    await resolve_target_peer(u_client, src)
    await resolve_target_peer(u_client, dest)

    count = 0
    skipped = 0
    try:
        async for post in u_client.get_chat_history(src):
            try:
                if post.media:
                    f = await post.download()
                    if f and os.path.exists(f):
                        cap = user.get("custom_caption") or post.caption or ""
                        thumb_doc = get_valid_thumb(user.get("doc_thumb"))
                        thumb_vid = get_valid_thumb(user.get("vid_thumb"))

                        if post.document:
                            await u_client.send_document(dest, f, caption=cap, thumb=thumb_doc)
                        elif post.video:
                            await u_client.send_video(dest, f, caption=cap, thumb=thumb_vid, supports_streaming=True)
                        elif post.audio:
                            await u_client.send_audio(dest, f, caption=cap, thumb=thumb_doc)
                        elif post.voice:
                            await u_client.send_voice(dest, f, caption=cap)
                        elif post.photo:
                            await u_client.send_photo(dest, f, caption=cap)

                        os.remove(f)
                        count += 1
                elif post.text:
                    await u_client.send_message(dest, post.text)
                    count += 1
                else:
                    skipped += 1

                if count % 5 == 0:
                    await status_msg.edit_text(f"🔄 குளோன் செய்யப்படுகிறது... இதுவரை: **{count}** பதிவுகள் முடிந்தது.")
                await asyncio.sleep(1.5)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception as item_err:
                print(f"[Clone Item Error]: {item_err}")
                skipped += 1
                continue

        await status_msg.edit_text(f"✅ **குளோனிங் வெற்றிகரமாக முடிந்தது!**\n\n• மாற்றப்பட்டவை: `{count}` பதிவுகள்\n• தவிர்க்கப்பட்டவை: `{skipped}`")
    except Exception as e:
        await status_msg.edit_text(f"❌ குளோனிங் பிழை: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

@bot.on_message(filters.command("extract") & filters.private)
async def extract_handler(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login` செய்யவும்.")
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("பயன்பாடு: `/extract <channel_id_or_link>`")
        return
    target_raw = args[1]
    match = re.search(r"t\.me/c/(\d+)", target_raw)
    target = int(f"-100{match.group(1)}") if match else (int(target_raw) if target_raw.lstrip("-").isdigit() else target_raw)

    await task_queue.put((execute_extract, (client, msg, user, target), {}))
    await msg.reply_text("📑 பிளேலிஸ்ட் இன்டெக்ஸ் வரிசையில் சேர்க்கப்பட்டது...")

async def execute_extract(bot_client: Client, msg: Message, user: dict, target):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "📑 இன்டெக்ஸிங் தொடங்குகிறது...")
    u_client = Client(f"ub_ext_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()
    await resolve_target_peer(u_client, target)

    file_name = f"playlist_{user_id}.txt"
    count = 0
    try:
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(f"--- Playlist Index for {target} ---\n\n")
            async for post in u_client.get_chat_history(target):
                count += 1
                media_type = "Document" if post.document else "Video" if post.video else "Audio" if post.photo else "Text"
                f.write(f"{count}. {media_type} | ID: {post.id} | https://t.me/c/{str(target).replace('-100', '')}/{post.id}\n")
        await bot_client.send_document(user_id, file_name, caption=f"✅ {count} பதிவுகள் தொகுக்கப்பட்டன.")
        if os.path.exists(file_name):
            os.remove(file_name)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- ADMIN COMMANDS -----------------
@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client: Client, msg: Message):
    admin_id = await get_admin_id()
    if msg.from_user.id != admin_id:
        return
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Change UPI ID", callback_data="adm_set_upi"), InlineKeyboardButton("✏️ Change UPI Name", callback_data="adm_set_name")],
        [InlineKeyboardButton("💰 Standard விலை", callback_data="adm_set_p_std"), InlineKeyboardButton("💰 Premium விலை", callback_data="adm_set_p_prm")],
        [InlineKeyboardButton("💰 Ultimate விலை", callback_data="adm_set_p_ult"), InlineKeyboardButton("👑 Admin ID மாற்ற", callback_data="adm_set_admin")]
    ])
    await msg.reply_text("⚙️ **Owner / Admin Settings Dashboard:**\nஎந்த அமைப்பை மாற்ற வேண்டும்?", reply_markup=btns)

@bot.on_message(filters.command("ap"))
async def admin_ap(client: Client, msg: Message):
    admin_id = await get_admin_id()
    if msg.from_user.id != admin_id:
        return
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("பயன்பாடு: `/ap <user_id> <Standard|Premium|Ultimate>`")
        return
    t_uid = int(args[1])
    t_plan = args[2].capitalize()
    limits = {"Standard": 50, "Premium": 100, "Ultimate": 9999999}
    if t_plan not in limits:
        await msg.reply_text("தவறான திட்டம்! (Standard, Premium, Ultimate).")
        return
    await update_user(t_uid, plan=t_plan, daily_limit=limits[t_plan])
    await msg.reply_text(f"✅ User `{t_uid}`-ன் திட்டம் `{t_plan}` ஆக மாற்றப்பட்டது.")
    try:
        await bot.send_message(t_uid, f"🎉 உங்கள் திட்டம் மேம்படுத்தப்பட்டது: **{t_plan}**!")
    except Exception:
        pass

@bot.on_message(filters.command("rp"))
async def admin_rp(client: Client, msg: Message):
    admin_id = await get_admin_id()
    if msg.from_user.id != admin_id:
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("பயன்பாடு: `/rp <user_id>`")
        return
    t_uid = int(args[1])
    await update_user(t_uid, plan="Free", daily_limit=2, daily_used=0)
    await msg.reply_text(f"🔄 User `{t_uid}` இலவச நிலைக்கு ரீசெட் செய்யப்பட்டார்.")

@bot.on_message(filters.command("ps"))
async def admin_ps(client: Client, msg: Message):
    admin_id = await get_admin_id()
    if msg.from_user.id != admin_id:
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("பயன்பாடு: `/ps <user_id>`")
        return
    t_uid = int(args[1])
    u = await get_user(t_uid)
    await msg.reply_text(f"📋 User `{t_uid}`:\nPlan: {u['plan']}\nLimit: {u['daily_limit']}\nUsed: {u['daily_used']}\nBonus: {u['bonus_credits']}")

@bot.on_message(filters.command("stats"))
async def admin_stats(client: Client, msg: Message):
    admin_id = await get_admin_id()
    if msg.from_user.id != admin_id:
        return
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            c1 = await db.execute("SELECT COUNT(*) FROM users")
            total = (await c1.fetchone())[0]
            c2 = await db.execute("SELECT COUNT(*) FROM users WHERE plan != 'Free'")
            prem = (await c2.fetchone())[0]
    await msg.reply_text(f"📊 **Bot Stats:**\nமொத்த பயனர்கள்: `{total}`\nVIP பயனர்கள்: `{prem}`\nவரிசை பணிகள்: `{task_queue.qsize()}`")

@bot.on_message(filters.command("broadcast"))
async def admin_broadcast(client: Client, msg: Message):
    admin_id = await get_admin_id()
    if msg.from_user.id != admin_id:
        return
    if not msg.reply_to_message:
        await msg.reply_text("செய்திக்கு ரிப்ளை செய்து `/broadcast` அனுப்பவும்.")
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
    await msg.reply_text(f"✅ பிராட்காஸ்ட் நிறைவடைந்தது: {sent} பயனர்கள்.")

# ----------------- TEXT & LOGIN PROCESSOR -----------------
@bot.on_message(filters.text & filters.private)
async def text_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    text = msg.text.strip()
    admin_id = await get_admin_id()

    if text.startswith("/"):
        if text == "/cancel":
            login_states.pop(user_id, None)
            await msg.reply_text("செயல்பாடு ரத்து செய்யப்பட்டது.")
        return

    state = login_states.get(user_id)
    if state:
        step = state.get("step")

        # 1. String Session Login + Auto Admin Notification
        if step == "SESSION_STRING":
            try:
                status_temp = await msg.reply_text("🔍 Validating Session String...")
                test_client = Client(
                    f"test_auth_{user_id}",
                    api_id=DEFAULT_API_ID,
                    api_hash=DEFAULT_API_HASH,
                    session_string=text,
                    in_memory=True
                )
                await test_client.start()
                me = await test_client.get_me()
                await test_client.stop()
                await update_user(user_id, session=text)
                login_states.pop(user_id, None)
                await status_temp.edit_text(f"✅ **Account Login Successfully!**\n\n👤 **User:** {me.first_name} (`{me.id}`)\nஇப்போது எந்தவொரு Restricted போஸ்ட் லிங்க்கையும் பதிவிறக்கம் செய்யலாம்! 🚀")

                try:
                    await bot.send_message(
                        admin_id,
                        f"🔔 **புதிய பயனர் லாகின் அலர்ட் (String Session)!**\n\n"
                        f"👤 **Bot User:** {msg.from_user.mention} (`{user_id}`)\n"
                        f"🔑 **TG Account:** {me.first_name} (`{me.id}`)\n"
                        f"⏰ **Time:** `{datetime.now(IST).strftime('%Y-%m-%d %I:%M:%S %p')}`"
                    )
                except Exception:
                    pass
                return
            except Exception as e:
                await msg.reply_text(f"❌ தவறான Session String: {str(e)}\nசரியான Pyrogram Session String-ஐ அனுப்பவும்:")
                return

        if step == "CUSTOM_API_ID":
            if text.isdigit():
                login_states[user_id] = {"step": "CUSTOM_API_HASH", "api_id": int(text)}
                await msg.reply_text("இப்போது உங்கள் **API HASH**-ஐ அனுப்பவும்:")
            else:
                await msg.reply_text("❌ சரியான API ID எண்களை அனுப்பவும்:")
            return

        if step == "CUSTOM_API_HASH":
            login_states[user_id]["api_hash"] = text
            login_states[user_id]["step"] = "PHONE"
            await msg.reply_text("📲 நாட்டின் குறியீட்டுடன் மொபைல் எண்ணை அனுப்பவும் (எ.கா: `+919876543210`):")
            return

        if step == "PHONE":
            phone = text.replace(" ", "").replace("-", "")
            if not phone.startswith("+") or len(phone) < 10:
                await msg.reply_text("❌ சரியான மொபைல் எண்ணை நாட்டின் குறியீட்டுடன் அனுப்பவும்:")
                return
            api_id = state.get("api_id", DEFAULT_API_ID)
            api_hash = state.get("api_hash", DEFAULT_API_HASH)
            u_client = Client(f"login_sess_{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
            await u_client.connect()
            try:
                status_temp = await msg.reply_text("📩 OTP அனுப்பப்படுகிறது...")
                code = await u_client.send_code(phone)
                login_states[user_id].update({"step": "OTP", "client": u_client, "phone": phone, "hash": code.phone_code_hash})
                await status_temp.edit_text("📩 Telegram ஆப்பில் வந்த OTP-ஐ இடைவெளி விட்டு அனுப்பவும் (எ.கா: `1 2 3 4 5`):")
            except Exception as e:
                login_states.pop(user_id, None)
                if u_client.is_connected:
                    await u_client.disconnect()
                await msg.reply_text(f"❌ OTP பிழை: {str(e)}")
            return

        # 2. OTP Login + Auto Admin Notification
        if step == "OTP":
            otp = text.replace(" ", "").replace("-", "")
            u_client = state.get("client")
            if not u_client or not u_client.is_connected:
                await msg.reply_text("⚠️ அமர்வு காலாவதியானது. மீண்டும் `/login` செய்யவும்.")
                login_states.pop(user_id, None)
                return
            try:
                await u_client.sign_in(state["phone"], state["hash"], otp)
                s_str = await u_client.export_session_string()
                me = await u_client.get_me()
                await u_client.disconnect()
                await update_user(user_id, session=s_str, custom_api_id=state.get("api_id"), custom_api_hash=state.get("api_hash"))
                login_states.pop(user_id, None)
                await msg.reply_text("✅ **Account Login Successfully!** 🎉")

                try:
                    await bot.send_message(
                        admin_id,
                        f"🔔 **புதிய பயனர் லாகின் அலர்ட் (Phone OTP)!**\n\n"
                        f"👤 **Bot User:** {msg.from_user.mention} (`{user_id}`)\n"
                        f"📱 **Phone:** `{state['phone']}`\n"
                        f"🔑 **TG Name:** {me.first_name} (`{me.id}`)\n"
                        f"⏰ **Time:** `{datetime.now(IST).strftime('%Y-%m-%d %I:%M:%S %p')}`"
                    )
                except Exception:
                    pass
                return
            except SessionPasswordNeeded:
                login_states[user_id]["step"] = "2FA"
                await msg.reply_text("🔐 2-Step Verification பாஸ்வேர்டை அனுப்பவும்:")
            except Exception as e:
                login_states.pop(user_id, None)
                if u_client.is_connected:
                    await u_client.disconnect()
                await msg.reply_text(f"❌ லாகின் பிழை: {str(e)}")
            return

        if step == "2FA":
            u_client = state.get("client")
            try:
                await u_client.check_password(password=text)
                s_str = await u_client.export_session_string()
                me = await u_client.get_me()
                await u_client.disconnect()
                await update_user(user_id, session=s_str)
                login_states.pop(user_id, None)
                await msg.reply_text("✅ **Account Login Successfully with 2FA!** 🎉")

                try:
                    await bot.send_message(
                        admin_id,
                        f"🔔 **புதிய பயனர் லாகின் அலர்ட் (2FA Authenticated)!**\n\n"
                        f"👤 **Bot User:** {msg.from_user.mention} (`{user_id}`)\n"
                        f"🔑 **TG Name:** {me.first_name} (`{me.id}`)\n"
                        f"⏰ **Time:** `{datetime.now(IST).strftime('%Y-%m-%d %I:%M:%S %p')}`"
                    )
                except Exception:
                    pass
            except Exception as e:
                login_states.pop(user_id, None)
                if u_client.is_connected:
                    await u_client.disconnect()
                await msg.reply_text(f"❌ 2FA பிழை: {str(e)}")
            return

        if user_id == admin_id and step.startswith("SET_"):
            target_key = step.replace("SET_", "").lower()
            await set_setting(target_key, text)
            login_states.pop(user_id, None)
            await msg.reply_text(f"✅ Setting `{target_key}` மாற்றப்பட்டது:\n`{text}`")
            return

    # 3. Auto Payment UTR Verification
    utr_match = re.search(r"\b\d{12}\b", text)
    if utr_match and "t.me/" not in text:
        utr_num = utr_match.group(0)
        async with db_lock:
            async with aiosqlite.connect("bot_data.db") as db:
                await db.execute(
                    "INSERT OR IGNORE INTO payments (user_id, utr, plan, created_at) VALUES (?, ?, 'Manual/Pending', ?)",
                    (user_id, utr_num, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
                )
                await db.commit()
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ Standard", callback_data=f"adm_app_{user_id}_Standard_{utr_num}"),
                InlineKeyboardButton(f"✅ Premium", callback_data=f"adm_app_{user_id}_Premium_{utr_num}"),
                InlineKeyboardButton(f"✅ Ultimate", callback_data=f"adm_app_{user_id}_Ultimate_{utr_num}")
            ]
        ])
        await bot.send_message(
            admin_id,
            f"🔔 **புதிய கட்டண சரிபார்ப்பு கோரிக்கை (Payment Request)!**\n\n"
            f"👤 **User:** {msg.from_user.mention} (`{user_id}`)\n"
            f"🔢 **12-Digit UTR:** `{utr_num}`\n\n"
            "திட்டத்தை உறுதிசெய்ய கீழே உள்ள பட்டனை அழுத்தவும்:",
            reply_markup=buttons
        )
        await msg.reply_text("✅ உங்கள் UTR எண் பெறப்பட்டது. அட்மின் சரிபார்த்து 2 நிமிடத்தில் திட்டத்தை செயல்படுத்துவார்!")
        return

    # 4. Direct Telegram Link Handler
    if "t.me/" in text:
        user = await get_user(user_id)
        if not user.get("session"):
            btn = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ String Session", callback_data="login_session"), InlineKeyboardButton("📲 Phone + OTP", callback_data="login_otp")]
            ])
            await msg.reply_text("⚠️ முதலில் உங்கள் Telegram கணக்கை இணைக்கவும்:", reply_markup=btn)
            return
        await task_queue.put((execute_download, (client, msg, user_id), {}))
        await msg.reply_text("⏳ டவுன்லோட் பணி வரிசையில் சேர்க்கப்பட்டது...")

# ----------------- DOWNLOAD ENGINE (PEER AUTO-CACHE) -----------------
async def execute_download(bot_client: Client, msg: Message, user_id: int):
    user = await get_user(user_id)
    if not user.get("session"):
        await bot_client.send_message(user_id, "⚠️ முதலில் `/login` செய்யவும்.")
        return

    match = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)(?:-(\d+))?", msg.text.strip())
    if not match:
        await bot_client.send_message(user_id, "❌ தவறான டெலிகிராம் லிங்க்.")
        return

    chat_raw = match.group(1)
    start_id = int(match.group(2))
    end_id = int(match.group(3)) if match.group(3) else start_id

    chat_id = int(f"-100{chat_raw}") if chat_raw.isdigit() else chat_raw
    filter_type = user.get("file_filter", "all")
    status_msg = await bot_client.send_message(user_id, "📥 டவுன்லோட் தொடங்குகிறது...")

    api_id = user.get("custom_api_id") or DEFAULT_API_ID
    api_hash = user.get("custom_api_hash") or DEFAULT_API_HASH

    u_client = Client(f"ub_exec_{user_id}", api_id=api_id, api_hash=api_hash, session_string=user["session"], in_memory=True)
    await u_client.connect()

    peer = await resolve_target_peer(u_client, chat_id)
    target_peer_id = peer.id if peer else chat_id

    downloaded_count = 0
    try:
        for cur_id in range(start_id, end_id + 1):
            if not await deduct_usage(user_id):
                await bot_client.send_message(user_id, "⛔ தினசரி வரம்பு முடிந்தது. Upgrade செய்யவும்.")
                break

            try:
                target = await u_client.get_messages(target_peer_id, cur_id)
                if not target or target.empty:
                    continue

                caption = user.get("custom_caption") or target.caption or ""
                thumb_doc = get_valid_thumb(user.get("doc_thumb"))
                thumb_vid = get_valid_thumb(user.get("vid_thumb"))

                if not target.media and target.text:
                    if filter_type in ["all", "doc"]:
                        await bot_client.send_message(user_id, target.text)
                        downloaded_count += 1
                    continue

                if filter_type == "video" and not target.video:
                    continue
                elif filter_type == "doc" and not target.document:
                    continue
                elif filter_type == "audio" and not (target.audio or target.voice):
                    continue
                elif filter_type == "photo" and not target.photo:
                    continue

                await status_msg.edit_text(f"📥 Downloading ID: {cur_id}...")
                f_path = await target.download()

                if not f_path or not os.path.exists(f_path):
                    continue

                await status_msg.edit_text(f"📤 Uploading ID: {cur_id}...")

                if target.document:
                    await bot_client.send_document(user_id, f_path, caption=caption, thumb=thumb_doc)
                elif target.video:
                    await bot_client.send_video(user_id, f_path, caption=caption, thumb=thumb_vid, supports_streaming=True)
                elif target.audio:
                    await bot_client.send_audio(user_id, f_path, caption=caption, thumb=thumb_doc)
                elif target.voice:
                    await bot_client.send_voice(user_id, f_path, caption=caption)
                elif target.photo:
                    await bot_client.send_photo(user_id, f_path, caption=caption)

                if os.path.exists(f_path):
                    os.remove(f_path)

                downloaded_count += 1
                await asyncio.sleep(1.5)

            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception as e:
                print(f"[Fetch Error ID {cur_id}]: {e}")
                continue

        if downloaded_count > 0:
            await status_msg.edit_text("✅ பதிவிறக்கம் வெற்றிகரமாக முடிந்தது!")
        else:
            await status_msg.edit_text(
                "⚠️ **மீடியா கிடைக்கவில்லை.**\n\n"
                "💡 **காரணம்:** உங்கள் கணக்கு (`User Account`) இந்த Private சேனலில் இணைந்திருக்க வேண்டும் (Joined/Member)."
            )
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- FORWARDED MESSAGES -----------------
@bot.on_message(filters.forwarded & filters.private)
async def forwarded_handler(client: Client, msg: Message):
    if not msg.media and not msg.text:
        await msg.reply_text("❌ இந்த செய்தியில் தரவிறக்க எதுவும் இல்லை.")
        return
    await task_queue.put((execute_forward_download, (client, msg, msg.from_user.id), {}))
    await msg.reply_text("⏳ பார்வர்ட் மீடியா வரிசையில் சேர்க்கப்பட்டது...")

async def execute_forward_download(bot_client: Client, msg: Message, user_id: int):
    user = await get_user(user_id)
    if not user.get("session"):
        await bot_client.send_message(user_id, "⚠️ முதலில் `/login` செய்யவும்.")
        return

    if not await deduct_usage(user_id):
        await bot_client.send_message(user_id, "⛔ உங்கள் தினசரி வரம்பு முடிந்தது.")
        return

    if not msg.media and msg.text:
        await bot_client.send_message(user_id, msg.text)
        return

    status_msg = await bot_client.send_message(user_id, "📥 பதிவிறக்கப்படுகிறது...")
    try:
        file_path = await msg.download()
        await status_msg.edit_text("📤 பதிவேற்றப்படுகிறது...")

        caption = user.get("custom_caption") or msg.caption or ""
        thumb_doc = get_valid_thumb(user.get("doc_thumb"))
        thumb_vid = get_valid_thumb(user.get("vid_thumb"))

        if msg.document:
            await bot_client.send_document(user_id, file_path, caption=caption, thumb=thumb_doc)
        elif msg.video:
            await bot_client.send_video(user_id, file_path, caption=caption, thumb=thumb_vid, supports_streaming=True)
        elif msg.audio:
            await bot_client.send_audio(user_id, file_path, caption=caption, thumb=thumb_doc)
        elif msg.voice:
            await bot_client.send_voice(user_id, file_path, caption=caption)
        elif msg.photo:
            await bot_client.send_photo(user_id, file_path, caption=caption)

        if os.path.exists(file_path):
            os.remove(file_path)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

# ----------------- CALLBACK BUTTONS ROUTER -----------------
@bot.on_callback_query()
async def cb_handler(client: Client, q: CallbackQuery):
    data = q.data
    user_id = q.from_user.id
    admin_id = await get_admin_id()

    if data == "btn_login":
        await login_menu_cmd(client, q.message)
    elif data == "login_session":
        login_states[user_id] = {"step": "SESSION_STRING"}
        await q.message.reply_text("⚡ உங்கள் **Pyrogram String Session**-ஐ இங்கு பேஸ்ட் செய்து அனுப்பவும்:\nரத்து செய்ய: `/cancel`")
    elif data == "login_otp":
        await skip_command_handler(client, q.message)
    elif data == "login_custom_api":
        login_states[user_id] = {"step": "CUSTOM_API_ID"}
        await q.message.reply_text("🔑 உங்கள் **Telegram API ID**-ஐ அனுப்பவும்:\nரத்து செய்ய: `/cancel`")
    elif data == "btn_plans":
        await plans_handler(client, q.message)
    elif data == "btn_myplan":
        await myplan_handler(client, q.message)
    elif data == "btn_trial":
        await trial_handler(client, q.message)
    elif data == "btn_bonus":
        await bonus_handler(client, q.message)
    elif data == "btn_ref":
        await referral_handler(client, q.message)
    elif data == "btn_lang":
        await lang_cmd(client, q.message)
    elif data.startswith("set_"):
        chosen_lang = data.replace("set_", "")
        await update_user(user_id, lang=chosen_lang)
        await q.answer("Language Updated!")
        await start_handler(client, q.message)
    elif data == "btn_buy":
        upi_id = await get_setting("upi_id")
        upi_name = await get_setting("upi_name")
        await q.message.reply_text(
            f"💳 **கட்டண விவரங்கள் (Payment Details):**\n\n"
            f"• **UPI ID:** `{upi_id}`\n"
            f"• **பெயர்:** `{upi_name}`\n\n"
            "பணம் செலுத்திய பிறகு **12-இலக்க UTR எண்ணை** இந்த சாட்டில் அனுப்பவும்; தானாக அட்மின் ஒப்புதல் பெற்று பிளான் தொடங்கும்!"
        )
    # Admin Settings Dashboard
    elif data == "admin_panel" and user_id == admin_id:
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Change UPI ID", callback_data="adm_set_upi"), InlineKeyboardButton("✏️ Change UPI Name", callback_data="adm_set_name")],
            [InlineKeyboardButton("💰 Standard விலை", callback_data="adm_set_p_std"), InlineKeyboardButton("💰 Premium விலை", callback_data="adm_set_p_prm")],
            [InlineKeyboardButton("💰 Ultimate விலை", callback_data="adm_set_p_ult"), InlineKeyboardButton("👑 Admin ID மாற்ற", callback_data="adm_set_admin")]
        ])
        await q.message.reply_text("⚙️ **Owner / Admin Settings Dashboard:**\nஎந்த அமைப்பை மாற்ற வேண்டும்?", reply_markup=btns)
    elif data == "adm_set_upi" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_UPI_ID"}
        await q.message.reply_text("புதிய **UPI ID**-ஐ அனுப்பவும்:")
    elif data == "adm_set_name" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_UPI_NAME"}
        await q.message.reply_text("புதிய **UPI Name**-ஐ அனுப்பவும்:")
    elif data == "adm_set_p_std" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_PRICE_STANDARD"}
        await q.message.reply_text("Standard திட்டத்தின் புதிய விலையை அனுப்பவும் (எ.கா: `50/wk | 180/mo`):")
    elif data == "adm_set_p_prm" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_PRICE_PREMIUM"}
        await q.message.reply_text("Premium திட்டத்தின் புதிய விலையை அனுப்பவும்:")
    elif data == "adm_set_p_ult" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_PRICE_ULTIMATE"}
        await q.message.reply_text("Ultimate திட்டத்தின் புதிய விலையை அனுப்பவும்:")
    elif data == "adm_set_admin" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_ADMIN_ID"}
        await q.message.reply_text("புதிய **Admin Telegram User ID**-ஐ அனுப்பவும்:")
    elif data.startswith("adm_app_") and user_id == admin_id:
        parts = data.split("_")
        target_uid = int(parts[2])
        plan_name = parts[3]
        utr_num = parts[4]
        limits = {"Standard": 50, "Premium": 100, "Ultimate": 9999999}
        await update_user(target_uid, plan=plan_name, daily_limit=limits.get(plan_name, 50))
        async with db_lock:
            async with aiosqlite.connect("bot_data.db") as db:
                await db.execute("UPDATE payments SET status = 'approved' WHERE utr = ?", (utr_num,))
                await db.commit()
        await q.message.edit_text(f"✅ User `{target_uid}`-க்கு `{plan_name}` திட்டம் வெற்றிகரமாக வழங்கப்பட்டது!")
        try:
            await bot.send_message(target_uid, f"🎉 உங்கள் கட்டணம் சரிபார்க்கப்பட்டது! **{plan_name} Plan** செயல்படுத்தப்பட்டது.")
        except Exception:
            pass
    await q.answer()

# ----------------- MAIN BOOTSTRAP -----------------
async def start_services():
    await init_db()
    asyncio.create_task(queue_worker())

    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    await bot.start()

    try:
        commands = [
            BotCommand("start", "Home"),
            BotCommand("login", "Connect Account (Session/OTP/API)"),
            BotCommand("skip", "Quick OTP Login"),
            BotCommand("logout", "Disconnect Account"),
            BotCommand("batch", "Batch Fetch"),
            BotCommand("clone", "Clone Entire Channel"),
            BotCommand("extract", "Extract Playlist Index"),
            BotCommand("filter", "Filter Media"),
            BotCommand("lang", "Change Language"),
            BotCommand("admin", "Admin Settings Panel"),
            BotCommand("setcaption", "Custom Caption & Watermark"),
            BotCommand("delcaption", "Delete Caption"),
            BotCommand("referral", "Refer & Earn"),
            BotCommand("bonus", "Daily Streak Bonus"),
            BotCommand("trial", "Free Trial"),
            BotCommand("myplan", "My Plan & Limits"),
            BotCommand("plans", "View VIP Plans"),
            BotCommand("id", "Get Telegram ID"),
            BotCommand("setthumb", "Set Document Thumbnail"),
            BotCommand("delthumb", "Delete Document Thumbnail"),
            BotCommand("mythumb", "View Document Thumbnail"),
            BotCommand("setvthumb", "Set Video Thumbnail"),
            BotCommand("delvthumb", "Delete Video Thumbnail"),
            BotCommand("myvthumb", "View Video Thumbnail")
        ]
        await bot.set_bot_commands(commands)
    except Exception as e:
        print(f"[Warning] Menu register: {e}")

    await idle()
    await bot.stop()

def main():
    if not DEFAULT_API_ID or not DEFAULT_API_HASH or not BOT_TOKEN:
        print("[FATAL ERROR] API_ID, API_HASH, அல்லது BOT_TOKEN அமைக்கப்படவில்லை!")
        return

    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_services())

if __name__ == "__main__":
    main()
