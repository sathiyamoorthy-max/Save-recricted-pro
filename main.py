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
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else 0

BOT_USERNAME = os.environ.get("BOT_USERNAME", "pro_saver_bot").strip()
PORT = int(os.environ.get("PORT", "8080").strip())

IST = pytz.timezone("Asia/Kolkata")

# Main Pyrogram Bot
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

# ----------------- MULTI-LANGUAGE ENGINE -----------------
LANG = {
    "ta": {
        "welcome": "👋 **வணக்கம் {name}!**\n\nஇது உலகின் நம்பர் 1 **Save Restricted, Batch & Channel Cloner Bot**.\n\n🏷️ **திட்டம்:** `{plan}`\n🔑 **கணக்கு நிலை:** {session}\n🎯 **மீடியா ஃபில்டர்:** `{filter}`\n\nRestricted சேனலின் போஸ்ட் லிங்கை இங்கு அனுப்புங்கள் அல்லது கணக்கை இணைக்க கீழே உள்ள பட்டன்களைப் பயன்படுத்தவும்.",
        "btn_session": "⚡ String Session Login",
        "btn_otp": "📲 Phone Number + OTP",
        "btn_custom_api": "🔑 Custom API ID/Hash",
        "btn_plans": "📦 VIP திட்டங்கள்",
        "btn_myplan": "📊 எனது திட்டம்",
        "btn_trial": "🎁 இலவச ட்ரையல்",
        "btn_bonus": "🎁 தினசரி போனஸ்",
        "btn_ref": "👥 ரெஃபர் & சம்பாதி",
        "btn_lang": "🌐 மொழியை மாற்ற",
        "not_connected": "⚠️ முதலில் உங்கள் Telegram கணக்கை இணைக்கவும். கீழே உள்ள பட்டனை அழுத்தவும்:",
        "limit_reached": "⛔ உங்கள் இன்றைய வரம்பு முடிந்தது. கூடுதல் பதிவிறக்கங்களுக்கு விஐபி பிளான் எடுக்கவும்.",
        "downloading": "📥 பதிவிறக்கப்படுகிறது... ID: {id}",
        "uploading": "📤 பதிவேற்றப்படுகிறது... ID: {id}",
        "success": "✅ பதிவிறக்கம் வெற்றிகரமாக நிறைவடைந்தது!",
        "error": "❌ பிழை: {err}"
    },
    "en": {
        "welcome": "👋 **Hello {name}!**\n\nWelcome to World's Best **Save Restricted, Batch & Channel Cloner Bot**.\n\n🏷️ **Plan:** `{plan}`\n🔑 **Account Status:** {session}\n🎯 **Media Filter:** `{filter}`\n\nSend any restricted post link to download, or connect your account using the buttons below.",
        "btn_session": "⚡ String Session Login",
        "btn_otp": "📲 Phone Number + OTP",
        "btn_custom_api": "🔑 Custom API ID/Hash",
        "btn_plans": "📦 VIP Plans",
        "btn_myplan": "📊 My Plan",
        "btn_trial": "🎁 Free Trial",
        "btn_bonus": "🎁 Daily Bonus",
        "btn_ref": "👥 Refer & Earn",
        "btn_lang": "🌐 Change Language",
        "not_connected": "⚠️ Please connect your Telegram account first. Tap the button below:",
        "limit_reached": "⛔ Daily limit reached. Upgrade to VIP for unlimited downloads.",
        "downloading": "📥 Downloading... ID: {id}",
        "uploading": "📤 Uploading... ID: {id}",
        "success": "✅ Download completed successfully!",
        "error": "❌ Error: {err}"
    },
    "hi": {
        "welcome": "👋 **नमस्ते {name}!**\n\nप्रतिबंधित सामग्री सेवर, बैच और चैनल क्लोनर बॉट में आपका स्वागत है।\n\n🏷️ **प्लान:** `{plan}`\n🔑 **सत्र स्थिति:** {session}\n🎯 **फ़िल्टर:** `{filter}`",
        "btn_session": "⚡ String Session लॉगिन",
        "btn_otp": "📲 फोन नंबर + OTP",
        "btn_custom_api": "🔑 कस्टम API ID/Hash",
        "btn_plans": "📦 वीआईपी प्लान्स",
        "btn_myplan": "📊 मेरा प्लान",
        "btn_trial": "🎁 फ्री ट्रायल",
        "btn_bonus": "🎁 दैनिक बोनस",
        "btn_ref": "👥 रेफर और कमाएं",
        "btn_lang": "🌐 भाषा बदलें",
        "not_connected": "⚠️ कृपया पहले अपना टेलीग्राम खाता जोड़ें।",
        "limit_reached": "⛔ दैनिक सीमा समाप्त। असीमित के लिए अपग्रेड करें।",
        "downloading": "📥 डाउनलोड हो रहा है... ID: {id}",
        "uploading": "📤 अपलोड हो रहा है... ID: {id}",
        "success": "✅ सफलतापूर्वक पूर्ण हुआ!",
        "error": "❌ त्रुटि: {err}"
    }
}

# ----------------- THREADED HTTP KEEP-ALIVE SERVER -----------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Save Restricted Bot is 100% Active 24/7!")

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        print(f"[*] Threaded Ping Server running on port {PORT}")
        server.serve_forever()
    except Exception as e:
        print(f"[Health Server Warning] {e}")

# ----------------- DATABASE MANAGEMENT -----------------
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
            await db.commit()

async def get_user(user_id, name="User"):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            user = await cursor.fetchone()
            if not user:
                plan = "Ultimate" if user_id == ADMIN_ID else "Free"
                limit = 9999999 if user_id == ADMIN_ID else 2
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
    if user_id == ADMIN_ID:
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
    print("[RESET] 12:00 AM IST Daily usage reset completed.")

# ----------------- ASYNC QUEUE WORKER -----------------
async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            handler, args, kwargs = task
            await asyncio.wait_for(handler(*args, **kwargs), timeout=1800)
        except Exception as e:
            print(f"[Queue Worker Error] {e}")
        finally:
            task_queue.task_done()

# ----------------- START & USER COMMANDS -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    user = await get_user(user_id, msg.from_user.first_name)
    lang = user.get("lang", "ta")
    t = LANG.get(lang, LANG["ta"])

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

    session_status = "✅ Connected" if user.get("session") else "❌ Not Connected"
    plan_display = "👑 Owner / Admin (Unlimited)" if user_id == ADMIN_ID else user["plan"]

    text = t["welcome"].format(
        name=msg.from_user.mention,
        plan=plan_display,
        session=session_status,
        filter=user.get("file_filter", "all").upper()
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(t["btn_session"], callback_data="login_session"), InlineKeyboardButton(t["btn_otp"], callback_data="login_otp")],
        [InlineKeyboardButton(t["btn_custom_api"], callback_data="login_custom_api")],
        [InlineKeyboardButton(t["btn_plans"], callback_data="btn_plans"), InlineKeyboardButton(t["btn_myplan"], callback_data="btn_myplan")],
        [InlineKeyboardButton(t["btn_bonus"], callback_data="btn_bonus"), InlineKeyboardButton(t["btn_trial"], callback_data="btn_trial")],
        [InlineKeyboardButton(t["btn_ref"], callback_data="btn_ref"), InlineKeyboardButton(t["btn_lang"], callback_data="btn_lang")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

# ----------------- ALL LOGIN FLOWS (SESSION, PHONE, CUSTOM API) -----------------
@bot.on_message(filters.command("login") & filters.private)
async def login_menu_cmd(_, msg: Message):
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ String Session Login (Fast & Safe)", callback_data="login_session")],
        [InlineKeyboardButton("📲 Mobile Number + OTP Login", callback_data="login_otp")],
        [InlineKeyboardButton("🔑 Custom API ID + API Hash Login", callback_data="login_custom_api")]
    ])
    await msg.reply_text(
        "🔑 **கணக்கு இணைக்கும் முறையைத் தேர்ந்தெடுக்கவும் (Choose Login Method):**\n\n"
        "1. **String Session Login:** மிக எளிதானது மற்றும் பாதுகாப்பானது. OTP தேவையில்லை.\n"
        "2. **Mobile Number + OTP:** பாட்டின் சாவிகள் மூலம் போன் நம்பர் & OTP லாகின்.\n"
        "3. **Custom API ID/Hash:** உங்கள் சொந்த Telegram API ID & Hash கொண்டு லாகின் செய்ய.",
        reply_markup=buttons
    )

@bot.on_message(filters.command("skip") & filters.private)
async def skip_command_handler(_, msg: Message):
    login_states[msg.from_user.id] = {
        "step": "PHONE",
        "api_id": DEFAULT_API_ID,
        "api_hash": DEFAULT_API_HASH
    }
    await msg.reply_text(
        "📲 **தயவுசெய்து உங்கள் மொபைல் எண்ணை நாட்டின் குறியீட்டுடன் (+91...) அனுப்பவும்:**\n\n"
        "உதாரணம்: `+919876543210`\n\n"
        "ரத்து செய்ய: `/cancel`"
    )

@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_, msg: Message):
    await update_user(msg.from_user.id, session=None)
    login_states.pop(msg.from_user.id, None)
    await msg.reply_text("🚪 உங்கள் கணக்கு இணைப்பு துண்டிக்கப்பட்டது.")

@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(_, msg: Message):
    btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("தமிழ் 🇮🇳", callback_data="set_ta"), InlineKeyboardButton("English 🌐", callback_data="set_en")],
        [InlineKeyboardButton("हिन्दी 🇮🇳", callback_data="set_hi")]
    ])
    await msg.reply_text("மொழியைத் தேர்ந்தெடுக்கவும் / Select Language:", reply_markup=btn)

@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_handler(_, msg: Message):
    user = await get_user(msg.from_user.id, msg.from_user.first_name)
    is_unlimited = (user["plan"] == "Ultimate" or msg.from_user.id == ADMIN_ID)
    limit = "Unlimited" if is_unlimited else str(user["daily_limit"])
    used = user["daily_used"]
    bonus = user.get("bonus_credits", 0)
    left = "Unlimited" if is_unlimited else str(max(0, user["daily_limit"] - used) + bonus)

    reset_str = datetime.now(IST).strftime("%d-%m-%Y at 12:00 AM IST")
    text = (
        "📋 **USER PLAN DATA :**\n\n"
        f"👤 **USER**       : {user['name']}\n"
        f"⚡ **USER ID**    : `{user['user_id']}`\n"
        f"🏷️ **PLAN**       : {'👑 Admin (Unlimited)' if msg.from_user.id == ADMIN_ID else user['plan']}\n"
        f"📊 **TODAY**      : {used}/{limit} USED\n"
        f"🎁 **BONUS**      : {bonus} CREDITS\n"
        f"🎯 **TOTAL LEFT** : {left}\n\n"
        f"🔄 **RESETS**     : {reset_str}"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬆️ Upgrade Plan", callback_data="btn_plans"), InlineKeyboardButton("🎁 Free Trial", callback_data="btn_trial")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("plans") & filters.private)
async def plans_handler(_, msg: Message):
    text = (
        "💎 **PREMIUM PLANS (100% UNLIMITED ACCESS)** 💎\n\n"
        "🥈 **Standard:** 50 files/day (₹50/wk | ₹180/mo)\n"
        "🥇 **Premium:** 100 files/day (₹80/wk | ₹280/mo)\n"
        "🔷 **Ultimate:** ♾️ Unlimited Downloads + Channel Cloner (₹130/wk | ₹450/mo)"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Buy Plan (UPI)", callback_data="btn_buy")],
        [InlineKeyboardButton("💬 Admin Contact", url=f"tg://user?id={ADMIN_ID}")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("trial") & filters.private)
async def trial_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Free" and msg.from_user.id != ADMIN_ID:
        await msg.reply_text("நீங்கள் ஏற்கனவே விஐபி அல்லது ட்ரையல் திட்டத்தில் உள்ளீர்கள்.")
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
    reward_text = f"🎉 Daily bonus claimed!\n🔥 Streak: {streak} days\n🎁 +1 bonus download added."
    if streak >= 30 and user["plan"] == "Free":
        await update_user(msg.from_user.id, plan="Standard", daily_limit=50)
        reward_text += "\n\n🏆 30-day streak! You got free Standard plan!"
    await msg.reply_text(reward_text)

@bot.on_message(filters.command("referral") & filters.private)
async def referral_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{msg.from_user.id}"
    text = (
        "👥 **REFER & EARN PROGRAM**\n\n"
        "உங்கள் தனித்துவமான ரெஃபரல் லிங்க்:\n"
        f"`{ref_link}`\n\n"
        f"• அழைத்த நபர்கள்: `{user.get('ref_count', 0)}`\n"
        f"• போனஸ் கிரெடிட்கள்: `{user.get('bonus_credits', 0)}`\n\n"
        "💡 ஒவ்வொரு ரெஃபரலுக்கும் +2 கூடுதல் டவுன்லோட்கள் கிடைக்கும்!"
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
    await msg.reply_text(f"🆔 Your Telegram ID: `{msg.from_user.id}`")

# ----------------- THUMBNAIL ENGINE -----------------
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
    if user.get("doc_thumb") and
