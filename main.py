import os
import re
import time
import asyncio
import threading
from urllib.parse import quote
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
    ChannelPrivate
)

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

LANG = {
    "ta": {
        "welcome": (
            "🌟 **வணக்கம் {name}! Welcome to Save Restricted Bot ⚡**\n\n"
            "🏷️ **உங்கள் திட்டம்:** `{plan}`\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🚀 **இந்த பாட் என்ன செய்யும்?**\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📩 **Restricted Content Downloader:**\n"
            "எந்தவொரு Telegram பிரைவேட் / பப்ளிக் போஸ்ட் லிங்க்கையும் அனுப்புங்கள், உடனே ஃபைல் கிடைக்கும்!\n\n"
            "📦 **Range Batch பதிவிறக்கம் *(Standard+)*:**\n"
            "ஒரே நேரத்தில் 1 முதல் 1000 பதிவுகள் வரை ஃபில்டர் வசதியுடன் பதிவிறக்கலாம்.\n\n"
            "🌐 **Ultimate Cloner *(Ultimate Only)*:**\n"
            "முழு பிரைவேட் சேனல் கோப்புகளையும் உங்கள் சாட்டிற்கோ அல்லது உங்கள் சேனலுக்கோ வரம்பின்றி மாற்றலாம்!\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🎯 **Quick Start:**\n"
            "1️⃣ லிங்க் அனுப்புங்கள் ➔ ஃபைல் பெறுங்கள்\n"
            "2️⃣ `/trial` — 24 மணி நேர இலவச Standard ட்ரையல்\n"
            "3️⃣ `/plans` — VIP திட்டங்கள்\n"
            "4️⃣ புரோமோ கோட் உள்ளதா? ➔ `/redeem CODE`"
        ),
        "btn_buy": "🛒 BUY SUBSCRIPTION NOW 💰",
        "btn_trial": "🎁 Free Trial (24hr)",
        "btn_plans": "💼 View Plans",
        "btn_login": "🔑 Connect Account",
        "btn_myplan": "📊 My Plan",
        "btn_settings": "⚙️ Settings",
        "btn_lang": "🌐 மொழியை மாற்ற / Language",
        "not_connected": "⚠️ முதலில் `/login` செய்து உங்கள் Telegram கணக்கை இணைக்கவும்.",
        "limit_reached": "⛔ உங்கள் வரம்பு முடிந்தது. வரம்பற்ற பதிவிறக்கத்திற்கு `/plans` பார்க்கவும்."
    },
    "en": {
        "welcome": (
            "🌟 **Hey {name}! Welcome to Save Restricted Bot ⚡**\n\n"
            "🏷️ **Your Plan:** `{plan}`\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🚀 **What this bot does:**\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📩 **Fetch Restricted Content:**\n"
            "Paste any Telegram post link — even from private/restricted channels & groups!\n\n"
            "📦 **Batch & Range Fetch *(Standard+)*:**\n"
            "Fetch 1 to 1000 messages at once with smart media filter (video/audio/pdf/photo...)\n\n"
            "🌐 **Channel & Group Cloner *(Ultimate)*:**\n"
            "Clone entire private channel media straight into your chats or destination channels!\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🎯 **Quick Start:**\n"
            "1️⃣ Paste a Telegram link → get the file\n"
            "2️⃣ `/trial` — FREE 1-day Standard access\n"
            "3️⃣ `/plans` — Upgrade VIP Plans\n"
            "4️⃣ Have a promo code? → `/redeem CODE`"
        ),
        "btn_buy": "🛒 BUY SUBSCRIPTION NOW 💰",
        "btn_trial": "🎁 Free Trial (24hr)",
        "btn_plans": "💼 View Plans",
        "btn_login": "🔑 Connect Account",
        "btn_myplan": "📊 My Plan",
        "btn_settings": "⚙️ Settings",
        "btn_lang": "🌐 Change Language",
        "not_connected": "⚠️ Please connect your Telegram account first using `/login`.",
        "limit_reached": "⛔ Daily limit reached. Check `/plans` for unlimited access."
    },
    "hi": {
        "welcome": (
            "🌟 **नमस्ते {name}! Save Restricted Bot में आपका स्वागत है ⚡**\n\n"
            "🏷️ **आपका प्लान:** `{plan}`\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🚀 **यह बॉट क्या कर सकता है?**\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📩 **प्रतिबंधित सामग्री डाउनलोड:**\n"
            "किसी भी प्राइवेट या रिस्ट्रिक्टेड चैनल की लिंक भेजें और फाइल तुरंत पाएं!\n\n"
            "📦 **बैच डाउनलोड *(Standard+)*:**\n"
            "एक साथ 1 से 1000 फाइलों को स्मार्ट मीडिया फिल्टर के साथ डाउनलोड करें।\n\n"
            "🌐 **अल्टीमेट क्लोनर *(Ultimate)*:**\n"
            "पूरे प्राइवेट चैनल को अपने चैट या नए चैनल में आसानी से क्लोन करें!"
        ),
        "btn_buy": "🛒 सब्स्क्रिप्शन खरीदें 💰",
        "btn_trial": "🎁 फ्री ट्रायल (24hr)",
        "btn_plans": "💼 प्लान्स देखें",
        "btn_login": "🔑 अकाउंट कनेक्ट करें",
        "btn_myplan": "📊 मेरा प्लान",
        "btn_settings": "⚙️ सेटिंग्स",
        "btn_lang": "🌐 भाषा बदलें",
        "not_connected": "⚠️ कृपया पहले `/login` करके अपना टेलीग्राम अकाउंट जोड़ें।",
        "limit_reached": "⛔ दैनिक सीमा समाप्त। अनलिमिटेड के लिए `/plans` देखें।"
    }
}

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Save Restricted Ultra Engine Online 24/7!")

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        server.serve_forever()
    except Exception as e:
        print(f"[Health Server Warning] {e}")

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
                    duration TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    plan TEXT,
                    days INTEGER,
                    uses_left INTEGER
                )
            """)
            default_settings = [
                ("admin_id", str(DEFAULT_ADMIN_ID)),
                ("upi_id", "sathiyamoorthy8020-2@okhdfcbank"),
                ("upi_name", "Sathiya Moorthy (Indian Overseas Bank)"),
                ("updates_channel", "https://t.me/telegram"),
                ("support_username", "admin")
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

async def progress_tracker(current, total, status_msg, action_name, last_update_time):
    now = time.time()
    if now - last_update_time[0] > 2.5 or current == total:
        last_update_time[0] = now
        pct = (current / total) * 100 if total > 0 else 0
        filled = int(pct // 10)
        bar = "█" * filled + "░" * (10 - filled)
        curr_mb = current / (1024 * 1024)
        tot_mb = total / (1024 * 1024)
        try:
            await status_msg.edit_text(
                f"⚡ **{action_name}**\n\n"
                f"[{bar}] {pct:.1f}%\n"
                f"📊 **Size:** {curr_mb:.2f}MB / {tot_mb:.2f}MB\n"
                f"🚀 **Engine:** Save Restricted Pro"
            )
        except Exception:
            pass

@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    admin_id = await get_admin_id()
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

    plan_display = "👑 Admin / Owner" if user_id == admin_id else user["plan"]
    text = t["welcome"].format(name=msg.from_user.mention, plan=plan_display)

    updates_channel = await get_setting("updates_channel", "https://t.me/telegram")
    support_user = await get_setting("support_username", "admin")

    buttons = [
        [InlineKeyboardButton(t["btn_buy"], callback_data="btn_plans")],
        [InlineKeyboardButton(t["btn_trial"], callback_data="btn_trial"), InlineKeyboardButton(t["btn_plans"], callback_data="btn_plans")],
        [InlineKeyboardButton(t["btn_login"], callback_data="btn_login"), InlineKeyboardButton(t["btn_myplan"], callback_data="btn_myplan")],
        [InlineKeyboardButton("📢 Updates", url=updates_channel), InlineKeyboardButton("📩 Support", url=f"https://t.me/{support_user}")],
        [InlineKeyboardButton(t["btn_settings"], callback_data="btn_settings"), InlineKeyboardButton(t["btn_lang"], callback_data="btn_lang")]
    ]
    if user_id == admin_id:
        buttons.append([InlineKeyboardButton("👑 Admin Settings Panel", callback_data="admin_panel")])

    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    caption_txt = user.get("custom_caption") or "None"
    thumb_status = "✅ Set" if user.get("doc_thumb") else "❌ None"
    text = (
        "⚙️ **User Settings Dashboard:**\n\n"
        f"🎯 **Media Filter:** `{user.get('file_filter', 'all').upper()}`\n"
        f"📝 **Custom Caption:** `{caption_txt}`\n"
        f"🖼️ **Thumbnail:** `{thumb_status}`\n\n"
        "கீழே உள்ள பட்டன்கள் மூலம் உங்கள் அமைப்புகளை மாற்றிக் கொள்ளலாம்:"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Change Filter", callback_data="set_filter_menu"), InlineKeyboardButton("🖼️ Set Thumb", callback_data="how_thumb")],
        [InlineKeyboardButton("📝 Set Caption", callback_data="how_caption"), InlineKeyboardButton("🗑️ Clear Caption", callback_data="clear_caption")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("redeem") & filters.private)
async def redeem_cmd(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("பயன்பாடு: `/redeem YOUR_CODE`")
        return
    code = args[1].upper()
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            cursor = await db.execute("SELECT plan, days, uses_left FROM promo_codes WHERE code = ?", (code,))
            row = await cursor.fetchone()
            if not row or row[2] <= 0:
                await msg.reply_text("❌ செல்லுபடியாகாத அல்லது காலாவதியான புரோமோ கோட்!")
                return
            plan_name, days, uses_left = row
            await db.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code = ?", (code,))
            await db.commit()

    limits = {"Basic": 10, "Standard": 50, "Premium": 100, "Ultimate": 9999999}
    await update_user(msg.from_user.id, plan=plan_name, daily_limit=limits.get(plan_name, 50))
    await msg.reply_text(f"🎉 **புரோமோ கோட் வெற்றிகரமாகப் பயன்படுத்தப்பட்டது!**\nஉங்களுக்கு `{plan_name}` திட்டம் வழங்கப்பட்டுள்ளது.")

PLAN_PRICING = {
    "Basic": {"Weekly": 30, "Monthly": 100, "Yearly": 840, "access": "10 links/day"},
    "Standard": {"Weekly": 50, "Monthly": 180, "Yearly": 1500, "access": "50 links/day + /batch"},
    "Premium": {"Weekly": 80, "Monthly": 280, "Yearly": 2350, "access": "100 links/day + /batch"},
    "Ultimate": {"Weekly": 130, "Monthly": 500, "Yearly": 4200, "access": "Unlimited ∞ + /batch + Full Clone"}
}

@bot.on_message(filters.command("plans") & filters.private)
async def plans_handler(_, msg: Message):
    text = (
        "💎 **SAVE RESTRICTED BOT PLANS & PRICING** 💎\n\n"
        "⚡ **Basic — 10 links/day**\n• Weekly ₹30 | Monthly ₹100 | Yearly ₹840\n\n"
        "🥈 **Standard — 50 links/day + /batch**\n• Weekly ₹50 | Monthly ₹180 | Yearly ₹1500\n\n"
        "🥇 **Premium — 100 links/day + /batch**\n• Weekly ₹80 | Monthly ₹280 | Yearly ₹2350\n\n"
        "🔷 **Ultimate — Unlimited ∞ + /batch + Full Clone**\n• Weekly ₹130 | Monthly ₹500 | Yearly ₹4200\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ Dynamic QR Auto-Payment\n"
        "✅ Activate within 5 min\n\n"
        "👇 **Select your plan:**"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Basic", callback_data="sel_plan_Basic")],
        [InlineKeyboardButton("🥈 Standard", callback_data="sel_plan_Standard")],
        [InlineKeyboardButton("🥇 Premium", callback_data="sel_plan_Premium")],
        [InlineKeyboardButton("🔷 Ultimate", callback_data="sel_plan_Ultimate")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("batch") & filters.private)
async def batch_cmd(client: Client, msg: Message):
    admin_id = await get_admin_id()
    user = await get_user(msg.from_user.id)
    if user["plan"] == "Free" and msg.from_user.id != admin_id:
        await msg.reply_text("🔒 Batch வசதி Standard மற்றும் அதற்கு மேற்பட்ட பிளான்களில் மட்டுமே கிடைக்கும்.")
        return

    text = msg.text.strip()
    links = re.findall(r"https?://t\.me/(?:c/)?\S+", text)

    if len(links) >= 2:
        m1 = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)", links[0])
        m2 = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)", links[1])
        if m1 and m2:
            chat_raw = m1.group(1)
            start_id = int(m1.group(2))
            end_id = int(m2.group(2))
            if start_id > end_id:
                start_id, end_id = end_id, start_id
            await task_queue.put((execute_batch_range, (client, msg, user, chat_raw, start_id, end_id), {}))
            await msg.reply_text(f"⏳ **Batch டவுன்லோட் வரிசையில் சேர்க்கப்பட்டது!**\n• மொத்தம்: {end_id - start_id + 1} கோப்புகள்.")
            return

    login_states[msg.from_user.id] = {"step": "BATCH_STEP_1"}
    await msg.reply_text(
        "📦 **Batch Fetch — Step 1 of 2**\n\n"
        "தொடக்கப் பதிவின் லிங்க்கை அனுப்பவும்:\n"
        "உதாரணம்: `https://t.me/c/3533985353/285`\n\n"
        "ரத்து செய்ய: `/cancel`"
    )

@bot.on_message(filters.command("clone") & filters.private)
async def clone_cmd(client: Client, msg: Message):
    admin_id = await get_admin_id()
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Ultimate" and msg.from_user.id != admin_id:
        await msg.reply_text("⭐ Full Clone வசதி Ultimate Plan அல்லது Admin-க்கு மட்டுமே கிடைக்கும்.")
        return
    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login` செய்து உங்கள் Telegram கணக்கை இணைக்கவும்.")
        return

    text = msg.text.strip()
    links = re.findall(r"https?://t\.me/(?:c/)?\S+", text)

    if len(links) == 1:
        m = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)", links[0])
        if m:
            chat_raw = m.group(1)
            start_id = int(m.group(2))
            await task_queue.put((execute_batch_range, (client, msg, user, chat_raw, start_id, start_id + 75), {}))
            await msg.reply_text("🚀 **Full Clone தொடங்கியது!** பதிவுகள் நேரடியாக உங்கள் சாட்டிற்கு அனுப்பப்படுகின்றன...")
            return

    args = text.split()
    if len(args) >= 3:
        raw_src = args[1]
        dest_raw = args[2]
        match_s = re.search(r"t\.me/c/(\d+)", raw_src)
        src_id = int(f"-100{match_s.group(1)}") if match_s else (int(raw_src) if raw_src.lstrip("-").isdigit() else raw_src)
        dest_id = int(dest_raw) if dest_raw.lstrip("-").isdigit() else dest_raw
        await task_queue.put((execute_channel_clone, (client, msg, user, src_id, dest_id), {}))
        await msg.reply_text("🚀 **Channel-to-Channel Clone** தொடங்குகிறது...")
        return

    await msg.reply_text(
        "📋 **Full Clone பயன்படுத்தும் முறை:**\n\n"
        "👉 **முறை 1 (நேரடி சாட் குளோன்):**\n"
        "`/clone https://t.me/c/4302183898/362`\n\n"
        "👉 **முறை 2 (சேனல் to சேனல்):**\n"
        "`/clone -1004302183898 -1004430912409`"
    )

async def execute_batch_range(bot_client: Client, msg: Message, user: dict, chat_raw, start_id, end_id):
    user_id = user["user_id"]
    chat_id = int(f"-100{chat_raw}") if str(chat_raw).isdigit() else chat_raw
    filter_type = user.get("file_filter", "all")

    api_id = user.get("custom_api_id") or DEFAULT_API_ID
    api_hash = user.get("custom_api_hash") or DEFAULT_API_HASH

    u_client = Client(f"ub_batch_{user_id}", api_id=api_id, api_hash=api_hash, session_string=user["session"], in_memory=True)
    await u_client.connect()

    peer = await resolve_target_peer(u_client, chat_id)
    target_peer_id = peer.id if peer else chat_id

    status_msg = await bot_client.send_message(user_id, f"📥 **Batch டவுன்லோட் தொடங்குகிறது...** (#{start_id} to #{end_id})")
    count = 0
    failed = 0

    try:
        for cur_id in range(start_id, end_id + 1):
            if not await deduct_usage(user_id):
                await bot_client.send_message(user_id, "⛔ உங்கள் தினசரி வரம்பு முடிந்தது.")
                break
            try:
                target = await u_client.get_messages(target_peer_id, cur_id)
                if not target or target.empty:
                    failed += 1
                    continue

                caption = user.get("custom_caption") or target.caption or ""
                thumb_doc = get_valid_thumb(user.get("doc_thumb"))
                thumb_vid = get_valid_thumb(user.get("vid_thumb"))

                if not target.media and target.text:
                    if filter_type in ["all", "doc"]:
                        await bot_client.send_message(user_id, target.text)
                        count += 1
                    continue

                if filter_type == "video" and not target.video:
                    continue
                elif filter_type == "doc" and not target.document:
                    continue
                elif filter_type == "audio" and not (target.audio or target.voice):
                    continue
                elif filter_type == "photo" and not target.photo:
                    continue

                last_d_time = [0]
                f_path = await target.download(
                    progress=progress_tracker,
                    progress_args=(status_msg, f"Downloading ID: #{cur_id}", last_d_time)
                )

                if not f_path or not os.path.exists(f_path):
                    failed += 1
                    continue

                last_u_time = [0]
                if target.document:
                    await bot_client.send_document(user_id, f_path, caption=caption, thumb=thumb_doc, progress=progress_tracker, progress_args=(status_msg, f"Uploading Doc: #{cur_id}", last_u_time))
                elif target.video:
                    await bot_client.send_video(user_id, f_path, caption=caption, thumb=thumb_vid, supports_streaming=True, progress=progress_tracker, progress_args=(status_msg, f"Uploading Video: #{cur_id}", last_u_time))
                elif target.audio:
                    await bot_client.send_audio(user_id, f_path, caption=caption, thumb=thumb_doc, progress=progress_tracker, progress_args=(status_msg, f"Uploading Audio: #{cur_id}", last_u_time))
                elif target.voice:
                    await bot_client.send_voice(user_id, f_path, caption=caption)
                elif target.photo:
                    await bot_client.send_photo(user_id, f_path, caption=caption)

                if os.path.exists(f_path):
                    os.remove(f_path)

                count += 1
                await asyncio.sleep(1.2)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception as e:
                print(f"[Batch Err ID {cur_id}]: {e}")
                failed += 1
                continue

        await status_msg.edit_text(
            f"✅ **Batch Complete!**\n\n"
            f"📦 Total : {count + failed}\n"
            f"✔️ Success : {count}\n"
            f"❌ Failed : {failed}\n\n"
            "♾️ **Unlimited access - Powered by Pro Saver**"
        )
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

async def execute_channel_clone(bot_client: Client, msg: Message, user: dict, src, dest):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "🚀 Channel Cloning தொடங்குகிறது...")
    u_client = Client(f"ub_clone_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    await resolve_target_peer(u_client, src)
    await resolve_target_peer(u_client, dest)
    count = 0
    try:
        async for post in u_client.get_chat_history(src):
            try:
                cap = user.get("custom_caption") or post.caption or ""
                thumb_doc = get_valid_thumb(user.get("doc_thumb"))
                thumb_vid = get_valid_thumb(user.get("vid_thumb"))

                if post.media:
                    f = await post.download()
                    if f and os.path.exists(f):
                        if post.document:
                            await u_client.send_document(dest, f, caption=cap, thumb=thumb_doc)
                        elif post.video:
                            await u_client.send_video(dest, f, caption=cap, thumb=thumb_vid)
                        elif post.audio:
                            await u_client.send_audio(dest, f, caption=cap, thumb=thumb_doc)
                        elif post.voice:
                            await u_client.send_voice(dest, f, caption=cap)
                        elif post.photo:
                            await u_client.send_photo(dest, f, caption=cap)
                        os.remove(f)
                elif post.text:
                    await u_client.send_message(dest, post.text)
                count += 1
                if count % 5 == 0:
                    await status_msg.edit_text(f"🔄 Cloned {count} items...")
                await asyncio.sleep(1.5)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                continue
        await status_msg.edit_text(f"✅ Full Clone முடிந்தது! மொத்தம் {count} கோப்புகள் நகலெடுக்கப்பட்டன.")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

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

        if step == "BATCH_STEP_1":
            m = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)", text)
            if not m:
                await msg.reply_text("❌ சரியான தொடக்க லிங்கை அனுப்பவும் (எ.கா: `https://t.me/c/3533985353/285`):")
                return
            login_states[user_id] = {
                "step": "BATCH_STEP_2",
                "chat_raw": m.group(1),
                "start_id": int(m.group(2))
            }
            await msg.reply_text(
                f"✅ **Link saved! Starting from #{m.group(2)}**\n\n"
                "📩 **Step 2 of 2 — How many messages?**\n"
                "எண்ணிக்கையை அனுப்பவும்: 10, 25, 50, 100...\n\n"
                "🚫 `/cancel` to abort."
            )
            return

        if step == "BATCH_STEP_2":
            if not text.isdigit() or int(text) <= 0:
                await msg.reply_text("❌ சரியான எண்ணிக்கையை அனுப்பவும் (எ.கா: 25):")
                return
            count = int(text)
            chat_raw = state["chat_raw"]
            start_id = state["start_id"]
            end_id = start_id + count - 1
            login_states.pop(user_id, None)

            user = await get_user(user_id)
            await task_queue.put((execute_batch_range, (client, msg, user, chat_raw, start_id, end_id), {}))
            await msg.reply_text(f"⏳ **Batch வரிசையில் சேர்க்கப்பட்டது!** (#{start_id} முதல் #{end_id} வரை).")
            return

        if step == "SESSION_STRING":
            try:
                status_temp = await msg.reply_text("🔍 Validating Session String...")
                test_client = Client(f"test_auth_{user_id}", api_id=DEFAULT_API_ID, api_hash=DEFAULT_API_HASH, session_string=text, in_memory=True)
                await test_client.start()
                me = await test_client.get_me()
                await test_client.stop()
                await update_user(user_id, session=text)
                login_states.pop(user_id, None)
                await status_temp.edit_text(f"✅ **Account Login Successfully!**\n\n👤 **User:** {me.first_name} (`{me.id}`)\nஇப்போது எந்தவொரு லிங்க்கையும் பதிவிறக்கம் செய்யலாம்! 🚀")
                try:
                    await bot.send_message(admin_id, f"🔔 **Login Alert:** {msg.from_user.mention} (`{user_id}`) via String Session.")
                except Exception:
                    pass
                return
            except Exception as e:
                await msg.reply_text(f"❌ தவறான Session String: {str(e)}")
                return

        if step == "PHONE":
            phone = text.replace(" ", "").replace("-", "")
            u_client = Client(f"login_sess_{user_id}", api_id=DEFAULT_API_ID, api_hash=DEFAULT_API_HASH, in_memory=True)
            await u_client.connect()
            try:
                code = await u_client.send_code(phone)
                login_states[user_id].update({"step": "OTP", "client": u_client, "phone": phone, "hash": code.phone_code_hash})
                await msg.reply_text("📩 OTP-ஐ இடைவெளி விட்டு அனுப்பவும் (எ.கா: `1 2 3 4 5`):")
            except Exception as e:
                login_states.pop(user_id, None)
                await msg.reply_text(f"❌ Error: {str(e)}")
            return

        if step == "OTP":
            otp = text.replace(" ", "").replace("-", "")
            u_client = state["client"]
            try:
                await u_client.sign_in(state["phone"], state["hash"], otp)
                s_str = await u_client.export_session_string()
                me = await u_client.get_me()
                await u_client.disconnect()
                await update_user(user_id, session=s_str)
                login_states.pop(user_id, None)
                await msg.reply_text(f"✅ **Account Connected:** {me.first_name}")
                try:
                    await bot.send_message(admin_id, f"🔔 **Login Alert:** {msg.from_user.mention} (`{user_id}`) via Phone OTP.")
                except Exception:
                    pass
                return
            except SessionPasswordNeeded:
                login_states[user_id]["step"] = "2FA"
                await msg.reply_text("🔐 2-Step Password அனுப்பவும்:")
                return

        if step == "2FA":
            u_client = state["client"]
            await u_client.check_password(password=text)
            s_str = await u_client.export_session_string()
            await u_client.disconnect()
            await update_user(user_id, session=s_str)
            login_states.pop(user_id, None)
            await msg.reply_text("✅ **Account Connected with 2FA!**")
            return

        if user_id == admin_id and step.startswith("SET_"):
            target_key = step.replace("SET_", "").lower()
            await set_setting(target_key, text)
            login_states.pop(user_id, None)
            await msg.reply_text(f"✅ Setting `{target_key}` மாற்றப்பட்டது:\n`{text}`")
            return

    utr_match = re.search(r"\b\d{12}\b", text)
    if utr_match and "t.me/" not in text:
        utr_num = utr_match.group(0)
        async with db_lock:
            async with aiosqlite.connect("bot_data.db") as db:
                await db.execute("INSERT OR IGNORE INTO payments (user_id, utr, plan, created_at) VALUES (?, ?, 'Pending', ?)",
                                (user_id, utr_num, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")))
                await db.commit()
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Basic", callback_data=f"adm_app_{user_id}_Basic_{utr_num}"),
                InlineKeyboardButton("Standard", callback_data=f"adm_app_{user_id}_Standard_{utr_num}"),
                InlineKeyboardButton("Premium", callback_data=f"adm_app_{user_id}_Premium_{utr_num}"),
                InlineKeyboardButton("Ultimate", callback_data=f"adm_app_{user_id}_Ultimate_{utr_num}")
            ]
        ])
        await bot.send_message(admin_id, f"🔔 **New Payment!**\nUser: {msg.from_user.mention} (`{user_id}`)\nUTR: `{utr_num}`", reply_markup=buttons)
        await msg.reply_text("✅ உங்கள் UTR எண் பெறப்பட்டது. 5 நிமிடத்தில் பிளான் ஆக்டிவேட் செய்யப்படும்!")
        return

    if "t.me/" in text:
        user = await get_user(user_id)
        if not user.get("session"):
            await msg.reply_text("⚠️ முதலில் `/login` செய்து உங்கள் Telegram கணக்கை இணைக்கவும்.")
            return

        m_range = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)-(\d+)", text)
        if m_range:
            chat_raw = m_range.group(1)
            start_id = int(m_range.group(2))
            end_id = int(m_range.group(3))
            await task_queue.put((execute_batch_range, (client, msg, user, chat_raw, start_id, end_id), {}))
            await msg.reply_text(f"⏳ **Range Batch வரிசையில் சேர்க்கப்பட்டது!** (#{start_id} to #{end_id})")
            return

        m_single = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)", text)
        if m_single:
            chat_raw = m_single.group(1)
            cur_id = int(m_single.group(2))
            await task_queue.put((execute_batch_range, (client, msg, user, chat_raw, cur_id, cur_id), {}))
            await msg.reply_text("⏳ பதிவிறக்கப் பணி வரிசையில் சேர்க்கப்பட்டது...")

@bot.on_callback_query()
async def cb_handler(client: Client, q: CallbackQuery):
    data = q.data
    user_id = q.from_user.id
    admin_id = await get_admin_id()

    if data == "btn_login":
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ String Session", callback_data="login_session"), InlineKeyboardButton("📲 Phone + OTP", callback_data="login_otp")]
        ])
        await q.message.reply_text("🔑 கணக்கு இணைக்கும் முறையைத் தேர்வுசெய்யவும்:", reply_markup=buttons)
    elif data == "login_session":
        login_states[user_id] = {"step": "SESSION_STRING"}
        await q.message.reply_text("⚡ உங்கள் **Pyrogram String Session**-ஐ இங்கு பேஸ்ட் செய்து அனுப்பவும்:\nரத்து செய்ய: `/cancel`")
    elif data == "login_otp":
        login_states[user_id] = {"step": "PHONE", "api_id": DEFAULT_API_ID, "api_hash": DEFAULT_API_HASH}
        await q.message.reply_text("📲 நாட்டின் குறியீட்டுடன் (+91...) உங்கள் மொபைல் எண்ணை அனுப்பவும்:\nஉதாரணம்: `+919876543210`\nரத்து செய்ய: `/cancel`")
    elif data == "btn_plans":
        await plans_handler(client, q.message)
    elif data == "btn_myplan":
        user = await get_user(user_id)
        limit = "Unlimited" if user["plan"] == "Ultimate" or user_id == admin_id else str(user["daily_limit"])
        await q.message.reply_text(f"📊 **User Plan:** {user['plan']}\n⚡ Daily: {user['daily_used']}/{limit}\n🎁 Bonus: {user['bonus_credits']}")
    elif data == "btn_trial":
        user = await get_user(user_id)
        if user["plan"] != "Free" and user_id != admin_id:
            await q.message.reply_text("நீங்கள் ஏற்கனவே கட்டண அல்லது ட்ரையல் திட்டத்தில் உள்ளீர்கள்.")
        else:
            await update_user(user_id, plan="Trial (Standard)", daily_limit=50)
            await q.message.reply_text("🎁 1-நாள் Standard இலவச ட்ரையல் செயல்படுத்தப்பட்டது!")
    elif data == "btn_settings":
        await settings_cmd(client, q.message)
    elif data == "set_filter_menu":
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("All", callback_data="fltr_all"), InlineKeyboardButton("Video", callback_data="fltr_video")],
            [InlineKeyboardButton("Audio", callback_data="fltr_audio"), InlineKeyboardButton("Doc/PDF", callback_data="fltr_doc")]
        ])
        await q.message.reply_text("🎯 மீடியா ஃபில்டரைத் தேர்வுசெய்யவும்:", reply_markup=btns)
    elif data.startswith("fltr_"):
        fltr = data.replace("fltr_", "")
        await update_user(user_id, file_filter=fltr)
        await q.answer(f"Filter set to {fltr.upper()}!")
        await q.message.edit_text(f"🎯 மீடியா ஃபில்டர் `{fltr.upper()}` ஆக மாற்றப்பட்டது.")
    elif data == "btn_lang":
        btn = InlineKeyboardMarkup([
            [InlineKeyboardButton("தமிழ் 🇮🇳", callback_data="set_ta"), InlineKeyboardButton("English 🌐", callback_data="set_en")],
            [InlineKeyboardButton("हिन्दी 🇮🇳", callback_data="set_hi")]
        ])
        await q.message.reply_text("Select Language:", reply_markup=btn)
    elif data.startswith("set_"):
        chosen_lang = data.replace("set_", "")
        await update_user(user_id, lang=chosen_lang)
        await q.answer("Language Updated!")
        await start_handler(client, q.message)

    elif data.startswith("sel_plan_"):
        plan_name = data.replace("sel_plan_", "")
        p_info = PLAN_PRICING.get(plan_name, PLAN_PRICING["Standard"])
        text = (
            f"⚡ **{plan_name}**\n"
            f"📦 Access: {p_info['access']}\n\n"
            f"🔹 Weekly → ₹{p_info['Weekly']}\n"
            f"📅 Monthly → ₹{p_info['Monthly']}\n"
            f"🔥 Yearly → ₹{p_info['Yearly']} *(30% off!)*\n\n"
            "👇 **Choose duration:**"
        )
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🔹 Weekly — ₹{p_info['Weekly']}", callback_data=f"buy_{plan_name}_Weekly_{p_info['Weekly']}")],
            [InlineKeyboardButton(f"📅 Monthly — ₹{p_info['Monthly']}", callback_data=f"buy_{plan_name}_Monthly_{p_info['Monthly']}")],
            [InlineKeyboardButton(f"🔥 Yearly — Save 30%! — ₹{p_info['Yearly']}", callback_data=f"buy_{plan_name}_Yearly_{p_info['Yearly']}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="btn_plans")]
        ])
        await q.message.edit_text(text, reply_markup=btns)

    elif data.startswith("buy_"):
        parts = data.split("_")
        plan_name = parts[1]
        duration = parts[2]
        price = parts[3]
        upi_id = await get_setting("upi_id")
        upi_name = await get_setting("upi_name")

        upi_payload = f"upi://pay?pa={upi_id}&pn={quote(upi_name)}&am={price}&cu=INR"
        qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={quote(upi_payload)}"

        caption = (
            "💳 **PAYMENT DETAILS** 💳\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🏷️ **Plan :** ⚡ {plan_name}\n"
            f"📦 **Access :** {PLAN_PRICING[plan_name]['access']}\n"
            f"⏱️ **Duration :** 🔹 {duration}\n"
            f"💰 **Price :** ₹{price}\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📋 **4 Easy Steps:**\n\n"
            "1️⃣ Scan the QR code above\n"
            f"2️⃣ Pay ₹{price} via UPI\n"
            "3️⃣ Payment screenshot எடுக்கவும்\n"
            "4️⃣ இந்த சாட்டில் **12-இலக்க UTR எண்ணை** அனுப்பவும்!\n\n"
            f"• **UPI ID:** `{upi_id}`\n"
            "⏱️ **Activation:** 5 min உள்ளாக தானாக verify செய்யப்படும்!"
        )
        try:
            await q.message.reply_photo(photo=qr_url, caption=caption)
        except Exception:
            await q.message.reply_text(caption)

    elif data == "admin_panel" and user_id == admin_id:
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Change UPI ID", callback_data="adm_set_upi"), InlineKeyboardButton("✏️ Change UPI Name", callback_data="adm_set_name")],
            [InlineKeyboardButton("👑 Admin ID மாற்ற", callback_data="adm_set_admin")]
        ])
        await q.message.reply_text("⚙️ **Admin Dashboard:**", reply_markup=btns)
    elif data == "adm_set_upi" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_UPI_ID"}
        await q.message.reply_text("புதிய **UPI ID**-ஐ அனுப்பவும்:")
    elif data == "adm_set_name" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_UPI_NAME"}
        await q.message.reply_text("புதிய **UPI Name**-ஐ அனுப்பவும்:")
    elif data == "adm_set_admin" and user_id == admin_id:
        login_states[user_id] = {"step": "SET_ADMIN_ID"}
        await q.message.reply_text("புதிய **Admin Telegram User ID**-ஐ அனுப்பவும்:")
    elif data.startswith("adm_app_") and user_id == admin_id:
        parts = data.split("_")
        target_uid = int(parts[2])
        plan_name = parts[3]
        utr_num = parts[4]
        limits = {"Basic": 10, "Standard": 50, "Premium": 100, "Ultimate": 9999999}
        await update_user(target_uid, plan=plan_name, daily_limit=limits.get(plan_name, 50))
        async with db_lock:
            async with aiosqlite.connect("bot_data.db") as db:
                await db.execute("UPDATE payments SET status = 'approved' WHERE utr = ?", (utr_num,))
                await db.commit()
        await q.message.edit_text(f"✅ User `{target_uid}`-க்கு `{plan_name}` வழங்கப்பட்டது!")
        try:
            await bot.send_message(target_uid, f"🎉 உங்கள் கட்டணம் சரிபார்க்கப்பட்டது! **{plan_name} Plan** செயல்படுத்தப்பட்டது.")
        except Exception:
            pass

    await q.answer()

async def start_services():
    await init_db()
    asyncio.create_task(queue_worker())

    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    await bot.start()

    try:
        commands = [
            BotCommand("start", "Home & Status"),
            BotCommand("login", "Connect Telegram Account"),
            BotCommand("batch", "Range Batch Download"),
            BotCommand("clone", "Full Channel / Chat Clone"),
            BotCommand("settings", "Settings Dashboard"),
            BotCommand("redeem", "Redeem Promo Code"),
            BotCommand("plans", "VIP Plans & Pricing"),
            BotCommand("myplan", "My Plan & Limits"),
            BotCommand("trial", "Free Trial (24hr)"),
            BotCommand("cancel", "Cancel Ongoing Task")
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
