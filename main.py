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
    FloodWait
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

# Primary Bot Client with memory session
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

# ----------------- THREADED HTTP SERVER (RENDER 24/7 PING) -----------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Save Restricted Pro Bot is 100% Active!")

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        print(f"[*] Threaded Ping Server running on port {PORT}")
        server.serve_forever()
    except Exception as e:
        print(f"[Health Server Error] {e}")

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
                    session TEXT,
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
                await db.execute(
                    "INSERT INTO users (user_id, name, plan, daily_limit, daily_used) VALUES (?, ?, 'Free', 2, 0)",
                    (user_id, name)
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
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT plan, daily_limit, daily_used, bonus_credits FROM users WHERE user_id = ?", (user_id,))
            u = await cursor.fetchone()
            if not u:
                return False

            if u["plan"] == "Ultimate" or user_id == ADMIN_ID:
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

# ----------------- ASYNC TASK QUEUE WORKER -----------------
async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            handler, args, kwargs = task
            await asyncio.wait_for(handler(*args, **kwargs), timeout=900)
        except Exception as e:
            print(f"[Queue Worker Error] {e}")
        finally:
            task_queue.task_done()

# ----------------- USER COMMANDS -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    user = await get_user(user_id, msg.from_user.first_name)

    # Referral checking
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
    text = (
        f"👋 **வணக்கம் {msg.from_user.mention}!**\n\n"
        "Welcome to **Save Restricted Pro Bot**!\n\n"
        f"🏷️ **திட்டம்:** `{user['plan']}`\n"
        f"🔑 **கணக்கு:** {session_status}\n"
        f"🎯 **ஃபில்டர்:** `{user.get('file_filter', 'all').upper()}`\n\n"
        "Restricted சேனலின் போஸ்ட் லிங்கை இங்கு அனுப்புங்கள் அல்லது `/login` செய்து உங்கள் கணக்கை இணையுங்கள்."
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Connect Account", callback_data="btn_login"), InlineKeyboardButton("📦 Plans", callback_data="btn_plans")],
        [InlineKeyboardButton("📊 My Plan", callback_data="btn_myplan"), InlineKeyboardButton("🎁 Free Trial", callback_data="btn_trial")],
        [InlineKeyboardButton("🎁 Daily Bonus", callback_data="btn_bonus"), InlineKeyboardButton("👥 Refer & Earn", callback_data="btn_ref")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("login") & filters.private)
async def login_handler(_, msg: Message):
    login_states[msg.from_user.id] = {"step": "API_ID"}
    await msg.reply_text(
        "Send your **API ID**.\n\n"
        "Or click on **/skip** to use default bot keys."
    )

@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_, msg: Message):
    await update_user(msg.from_user.id, session=None)
    login_states.pop(msg.from_user.id, None)
    await msg.reply_text("🚪 உங்கள் கணக்கு இணைப்பு வெற்றிகரமாக துண்டிக்கப்பட்டது.")

@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_handler(_, msg: Message):
    user = await get_user(msg.from_user.id, msg.from_user.first_name)
    plan = user["plan"]
    limit = "Unlimited" if plan == "Ultimate" else str(user["daily_limit"])
    used = user["daily_used"]
    bonus = user.get("bonus_credits", 0)
    left = "Unlimited" if plan == "Ultimate" else str(max(0, user["daily_limit"] - used) + bonus)

    reset_str = datetime.now(IST).strftime("%d-%m-%Y at 12:00 AM IST")
    text = (
        "📋 **USER PLAN DATA :**\n\n"
        f"👤 **USER**       : {user['name']}\n"
        f"⚡ **USER ID**    : `{user['user_id']}`\n"
        f"🏷️ **PLAN**       : {plan}\n"
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
        "💎 **PREMIUM PLANS** 💎\n\n"
        "🥈 **Standard:** 50 files/day (₹50/wk | ₹180/mo)\n"
        "🥇 **Premium:** 100 files/day (₹80/wk | ₹280/mo)\n"
        "🔷 **Ultimate:** Unlimited + Channel Cloner (₹130/wk | ₹450/mo)"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Buy Plan (UPI)", callback_data="btn_buy")],
        [InlineKeyboardButton("💬 Admin Contact", url=f"tg://user?id={ADMIN_ID}")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("trial") & filters.private)
async def trial_handler(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Free":
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

# ----------------- THUMBNAIL MANAGEMENT -----------------
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
        await msg.reply_text("❌ Document Thumbnail எதுவும் வைக்கப்படவில்லை.")

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
        await msg.reply_text("❌ Video Thumbnail எதுவும் வைக்கப்படவில்லை.")

# ----------------- ROBUST OTP LOGIN / /skip WORKFLOW -----------------
@bot.on_message(filters.text & filters.private)
async def text_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    text = msg.text.strip()

    if text == "/cancel":
        login_states.pop(user_id, None)
        await msg.reply_text("செயல்பாடு ரத்து செய்யப்பட்டது.")
        return

    state = login_states.get(user_id)
    if state:
        step = state.get("step")

        if step == "API_ID":
            if text == "/skip":
                login_states[user_id] = {"step": "PHONE", "api_id": DEFAULT_API_ID, "api_hash": DEFAULT_API_HASH}
                await msg.reply_text(
                    "Please send your phone number which includes country code\n"
                    "Example: `+919876543210`\n\n"
                    "Enter /cancel to cancel"
                )
            elif text.isdigit():
                login_states[user_id] = {"step": "API_HASH", "api_id": int(text)}
                await msg.reply_text("Now send your API HASH:")
            else:
                await msg.reply_text("Please enter a valid API ID or send /skip.")
            return

        if step == "API_HASH":
            login_states[user_id]["api_hash"] = text
            login_states[user_id]["step"] = "PHONE"
            await msg.reply_text(
                "Please send your phone number which includes country code\n"
                "Example: `+919876543210`"
            )
            return

        if step == "PHONE":
            phone = text.replace(" ", "").replace("-", "")
            if not phone.startswith("+"):
                await msg.reply_text("❌ தயவுசெய்து நாட்டின் குறியீட்டுடன் (+91...) அனுப்பவும்.")
                return

            api_id = state.get("api_id", DEFAULT_API_ID)
            api_hash = state.get("api_hash", DEFAULT_API_HASH)
            u_client = Client(f"login_sess_{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
            await u_client.connect()
            try:
                status_temp = await msg.reply_text("Sending OTP...")
                code = await u_client.send_code(phone)
                login_states[user_id].update({
                    "step": "OTP",
                    "client": u_client,
                    "phone": phone,
                    "hash": code.phone_code_hash
                })
                await status_temp.edit_text(
                    "Please check for an OTP in official Telegram account.\n\n"
                    "If OTP is 12345, please send it with spaces as `1 2 3 4 5`.\n\n"
                    "Enter /cancel to cancel the process."
                )
            except Exception as e:
                login_states.pop(user_id, None)
                if u_client.is_connected:
                    await u_client.disconnect()
                await msg.reply_text(f"❌ Error sending OTP: {str(e)}")
            return

        if step == "OTP":
            otp = text.replace(" ", "").replace("-", "")
            u_client = state["client"]
            try:
                await u_client.sign_in(state["phone"], state["hash"], otp)
                s_str = await u_client.export_session_string()
                await u_client.disconnect()
                await update_user(user_id, session=s_str)
                login_states.pop(user_id, None)
                await msg.reply_text(
                    "✅ **Account Login Successfully!**\n\n"
                    "Now send any restricted channel link to download files.\n"
                    "If you get any Auth error in the future, `/logout` and `/login` again."
                )
            except SessionPasswordNeeded:
                login_states[user_id]["step"] = "2FA"
                await msg.reply_text("🔐 Two-Step Verification பாஸ்வேர்டை அனுப்பவும்:")
            except (PhoneCodeInvalid, PhoneCodeExpired):
                await msg.reply_text("❌ தவறான அல்லது காலாவதியான OTP. மீண்டும் இடைவெளி விட்டு அனுப்பவும்: `1 2 3 4 5`")
            except Exception as e:
                login_states.pop(user_id, None)
                if u_client.is_connected:
                    await u_client.disconnect()
                await msg.reply_text(f"❌ Login Error: {str(e)}")
            return

        if step == "2FA":
            u_client = state["client"]
            try:
                await u_client.check_password(password=text)
                s_str = await u_client.export_session_string()
                await u_client.disconnect()
                await update_user(user_id, session=s_str)
                login_states.pop(user_id, None)
                await msg.reply_text("✅ **Account Login Successfully with 2FA!**")
            except PasswordHashInvalid:
                await msg.reply_text("❌ தவறான கடவுச்சொல். மீண்டும் முயற்சிக்கவும்:")
            except Exception as e:
                login_states.pop(user_id, None)
                if u_client.is_connected:
                    await u_client.disconnect()
                await msg.reply_text(f"❌ 2FA Error: {str(e)}")
            return

    # Direct Link Detection
    if "t.me/" in text:
        await task_queue.put((execute_download, (client, msg, user_id), {}))
        await msg.reply_text("⏳ பதிவிறக்கப் பணி வரிசையில் சேர்க்கப்பட்டது...")

# ----------------- DOWNLOAD ENGINE -----------------
async def execute_download(bot_client: Client, msg: Message, user_id: int):
    user = await get_user(user_id)
    if not user.get("session"):
        await bot_client.send_message(user_id, "⚠️ முதலில் `/login` செய்து உங்கள் கணக்கை இணைக்கவும்.")
        return

    match = re.search(r"t\.me/(c/)?([a-zA-Z0-9_]+)/(\d+)(?:-(\d+))?", msg.text.strip())
    if not match:
        await bot_client.send_message(user_id, "❌ தவறான டெலிகிராம் லிங்க்.")
        return

    is_private = bool(match.group(1))
    chat_raw = match.group(2)
    start_id = int(match.group(3))
    end_id = int(match.group(4)) if match.group(4) else start_id
    chat_id = int(f"-100{chat_raw}") if is_private or chat_raw.isdigit() else chat_raw

    filter_type = user.get("file_filter", "all")
    status_msg = await bot_client.send_message(user_id, "📥 டவுன்லோட் தொடங்குகிறது...")
    u_client = Client(f"ub_exec_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    try:
        for cur_id in range(start_id, end_id + 1):
            if not await deduct_usage(user_id):
                await bot_client.send_message(user_id, "⛔ உங்கள் தினசரி வரம்பு முடிந்தது. தொடர விஐபி எடுக்கவும்.")
                break

            try:
                target = await u_client.get_messages(chat_id, cur_id)
                if not target or not target.media:
                    continue

                if filter_type == "video" and not target.video:
                    continue
                elif filter_type == "doc" and not target.document:
                    continue
                elif filter_type == "audio" and not target.audio:
                    continue
                elif filter_type == "photo" and not target.photo:
                    continue

                await status_msg.edit_text(f"📥 Downloading ID: {cur_id}...")
                f_path = await target.download()

                await status_msg.edit_text(f"📤 Uploading ID: {cur_id} (Original Quality)...")
                caption = user.get("custom_caption") or target.caption or ""

                thumb_doc = user.get("doc_thumb") if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]) else None
                thumb_vid = user.get("vid_thumb") if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]) else None

                if target.document:
                    await bot_client.send_document(user_id, f_path, caption=caption, thumb=thumb_doc)
                elif target.video:
                    await bot_client.send_video(user_id, f_path, caption=caption, thumb=thumb_vid, supports_streaming=True)
                elif target.audio:
                    await bot_client.send_audio(user_id, f_path, caption=caption, thumb=thumb_doc)
                elif target.photo:
                    await bot_client.send_photo(user_id, f_path, caption=caption)

                if os.path.exists(f_path):
                    os.remove(f_path)
                await asyncio.sleep(1.5)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                continue

        await status_msg.edit_text("✅ பதிவிறக்கம் வெற்றிகரமாக முடிந்தது!")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- FORWARDED MESSAGES HANDLER -----------------
@bot.on_message(filters.forwarded & filters.private)
async def forwarded_handler(client: Client, msg: Message):
    if not msg.media:
        await msg.reply_text("❌ இந்த பார்வர்ட் செய்தியில் மீடியா எதுவும் இல்லை.")
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

    status_msg = await bot_client.send_message(user_id, "📥 பதிவிறக்கப்படுகிறது...")
    try:
        file_path = await msg.download()
        await status_msg.edit_text("📤 பதிவேற்றப்படுகிறது...")

        caption = user.get("custom_caption") or msg.caption or ""
        thumb_doc = user.get("doc_thumb") if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]) else None
        thumb_vid = user.get("vid_thumb") if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]) else None

        if msg.document:
            await bot_client.send_document(user_id, file_path, caption=caption, thumb=thumb_doc)
        elif msg.video:
            await bot_client.send_video(user_id, file_path, caption=caption, thumb=thumb_vid, supports_streaming=True)
        elif msg.audio:
            await bot_client.send_audio(user_id, file_path, caption=caption, thumb=thumb_doc)
        elif msg.photo:
            await bot_client.send_photo(user_id, file_path, caption=caption)

        if os.path.exists(file_path):
            os.remove(file_path)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

# ----------------- BATCH, CLONE & EXTRACT HANDLERS -----------------
@bot.on_message(filters.command("batch") & filters.private)
async def batch_handler(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] == "Free":
        await msg.reply_text("🔒 Batch வசதி Standard மற்றும் அதற்கு மேற்பட்ட பிளான்களில் மட்டுமே கிடைக்கும்.")
        return
    await msg.reply_text("📦 Batch Mode: `https://t.me/c/1234567890/10-30` என்ற வடிவில் அனுப்பவும்.")

@bot.on_message(filters.command("clone") & filters.private)
async def clone_handler(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Ultimate" and msg.from_user.id != ADMIN_ID:
        await msg.reply_text("⭐ சேனல் குளோனிங் வசதி Ultimate Plan-ல் மட்டுமே கிடைக்கும்.")
        return
    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login` செய்யவும்.")
        return
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("பயன்பாடு: `/clone <source_id> <target_id>`")
        return
    await task_queue.put((execute_clone, (client, msg, user, args[1], args[2]), {}))
    await msg.reply_text("🚀 சேனல் குளோனிங் வரிசையில் சேர்க்கப்பட்டது...")

async def execute_clone(bot_client: Client, msg: Message, user: dict, src_raw, dest_raw):
    user_id = user["user_id"]
    src = int(src_raw) if src_raw.lstrip("-").isdigit() else src_raw
    dest = int(dest_raw) if dest_raw.lstrip("-").isdigit() else dest_raw

    status_msg = await bot_client.send_message(user_id, "🚀 குளோனிங் தொடங்குகிறது...")
    u_client = Client(f"ub_clone_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()
    count = 0
    try:
        async for post in u_client.get_chat_history(src):
            if post.media:
                if not await deduct_usage(user_id):
                    await bot_client.send_message(user_id, "⛔ வரம்பு முடிந்தது.")
                    break
                try:
                    f = await post.download()
                    cap = user.get("custom_caption") or post.caption or ""
                    thumb_doc = user.get("doc_thumb") if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]) else None
                    thumb_vid = user.get("vid_thumb") if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]) else None

                    if post.document:
                        await u_client.send_document(dest, f, caption=cap, thumb=thumb_doc)
                    elif post.video:
                        await u_client.send_video(dest, f, caption=cap, thumb=thumb_vid)
                    elif post.audio:
                        await u_client.send_audio(dest, f, caption=cap, thumb=thumb_doc)
                    elif post.photo:
                        await u_client.send_photo(dest, f, caption=cap)

                    if os.path.exists(f):
                        os.remove(f)
                    count += 1
                    if count % 5 == 0:
                        await status_msg.edit_text(f"🔄 Cloned {count} files...")
                    await asyncio.sleep(2)
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                except Exception:
                    continue
        await status_msg.edit_text(f"✅ குளோனிங் முடிந்தது. மொத்தம் மாற்றப்பட்ட கோப்புகள்: {count}")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
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
        await msg.reply_text("பயன்பாடு: `/extract <channel_id>`")
        return
    target = int(args[1]) if args[1].lstrip("-").isdigit() else args[1]
    await task_queue.put((execute_extract, (client, msg, user, target), {}))
    await msg.reply_text("📑 பிளேலிஸ்ட் தொகுக்கப்பட்டு வருகிறது...")

async def execute_extract(bot_client: Client, msg: Message, user: dict, target):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "📑 தொகுக்கப்படுகிறது...")
    u_client = Client(f"ub_ext_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()
    file_name = f"playlist_{user_id}.txt"
    count = 0
    try:
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(f"--- Playlist for {target} ---\n\n")
            async for post in u_client.get_chat_history(target):
                if post.media:
                    count += 1
                    media_type = "Document" if post.document else "Video" if post.video else "Audio" if post.photo else "Media"
                    f.write(f"{count}. {media_type} | ID: {post.id} | https://t.me/c/{str(target).replace('-100', '')}/{post.id}\n")
        await bot_client.send_document(user_id, file_name, caption=f"✅ {count} மீடியா லிங்க்குகள் தொகுக்கப்பட்டன.")
        if os.path.exists(file_name):
            os.remove(file_name)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- ADMIN COMMANDS (/ap, /rp, /ps, /stats, /broadcast) -----------------
@bot.on_message(filters.command("ap") & filters.user(ADMIN_ID))
async def admin_ap(_, msg: Message):
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("Usage: `/ap <user_id> <Standard|Premium|Ultimate>`")
        return
    t_uid = int(args[1])
    t_plan = args[2].capitalize()
    limits = {"Standard": 50, "Premium": 100, "Ultimate": 999999}
    if t_plan not in limits:
        await msg.reply_text("தவறான திட்டம்! (Standard, Premium, Ultimate).")
        return
    await update_user(t_uid, plan=t_plan, daily_limit=limits[t_plan])
    await msg.reply_text(f"✅ User `{t_uid}` திட்டம் `{t_plan}` ஆக மாற்றப்பட்டது.")

@bot.on_message(filters.command("rp") & filters.user(ADMIN_ID))
async def admin_rp(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("Usage: `/rp <user_id>`")
        return
    t_uid = int(args[1])
    await update_user(t_uid, plan="Free", daily_limit=2, daily_used=0)
    await msg.reply_text(f"🔄 User `{t_uid}` இலவச நிலைக்கு ரீசெட் செய்யப்பட்டார்.")

@bot.on_message(filters.command("ps") & filters.user(ADMIN_ID))
async def admin_ps(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("Usage: `/ps <user_id>`")
        return
    t_uid = int(args[1])
    u = await get_user(t_uid)
    await msg.reply_text(f"📋 User `{t_uid}`:\nPlan: {u['plan']}\nLimit: {u['daily_limit']}\nUsed: {u['daily_used']}\nBonus: {u['bonus_credits']}")

@bot.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def admin_stats(_, msg: Message):
    async with db_lock:
        async with aiosqlite.connect("bot_data.db") as db:
            c1 = await db.execute("SELECT COUNT(*) FROM users")
            total = (await c1.fetchone())[0]
            c2 = await db.execute("SELECT COUNT(*) FROM users WHERE plan != 'Free'")
            prem = (await c2.fetchone())[0]
    await msg.reply_text(f"📊 **Statistics:**\nTotal Users: `{total}`\nVIP Members: `{prem}`\nQueue Tasks: `{task_queue.qsize()}`")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def admin_broadcast(_, msg: Message):
    if not msg.reply_to_message:
        await msg.reply_text("மெசேஜுக்கு ரிப்ளை செய்து `/broadcast` அனுப்பவும்.")
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

# ----------------- CALLBACK BUTTONS -----------------
@bot.on_callback_query()
async def cb_handler(client: Client, q: CallbackQuery):
    data = q.data
    user_id = q.from_user.id
    if data == "btn_login":
        await login_handler(client, q.message)
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
    elif data == "btn_buy":
        await q.message.reply_text("💳 **Payment (UPI):**\n\nUPI ID: `your-upi@okaxis`\nபணம் செலுத்தியதும் ஸ்கிரீன்ஷாட்டை அட்மினுக்கு அனுப்பவும்.")
    await q.answer()

# ----------------- MAIN BOOTSTRAP (FLAWLESS INITIALIZATION) -----------------
async def start_services():
    # 1. Database Init
    await init_db()

    # 2. Start Background Queue Worker
    asyncio.create_task(queue_worker())

    # 3. Midnight Scheduler
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    # 4. Start Client FIRST
    await bot.start()
    print("[*] Pyrogram Client Connected Successfully!")

    # 5. Set Menu Commands
    try:
        commands = [
            BotCommand("start", "Home"),
            BotCommand("login", "Connect Account"),
            BotCommand("logout", "Logout Session"),
            BotCommand("batch", "Batch Fetch (Standard+)"),
            BotCommand("clone", "Clone Entire Channel"),
            BotCommand("extract", "Extract Playlist Index"),
            BotCommand("filter", "Set Media Filter"),
            BotCommand("setcaption", "Custom Caption & Watermark"),
            BotCommand("delcaption", "Delete Caption"),
            BotCommand("referral", "Refer & Earn"),
            BotCommand("bonus", "Daily Streak Bonus"),
            BotCommand("trial", "1-Day Free Trial"),
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
        print("[*] Commands menu registered successfully!")
    except Exception as e:
        print(f"[Warning] Menu register: {e}")

    print("========================================")
    print("  [SUCCESS] Bot is online and polling!  ")
    print("========================================")

    # 6. Keep Bot Active without Freezing
    await idle()
    await bot.stop()

def main():
    if not DEFAULT_API_ID or not DEFAULT_API_HASH or not BOT_TOKEN:
        print("[FATAL ERROR] API_ID, API_HASH, அல்லது BOT_TOKEN அமைக்கப்படவில்லை! Render Environment Variables-ஐ சரிபார்க்கவும்.")
        return

    # Render Web Service Health Ping
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()

    # Safe Event Loop Launch
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_services())

if __name__ == "__main__":
    main()
