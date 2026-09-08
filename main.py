import os
import re
import asyncio
from datetime import datetime, timedelta
import pytz
import aiosqlite
from aiohttp import web
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
DEFAULT_API_ID = int(os.environ.get("API_ID", "0"))
DEFAULT_API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "pro_saver_bot")
PORT = int(os.environ.get("PORT", "8080"))

IST = pytz.timezone("Asia/Kolkata")
bot = Client(
    "pro_saver_session",
    api_id=DEFAULT_API_ID,
    api_hash=DEFAULT_API_HASH,
    bot_token=BOT_TOKEN
)

login_states = {}
task_queue = asyncio.Queue()

# ----------------- WEB SERVER (RENDER HEALTH CHECK) -----------------
async def ping_handler(request):
    return web.Response(text="Bot is running active!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", ping_handler)
    app.router.add_get("/health", ping_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[OK] Web health server listening on port {PORT}")

# ----------------- DATABASE -----------------
async def init_db():
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
                doc_thumb TEXT,
                vid_thumb TEXT,
                custom_caption TEXT,
                file_filter TEXT DEFAULT 'all'
            )
        """)
        await db.commit()

async def get_user(user_id, name="User"):
    async with aiosqlite.connect("bot_data.db") as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = await cursor.fetchone()
        if not user:
            await db.execute(
                "INSERT INTO users (user_id, name, plan, daily_limit, daily_used, lang) VALUES (?, ?, 'Free', 2, 0, 'ta')",
                (user_id, name)
            )
            await db.commit()
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            user = await cursor.fetchone()
        else:
            # Update name if different
            if user["name"] != name:
                await db.execute("UPDATE users SET name = ? WHERE user_id = ?", (name, user_id))
                await db.commit()
                cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
                user = await cursor.fetchone()
        return dict(user)

async def update_user(user_id, **kwargs):
    async with aiosqlite.connect("bot_data.db") as db:
        for k, v in kwargs.items():
            await db.execute(f"UPDATE users SET {k} = ? WHERE user_id = ?", (v, user_id))
        await db.commit()

async def deduct_usage(user_id):
    """Atomically deduct one usage from daily limit or bonus credits. Returns True if allowed."""
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
    async with aiosqlite.connect("bot_data.db") as db:
        await db.execute("UPDATE users SET daily_used = 0")
        await db.commit()
    print("[RESET] Daily usage reset completed at 12:00 AM IST.")

# ----------------- QUEUE WORKER -----------------
async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            handler, args, kwargs = task
            await asyncio.wait_for(handler(*args, **kwargs), timeout=600)
        except Exception as e:
            print(f"[Queue Error] {e}")
        finally:
            task_queue.task_done()

# ----------------- MULTI-LANGUAGE DICTIONARY -----------------
LANGUAGES = {
    "ta": {
        "welcome": "👋 **வணக்கம் {name}!**\n\nஇது உலகின் நம்பர் 1 **Restricted Saver, Cloner & Media Extractor Bot**.\n\n**நிலைமை:** `{plan}`\n**அக்கவுண்ட்:** {session}\n**மீடியா ஃபில்டர்:** `{filter}`\n\nசேனல் லிங்க்கை அனுப்பவும் அல்லது கீழேயுள்ள மெனுவை பயன்படுத்தவும்.",
        "btn_login": "🔑 கணக்கு இணைக்க",
        "btn_lang": "🌐 மொழியை மாற்ற",
        "btn_plans": "💎 திட்டங்கள்",
        "btn_myplan": "📊 எனது திட்டம்",
        "btn_trial": "🎁 இலவச ட்ரையல்",
        "btn_buy": "🛒 வாங்க",
        "btn_bonus": "🎁 தினசரி போனஸ்",
        "btn_ref": "👥 ரெஃபர் & சம்பாதி",
        "ask_api": "உங்கள் Telegram **API ID**-ஐ அனுப்பவும்.\n\nஅல்லது பாட்டின் சொந்த சாவியைப் பயன்படுத்த **/skip** அழுத்தவும்.",
        "ask_hash": "இப்போது உங்கள் **API HASH**-ஐ அனுப்பவும்:",
        "ask_phone": "நாட்டின் குறியீட்டுடன் போன் நம்பரை அனுப்பவும்:\nஉதாரணம்: `+919876543210`\nரத்து செய்ய: `/cancel`",
        "ask_otp": "டெலிகிராமில் வந்த OTP-ஐ இடைவெளி விட்டு அனுப்பவும்: `1 2 3 4 5`",
        "ask_2fa": "🔐 உங்கள் Two-Step Verification பாஸ்வேர்டை அனுப்பவும்:",
        "login_success": "✅ கணக்கு வெற்றிகரமாக இணைக்கப்பட்டது!\n\nAuth Key பிரச்சனை வந்தால் `/logout` செய்துவிட்டு மீண்டும் `/login` செய்யவும்.",
        "logged_out": "🚪 கணக்கு இணைப்பு துண்டிக்கப்பட்டது.",
        "limit_reached": "⛔ இன்றைய இலவச வரம்பு முடிந்தது. வரம்பற்ற பதிவிறக்கத்திற்கு விஐபி எடுக்கவும்.",
        "downloading": "📥 பதிவிறக்கப்படுகிறது...",
        "uploading": "📤 பதிவேற்றப்படுகிறது...",
        "completed": "✅ செயல்பாடு வெற்றிகரமாக முடிந்தது!",
        "not_connected": "⚠️ முதலில் `/login` செய்து கணக்கை இணைக்கவும்.",
        "invalid_link": "❌ தவறான டெலிகிராம் லிங்க்."
    },
    "en": {
        "welcome": "👋 **Hello {name}!**\n\nWelcome to World's Best **Restricted Saver, Cloner & Media Extractor Bot**.\n\n**Plan:** `{plan}`\n**Session:** {session}\n**Media Filter:** `{filter}`\n\nSend any restricted link to download or use commands from menu.",
        "btn_login": "🔑 Connect Account",
        "btn_lang": "🌐 Change Language",
        "btn_plans": "💎 Plans",
        "btn_myplan": "📊 My Plan",
        "btn_trial": "🎁 Free Trial",
        "btn_buy": "🛒 Buy Plan",
        "btn_bonus": "🎁 Daily Bonus",
        "btn_ref": "👥 Refer & Earn",
        "ask_api": "Send your Telegram **API ID**.\n\nOr click on **/skip** to use default bot API keys.",
        "ask_hash": "Now send your **API HASH**:",
        "ask_phone": "Send your phone number with country code:\nExample: `+919876543210`\nCancel: `/cancel`",
        "ask_otp": "Enter OTP with spaces: `1 2 3 4 5`",
        "ask_2fa": "🔐 Enter your 2-Step Verification Password:",
        "login_success": "✅ Account Connected Successfully!\n\nIf you get any Auth Key errors, `/logout` and `/login` again.",
        "logged_out": "🚪 Disconnected successfully.",
        "limit_reached": "⛔ Daily free limit reached. Upgrade plan for unlimited access.",
        "downloading": "📥 Downloading...",
        "uploading": "📤 Uploading...",
        "completed": "✅ Completed Successfully!",
        "not_connected": "⚠️ Please `/login` first to connect your account.",
        "invalid_link": "❌ Invalid Telegram link."
    },
    "hi": {
        "welcome": "👋 **नमस्ते {name}!**\n\nप्रतिबंधित सामग्री सेवर, बैच और चैनल क्लोनर बॉट में आपका स्वागत है।\n\n**योजना:** `{plan}`\n**सत्र:** {session}\n**मीडिया फ़िल्टर:** `{filter}`",
        "btn_login": "🔑 खाता जोड़ें",
        "btn_lang": "🌐 भाषा बदलें",
        "btn_plans": "💎 प्लान्स",
        "btn_myplan": "📊 मेरा प्लान",
        "btn_trial": "🎁 फ्री ट्रायल",
        "btn_buy": "🛒 अपग्रेड करें",
        "btn_bonus": "🎁 दैनिक बोनस",
        "btn_ref": "👥 रेफर और कमाएं",
        "ask_api": "अपना Telegram **API ID** भेजें या डिफ़ॉल्ट के लिए **/skip** दबाएं।",
        "ask_hash": "अब अपना **API HASH** भेजें:",
        "ask_phone": "देश कोड के साथ फ़ोन नंबर भेजें:\nउदाहरण: `+919876543210`",
        "ask_otp": "ओटीपी स्पेस के साथ भेजें: `1 2 3 4 5`",
        "ask_2fa": "🔐 अपना 2-Step पासवर्ड दर्ज करें:",
        "login_success": "✅ खाता सफलतापूर्वक जुड़ गया!",
        "logged_out": "🚪 खाता हटा दिया गया।",
        "limit_reached": "⛔ दैनिक सीमा समाप्त। अनलिमिटेड के लिए अपग्रेड करें।",
        "downloading": "📥 डाउनलोड हो रहा है...",
        "uploading": "📤 अपलोड हो रहा है...",
        "completed": "✅ सफलतापूर्वक पूर्ण हुआ!",
        "not_connected": "⚠️ कृपया पहले `/login` करें।",
        "invalid_link": "❌ अमान्य टेलीग्राम लिंक।"
    }
}

# ----------------- COMMANDS MENU -----------------
async def set_bot_commands():
    commands = [
        BotCommand("start", "Home"),
        BotCommand("login", "Connect Account"),
        BotCommand("logout", "Logout Session"),
        BotCommand("batch", "Batch Fetch (Standard+)"),
        BotCommand("clone", "Clone Channel / Group"),
        BotCommand("extract", "Extract Channel Playlist"),
        BotCommand("filter", "Set Media Filter"),
        BotCommand("setcaption", "Custom Caption & Watermark"),
        BotCommand("delcaption", "Remove Caption"),
        BotCommand("referral", "Refer Friends & Earn"),
        BotCommand("bonus", "Daily Streak Bonus"),
        BotCommand("trial", "Free Trial (1-Day)"),
        BotCommand("myplan", "My Plan & Usage"),
        BotCommand("plans", "Premium Plans"),
        BotCommand("id", "Get Telegram ID"),
        BotCommand("setthumb", "Set Doc Thumbnail"),
        BotCommand("delthumb", "Remove Doc Thumbnail"),
        BotCommand("mythumb", "View Doc Thumbnail"),
        BotCommand("setvthumb", "Set Video Thumbnail"),
        BotCommand("delvthumb", "Remove Video Thumbnail"),
        BotCommand("myvthumb", "View Video Thumbnail"),
        BotCommand("ap", "Assign Plan (Admin)"),
        BotCommand("rp", "Reset User (Admin)"),
        BotCommand("ps", "Check Plan (Admin)"),
        BotCommand("stats", "Bot Statistics (Admin)"),
        BotCommand("broadcast", "Broadcast Message (Admin)")
    ]
    await bot.set_bot_commands(commands)

# ----------------- START & REFERRAL -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, msg: Message):
    user_id = msg.from_user.id
    args = msg.text.split()
    user = await get_user(user_id, msg.from_user.first_name)

    # Referral handling
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].replace("ref_", ""))
            if referrer_id != user_id and not user.get("referred_by"):
                await update_user(user_id, referred_by=referrer_id)
                async with aiosqlite.connect("bot_data.db") as db:
                    await db.execute("UPDATE users SET bonus_credits = bonus_credits + 2, ref_count = ref_count + 1 WHERE user_id = ?", (referrer_id,))
                    await db.commit()
                try:
                    await bot.send_message(referrer_id, f"🎉 **புதிய ரெஃபரல்!** {msg.from_user.first_name} உங்கள் லிங்க் மூலம் இணைந்தார். உங்களுக்கு **+2 போனஸ் பதிவிறக்கங்கள்** சேர்க்கப்பட்டன!")
                except:
                    pass
        except:
            pass

    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])
    session_status = "✅ Connected" if user.get("session") else "❌ Not Connected"
    filter_type = user.get("file_filter", "all").upper()

    text = t["welcome"].format(
        name=msg.from_user.mention,
        plan=user["plan"],
        session=session_status,
        filter=filter_type
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(t["btn_login"], callback_data="btn_login"), InlineKeyboardButton(t["btn_lang"], callback_data="btn_lang")],
        [InlineKeyboardButton(t["btn_plans"], callback_data="btn_plans"), InlineKeyboardButton(t["btn_myplan"], callback_data="btn_myplan")],
        [InlineKeyboardButton(t["btn_bonus"], callback_data="btn_bonus"), InlineKeyboardButton(t["btn_ref"], callback_data="btn_ref")],
        [InlineKeyboardButton(t["btn_trial"], callback_data="btn_trial")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

# ----------------- LOGIN / LOGOUT -----------------
@bot.on_message(filters.command("login") & filters.private)
async def login_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])
    login_states[msg.from_user.id] = {"step": "API_ID"}
    await msg.reply_text(t["ask_api"])

@bot.on_message(filters.command("logout") & filters.private)
async def logout_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])
    await update_user(msg.from_user.id, session=None)
    await msg.reply_text(t["logged_out"])

# ----------------- TEXT & LINK HANDLER -----------------
@bot.on_message(filters.text & filters.private)
async def text_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    text = msg.text.strip()
    user = await get_user(user_id)
    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])

    if text == "/cancel":
        login_states.pop(user_id, None)
        await msg.reply_text("Cancelled.")
        return

    state = login_states.get(user_id)
    if state:
        step = state.get("step")
        if step == "API_ID":
            if text == "/skip":
                login_states[user_id] = {"step": "PHONE", "api_id": DEFAULT_API_ID, "api_hash": DEFAULT_API_HASH}
                await msg.reply_text(t["ask_phone"])
            elif text.isdigit():
                login_states[user_id] = {"step": "API_HASH", "api_id": int(text)}
                await msg.reply_text(t["ask_hash"])
            return

        if step == "API_HASH":
            login_states[user_id]["api_hash"] = text
            login_states[user_id]["step"] = "PHONE"
            await msg.reply_text(t["ask_phone"])
            return

        if step == "PHONE":
            phone = text.replace(" ", "")
            u_client = Client(f"session_{user_id}", api_id=state.get("api_id"), api_hash=state.get("api_hash"), in_memory=True)
            await u_client.connect()
            try:
                await msg.reply_text("Sending OTP...")
                code = await u_client.send_code(phone)
                login_states[user_id].update({"step": "OTP", "client": u_client, "phone": phone, "hash": code.phone_code_hash})
                await msg.reply_text(t["ask_otp"])
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
                await u_client.disconnect()
                await update_user(user_id, session=s_str)
                login_states.pop(user_id, None)
                await msg.reply_text(t["login_success"])
            except SessionPasswordNeeded:
                login_states[user_id]["step"] = "2FA"
                await msg.reply_text(t["ask_2fa"])
            except Exception as e:
                login_states.pop(user_id, None)
                await msg.reply_text(f"❌ Error: {str(e)}")
            return

        if step == "2FA":
            u_client = state["client"]
            try:
                await u_client.check_password(password=text)
                s_str = await u_client.export_session_string()
                await u_client.disconnect()
                await update_user(user_id, session=s_str)
                login_states.pop(user_id, None)
                await msg.reply_text(t["login_success"])
            except Exception as e:
                login_states.pop(user_id, None)
                await msg.reply_text(f"❌ Error: {str(e)}")
            return

    # Direct link detection
    if "t.me/" in text:
        await task_queue.put((execute_download, (client, msg, user_id), {}))
        await msg.reply_text("⏳ டவுன்லோட் வரிசையில் சேர்க்கப்பட்டது...")

# ----------------- DOWNLOAD EXECUTION -----------------
async def execute_download(bot_client: Client, msg: Message, user_id: int):
    user = await get_user(user_id)
    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])

    if not user.get("session"):
        await bot_client.send_message(user_id, t["not_connected"])
        return

    match = re.search(r"t\.me/(c/)?([a-zA-Z0-9_]+)/(\d+)(?:-(\d+))?", msg.text.strip())
    if not match:
        await bot_client.send_message(user_id, t["invalid_link"])
        return

    is_private = bool(match.group(1))
    chat_raw = match.group(2)
    start_id = int(match.group(3))
    end_id = int(match.group(4)) if match.group(4) else start_id
    chat_id = int(f"-100{chat_raw}") if is_private or chat_raw.isdigit() else chat_raw

    filter_type = user.get("file_filter", "all")
    status_msg = await bot_client.send_message(user_id, t["downloading"])
    u_client = Client(f"ub_exec_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    try:
        for cur_id in range(start_id, end_id + 1):
            # Check usage before each file
            if not await deduct_usage(user_id):
                await bot_client.send_message(user_id, t["limit_reached"])
                break

            try:
                target = await u_client.get_messages(chat_id, cur_id)
                if not target or not target.media:
                    continue

                # Apply filter
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

                await status_msg.edit_text(f"📤 Uploading ID: {cur_id}...")
                caption = user.get("custom_caption") or target.caption or ""

                # Thumbnails
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
                await asyncio.sleep(1)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                continue

        await status_msg.edit_text(t["completed"])
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- FORWARDED MESSAGE HANDLER -----------------
@bot.on_message(filters.forwarded & filters.private)
async def forwarded_catcher(client: Client, msg: Message):
    user_id = msg.from_user.id
    user = await get_user(user_id)
    if not msg.media:
        await msg.reply_text("❌ இந்த பார்வர்ட் மெசேஜில் மீடியா எதுவும் இல்லை.")
        return

    await task_queue.put((execute_forward_download, (client, msg, user_id), {}))
    await msg.reply_text("⏳ பார்வர்ட் மீடியா வரிசையில் சேர்க்கப்பட்டது...")

async def execute_forward_download(bot_client: Client, msg: Message, user_id: int):
    user = await get_user(user_id)
    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])

    if not user.get("session"):
        await bot_client.send_message(user_id, t["not_connected"])
        return

    # Check usage
    if not await deduct_usage(user_id):
        await bot_client.send_message(user_id, t["limit_reached"])
        return

    status_msg = await bot_client.send_message(user_id, t["downloading"])
    try:
        file_path = await msg.download()
        await status_msg.edit_text(t["uploading"])

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
        await status_msg.edit_text(t["completed"])
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

# ----------------- BATCH, CLONE, EXTRACT -----------------
@bot.on_message(filters.command("batch") & filters.private)
async def batch_cmd(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    lang = user.get("lang", "ta")
    t = LANGUAGES.get(lang, LANGUAGES["ta"])
    if user["plan"] == "Free":
        await msg.reply_text("🔒 Batch is available for Standard and above plans.")
        return
    await msg.reply_text("📦 Batch mode: send start-end link like `https://t.me/c/1234567890/10-30`")

@bot.on_message(filters.command("clone") & filters.private)
async def clone_cmd(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Ultimate" and user["user_id"] != ADMIN_ID:
        await msg.reply_text("⭐ Channel cloning is only for Ultimate plan.")
        return
    if not user.get("session"):
        await msg.reply_text("⚠️ First /login to connect your account.")
        return
    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("Usage: `/clone <source_id> <target_id>`")
        return
    src = args[1]
    dest = args[2]
    await task_queue.put((execute_clone, (client, msg, user, src, dest), {}))
    await msg.reply_text("🚀 Cloning queued...")

async def execute_clone(bot_client: Client, msg: Message, user: dict, src, dest):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "🚀 Cloning started...")
    u_client = Client(f"ub_clone_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()
    count = 0
    try:
        async for post in u_client.get_chat_history(src):
            if post.media:
                if not await deduct_usage(user_id):
                    await bot_client.send_message(user_id, "⛔ Limit reached during clone.")
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
                except:
                    continue
        await status_msg.edit_text(f"✅ Cloning completed. Total files: {count}")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

@bot.on_message(filters.command("extract") & filters.private)
async def extract_cmd(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if not user.get("session"):
        await msg.reply_text("⚠️ First /login to connect your account.")
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("Usage: `/extract <channel_id>`")
        return
    target = args[1]
    if target.lstrip("-").isdigit():
        target = int(target)
    await task_queue.put((execute_extract, (client, msg, user, target), {}))
    await msg.reply_text("📑 Extract queued...")

async def execute_extract(bot_client: Client, msg: Message, user: dict, target):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "📑 Extracting playlist...")
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
                    media_type = "Document" if post.document else "Video" if post.video else "Audio" if post.audio else "Photo"
                    f.write(f"{count}. {media_type} | ID: {post.id} | https://t.me/c/{str(target).replace('-100', '')}/{post.id}\n")
        await bot_client.send_document(user_id, file_name, caption=f"✅ {count} media links extracted.")
        if os.path.exists(file_name):
            os.remove(file_name)
        await status_msg.edit_text("✅ Extraction completed.")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- THUMBNAIL MANAGEMENT -----------------
@bot.on_message(filters.command("setthumb") & filters.private)
async def set_thumb(_, msg: Message):
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with /setthumb")
        return
    os.makedirs("thumbnails", exist_ok=True)
    path = f"thumbnails/thumb_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=path)
    await update_user(msg.from_user.id, doc_thumb=path)
    await msg.reply_text("✅ Document thumbnail saved.")

@bot.on_message(filters.command("delthumb") & filters.private)
async def del_thumb(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]):
        os.remove(user["doc_thumb"])
    await update_user(msg.from_user.id, doc_thumb=None)
    await msg.reply_text("🗑️ Document thumbnail removed.")

@bot.on_message(filters.command("mythumb") & filters.private)
async def mythumb(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("doc_thumb") and os.path.exists(user["doc_thumb"]):
        await msg.reply_photo(user["doc_thumb"], caption="Your document thumbnail")
    else:
        await msg.reply_text("No thumbnail set.")

@bot.on_message(filters.command("setvthumb") & filters.private)
async def set_vthumb(_, msg: Message):
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with /setvthumb")
        return
    os.makedirs("thumbnails", exist_ok=True)
    path = f"thumbnails/vthumb_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=path)
    await update_user(msg.from_user.id, vid_thumb=path)
    await msg.reply_text("✅ Video thumbnail saved.")

@bot.on_message(filters.command("delvthumb") & filters.private)
async def del_vthumb(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]):
        os.remove(user["vid_thumb"])
    await update_user(msg.from_user.id, vid_thumb=None)
    await msg.reply_text("🗑️ Video thumbnail removed.")

@bot.on_message(filters.command("myvthumb") & filters.private)
async def myvthumb(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user.get("vid_thumb") and os.path.exists(user["vid_thumb"]):
        await msg.reply_photo(user["vid_thumb"], caption="Your video thumbnail")
    else:
        await msg.reply_text("No video thumbnail set.")

# ----------------- CAPTION & FILTER -----------------
@bot.on_message(filters.command("setcaption") & filters.private)
async def set_caption_cmd(_, msg: Message):
    caption = msg.text.partition(" ")[2].strip()
    if not caption:
        await msg.reply_text("Usage: `/setcaption your caption`")
        return
    await update_user(msg.from_user.id, custom_caption=caption)
    await msg.reply_text(f"✅ Custom caption set: `{caption}`")

@bot.on_message(filters.command("delcaption") & filters.private)
async def del_caption_cmd(_, msg: Message):
    await update_user(msg.from_user.id, custom_caption=None)
    await msg.reply_text("🗑️ Custom caption removed.")

@bot.on_message(filters.command("filter") & filters.private)
async def filter_cmd(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2 or args[1].lower() not in ["all", "video", "doc", "audio", "photo"]:
        await msg.reply_text("Usage: `/filter <all|video|doc|audio|photo>`")
        return
    chosen = args[1].lower()
    await update_user(msg.from_user.id, file_filter=chosen)
    await msg.reply_text(f"🎯 Media filter set to `{chosen.upper()}`")

# ----------------- REFERRAL & BONUS -----------------
@bot.on_message(filters.command("referral") & filters.private)
async def referral_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{msg.from_user.id}"
    text = (
        "👥 **REFER & EARN**\n\n"
        f"Your referral link:\n`{ref_link}`\n\n"
        f"Referrals: {user.get('ref_count', 0)}\n"
        f"Bonus credits: {user.get('bonus_credits', 0)}\n\n"
        "Each referral gives you +2 bonus downloads."
    )
    await msg.reply_text(text)

@bot.on_message(filters.command("bonus") & filters.private)
async def bonus_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    today = datetime.now(IST).strftime("%Y-%m-%d")
    if user.get("last_bonus_date") == today:
        await msg.reply_text("⚠️ You already claimed today's bonus.")
        return

    streak = (user.get("streak_count", 0) or 0) + 1
    await update_user(
        msg.from_user.id,
        last_bonus_date=today,
        streak_count=streak,
        bonus_credits=(user.get("bonus_credits", 0) or 0) + 1
    )
    reward_text = f"🎉 Daily bonus claimed!\n🔥 Streak: {streak} days\n🎁 +1 bonus download added."
    if streak >= 30 and user["plan"] == "Free":
        await update_user(msg.from_user.id, plan="Standard", daily_limit=50)
        reward_text += "\n\n🏆 30-day streak! You got free Standard plan!"
    await msg.reply_text(reward_text)

# ----------------- PLANS & MISC -----------------
@bot.on_message(filters.command("plans") & filters.private)
async def plans_cmd(_, msg: Message):
    text = (
        "💎 **PREMIUM PLANS**\n\n"
        "🥈 Standard: 50 files/day\n"
        "🥇 Premium: 100 files/day\n"
        "🔷 Ultimate: Unlimited + Cloning"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Buy Plan (UPI)", callback_data="btn_buy")],
        [InlineKeyboardButton("💬 Admin", url=f"tg://user?id={ADMIN_ID}")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("trial") & filters.private)
async def trial_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Free":
        await msg.reply_text("Already on a paid or trial plan.")
        return
    await update_user(msg.from_user.id, plan="Trial (Standard)", daily_limit=50)
    await msg.reply_text("🎁 1-day Standard trial activated!")

@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id, msg.from_user.first_name)
    plan = user["plan"]
    limit = "Unlimited" if plan == "Ultimate" else str(user["daily_limit"])
    used = user["daily_used"]
    bonus = user.get("bonus_credits", 0)
    left = "Unlimited" if plan == "Ultimate" else str(max(0, user["daily_limit"] - used) + bonus)
    text = (
        f"📋 **USER PLAN**\n\n"
        f"👤 Name: {user['name']}\n"
        f"🆔 ID: `{user['user_id']}`\n"
        f"🏷️ Plan: {plan}\n"
        f"📊 Used today: {used}/{limit}\n"
        f"🎁 Bonus: {bonus}\n"
        f"🎯 Total left: {left}\n"
    )
    buttons = InlineKeyboardMarkup([[InlineKeyboardButton("⬆️ Upgrade", callback_data="btn_plans")]])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("id") & filters.private)
async def id_cmd(_, msg: Message):
    await msg.reply_text(f"🆔 Your Telegram ID: `{msg.from_user.id}`")

# ----------------- ADMIN COMMANDS -----------------
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
        await msg.reply_text("Invalid plan.")
        return
    await update_user(t_uid, plan=t_plan, daily_limit=limits[t_plan])
    await msg.reply_text(f"✅ User {t_uid} plan changed to {t_plan}.")

@bot.on_message(filters.command("rp") & filters.user(ADMIN_ID))
async def admin_rp(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("Usage: `/rp <user_id>`")
        return
    t_uid = int(args[1])
    await update_user(t_uid, plan="Free", daily_limit=2, daily_used=0)
    await msg.reply_text(f"✅ User {t_uid} reset to Free.")

@bot.on_message(filters.command("ps") & filters.user(ADMIN_ID))
async def admin_ps(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("Usage: `/ps <user_id>`")
        return
    t_uid = int(args[1])
    user = await get_user(t_uid)
    await msg.reply_text(f"User {t_uid}:\nPlan: {user['plan']}\nLimit: {user['daily_limit']}\nUsed: {user['daily_used']}")

@bot.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def admin_stats(_, msg: Message):
    async with aiosqlite.connect("bot_data.db") as db:
        c1 = await db.execute("SELECT COUNT(*) FROM users")
        total = (await c1.fetchone())[0]
        c2 = await db.execute("SELECT COUNT(*) FROM users WHERE plan != 'Free'")
        prem = (await c2.fetchone())[0]
    await msg.reply_text(f"📊 Total users: {total}\nPremium: {prem}\nQueue: {task_queue.qsize()}")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def admin_broadcast(_, msg: Message):
    if not msg.reply_to_message:
        await msg.reply_text("Reply to a message with /broadcast")
        return
    async with aiosqlite.connect("bot_data.db") as db:
        c = await db.execute("SELECT user_id FROM users")
        rows = await c.fetchall()
    sent = 0
    for (uid,) in rows:
        try:
            await msg.reply_to_message.copy(uid)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await msg.reply_text(f"✅ Broadcast sent to {sent} users.")

# ----------------- CALLBACK HANDLER -----------------
@bot.on_callback_query()
async def cb_handler(client: Client, q: CallbackQuery):
    data = q.data
    user_id = q.from_user.id

    if data == "btn_login":
        await login_cmd(client, q.message)
    elif data == "btn_plans":
        await plans_cmd(client, q.message)
    elif data == "btn_myplan":
        await myplan_cmd(client, q.message)
    elif data == "btn_trial":
        await trial_cmd(client, q.message)
    elif data == "btn_bonus":
        await bonus_cmd(client, q.message)
    elif data == "btn_ref":
        await referral_cmd(client, q.message)
    elif data == "btn_buy":
        await q.message.reply_text("UPI ID: `your-upi@okaxis`\nSend screenshot to admin.")
    elif data == "btn_lang":
        # Language selection inline
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("தமிழ்", callback_data="set_ta"),
             InlineKeyboardButton("English", callback_data="set_en")],
            [InlineKeyboardButton("हिन्दी", callback_data="set_hi")]
        ])
        await q.message.edit_text("Select language:", reply_markup=buttons)
    elif data.startswith("set_"):
        lang = data.replace("set_", "")
        await update_user(user_id, lang=lang)
        await q.answer(f"Language changed to {lang}")
        # Refresh start message
        await start_cmd(client, q.message)
    await q.answer()

# ----------------- MAIN BOOTSTRAP -----------------
async def main():
    if not DEFAULT_API_ID or not DEFAULT_API_HASH or not BOT_TOKEN:
        print("[FATAL] API_ID, API_HASH, BOT_TOKEN not set.")
        return

    await init_db()

    # Web server
    await start_web_server()

    # Queue worker
    asyncio.create_task(queue_worker())

    # Scheduler for midnight reset
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    await bot.start()
    await set_bot_commands()
    print("Bot is running...")
    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
