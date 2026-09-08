import os
import re
import asyncio
from datetime import datetime
import pytz
import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from pyrogram import Client, filters
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
DEFAULT_API_ID = int(os.environ.get("API_ID", "1234567"))
DEFAULT_API_HASH = os.environ.get("API_HASH", "your_api_hash_here")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token_here")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "1000931167"))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "SaveRestrictedProBot")

IST = pytz.timezone("Asia/Kolkata")
bot = Client("MegaPartnerSaverBot", api_id=DEFAULT_API_ID, api_hash=DEFAULT_API_HASH, bot_token=BOT_TOKEN)

login_states = {}
task_queue = asyncio.Queue()

# ----------------- SQLITE DATABASE ENGINE -----------------
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
        return dict(user)

async def update_user(user_id, **kwargs):
    async with aiosqlite.connect("bot_data.db") as db:
        for k, v in kwargs.items():
            await db.execute(f"UPDATE users SET {k} = ? WHERE user_id = ?", (v, user_id))
        await db.commit()

# Daily reset at 12:00 AM IST
async def daily_reset_job():
    async with aiosqlite.connect("bot_data.db") as db:
        await db.execute("UPDATE users SET daily_used = 0")
        await db.commit()
    print("Daily usage reset completed at 12:00 AM IST.")

# ----------------- ASYNC WORKER ENGINE (TASK QUEUE) -----------------
async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            handler, args, kwargs = task
            await handler(*args, **kwargs)
        except Exception as e:
            print(f"Queue execution error: {e}")
        finally:
            task_queue.task_done()

# ----------------- COMMANDS MENU -----------------
async def set_bot_commands():
    commands = [
        BotCommand("start", "Home"),
        BotCommand("login", "Connect Account"),
        BotCommand("logout", "Logout Session"),
        BotCommand("batch", "Batch Fetch (Standard+)"),
        BotCommand("clone", "Clone Channel / Group"),
        BotCommand("extract", "Extract Channel Playlist (.txt)"),
        BotCommand("filter", "Set Media Filter (video/doc/all)"),
        BotCommand("setcaption", "Custom Caption & Watermark"),
        BotCommand("delcaption", "Remove Caption"),
        BotCommand("referral", "Refer Friends & Earn"),
        BotCommand("bonus", "Daily Streak Bonus"),
        BotCommand("trial", "Free Trial (1-Day)"),
        BotCommand("myplan", "My Plan & Statistics"),
        BotCommand("plans", "Premium Plans"),
        BotCommand("lang", "Change Language"),
        BotCommand("id", "Get Telegram ID"),
        BotCommand("setthumb", "Set Doc/Audio Thumbnail"),
        BotCommand("delthumb", "Remove Doc/Audio Thumbnail"),
        BotCommand("setvthumb", "Set Video Thumbnail"),
        BotCommand("delvthumb", "Remove Video Thumbnail"),
        BotCommand("ap", "Assign Plan (Admin)"),
        BotCommand("rp", "Reset User (Admin)"),
        BotCommand("ps", "Check Plan (Admin)"),
        BotCommand("stats", "Bot Statistics (Admin)"),
        BotCommand("broadcast", "Broadcast Message (Admin)")
    ]
    await bot.set_bot_commands(commands)

# ----------------- START & REFERRAL ENGINE -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, msg: Message):
    user_id = msg.from_user.id
    args = msg.text.split()
    user = await get_user(user_id, msg.from_user.first_name)

    # Referral checking
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].replace("ref_", ""))
            if referrer_id != user_id and not user.get("referred_by"):
                await update_user(user_id, referred_by=referrer_id)
                # Referrer gets 2 bonus downloads
                async with aiosqlite.connect("bot_data.db") as db:
                    await db.execute("UPDATE users SET bonus_credits = bonus_credits + 2, ref_count = ref_count + 1 WHERE user_id = ?", (referrer_id,))
                    await db.commit()
                try:
                    await bot.send_message(referrer_id, f"🎉 **புதிய ரெஃபரல்!** {msg.from_user.first_name} உங்கள் லிங்க் மூலம் இணைந்தார். உங்களுக்கு **+2 போனஸ் பதிவிறக்கங்கள்** சேர்க்கப்பட்டன!")
                except Exception:
                    pass
        except Exception:
            pass

    session_status = "✅ Connected" if user.get("session") else "❌ Not Connected"
    text = (
        f"👋 **வணக்கம் {msg.from_user.mention}!**\n\n"
        "இது உலகின் நம்பர் 1 **Restricted Saver, Cloner & Media Extractor Bot**.\n\n"
        f"**நிலைமை:** `{user['plan']}`\n"
        f"**அக்கவுண்ட்:** {session_status}\n"
        f"**மீடியா ஃபில்டர்:** `{user.get('file_filter', 'all').upper()}`\n\n"
        "🔗 **நேரடி லிங்க் அல்லது ஃபார்வர்ட் செய்யப்பட்ட மெசேஜை இங்கு அனுப்பினால் போதும்!**"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Connect Account", callback_data="btn_login"), InlineKeyboardButton("📦 Plans", callback_data="btn_plans")],
        [InlineKeyboardButton("🎁 Daily Bonus", callback_data="btn_bonus"), InlineKeyboardButton("👥 Refer & Earn", callback_data="btn_ref")],
        [InlineKeyboardButton("📊 My Plan", callback_data="btn_myplan"), InlineKeyboardButton("🌐 Language", callback_data="btn_lang")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

# ----------------- DAILY BONUS & STREAK -----------------
@bot.on_message(filters.command("bonus") & filters.private)
async def bonus_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    today = datetime.now(IST).strftime("%Y-%m-%d")

    if user.get("last_bonus_date") == today:
        await msg.reply_text("⚠️ இன்றைய போனஸை ஏற்கனவே பெற்றுவிட்டீர்கள்! நாளை மீண்டும் வாருங்கள்.")
        return

    streak = (user.get("streak_count", 0) or 0) + 1
    await update_user(
        msg.from_user.id,
        last_bonus_date=today,
        streak_count=streak,
        bonus_credits=(user.get("bonus_credits", 0) or 0) + 1
    )

    reward_text = f"🎉 **தினசரி போனஸ் பெறப்பட்டது!**\n\n🔥 **Streak:** {streak} Days\n🎁 **போனஸ்:** +1 பதிவிறக்க கிரெடிட் சேர்க்கப்பட்டது."
    if streak >= 30 and user["plan"] == "Free":
        await update_user(msg.from_user.id, plan="Standard", daily_limit=50)
        reward_text += "\n\n🏆 **30 Days Streak வெற்றி!** உங்களுக்கு இலவசமாக **Standard VIP Plan** செயல்படுத்தப்பட்டது!"

    await msg.reply_text(reward_text)

# ----------------- REFERRAL SYSTEM -----------------
@bot.on_message(filters.command("referral") & filters.private)
async def referral_cmd(_, msg: Message):
    user = await get_user(msg.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{msg.from_user.id}"
    text = (
        "👥 **REFER & EARN PROGRAM** 👥\n\n"
        "உங்கள் தனித்துவமான ரெஃபரல் லிங்க் மூலம் நண்பர்களை அழைக்கவும்:\n"
        f"`{ref_link}`\n\n"
        f"• **அழைக்கப்பட்ட நபர்கள்:** `{user.get('ref_count', 0)}`\n"
        f"• **கிடைத்த போனஸ் கிரெடிட்கள்:** `{user.get('bonus_credits', 0)}`\n\n"
        "💡 *ஒவ்வொரு ரெஃபரலுக்கும் உங்களுக்கு +2 போனஸ் பதிவிறக்கங்கள் வழங்கப்படும்!*"
    )
    await msg.reply_text(text)

# ----------------- CAPTION & WATERMARK CUSTOMIZATION -----------------
@bot.on_message(filters.command("setcaption") & filters.private)
async def set_caption_cmd(_, msg: Message):
    caption = msg.text.partition(" ")[2].strip()
    if not caption:
        await msg.reply_text("பயன்பாடு:\n`/setcaption உங்கள் தனிப்பட்ட கேப்ஷன் அல்லது வாட்டர்மார்க்`\n\nஉதாரணம்:\n`/setcaption ⚡ Downloaded via @MyChannel`")
        return
    await update_user(msg.from_user.id, custom_caption=caption)
    await msg.reply_text(f"✅ **தனிப்பட்ட கேப்ஷன் சேமிக்கப்பட்டது:**\n\n`{caption}`")

@bot.on_message(filters.command("delcaption") & filters.private)
async def del_caption_cmd(_, msg: Message):
    await update_user(msg.from_user.id, custom_caption=None)
    await msg.reply_text("🗑️ தனிப்பட்ட கேப்ஷன் நீக்கப்பட்டது. பழைய அசல் கேப்ஷன் பயன்படுத்தப்படும்.")

# ----------------- FILE TYPE FILTER -----------------
@bot.on_message(filters.command("filter") & filters.private)
async def filter_cmd(_, msg: Message):
    args = msg.text.split()
    if len(args) < 2 or args[1].lower() not in ["all", "video", "doc", "audio", "photo"]:
        await msg.reply_text("பயன்பாடு: `/filter <all|video|doc|audio|photo>`\n\nஉதாரணம்:\n`/filter video` (வீடியோக்களை மட்டும் டவுன்லோட் செய்ய)")
        return
    chosen = args[1].lower()
    await update_user(msg.from_user.id, file_filter=chosen)
    await msg.reply_text(f"🎯 **மீடியா ஃபில்டர் மாற்றப்பட்டது:** `{chosen.upper()}`")

# ----------------- PLAYLIST / BULK LINK EXTRACTOR -----------------
@bot.on_message(filters.command("extract") & filters.private)
async def extract_cmd(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login` செய்யவும்.")
        return

    args = msg.text.split()
    if len(args) < 2:
        await msg.reply_text("பயன்பாடு: `/extract <channel_id_or_username>`\nஉதாரணம்: `/extract -1001234567890`")
        return

    target = int(args[1]) if args[1].lstrip("-").isdigit() else args[1]
    status_msg = await msg.reply_text("📑 சேனல் மீடியா போஸ்ட்கள் தொகுக்கப்படுகின்றன...")

    u_client = Client(f"ub_ext_{msg.from_user.id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    file_name = f"playlist_{msg.from_user.id}.txt"
    try:
        count = 0
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(f"--- Channel Media Playlist ({target}) ---\n\n")
            async for post in u_client.get_chat_history(target, limit=1000):
                if post.media:
                    count += 1
                    media_type = "Document" if post.document else "Video" if post.video else "Audio" if post.audio else "Photo"
                    f.write(f"[{count}] Type: {media_type} | Message ID: {post.id} | Link: https://t.me/c/{str(target).replace('-100', '')}/{post.id}\n")

        await status_msg.edit_text(f"✅ {count} மீடியா பதிவுகள் தொகுக்கப்பட்டன. கோப்பு அனுப்பப்படுகிறது...")
        await client.send_document(msg.from_user.id, file_name, caption=f"📑 **Channel Playlist Index**\nTotal Media Posts: {count}")
        if os.path.exists(file_name):
            os.remove(file_name)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ பிழை: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- FORWARDED MESSAGES AUTO-CATCHER -----------------
@bot.on_message(filters.forwarded & filters.private)
async def forwarded_catcher(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if not msg.media:
        await msg.reply_text("❌ இந்த பார்வர்ட் மெசேஜில் டவுன்லோட் செய்ய மீடியா எதுவும் இல்லை.")
        return

    await task_queue.put((execute_single_forward_download, (client, msg, user), {}))
    await msg.reply_text("⏳ பார்வர்ட் செய்யப்பட்ட மீடியா வரிசையில் (Queue) சேர்க்கப்பட்டது...")

async def execute_single_forward_download(bot_client: Client, msg: Message, user: dict):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "📥 பார்வர்ட் மீடியா அசல் தரத்தில் பதிவிறக்கப்படுகிறது...")
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
        await status_msg.edit_text(f"❌ பிழை: {str(e)}")

# ----------------- LOGIN / /skip FLOW -----------------
@bot.on_message(filters.command("login") & filters.private)
async def login_cmd(_, msg: Message):
    text = "Send Your API ID.\n\nOr click on /skip to skip this process and login by phone no."
    login_states[msg.from_user.id] = {"step": "API_ID"}
    await msg.reply_text(text)

@bot.on_message(filters.command("logout") & filters.private)
async def logout_cmd(_, msg: Message):
    await update_user(msg.from_user.id, session=None)
    await msg.reply_text("🚪 உங்கள் கணக்கு இணைப்பு துண்டிக்கப்பட்டது.")

@bot.on_message(filters.text & filters.private)
async def text_and_link_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    text = msg.text.strip()
    user = await get_user(user_id)

    if text == "/cancel":
        login_states.pop(user_id, None)
        await msg.reply_text("செயல்பாடு ரத்து செய்யப்பட்டது.")
        return

    # Login Step Engine
    state = login_states.get(user_id)
    if state:
        step = state.get("step")
        if step == "API_ID":
            if text == "/skip":
                login_states[user_id] = {"step": "PHONE", "api_id": DEFAULT_API_ID, "api_hash": DEFAULT_API_HASH}
                await msg.reply_text("Please send your phone number with country code:\nExample: `+919876543210`")
            elif text.isdigit():
                login_states[user_id] = {"step": "API_HASH", "api_id": int(text)}
                await msg.reply_text("Now send your API HASH:")
            return

        if step == "API_HASH":
            login_states[user_id]["api_hash"] = text
            login_states[user_id]["step"] = "PHONE"
            await msg.reply_text("Please send your phone number with country code:\nExample: `+919876543210`")
            return

        if step == "PHONE":
            phone = text.replace(" ", "")
            u_client = Client(f"session_{user_id}", api_id=state.get("api_id"), api_hash=state.get("api_hash"), in_memory=True)
            await u_client.connect()
            try:
                await msg.reply_text("Sending OTP...")
                code = await u_client.send_code(phone)
                login_states[user_id].update({"step": "OTP", "client": u_client, "phone": phone, "hash": code.phone_code_hash})
                await msg.reply_text("Please enter OTP with spaces: `1 2 3 4 5`")
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
                await msg.reply_text("✅ Account Login Successfully!")
            except SessionPasswordNeeded:
                login_states[user_id]["step"] = "2FA"
                await msg.reply_text("🔐 Enter your 2-Step Verification Password:")
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
                await msg.reply_text("✅ Account Login Successfully!")
            except Exception as e:
                login_states.pop(user_id, None)
                await msg.reply_text(f"❌ Error: {str(e)}")
            return

    # Direct Link Detection
    if "t.me/" in text:
        await task_queue.put((execute_link_download, (client, msg, user), {}))
        await msg.reply_text("⏳ டவுன்லோட் கோரிக்கை பணி வரிசையில் (Queue) சேர்க்கப்பட்டது...")

# ----------------- QUEUED DOWNLOAD EXECUTION -----------------
async def execute_link_download(bot_client: Client, msg: Message, user: dict):
    user_id = user["user_id"]
    if not user.get("session"):
        await bot_client.send_message(user_id, "⚠️ முதலில் `/login` செய்யவும்.")
        return

    plan = user["plan"]
    limit = user["daily_limit"] + user.get("bonus_credits", 0)
    used = user["daily_used"]

    if plan != "Ultimate" and used >= limit:
        await bot_client.send_message(user_id, "⛔ தினசரி வரம்பு முடிந்தது. போனஸ் பெற நண்பர்களை ரெஃபர் செய்யவும் அல்லது விஐபி எடுக்கவும்.")
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
    status_msg = await bot_client.send_message(user_id, "📥 டவுன்லோட் துவங்குகிறது...")
    u_client = Client(f"ub_exec_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    try:
        for cur_id in range(start_id, end_id + 1):
            try:
                target = await u_client.get_messages(chat_id, cur_id)
                if not target or not target.media:
                    continue

                # Filter condition check
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

                await update_user(user_id, daily_used=user["daily_used"] + 1)
                await asyncio.sleep(1.5)

            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                continue

        await status_msg.edit_text("✅ பதிவிறக்கம் வெற்றிகரமாக நிறைவடைந்தது!")
    except Exception as e:
        await status_msg.edit_text(f"❌ பிழை: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- ADVANCED CHANNEL & GROUP CLONER -----------------
@bot.on_message(filters.command("clone") & filters.private)
async def clone_command(client: Client, msg: Message):
    user = await get_user(msg.from_user.id)
    if user["plan"] != "Ultimate" and msg.from_user.id != ADMIN_ID:
        await msg.reply_text("⭐ குரூப் மற்றும் சேனல் குளோனிங் வசதி **Ultimate Plan**-ல் மட்டுமே கிடைக்கும்.")
        return

    if not user.get("session"):
        await msg.reply_text("⚠️ முதலில் `/login` செய்யவும்.")
        return

    args = msg.text.split()
    if len(args) < 3:
        await msg.reply_text("பயன்பாடு:\n`/clone <source_id> <target_id>`\nஉதாரணம்:\n`/clone -1001234567890 -1009876543210`")
        return

    src = int(args[1]) if args[1].lstrip("-").isdigit() else args[1]
    dest = int(args[2]) if args[2].lstrip("-").isdigit() else args[2]

    await task_queue.put((execute_channel_clone, (client, msg, user, src, dest), {}))
    await msg.reply_text("🚀 குளோனிங் பணி வரிசையில் சேர்க்கப்பட்டது...")

async def execute_channel_clone(bot_client: Client, msg: Message, user: dict, src, dest):
    user_id = user["user_id"]
    status_msg = await bot_client.send_message(user_id, "🚀 சேனல்/குரூப் குளோனிங் தொடங்குகிறது...")
    u_client = Client(f"ub_clone_q_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    filter_type = user.get("file_filter", "all")
    count = 0
    try:
        async for post in u_client.get_chat_history(src):
            if post.media:
                if filter_type == "video" and not post.video:
                    continue
                elif filter_type == "doc" and not post.document:
                    continue

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
                        await status_msg.edit_text(f"🔄 Cloned {count} files successfully...")
                    await asyncio.sleep(2)
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                except Exception:
                    continue

        await status_msg.edit_text(f"✅ குளோனிங் வெற்றிகரமாக நிறைவடைந்தது! மொத்தம்: {count} கோப்புகள்.")
    except Exception as e:
        await status_msg.edit_text(f"❌ பிழை: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- ADMIN SUITE -----------------
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
        await msg.reply_text("தவறான திட்டம்!")
        return
    await update_user(t_uid, plan=t_plan, daily_limit=limits[t_plan])
    await msg.reply_text(f"✅ User `{t_uid}` plan changed to `{t_plan}`.")

@bot.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def admin_stats(_, msg: Message):
    async with aiosqlite.connect("bot_data.db") as db:
        c1 = await db.execute("SELECT COUNT(*) FROM users")
        total = (await c1.fetchone())[0]
        c2 = await db.execute("SELECT COUNT(*) FROM users WHERE plan != 'Free'")
        prem = (await c2.fetchone())[0]
    await msg.reply_text(f"📊 **Statistics:**\nTotal Users: `{total}`\nVIP Members: `{prem}`\nActive Tasks in Queue: `{task_queue.qsize()}`")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def admin_broadcast(_, msg: Message):
    if not msg.reply_to_message:
        await msg.reply_text("மெசேஜுக்கு ரிப்ளை செய்து `/broadcast` அனுப்பவும்.")
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
        except Exception:
            pass
    await msg.reply_text(f"✅ பிராட்காஸ்ட் நிறைவடைந்தது: {sent} பயனர்கள்.")

# ----------------- MAIN APP INITIALIZATION -----------------
async def main():
    await init_db()
    
    # Start Background Task Worker Engine
    asyncio.create_task(queue_worker())

    # Daily reset job at 12:00 AM IST
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    await bot.start()
    await set_bot_commands()
    print("Mega All-in-One Bot (Queue Engine + Cloner + Referral) is running!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
