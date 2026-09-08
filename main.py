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
# Render Dashboard -> Environment Variables-ல் இவை சரியாக கொடுக்கப்பட்டுள்ளதா என பார்க்கவும்
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
        return dict(user)

async def update_user(user_id, **kwargs):
    async with aiosqlite.connect("bot_data.db") as db:
        for k, v in kwargs.items():
            await db.execute(f"UPDATE users SET {k} = ? WHERE user_id = ?", (v, user_id))
        await db.commit()

async def deduct_usage(user_id):
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
        BotCommand("setvthumb", "Set Video Thumbnail"),
        BotCommand("delvthumb", "Remove Video Thumbnail")
    ]
    await bot.set_bot_commands(commands)

# ----------------- TELEGRAM HANDLERS -----------------
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, msg: Message):
    user_id = msg.from_user.id
    user = await get_user(user_id, msg.from_user.first_name)
    session_status = "✅ Connected" if user.get("session") else "❌ Not Connected"

    text = (
        f"👋 **வணக்கம் {msg.from_user.mention}!**\n\n"
        "Welcome to **Save Restricted Pro Bot**!\n\n"
        f"🏷️ **Plan:** `{user['plan']}`\n"
        f"🔑 **Session:** {session_status}\n"
        f"🎯 **Filter:** `{user.get('file_filter', 'all').upper()}`\n\n"
        "Restricted சேனல் போஸ்ட் லிங்கை இங்கு அனுப்புங்கள் அல்லது `/login` செய்து உங்கள் கணக்கை இணையுங்கள்."
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Connect Account", callback_data="btn_login"), InlineKeyboardButton("📦 Plans", callback_data="btn_plans")],
        [InlineKeyboardButton("🎁 Daily Bonus", callback_data="btn_bonus"), InlineKeyboardButton("👥 Refer & Earn", callback_data="btn_ref")],
        [InlineKeyboardButton("📊 My Plan", callback_data="btn_myplan"), InlineKeyboardButton("🎁 Free Trial", callback_data="btn_trial")]
    ])
    await msg.reply_text(text, reply_markup=buttons)

@bot.on_message(filters.command("login") & filters.private)
async def login_cmd(_, msg: Message):
    login_states[msg.from_user.id] = {"step": "API_ID"}
    await msg.reply_text(
        "Send Your **API ID**.\n\n"
        "Or click on **/skip** to use default bot API keys."
    )

@bot.on_message(filters.command("logout") & filters.private)
async def logout_cmd(_, msg: Message):
    await update_user(msg.from_user.id, session=None)
    await msg.reply_text("🚪 உங்கள் கணக்கு இணைப்பு துண்டிக்கப்பட்டது.")

@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_cmd(_, msg: Message):
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
async def plans_cmd(_, msg: Message):
    text = (
        "💎 **PREMIUM PLANS** 💎\n\n"
        "🥈 **Standard:** 50 files/day (₹50/wk | ₹180/mo)\n"
        "🥇 **Premium:** 100 files/day (₹80/wk | ₹280/mo)\n"
        "🔷 **Ultimate:** Unlimited + Cloning (₹130/wk | ₹450/mo)"
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
        await msg.reply_text("ஏற்கனவே நீங்கள் விஐபி அல்லது ட்ரையல் திட்டத்தில் உள்ளீர்கள்.")
        return
    await update_user(msg.from_user.id, plan="Trial (Standard)", daily_limit=50)
    await msg.reply_text("🎁 1-நாள் Standard இலவச ட்ரையல் செயல்படுத்தப்பட்டது!")

@bot.on_message(filters.command("id") & filters.private)
async def id_cmd(_, msg: Message):
    await msg.reply_text(f"🆔 **Your Telegram ID:** `{msg.from_user.id}`")

# ----------------- OTP LOGIN & TEXT HANDLER -----------------
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
                await msg.reply_text("நாட்டின் குறியீட்டுடன் போன் நம்பரை அனுப்பவும்:\nஉதாரணம்: `+919876543210`")
            elif text.isdigit():
                login_states[user_id] = {"step": "API_HASH", "api_id": int(text)}
                await msg.reply_text("இப்போது உங்கள் API HASH-ஐ அனுப்பவும்:")
            return

        if step == "API_HASH":
            login_states[user_id]["api_hash"] = text
            login_states[user_id]["step"] = "PHONE"
            await msg.reply_text("நாட்டின் குறியீட்டுடன் போன் நம்பரை அனுப்பவும்:\nஉதாரணம்: `+919876543210`")
            return

        if step == "PHONE":
            phone = text.replace(" ", "")
            u_client = Client(f"session_{user_id}", api_id=state.get("api_id"), api_hash=state.get("api_hash"), in_memory=True)
            await u_client.connect()
            try:
                await msg.reply_text("Sending OTP...")
                code = await u_client.send_code(phone)
                login_states[user_id].update({"step": "OTP", "client": u_client, "phone": phone, "hash": code.phone_code_hash})
                await msg.reply_text("OTP-ஐ எண்களுக்கு இடையே இடைவெளி விட்டு அனுப்பவும்: `1 2 3 4 5`")
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
                await msg.reply_text("🔐 2-Step Verification Password அனுப்பவும்:")
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
        await task_queue.put((execute_download, (client, msg, user_id), {}))
        await msg.reply_text("⏳ டவுன்லோட் வரிசையில் சேர்க்கப்பட்டது...")

# ----------------- DOWNLOAD EXECUTION -----------------
async def execute_download(bot_client: Client, msg: Message, user_id: int):
    user = await get_user(user_id)
    if not user.get("session"):
        await bot_client.send_message(user_id, "⚠️ முதலில் `/login` செய்யவும்.")
        return

    match = re.search(r"t\.me/(c/)?([a-zA-Z0-9_]+)/(\d+)(?:-(\d+))?", msg.text.strip())
    if not match:
        await bot_client.send_message(user_id, "❌ செல்லுபடியாகாத Telegram லிங்க்.")
        return

    is_private = bool(match.group(1))
    chat_raw = match.group(2)
    start_id = int(match.group(3))
    end_id = int(match.group(4)) if match.group(4) else start_id
    chat_id = int(f"-100{chat_raw}") if is_private or chat_raw.isdigit() else chat_raw

    status_msg = await bot_client.send_message(user_id, "📥 டவுன்லோட் தொடங்குகிறது...")
    u_client = Client(f"ub_exec_{user_id}", session_string=user["session"], in_memory=True)
    await u_client.connect()

    try:
        for cur_id in range(start_id, end_id + 1):
            allowed = await deduct_usage(user_id)
            if not allowed:
                await bot_client.send_message(user_id, "⛔ உங்கள் தினசரி வரம்பு முடிந்தது.")
                break

            try:
                target = await u_client.get_messages(chat_id, cur_id)
                if not target or not target.media:
                    continue

                await status_msg.edit_text(f"📥 Downloading ID: {cur_id}...")
                f_path = await target.download()

                await status_msg.edit_text(f"📤 Uploading ID: {cur_id}...")
                caption = user.get("custom_caption") or target.caption or ""

                if target.document:
                    await bot_client.send_document(user_id, f_path, caption=caption)
                elif target.video:
                    await bot_client.send_video(user_id, f_path, caption=caption, supports_streaming=True)
                elif target.audio:
                    await bot_client.send_audio(user_id, f_path, caption=caption)
                elif target.photo:
                    await bot_client.send_photo(user_id, f_path, caption=caption)

                if os.path.exists(f_path):
                    os.remove(f_path)
                await asyncio.sleep(1)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                continue

        await status_msg.edit_text("✅ முடிந்தது!")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if u_client.is_connected:
            await u_client.disconnect()

# ----------------- CALLBACK BUTTONS -----------------
@bot.on_callback_query()
async def cb_handler(client: Client, q: CallbackQuery):
    data = q.data
    if data == "btn_login":
        await login_cmd(client, q.message)
    elif data == "btn_plans":
        await plans_cmd(client, q.message)
    elif data == "btn_myplan":
        await myplan_cmd(client, q.message)
    elif data == "btn_trial":
        await trial_cmd(client, q.message)
    elif data == "btn_buy":
        await q.message.reply_text("UPI ID: `your-upi@okaxis`\nSend screenshot to admin.")
    await q.answer()

# ----------------- MAIN BOOTSTRAP -----------------
async def main():
    if not DEFAULT_API_ID or not DEFAULT_API_HASH or not BOT_TOKEN:
        print("[FATAL ERROR] API_ID, API_HASH, அல்லது BOT_TOKEN Environment Variable அமைக்கப்படவில்லை!")
        return

    await init_db()
    
    # 1. Background Web server for Render health check
    await start_web_server()

    # 2. Queue worker
    asyncio.create_task(queue_worker())

    # 3. Scheduler for Midnight reset
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(daily_reset_job, "cron", hour=0, minute=0)
    scheduler.start()

    # 4. Start Telegram Client
    await bot.start()
    await set_bot_commands()
    print("========================================")
    print("  [SUCCESS] Bot is online and polling!  ")
    print("========================================")

    # Blocks properly until termination without breaking asyncio loop
    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
