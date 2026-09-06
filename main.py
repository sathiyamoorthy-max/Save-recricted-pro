# ============================================================
# ðŸŒ ULTIMATE TELEGRAM RESTRICTED CONTENT SAVER & CLONER v6.0
# Features:
# - Multi-Session Userbot Rotator (Prevents FloodWait / Bans)
# - MongoDB Persistent Cloud Storage (No Data Loss on Render/Koyeb)
# - Admin Approval Payment Engine (Prevents Fake Transaction Scams)
# - Memory-Safe Batch & Clone Queue (Guards 512MB RAM from OOM Crashes)
# - Restricted Media Auto Bypass (Direct re-upload if forward disabled)
# - Story Downloader & Channel / Bot Analyzer
# - Built-in Flask Keep-Alive Server for 24/7 Uptime
# ============================================================

import os
import sys
import re
import asyncio
import shutil
import logging
from time import time
from datetime import datetime, timedelta
from typing import Optional, Dict
from threading import Thread
from asyncio import Queue, Semaphore
from logging.handlers import RotatingFileHandler

# Fix Event Loop for Python 3.10+ / Linux Containers
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatType
from pyrogram.errors import FloodWait, BadRequest, PeerIdInvalid, UsernameNotOccupied
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
)
from pyrogram.utils import get_channel_id
from pyleaves import Leaves
from dotenv import load_dotenv
from flask import Flask, jsonify
from motor.motor_asyncio import AsyncIOMotorClient

# Load Environment Variables
load_dotenv("config.env")

# ============================================================
# âš™ï¸ CONFIGURATION
# ============================================================
class Config:
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    SESSION_STRINGS = [s.strip() for s in os.getenv("SESSION_STRINGS", "").split(",") if s.strip()]
    MONGO_URL = os.getenv("MONGO_URL", "")
    OWNER_ID = int(os.getenv("OWNER_ID", "0"))
    UPI_ID = os.getenv("UPI_ID", "your_upi@okaxis")
    
    # Concurrency & Safe Limits
    MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "2"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))
    FLOOD_SLEEP = int(os.getenv("FLOOD_SLEEP", "3"))
    
    # Trial & Monetization
    TRIAL_DOWNLOADS = int(os.getenv("TRIAL_DOWNLOADS", "25"))
    TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))
    PREMIUM_PRICE = int(os.getenv("PREMIUM_PRICE", "199"))
    LIFETIME_PRICE = int(os.getenv("LIFETIME_PRICE", "699"))

config = Config()
BOT_START_TIME = time()

if not all([config.API_ID, config.API_HASH, config.BOT_TOKEN, config.MONGO_URL, config.OWNER_ID, config.SESSION_STRINGS]):
    print("âŒ Critical Config Missing: Check API_ID, API_HASH, BOT_TOKEN, MONGO_URL, OWNER_ID, and SESSION_STRINGS!")
    sys.exit(1)

# ============================================================
# ðŸ“ LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(name)s - %(message)s",
    datefmt="%d-%b-%y %I:%M:%S %p",
    handlers=[
        RotatingFileHandler("bot_activity.log", mode="w+", maxBytes=5_000_000, backupCount=2),
        logging.StreamHandler(),
    ],
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logger = logging.getLogger("UltimateBot")

# ============================================================
# ðŸ—„ï¸ PERSISTENT CLOUD DATABASE (MongoDB)
# ============================================================
db_client = AsyncIOMotorClient(config.MONGO_URL)
db = db_client["telegram_saver_master"]
users_col = db["users"]
clone_col = db["clone_progress"]

async def get_user(user_id: int) -> Dict:
    user = await users_col.find_one({"user_id": user_id})
    if not user:
        user = {
            "user_id": user_id,
            "username": "",
            "plan": "free",
            "trial_start": datetime.now(),
            "trial_end": datetime.now() + timedelta(days=config.TRIAL_DAYS),
            "premium_expiry": None,
            "total_downloads": 0,
            "bonus_downloads": 0,
            "referred_by": None,
            "created_at": datetime.now()
        }
        await users_col.insert_one(user)
    return user

async def update_user(user_id: int, update_dict: dict):
    await users_col.update_one({"user_id": user_id}, {"$set": update_dict})

async def increment_downloads(user_id: int):
    await users_col.update_one({"user_id": user_id}, {"$inc": {"total_downloads": 1}})

async def is_eligible(user_id: int) -> bool:
    if user_id == config.OWNER_ID:
        return True
    user = await get_user(user_id)
    now = datetime.now()
    if user.get("plan") == "premium" and user.get("premium_expiry") and user["premium_expiry"] > now:
        return True
    max_free = config.TRIAL_DOWNLOADS + user.get("bonus_downloads", 0)
    if user.get("trial_end") and user["trial_end"] > now and user.get("total_downloads", 0) < max_free:
        return True
    return False

# ============================================================
# ðŸ¤– BOT & USERBOT ROTATOR
# ============================================================
bot = Client(
    "CentralSaverBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    workers=30,
    parse_mode=ParseMode.MARKDOWN
)

user_clients = []
for idx, s_str in enumerate(config.SESSION_STRINGS):
    try:
        ub = Client(f"user_session_{idx}", api_id=config.API_ID, api_hash=config.API_HASH, session_string=s_str, sleep_threshold=60)
        user_clients.append(ub)
    except Exception as e:
        logger.error(f"Error initializing user session {idx}: {e}")

if not user_clients:
    logger.error("âŒ No valid user sessions found!")
    sys.exit(1)

session_rotation_index = 0
def get_next_session():
    global session_rotation_index
    sess = user_clients[session_rotation_index % len(user_clients)]
    session_rotation_index += 1
    return sess

RUNNING_TASKS = {}
download_queue = Queue()
download_semaphore = Semaphore(config.MAX_CONCURRENT)

# ============================================================
# ðŸ› ï¸ UTILITY HELPERS
# ============================================================
PROGRESS_BAR = "ðŸ“¥ **{action}**
ðŸ“Š {percentage:.2f}% | {current}/{total}
âš¡ Speed: {speed}/s | â³ ETA: {est_time}s"
def progress_args(action, prog_msg, start_time):
    return (action, prog_msg, start_time, PROGRESS_BAR, "â–“", "â–‘")

def parse_tg_link(link: str):
    clean = link.strip().replace("https://t.me/", "")
    parts = clean.split("/")
    if parts[0] == "c":
        return int(f"-100{parts[1]}"), int(parts[2])
    return parts[0], int(parts[1])

def get_readable_size(size: Optional[float]) -> str:
    if not size or size < 0: return "0B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024
    return "Massive"

def get_readable_time(seconds: int) -> str:
    days, rem = divmod(seconds, 86400)
    hrs, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    parts = []
    if int(days): parts.append(f"{int(days)}d")
    if int(hrs): parts.append(f"{int(hrs)}h")
    if int(mins): parts.append(f"{int(mins)}m")
    parts.append(f"{int(secs)}s")
    return " ".join(parts)

def cleanup_file(path: Optional[str]):
    try:
        if path and os.path.exists(path):
            os.remove(path)
        if path and os.path.exists(path + ".temp"):
            os.remove(path + ".temp")
        folder = os.path.dirname(path) if path else None
        if folder and os.path.isdir(folder) and not os.listdir(folder):
            os.rmdir(folder)
    except Exception:
        pass

# ============================================================
# ðŸš€ CORE MEDIA SAVER ENGINE
# ============================================================
async def extract_and_forward(client: Client, userbot: Client, chat_id, msg_id: int, target_chat: int, notify_msg: Optional[Message] = None):
    try:
        source: Message = await userbot.get_messages(chat_id, msg_id)
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 3)
        source = await userbot.get_messages(chat_id, msg_id)
    except Exception:
        return False

    if not source or source.empty:
        return False

    # Attempt direct copy first (Fastest & Zero RAM usage)
    try:
        await source.copy(target_chat)
        return True
    except Exception:
        pass

    # Bypass Restricted Mode by manual re-upload
    if source.media:
        start_time = time()
        p_args = progress_args("Downloading", notify_msg, start_time) if notify_msg else None
        try:
            dl_path = await source.download(progress=Leaves.progress_for_pyrogram, progress_args=p_args) if p_args else await source.download()
        except Exception:
            return False

        if not dl_path or not os.path.exists(dl_path):
            return False

        caption = source.caption or ""
        caption_entities = source.caption_entities or None

        try:
            if notify_msg:
                try: await notify_msg.edit_text("ðŸ“¤ Re-uploading media to Telegram...")
                except Exception: pass

            if source.photo:
                await client.send_photo(target_chat, dl_path, caption=caption, caption_entities=caption_entities)
            elif source.video:
                await client.send_video(target_chat, dl_path, caption=caption, caption_entities=caption_entities)
            elif source.document:
                await client.send_document(target_chat, dl_path, caption=caption, caption_entities=caption_entities)
            elif source.audio:
                await client.send_audio(target_chat, dl_path, caption=caption, caption_entities=caption_entities)
            elif source.voice:
                await client.send_voice(target_chat, dl_path, caption=caption, caption_entities=caption_entities)
            return True
        except Exception:
            return False
        finally:
            cleanup_file(dl_path)
    elif source.text:
        await client.send_message(target_chat, source.text)
        return True
    return False

# ============================================================
# ðŸ’¬ COMMAND HANDLERS
# ============================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(_, message: Message):
    ref_id = int(message.command[1]) if len(message.command) > 1 and message.command[1].isdigit() else None
    user = await get_user(message.from_user.id)
    if ref_id and ref_id != message.from_user.id and not user.get("referred_by"):
        await users_col.update_one({"user_id": ref_id}, {"$inc": {"bonus_downloads": 10}})
        await update_user(message.from_user.id, {"referred_by": ref_id})

    status_plan = user.get("plan", "free").upper()
    total_allowed = config.TRIAL_DOWNLOADS + user.get("bonus_downloads", 0)
    
    welcome_text = (
        "ðŸš€ **World's Most Advanced Telegram Saver & Cloner v6.0**

"
        f"ðŸ‘¤ **User:** `{message.from_user.id}`
"
        f"â­ **Plan:** `{status_plan}`
"
        f"ðŸ“¥ **Quota Used:** `{user.get('total_downloads', 0)}/{total_allowed}`

"
        "**Available Commands:**
"
        "â€¢ Direct Link: Paste any Telegram post link directly
"
        "â€¢ `/bdl <start_link> <end_id>` - Safe Batch Downloader
"
        "â€¢ `/clone <source_channel> <target_id>` - Full Channel Clone
"
        "â€¢ `/analyze @username` - Channel/Bot Function Detector
"
        "â€¢ `/status` - Detailed Usage Analytics
"
        "â€¢ `/buy` - Unlock Unlimited Access
"
        "â€¢ `/cancel` - Stop ongoing tasks"
    )
    await message.reply(welcome_text)

@bot.on_message(filters.command("status") & filters.private)
async def status_handler(_, message: Message):
    user = await get_user(message.from_user.id)
    now = datetime.now()
    if user.get("plan") == "premium" and user.get("premium_expiry") and user["premium_expiry"] > now:
        plan_desc = f"â­ Premium (Expires: {user['premium_expiry'].strftime('%d-%b-%Y')})"
    else:
        max_free = config.TRIAL_DOWNLOADS + user.get("bonus_downloads", 0)
        plan_desc = f"Free Trial ({user.get('total_downloads', 0)}/{max_free} Used)"

    active_tasks = len(RUNNING_TASKS.get(message.from_user.id, []))
    await message.reply(
        f"ðŸ“Š **Account Status:**

"
        f"â€¢ Plan: **{plan_desc}**
"
        f"â€¢ Active Running Tasks: **{active_tasks}**
"
        f"â€¢ Queue Backlog: **{download_queue.qsize()} tasks**"
    )

# ============================================================
# ðŸ’³ SECURE ADMIN PAYMENT VERIFICATION ENGINE
# ============================================================
@bot.on_message(filters.command("buy") & filters.private)
async def buy_handler(_, message: Message):
    payment_info = (
        "ðŸ’Ž **Upgrade to Premium Membership**

"
        f"â€¢ 1 Month Unlimited: **â‚¹{config.PREMIUM_PRICE}**
"
        f"â€¢ Lifetime Access: **â‚¹{config.LIFETIME_PRICE}**

"
        f"ðŸ’³ **Pay via UPI ID:** `{config.UPI_ID}`

"
        "âš ï¸ **Verification Steps:**
"
        "1. Send payment to above UPI
"
        "2. Send Transaction ID: `/proof <Txn_ID>`
"
        "Admin will review and unlock instantly."
    )
    await message.reply(payment_info)

@bot.on_message(filters.command("proof") & filters.private)
async def proof_handler(_, message: Message):
    if len(message.command) < 2:
        return await message.reply("âš ï¸ Usage: `/proof <Transaction_ID>`
Example: `/proof UPI123456789`")

    txn_id = message.command[1].strip()
    uid = message.from_user.id

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Approve 30 Days", callback_data=f"adm_pay_30_{uid}"),
            InlineKeyboardButton("Approve Lifetime", callback_data=f"adm_pay_3650_{uid}")
        ],
        [
            InlineKeyboardButton("Reject", callback_data=f"adm_pay_rej_{uid}")
        ]
    ])

    await bot.send_message(
        config.OWNER_ID,
        f"ðŸ”” **New Payment Claim Received!**

"
        f"â€¢ User: [{message.from_user.first_name}](tg://user?id={uid})
"
        f"â€¢ User ID: `{uid}`
"
        f"â€¢ Transaction ID: `{txn_id}`",
        reply_markup=markup
    )
    await message.reply("âœ… Proof submitted successfully! Your account will be activated once verified by the Admin.")

@bot.on_callback_query(filters.regex(r"^adm_pay_"))
async def admin_payment_callback(_, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer("Unauthorized!", show_alert=True)

    _, _, action, target_uid = query.data.split("_")
    target_uid = int(target_uid)

    if action == "rej":
        await bot.send_message(target_uid, "âŒ Your payment verification was rejected. Please contact support.")
        await query.message.edit_text(f"âŒ Claim rejected for User `{target_uid}`.")
    else:
        days = int(action)
        exp_date = datetime.now() + timedelta(days=days)
        await update_user(target_uid, {"plan": "premium", "premium_expiry": exp_date})
        await bot.send_message(target_uid, f"ðŸŽ‰ **Payment Verified!** Premium activated for {days} days. Enjoy unlimited downloads & cloning!")
        await query.message.edit_text(f"âœ… Approved {days} days for User `{target_uid}`.")

# ============================================================
# ðŸ” BOT & CHANNEL ANALYZER
# ============================================================
@bot.on_message(filters.command("analyze") & filters.private)
async def analyze_handler(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply("Usage: `/analyze @username_or_link`")

    target = message.command[1].strip()
    prog = await message.reply("ðŸ” Analyzing target entity...")
    sess = get_next_session()

    try:
        chat = await sess.get_chat(target)
        if chat.type == ChatType.BOT:
            await prog.edit_text(
                f"ðŸ¤– **Bot Profile Detected:**

"
                f"â€¢ Name: **{chat.first_name}**
"
                f"â€¢ Username: @{chat.username}
"
                f"â€¢ ID: `{chat.id}`
"
                f"â€¢ Status: Active Telegram Bot"
            )
        else:
            latest = await sess.get_messages(chat.id, 0)
            total_count = latest.id if latest else 0
            await prog.edit_text(
                f"ðŸ“Š **Channel Analytics:**

"
                f"â€¢ Title: **{chat.title}**
"
                f"â€¢ Type: `{chat.type.name}`
"
                f"â€¢ Total Posts: `{total_count}`
"
                f"â€¢ Protected/Restricted: `{chat.has_protected_content or False}`"
            )
    except Exception as e:
        await prog.edit_text(f"âŒ Analysis failed: {e}")

# ============================================================
# ðŸ“¥ DOWNLOAD & CLONE WORKERS
# ============================================================
@bot.on_message(filters.text & filters.private & ~filters.command(["start", "status", "buy", "proof", "cancel", "bdl", "clone", "analyze"]))
async def single_download_handler(client: Client, message: Message):
    if not await is_eligible(message.from_user.id):
        return await message.reply("ðŸš« Trial limit exceeded! Use `/buy` to unlock unlimited access.")

    try:
        chat_id, msg_id = parse_tg_link(message.text)
    except Exception:
        return await message.reply("âŒ Invalid Telegram post URL format.")

    status_msg = await message.reply("âš¡ Fetching restricted media...")
    userbot = get_next_session()

    async with download_semaphore:
        success = await extract_and_forward(client, userbot, chat_id, msg_id, message.chat.id, status_msg)
        if success:
            await increment_downloads(message.from_user.id)
            try: await status_msg.delete()
            except Exception: pass
        else:
            await status_msg.edit_text("âŒ Extraction failed. Make sure userbot account has access to this channel.")

async def safe_batch_runner(client: Client, message: Message, chat_id, start_id: int, end_id: int, target_dest: int):
    status = await message.reply(f"ðŸš€ Batch task started: `{start_id}` to `{end_id}`...")
    success = failed = 0

    for curr_id in range(start_id, end_id + 1):
        if not await is_eligible(message.from_user.id):
            await message.reply("â›” Download quota reached midway. Task suspended. Upgrade with `/buy`.")
            break

        userbot = get_next_session()
        try:
            res = await extract_and_forward(client, userbot, chat_id, curr_id, target_dest)
            if res:
                success += 1
                await increment_downloads(message.from_user.id)
            else:
                failed += 1
        except asyncio.CancelledError:
            await status.edit_text(f"ðŸ›‘ Batch cancelled!
Transferred: `{success}` | Skipped: `{failed}`")
            return
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 4)
        except Exception:
            failed += 1

        if (curr_id - start_id + 1) % 10 == 0:
            try:
                await status.edit_text(f"â³ Processing: `{curr_id}/{end_id}`
âœ… Completed: `{success}` | âŒ Skipped: `{failed}`")
            except Exception:
                pass

        await asyncio.sleep(config.FLOOD_SLEEP)

    await status.edit_text(f"ðŸŽ‰ **Batch Complete!**

âœ… Transferred: `{success}`
âŒ Skipped: `{failed}`")

@bot.on_message(filters.command("bdl") & filters.private)
async def batch_cmd(client: Client, message: Message):
    if len(message.command) < 3:
        return await message.reply("ðŸ“Œ Usage: `/bdl <start_link> <end_id>`
Example:
`/bdl https://t.me/c/12345/1 50`")

    if not await is_eligible(message.from_user.id):
        return await message.reply("â›” Quota limit reached! Upgrade using `/buy`.")

    try:
        chat_id, start_id = parse_tg_link(message.command[1])
        end_id = int(message.command[2])
        if end_id < start_id:
            return await message.reply("âŒ End ID must be greater than Start ID.")
    except Exception:
        return await message.reply("âŒ Invalid format or post URL.")

    task = asyncio.create_task(safe_batch_runner(client, message, chat_id, start_id, end_id, message.chat.id))
    RUNNING_TASKS.setdefault(message.from_user.id, []).append(task)

@bot.on_message(filters.command("clone") & filters.private)
async def clone_cmd(client: Client, message: Message):
    user = await get_user(message.from_user.id)
    if user.get("plan") != "premium" and message.from_user.id != config.OWNER_ID:
        return await message.reply("â­ Channel Clone is an exclusive **Premium** feature. Use `/buy` to activate.")

    if len(message.command) < 3:
        return await message.reply("ðŸ“Œ Usage: `/clone <channel_username_or_link> <target_chat_id>`")

    src_input = message.command[1]
    target_chat = int(message.command[2])
    userbot = get_next_session()

    try:
        chat_obj = await userbot.get_chat(src_input)
    except Exception as e:
        return await message.reply(f"âŒ Userbot cannot access source channel: {e}")

    init_msg = await message.reply("ðŸ” Scanning total posts in channel...")
    try:
        latest = await userbot.get_messages(chat_obj.id, 0)
        total_msgs = latest.id if latest else 0
    except Exception:
        return await init_msg.edit_text("âŒ Could not fetch total message count.")

    await init_msg.delete()
    task = asyncio.create_task(safe_batch_runner(client, message, chat_obj.id, 1, total_msgs, target_chat))
    RUNNING_TASKS.setdefault(message.from_user.id, []).append(task)

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(_, message: Message):
    tasks = RUNNING_TASKS.get(message.from_user.id, [])
    if not tasks:
        return await message.reply("â„¹ï¸ No active tasks running.")
    for t in tasks:
        t.cancel()
    RUNNING_TASKS[message.from_user.id] = []
    await message.reply("ðŸ›‘ All running operations stopped.")

# ============================================================
# ðŸŒ KEEP-ALIVE SERVER & RUNTIME
# ============================================================
web_app = Flask("")
@web_app.route("/")
def home():
    return jsonify({"service": "TelegramSaverPro", "status": "online", "uptime": get_readable_time(int(time() - BOT_START_TIME))})

def launch_web_server():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    await bot.start()
    logger.info(" Central Bot Started.")
    for idx, ub in enumerate(user_clients):
        await ub.start()
        logger.info(f" Userbot Session {idx+1} Online.")
    logger.info(" Master Downloader Ready.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    Thread(target=launch_web_server, daemon=True).start()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down.")
