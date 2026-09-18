#!/usr/bin/env python3
"""
SavePro Global V3
=================
A production-oriented Telegram utility/business bot built with Kurigram
(Pyrogram-compatible API), SQLite, APScheduler and encrypted user sessions.

Important: use this bot only with Telegram content/accounts/chats you are
allowed to access and process. Respect Telegram rules and applicable rights.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import re
import secrets
import shutil
import signal
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import aiosqlite
import pytz
import qrcode
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from cryptography.fernet import Fernet, InvalidToken
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (
    ChannelPrivate,
    FloodWait,
    PasswordHashInvalid,
    PeerIdInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    SessionPasswordNeeded,
)
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

load_dotenv()

APP_NAME = os.getenv("APP_NAME", "SavePro Global V3").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
API_ID = int(API_ID_RAW) if API_ID_RAW.isdigit() else 0
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@").strip()

OWNER_ID_RAW = os.getenv("OWNER_ID", os.getenv("ADMIN_ID", "")).strip()
OWNER_ID = int(OWNER_ID_RAW) if OWNER_ID_RAW.isdigit() else 0
EXTRA_ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
if OWNER_ID:
    EXTRA_ADMIN_IDS.add(OWNER_ID)

TZ_NAME = os.getenv("TIMEZONE", "Asia/Kolkata")
TZ = pytz.timezone(TZ_NAME)
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "bot_data.db")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
TEMP_DIR = DATA_DIR / "tmp"
THUMB_DIR = DATA_DIR / "thumbnails"
LOG_DIR = DATA_DIR / "logs"

MAX_BATCH_SIZE = max(1, int(os.getenv("MAX_BATCH_SIZE", "250")))
PREMIUM_BATCH_SIZE = max(MAX_BATCH_SIZE, int(os.getenv("PREMIUM_BATCH_SIZE", "1000")))
# Ultimate has no daily quota. This is only a per-job safety ceiling; set 0 for no explicit range ceiling.
ULTIMATE_BATCH_SIZE = max(0, int(os.getenv("ULTIMATE_BATCH_SIZE", "0")))
CLONE_PROGRESS_EVERY = max(1, int(os.getenv("CLONE_PROGRESS_EVERY", "10")))
MAX_RETRIES = max(1, int(os.getenv("MAX_RETRIES", "3")))
MAX_QUEUE_PER_USER = max(1, int(os.getenv("MAX_QUEUE_PER_USER", "5")))
WORKER_COUNT = min(8, max(1, int(os.getenv("WORKER_COUNT", "2"))))
JOB_TIMEOUT_SECONDS = max(300, int(os.getenv("JOB_TIMEOUT_SECONDS", "7200")))
# 0 disables a hard timeout for full-channel clone jobs; checkpoint/pause/cancel still apply.
CLONE_JOB_TIMEOUT_SECONDS = max(0, int(os.getenv("CLONE_JOB_TIMEOUT_SECONDS", "0")))
FREE_DAILY_LIMIT = max(0, int(os.getenv("FREE_DAILY_LIMIT", "2")))
TRIAL_DAILY_LIMIT = max(1, int(os.getenv("TRIAL_DAILY_LIMIT", "50")))
RATE_LIMIT_MESSAGES = max(3, int(os.getenv("RATE_LIMIT_MESSAGES", "12")))
RATE_LIMIT_WINDOW = max(5, int(os.getenv("RATE_LIMIT_WINDOW", "15")))
SUPPORT_USERNAME_ENV = os.getenv("SUPPORT_USERNAME", "admin").lstrip("@").strip()
UPDATES_CHANNEL_ENV = os.getenv("UPDATES_CHANNEL", "https://t.me/telegram").strip()
UPI_ID_ENV = os.getenv("UPI_ID", "").strip()
UPI_NAME_ENV = os.getenv("UPI_NAME", "").strip()
TERMS_URL = os.getenv("TERMS_URL", "").strip()

for d in (DATA_DIR, TEMP_DIR, THUMB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

START_TS = time.time()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("savepro")

if not API_ID or not API_HASH or not BOT_TOKEN:
    log.warning("API_ID/API_HASH/BOT_TOKEN are not fully configured yet.")

# Kurigram/Pyrogram binds each Client to the asyncio event loop that exists
# when the Client is created.  Create one explicit application loop up-front
# and use that same loop for the complete lifetime of the bot.  This avoids
# Python 3.12 / Render errors such as "Future attached to a different loop".
APP_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(APP_LOOP)

# Kurigram keeps the same `pyrogram` import namespace.
bot = Client(
    "savepro_master_bot",
    api_id=API_ID or 1,
    api_hash=API_HASH or "placeholder",
    bot_token=BOT_TOKEN or "123456:placeholder",
    in_memory=True,
)

# ---------------------------------------------------------------------------
# BUSINESS CONFIG
# ---------------------------------------------------------------------------

PLAN_LIMITS = {
    "Free": FREE_DAILY_LIMIT,
    "Trial": TRIAL_DAILY_LIMIT,
    "Basic": 10,
    "Standard": 50,
    "Premium": 100,
    "Ultimate": 9_999_999,
}

PLAN_PRIORITY = {
    "Ultimate": 0,
    "Premium": 1,
    "Standard": 2,
    "Basic": 3,
    "Trial": 3,
    "Free": 4,
}

PLAN_PRICING: dict[str, dict[str, Any]] = {
    "Basic": {
        "Weekly": 30,
        "Monthly": 100,
        "Yearly": 840,
        "access": "10 jobs/day",
    },
    "Standard": {
        "Weekly": 50,
        "Monthly": 180,
        "Yearly": 1500,
        "access": "50 jobs/day + batch",
    },
    "Premium": {
        "Weekly": 80,
        "Monthly": 280,
        "Yearly": 2350,
        "access": "100 jobs/day + priority batch",
    },
    "Ultimate": {
        "Weekly": 130,
        "Monthly": 500,
        "Yearly": 4200,
        "access": "Unlimited usage + highest priority + full-channel clone/transfer",
    },
}

DURATION_DAYS = {"Weekly": 7, "Monthly": 30, "Yearly": 365}
VALID_LANGS = {"ta", "en", "hi", "te", "ml", "kn", "bn", "mr", "gu", "pa", "ur", "ar", "es", "fr", "de", "id"}
VALID_FILTERS = {"all", "video", "audio", "doc", "photo"}
ADMIN_ROLES = {"owner", "manager", "payments", "support", "marketing", "developer"}

# ---------------------------------------------------------------------------
# LOCALIZATION
# ---------------------------------------------------------------------------

LANG_META = {
    "ta": "தமிழ்", "en": "English", "hi": "हिन्दी", "te": "తెలుగు",
    "ml": "മലയാളം", "kn": "ಕನ್ನಡ", "bn": "বাংলা", "mr": "मराठी",
    "gu": "ગુજરાતી", "pa": "ਪੰਜਾਬੀ", "ur": "اردو", "ar": "العربية",
    "es": "Español", "fr": "Français", "de": "Deutsch", "id": "Bahasa Indonesia",
}

# Core user-facing localization. Technical/admin-only text intentionally falls back
# to English so a missing translation can never break a workflow.
LANG = {
    "ta": {
        "welcome": "🌟 **வணக்கம் {name}! {app}-க்கு வரவேற்கிறோம்**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nஉங்களுக்கு சட்டபூர்வமாக அணுகல் உள்ள Telegram content-ஐ மட்டும் பயன்படுத்தவும்.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "கீழே உள்ள menu-ஐ பயன்படுத்தவும்.", "limit": "⛔ இன்றைய பயன்பாட்டு வரம்பு முடிந்தது.",
    },
    "en": {
        "welcome": "🌟 **Welcome {name} to {app}**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nUse only Telegram content your connected account is legitimately allowed to access and process.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "Use the menu below.", "limit": "⛔ Your daily usage limit is exhausted.",
    },
    "hi": {
        "welcome": "🌟 **नमस्ते {name}! {app} में आपका स्वागत है**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nकेवल वही Telegram content उपयोग करें जिसे आपका connected account वैध रूप से access कर सकता है।",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "नीचे दिए menu का उपयोग करें।", "limit": "⛔ आज की usage limit पूरी हो गई है।",
    },
    "te": {
        "welcome": "🌟 **నమస్తే {name}! {app} కు స్వాగతం**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nమీ connected account చట్టబద్ధంగా access చేయగల Telegram content మాత్రమే ఉపయోగించండి.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "క్రింది menu ఉపయోగించండి.", "limit": "⛔ ఈరోజు usage limit పూర్తైంది.",
    },
    "ml": {
        "welcome": "🌟 **നമസ്കാരം {name}! {app}-ലേക്ക് സ്വാഗതം**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nനിങ്ങളുടെ connected account-ന് നിയമാനുസൃത access ഉള്ള Telegram content മാത്രം ഉപയോഗിക്കുക.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "താഴെയുള്ള menu ഉപയോഗിക്കുക.", "limit": "⛔ ഇന്നത്തെ usage limit കഴിഞ്ഞു.",
    },
    "kn": {
        "welcome": "🌟 **ನಮಸ್ಕಾರ {name}! {app} ಗೆ ಸ್ವಾಗತ**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nನಿಮ್ಮ connected account ಕಾನೂನುಬದ್ಧವಾಗಿ access ಮಾಡಬಹುದಾದ Telegram content ಮಾತ್ರ ಬಳಸಿ.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "ಕೆಳಗಿನ menu ಬಳಸಿ.", "limit": "⛔ ಇಂದಿನ usage limit ಮುಗಿದಿದೆ.",
    },
    "bn": {
        "welcome": "🌟 **স্বাগতম {name}! {app}-এ আপনাকে স্বাগতম**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nআপনার connected account বৈধভাবে access করতে পারে এমন Telegram content-ই ব্যবহার করুন।",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "নিচের menu ব্যবহার করুন।", "limit": "⛔ আজকের usage limit শেষ।",
    },
    "mr": {
        "welcome": "🌟 **नमस्कार {name}! {app} मध्ये स्वागत आहे**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nतुमच्या connected account ला वैध access असलेले Telegram contentच वापरा.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "खालील menu वापरा.", "limit": "⛔ आजची usage limit संपली.",
    },
    "gu": {
        "welcome": "🌟 **નમસ્તે {name}! {app} માં આપનું સ્વાગત છે**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nતમારા connected account ને કાયદેસર access હોય તે Telegram content જ વાપરો.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "નીચેનું menu વાપરો.", "limit": "⛔ આજની usage limit પૂર્ણ થઈ.",
    },
    "pa": {
        "welcome": "🌟 **ਸਤ ਸ੍ਰੀ ਅਕਾਲ {name}! {app} ਵਿੱਚ ਜੀ ਆਇਆਂ ਨੂੰ**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nਸਿਰਫ਼ ਉਹ Telegram content ਵਰਤੋ ਜਿਸ ਲਈ ਤੁਹਾਡੇ connected account ਕੋਲ ਕਾਨੂੰਨੀ access ਹੈ।",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "ਹੇਠਾਂ ਦਿੱਤਾ menu ਵਰਤੋ।", "limit": "⛔ ਅੱਜ ਦੀ usage limit ਖਤਮ ਹੋ ਗਈ ਹੈ।",
    },
    "ur": {
        "welcome": "🌟 **السلام علیکم {name}! {app} میں خوش آمدید**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nصرف وہ Telegram content استعمال کریں جس تک آپ کے connected account کی جائز رسائی ہو۔",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "نیچے دیا گیا menu استعمال کریں۔", "limit": "⛔ آج کی usage limit ختم ہو گئی ہے۔",
    },
    "ar": {
        "welcome": "🌟 **مرحباً {name}! أهلاً بك في {app}**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nاستخدم فقط محتوى Telegram الذي يملك حسابك المتصل صلاحية مشروعة للوصول إليه.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "استخدم القائمة أدناه.", "limit": "⛔ انتهى حد الاستخدام اليومي.",
    },
    "es": {
        "welcome": "🌟 **¡Hola {name}! Bienvenido a {app}**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nUsa solo contenido de Telegram al que tu cuenta conectada tenga acceso legítimo.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "Usa el menú de abajo.", "limit": "⛔ Se agotó tu límite diario.",
    },
    "fr": {
        "welcome": "🌟 **Bonjour {name} ! Bienvenue sur {app}**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nUtilisez uniquement le contenu Telegram auquel votre compte connecté a un accès légitime.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "Utilisez le menu ci-dessous.", "limit": "⛔ Votre limite quotidienne est épuisée.",
    },
    "de": {
        "welcome": "🌟 **Hallo {name}! Willkommen bei {app}**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nVerwende nur Telegram-Inhalte, auf die dein verbundenes Konto rechtmäßig zugreifen darf.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "Nutze das Menü unten.", "limit": "⛔ Dein Tageslimit ist aufgebraucht.",
    },
    "id": {
        "welcome": "🌟 **Halo {name}! Selamat datang di {app}**\n\n🏷️ **Plan:** `{plan}`\n⏳ **Expiry:** `{expiry}`\n🔐 **Account:** {session}\n🎯 **Filter:** `{filter}`\n📥 **Queue:** `{queue}`\n\nGunakan hanya konten Telegram yang secara sah dapat diakses akun terhubung Anda.",
        "connected": "✅ Connected", "not_connected": "❌ Not Connected", "choose": "Gunakan menu di bawah.", "limit": "⛔ Batas penggunaan harian habis.",
    },
}

MANUALS = {
    "en": (
        "📖 **SavePro Global V3 — User Manual**\n\n"
        "1️⃣ `/login` — connect your Telegram account securely.\n"
        "2️⃣ **Single save** — send one Telegram post link your account can legitimately access.\n"
        "3️⃣ `/batch <first-link> <count>` or `/batch <first-link> <last-link>` — batch processing.\n"
        "4️⃣ `/clone <source> <destination> all` — full accessible-channel clone on Ultimate. Use a number instead of `all` for a limited clone.\n"
        "5️⃣ `/pause JOB-...`, `/resume JOB-...`, `/cancel JOB-...` — control jobs.\n"
        "6️⃣ `/jobs` or `/history` — live/recent job status.\n"
        "7️⃣ `/clonehelp` — clone filters and examples.\n"
        "8️⃣ `/language` — change language. `/settings` — media filter, thumbs, caption and alerts.\n"
        "9️⃣ `/plans`, `/myplan`, `/paymenthistory` — subscription/business tools.\n"
        "🔟 `/support` — support ticket.\n\n"
        "🔐 Source: public/private chats your connected account can legitimately read. Destination: your account must have permission to post. Protected/access controls are not bypassed.\n"
        "⚠️ Never share OTP, 2FA password or session string with anyone else."
    ),
    "ta": (
        "📖 **SavePro Global V3 — User Manual**\n\n"
        "1️⃣ `/login` — உங்கள் Telegram account-ஐ secure-ஆ connect செய்யவும்.\n"
        "2️⃣ **Single Save** — உங்கள் accountக்கு legitimate access உள்ள ஒரு Telegram post link அனுப்பவும்.\n"
        "3️⃣ `/batch <first-link> <count>` அல்லது `/batch <first-link> <last-link>` — Batch.\n"
        "4️⃣ `/clone <source> <destination> all` — Ultimate plan-ல் access உள்ள முழு channel clone. `all` பதிலாக number கொடுத்தால் limited clone.\n"
        "5️⃣ `/pause JOB-...`, `/resume JOB-...`, `/cancel JOB-...` — Job control.\n"
        "6️⃣ `/jobs` அல்லது `/history` — Live/Recent job status.\n"
        "7️⃣ `/clonehelp` — Clone filters + examples.\n"
        "8️⃣ `/language` — மொழி மாற்றம். `/settings` — filter, thumbnail, caption, alerts.\n"
        "9️⃣ `/plans`, `/myplan`, `/paymenthistory` — Subscription/Business.\n"
        "🔟 `/support` — Support ticket.\n\n"
        "🔐 Source public/private chat-ஐ connected account legitimately read செய்ய முடிந்தால் பயன்படுத்தலாம். Destination-ல் post permission அவசியம். Telegram access/protected controls bypass செய்யப்படாது.\n"
        "⚠️ OTP, 2FA password அல்லது session string-ஐ யாரிடமும் share செய்ய வேண்டாம்."
    ),
    "hi": "📖 **SavePro Global V3 — User Manual**\n\n`/login` से account connect करें। Single save के लिए accessible post link भेजें। Batch: `/batch <link> <count>`। Full clone (Ultimate): `/clone <source> <destination> all`। Job control: `/pause`, `/resume`, `/cancel`। Status: `/jobs`। भाषा: `/language`। केवल वैध access वाले source का उपयोग करें; destination में post permission जरूरी है।",
    "te": "📖 **SavePro Global V3 — User Manual**\n\n`/login` తో account connect చేయండి. Single save కోసం accessible post link పంపండి. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Job control: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Language: `/language`. Sourceకి legitimate access మరియు destinationకి post permission అవసరం.",
    "ml": "📖 **SavePro Global V3 — User Manual**\n\n`/login` ഉപയോഗിച്ച് account connect ചെയ്യുക. Single save-ക്ക് accessible post link അയയ്ക്കുക. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Job control: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Language: `/language`. Source-ിന് legitimate access, destination-ിന് post permission ആവശ്യമാണ്.",
    "kn": "📖 **SavePro Global V3 — User Manual**\n\n`/login` ಮೂಲಕ account connect ಮಾಡಿ. Single saveಗೆ accessible post link ಕಳುಹಿಸಿ. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Job control: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Language: `/language`. Sourceಗೆ legitimate access ಮತ್ತು destinationಗೆ post permission ಅಗತ್ಯ.",
    "bn": "📖 **SavePro Global V3 — User Manual**\n\n`/login` দিয়ে account connect করুন। Single save-এর জন্য accessible post link পাঠান। Batch: `/batch <link> <count>`। Full clone (Ultimate): `/clone <source> <destination> all`। Job control: `/pause`, `/resume`, `/cancel`। Status: `/jobs`। Language: `/language`। Source-এ বৈধ access এবং destination-এ post permission দরকার।",
    "mr": "📖 **SavePro Global V3 — User Manual**\n\n`/login` ने account connect करा. Single save साठी accessible post link पाठवा. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Job control: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Language: `/language`. Source ला वैध access आणि destination ला post permission आवश्यक आहे.",
    "gu": "📖 **SavePro Global V3 — User Manual**\n\n`/login` થી account connect કરો. Single save માટે accessible post link મોકલો. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Job control: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Language: `/language`. Source માટે legitimate access અને destination માટે post permission જરૂરી છે.",
    "pa": "📖 **SavePro Global V3 — User Manual**\n\n`/login` ਨਾਲ account connect ਕਰੋ। Single save ਲਈ accessible post link ਭੇਜੋ। Batch: `/batch <link> <count>`। Full clone (Ultimate): `/clone <source> <destination> all`। Job control: `/pause`, `/resume`, `/cancel`। Status: `/jobs`। Language: `/language`। Source ਲਈ legitimate access ਅਤੇ destination ਵਿੱਚ post permission ਲਾਜ਼ਮੀ ਹੈ।",
    "ur": "📖 **SavePro Global V3 — User Manual**\n\n`/login` سے account connect کریں۔ Single save کے لیے accessible post link بھیجیں۔ Batch: `/batch <link> <count>`۔ Full clone (Ultimate): `/clone <source> <destination> all`۔ Job control: `/pause`, `/resume`, `/cancel`۔ Status: `/jobs`۔ Language: `/language`۔ Source تک جائز access اور destination میں post permission ضروری ہے۔",
    "ar": "📖 **SavePro Global V3 — User Manual**\n\nاستخدم `/login` لربط الحساب. للحفظ المفرد أرسل رابط منشور متاح لحسابك. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. التحكم: `/pause`, `/resume`, `/cancel`. الحالة: `/jobs`. اللغة: `/language`. يلزم وصول مشروع للمصدر وصلاحية النشر في الوجهة.",
    "es": "📖 **SavePro Global V3 — Manual**\n\nConecta tu cuenta con `/login`. Envía un enlace accesible para Single Save. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Control: `/pause`, `/resume`, `/cancel`. Estado: `/jobs`. Idioma: `/language`. Se requiere acceso legítimo al origen y permiso para publicar en el destino.",
    "fr": "📖 **SavePro Global V3 — Manuel**\n\nConnectez le compte avec `/login`. Envoyez un lien accessible pour Single Save. Batch : `/batch <link> <count>`. Full clone (Ultimate) : `/clone <source> <destination> all`. Contrôle : `/pause`, `/resume`, `/cancel`. État : `/jobs`. Langue : `/language`. Accès légitime à la source et permission de publier à destination requis.",
    "de": "📖 **SavePro Global V3 — Handbuch**\n\nKonto mit `/login` verbinden. Für Single Save einen zugänglichen Post-Link senden. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Steuerung: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Sprache: `/language`. Rechtmäßiger Zugriff auf die Quelle und Senderechte im Ziel sind erforderlich.",
    "id": "📖 **SavePro Global V3 — Panduan**\n\nHubungkan akun dengan `/login`. Kirim tautan post yang dapat diakses untuk Single Save. Batch: `/batch <link> <count>`. Full clone (Ultimate): `/clone <source> <destination> all`. Kontrol: `/pause`, `/resume`, `/cancel`. Status: `/jobs`. Bahasa: `/language`. Diperlukan akses sah ke sumber dan izin posting di tujuan.",
}


def normalize_lang(code: Optional[str]) -> str:
    raw = (code or "en").lower().replace("_", "-").split("-", 1)[0]
    return raw if raw in VALID_LANGS else "en"


def manual_for(lang: str) -> str:
    return MANUALS.get(normalize_lang(lang), MANUALS["en"])

# ---------------------------------------------------------------------------
# CRYPTO
# ---------------------------------------------------------------------------


def _fernet_key() -> bytes:
    configured = os.getenv("SESSION_ENCRYPTION_KEY", "").strip()
    if configured:
        try:
            raw = configured.encode()
            Fernet(raw)
            return raw
        except Exception:
            pass
        return base64.urlsafe_b64encode(hashlib.sha256(configured.encode()).digest())

    # Stable fallback so old sessions remain decryptable after restart.
    # Production deployments should always set SESSION_ENCRYPTION_KEY.
    seed = f"{BOT_TOKEN}|{API_HASH}|savepro-v2"
    log.warning(
        "SESSION_ENCRYPTION_KEY is not set. Using a derived fallback key. "
        "Set a dedicated secret in production."
    )
    return base64.urlsafe_b64encode(hashlib.sha256(seed.encode()).digest())


FERNET = Fernet(_fernet_key())


def encrypt_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return FERNET.encrypt(value.encode()).decode()


def decrypt_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return FERNET.decrypt(value.encode()).decode()
    except (InvalidToken, ValueError):
        return None

# ---------------------------------------------------------------------------
# STATE / QUEUES
# ---------------------------------------------------------------------------

DB_LOCK = asyncio.Lock()
LOGIN_STATES: dict[int, dict[str, Any]] = {}
RATE_BUCKETS: dict[int, deque[float]] = defaultdict(deque)
JOB_QUEUE: asyncio.PriorityQueue[tuple[int, int, str]] = asyncio.PriorityQueue()
JOB_SEQ = 0
SHUTDOWN_EVENT = asyncio.Event()
SCHEDULER: Optional[AsyncIOScheduler] = None

# ---------------------------------------------------------------------------
# GENERIC HELPERS
# ---------------------------------------------------------------------------


def now_dt() -> datetime:
    return datetime.now(TZ)


def now_iso() -> str:
    return now_dt().isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return TZ.localize(dt)
        return dt.astimezone(TZ)
    except Exception:
        return None


def human_dt(value: Optional[str]) -> str:
    dt = parse_dt(value)
    if not dt:
        return "No expiry"
    return dt.strftime("%d %b %Y, %I:%M %p")


def generate_id(prefix: str, digits: int = 6) -> str:
    stamp = now_dt().strftime("%y%m%d")
    return f"{prefix}-{stamp}-{secrets.token_hex(max(2, digits // 2)).upper()[:digits]}"


def safe_filename(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name or "file")
    return name[:180] or "file"


def get_valid_thumb(path: Optional[str]) -> Optional[str]:
    return path if path and os.path.exists(path) else None


def is_admin_id(user_id: int) -> bool:
    return user_id in EXTRA_ADMIN_IDS


def batch_limit_for(user: dict[str, Any]) -> Optional[int]:
    """Per-job batch ceiling. Ultimate has unlimited usage; 0 means no explicit range ceiling."""
    if is_admin_id(int(user.get("user_id") or 0)):
        return None
    plan = str(user.get("plan") or "Free")
    if plan == "Ultimate":
        return ULTIMATE_BATCH_SIZE or None
    if plan == "Premium":
        return PREMIUM_BATCH_SIZE
    return MAX_BATCH_SIZE


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


def progress_line(done: int, total: int, started_ts: float) -> str:
    elapsed = max(0.1, time.time() - started_ts)
    rate = done / elapsed if done else 0.0
    if total > 0:
        pct = min(100.0, (done / total) * 100)
        eta = ((total - done) / rate) if rate > 0 else 0
        return f"{done}/{total} • {pct:.1f}% • {rate:.2f}/s • ETA {format_duration(eta)}"
    return f"{done} processed • {rate:.2f}/s • {format_duration(elapsed)} elapsed"


def plan_days(duration: str) -> int:
    return DURATION_DAYS.get(duration, 30)


def create_qr_bytes(payload: str) -> io.BytesIO:
    qr = qrcode.QRCode(version=None, box_size=8, border=3)
    qr.add_data(payload)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = io.BytesIO()
    bio.name = "payment_qr.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def parse_tg_link(text: str) -> Optional[tuple[str, int]]:
    m = re.search(r"https?://t\.me/(?:c/)?([A-Za-z0-9_]+)/([0-9]+)", text)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def chat_ref_from_input(value: str) -> str:
    s = (value or "").strip()
    m = re.search(r"https?://t\.me/(?:c/)?([A-Za-z0-9_]+)(?:/[0-9]+)?", s)
    if m:
        return m.group(1)
    return s


def chat_ref_to_id(chat_raw: str) -> Any:
    s = str(chat_raw).strip()
    if re.fullmatch(r"-100[0-9]+", s):
        return int(s)
    if re.fullmatch(r"-[0-9]+", s):
        return int(s)
    if s.isdigit():
        return int(f"-100{s}")
    return s


def rate_limited(user_id: int) -> bool:
    if is_admin_id(user_id):
        return False
    now = time.time()
    bucket = RATE_BUCKETS[user_id]
    while bucket and now - bucket[0] > RATE_LIMIT_WINDOW:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_MESSAGES:
        return True
    bucket.append(now)
    return False


async def safe_answer(q: CallbackQuery, text: Optional[str] = None, alert: bool = False) -> None:
    try:
        await q.answer(text=text, show_alert=alert)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------


@asynccontextmanager
async def db_conn():
    db = await aiosqlite.connect(DB_PATH, timeout=30)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("PRAGMA busy_timeout=30000")
        yield db
    finally:
        await db.close()


async def table_columns(db: aiosqlite.Connection, table: str) -> set[str]:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    return {r[1] for r in rows}


async def ensure_column(db: aiosqlite.Connection, table: str, column_sql: str) -> None:
    col = column_sql.split()[0]
    cols = await table_columns(db, table)
    if col not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")


async def init_db() -> None:
    async with DB_LOCK:
        async with db_conn() as db:
            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    lang TEXT DEFAULT 'ta',
                    plan TEXT DEFAULT 'Free',
                    daily_limit INTEGER DEFAULT 2,
                    daily_used INTEGER DEFAULT 0,
                    bonus_credits INTEGER DEFAULT 0,
                    trial_used INTEGER DEFAULT 0,
                    plan_started_at TEXT,
                    plan_expires_at TEXT,
                    referred_by INTEGER,
                    ref_count INTEGER DEFAULT 0,
                    encrypted_session TEXT,
                    session TEXT,
                    custom_api_id INTEGER,
                    custom_api_hash TEXT,
                    doc_thumb TEXT,
                    vid_thumb TEXT,
                    custom_caption TEXT,
                    file_filter TEXT DEFAULT 'all',
                    is_banned INTEGER DEFAULT 0,
                    marketing_opt_in INTEGER DEFAULT 1,
                    notify_jobs INTEGER DEFAULT 1,
                    notify_expiry INTEGER DEFAULT 1,
                    created_at TEXT,
                    last_seen TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id TEXT UNIQUE,
                    user_id INTEGER NOT NULL,
                    plan TEXT,
                    duration TEXT,
                    amount INTEGER DEFAULT 0,
                    currency TEXT DEFAULT 'INR',
                    utr TEXT UNIQUE,
                    status TEXT DEFAULT 'awaiting_utr',
                    created_at TEXT,
                    submitted_at TEXT,
                    approved_at TEXT,
                    approved_by INTEGER,
                    rejected_at TEXT,
                    rejected_by INTEGER,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    plan TEXT,
                    days INTEGER DEFAULT 0,
                    uses_left INTEGER DEFAULT 0,
                    expires_at TEXT,
                    first_purchase_only INTEGER DEFAULT 0,
                    max_per_user INTEGER DEFAULT 1,
                    discount_percent INTEGER DEFAULT 0,
                    discount_flat INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS promo_redemptions (
                    code TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    redeemed_at TEXT,
                    PRIMARY KEY (code, user_id)
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT UNIQUE,
                    user_id INTEGER NOT NULL,
                    job_type TEXT NOT NULL,
                    source TEXT,
                    start_id INTEGER,
                    end_id INTEGER,
                    destination TEXT,
                    status TEXT DEFAULT 'queued',
                    priority INTEGER DEFAULT 4,
                    progress_total INTEGER DEFAULT 0,
                    progress_done INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    skipped_count INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0,
                    checkpoint_id INTEGER DEFAULT 0,
                    scan_complete INTEGER DEFAULT 0,
                    error_code TEXT,
                    payload_json TEXT,
                    status_message_id INTEGER,
                    created_at TEXT,
                    started_at TEXT,
                    paused_at TEXT,
                    updated_at TEXT,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS clone_items (
                    job_id TEXT NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    created_at TEXT,
                    PRIMARY KEY (job_id, source_message_id)
                );

                CREATE TABLE IF NOT EXISTS clone_dedupe (
                    user_id INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    job_id TEXT,
                    created_at TEXT,
                    PRIMARY KEY (user_id, source, destination, source_message_id)
                );

                CREATE TABLE IF NOT EXISTS job_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    event TEXT NOT NULL,
                    details TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS support_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT UNIQUE,
                    user_id INTEGER NOT NULL,
                    category TEXT,
                    message TEXT,
                    status TEXT DEFAULT 'open',
                    admin_note TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    role TEXT DEFAULT 'manager',
                    added_by INTEGER,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_id INTEGER,
                    action TEXT,
                    target_id TEXT,
                    details TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_code TEXT UNIQUE,
                    user_id INTEGER,
                    feature TEXT,
                    message TEXT,
                    traceback TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS subscription_reminders (
                    user_id INTEGER,
                    expiry TEXT,
                    reminder_type TEXT,
                    sent_at TEXT,
                    PRIMARY KEY (user_id, expiry, reminder_type)
                );
                """
            )

            # Migration support for older databases.
            user_cols = [
                "username TEXT",
                "trial_used INTEGER DEFAULT 0",
                "plan_started_at TEXT",
                "plan_expires_at TEXT",
                "encrypted_session TEXT",
                "marketing_opt_in INTEGER DEFAULT 1",
                "notify_jobs INTEGER DEFAULT 1",
                "notify_expiry INTEGER DEFAULT 1",
                "created_at TEXT",
                "last_seen TEXT",
                "vid_thumb TEXT",
            ]
            for col in user_cols:
                await ensure_column(db, "users", col)

            payment_cols = [
                "invoice_id TEXT",
                "amount INTEGER DEFAULT 0",
                "currency TEXT DEFAULT 'INR'",
                "submitted_at TEXT",
                "approved_at TEXT",
                "approved_by INTEGER",
                "rejected_at TEXT",
                "rejected_by INTEGER",
                "note TEXT",
            ]
            for col in payment_cols:
                await ensure_column(db, "payments", col)

            promo_cols = [
                "expires_at TEXT",
                "first_purchase_only INTEGER DEFAULT 0",
                "max_per_user INTEGER DEFAULT 1",
                "discount_percent INTEGER DEFAULT 0",
                "discount_flat INTEGER DEFAULT 0",
            ]
            for col in promo_cols:
                await ensure_column(db, "promo_codes", col)

            job_cols = [
                "skipped_count INTEGER DEFAULT 0",
                "retry_count INTEGER DEFAULT 0",
                "checkpoint_id INTEGER DEFAULT 0",
                "scan_complete INTEGER DEFAULT 0",
                "paused_at TEXT",
                "updated_at TEXT",
            ]
            for col in job_cols:
                await ensure_column(db, "jobs", col)

            # Helpful indexes. Partial unique invoice index works with legacy NULL rows.
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id) WHERE invoice_id IS NOT NULL")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority, id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_clone_dedupe_job ON clone_dedupe(job_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_clone_items_job_msg ON clone_items(job_id, source_message_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_plan ON users(plan)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen)")

            defaults = {
                "owner_id": str(OWNER_ID),
                "upi_id": UPI_ID_ENV,
                "upi_name": UPI_NAME_ENV,
                "updates_channel": UPDATES_CHANNEL_ENV,
                "support_username": SUPPORT_USERNAME_ENV,
                "maintenance_mode": "0",
                "maintenance_message": "🛠 Service maintenance is in progress. Please try again later.",
            }
            for key, value in defaults.items():
                await db.execute(
                    "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                    (key, value),
                )

            if OWNER_ID:
                await db.execute(
                    "INSERT OR IGNORE INTO admins(user_id, role, added_by, created_at) VALUES (?, 'owner', ?, ?)",
                    (OWNER_ID, OWNER_ID, now_iso()),
                )
            for aid in EXTRA_ADMIN_IDS:
                await db.execute(
                    "INSERT OR IGNORE INTO admins(user_id, role, added_by, created_at) VALUES (?, 'manager', ?, ?)",
                    (aid, OWNER_ID or aid, now_iso()),
                )

            # Old V1 trials did not have an expiry timestamp. Do not allow those
            # legacy trial labels to become accidental lifetime access.
            await db.execute(
                "UPDATE users SET plan='Free', daily_limit=?, daily_used=0, trial_used=1 "
                "WHERE plan LIKE 'Trial%' AND (plan_expires_at IS NULL OR plan_expires_at='')",
                (FREE_DAILY_LIMIT,),
            )

            # Encrypt legacy plaintext sessions once.
            cols = await table_columns(db, "users")
            if "session" in cols and "encrypted_session" in cols:
                cur = await db.execute(
                    "SELECT user_id, session FROM users WHERE session IS NOT NULL AND session != '' "
                    "AND (encrypted_session IS NULL OR encrypted_session = '')"
                )
                for row in await cur.fetchall():
                    await db.execute(
                        "UPDATE users SET encrypted_session = ?, session = NULL WHERE user_id = ?",
                        (encrypt_text(row[1]), row[0]),
                    )

            await db.commit()


async def get_setting(key: str, default: str = "") -> str:
    async with db_conn() as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return str(row[0]) if row and row[0] is not None else default


async def set_setting(key: str, value: Any) -> None:
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            await db.commit()


async def audit(actor_id: int, action: str, target_id: Any = None, details: Any = None) -> None:
    async with db_conn() as db:
        await db.execute(
            "INSERT INTO audit_logs(actor_id, action, target_id, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                actor_id,
                action,
                str(target_id) if target_id is not None else None,
                json.dumps(details, ensure_ascii=False, default=str) if details is not None else None,
                now_iso(),
            ),
        )
        await db.commit()


async def log_error(user_id: Optional[int], feature: str, exc: Exception) -> str:
    code = generate_id("ERR", 6)
    tb = traceback.format_exc(limit=20)
    msg = f"{type(exc).__name__}: {exc}"
    log.error("%s | %s | user=%s | %s\n%s", code, feature, user_id, msg, tb)
    try:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO errors(error_code, user_id, feature, message, traceback, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (code, user_id, feature, msg[:1000], tb[:8000], now_iso()),
            )
            await db.commit()
    except Exception:
        pass
    return code


async def get_admin_role(user_id: int) -> Optional[str]:
    if user_id == OWNER_ID and OWNER_ID:
        return "owner"
    async with db_conn() as db:
        cur = await db.execute("SELECT role FROM admins WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else None


async def has_admin_role(user_id: int, *roles: str) -> bool:
    role = await get_admin_role(user_id)
    if role == "owner":
        return True
    return bool(role and role in roles)


async def get_user(user_id: int, name: str = "User", username: Optional[str] = None, language_code: Optional[str] = None) -> dict[str, Any]:
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            if not row:
                plan = "Ultimate" if is_admin_id(user_id) else "Free"
                limit = PLAN_LIMITS[plan]
                initial_lang = normalize_lang(language_code) if language_code else "en"
                await db.execute(
                    """
                    INSERT INTO users(
                        user_id, name, username, plan, daily_limit, daily_used,
                        lang, created_at, last_seen
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (user_id, name, username, plan, limit, initial_lang, now_iso(), now_iso()),
                )
                await db.commit()
            else:
                await db.execute(
                    "UPDATE users SET name = COALESCE(?, name), username = COALESCE(?, username), last_seen = ? WHERE user_id = ?",
                    (name or None, username, now_iso(), user_id),
                )
                await db.commit()

    await expire_user_if_needed(user_id)
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else {}


async def update_user(user_id: int, **kwargs: Any) -> None:
    if not kwargs:
        return
    allowed = await _user_columns()
    safe = {k: v for k, v in kwargs.items() if k in allowed}
    if not safe:
        return
    parts = ", ".join(f"{k} = ?" for k in safe)
    values = list(safe.values()) + [user_id]
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(f"UPDATE users SET {parts} WHERE user_id = ?", values)
            await db.commit()


_USER_COL_CACHE: Optional[set[str]] = None


async def _user_columns() -> set[str]:
    global _USER_COL_CACHE
    if _USER_COL_CACHE is None:
        async with db_conn() as db:
            _USER_COL_CACHE = await table_columns(db, "users")
    return _USER_COL_CACHE


async def expire_user_if_needed(user_id: int) -> None:
    if is_admin_id(user_id):
        return
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute(
                "SELECT plan, plan_expires_at FROM users WHERE user_id = ?",
                (user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return
            expiry = parse_dt(row["plan_expires_at"])
            if row["plan"] != "Free" and expiry and now_dt() >= expiry:
                await db.execute(
                    "UPDATE users SET plan='Free', daily_limit=?, daily_used=0, plan_started_at=NULL, plan_expires_at=NULL WHERE user_id=?",
                    (FREE_DAILY_LIMIT, user_id),
                )
                await db.commit()


async def activate_plan(
    user_id: int,
    plan: str,
    days: int,
    actor_id: int = 0,
    reason: str = "manual",
) -> str:
    if plan not in PLAN_LIMITS or plan == "Free":
        raise ValueError("Invalid paid plan")
    days = max(1, int(days))
    user = await get_user(user_id)
    current_expiry = parse_dt(user.get("plan_expires_at"))
    base = current_expiry if current_expiry and current_expiry > now_dt() else now_dt()
    expiry = base + timedelta(days=days)
    await update_user(
        user_id,
        plan=plan,
        daily_limit=PLAN_LIMITS[plan],
        daily_used=0,
        plan_started_at=now_iso(),
        plan_expires_at=expiry.isoformat(),
    )
    await audit(actor_id or user_id, "plan_activated", user_id, {"plan": plan, "days": days, "reason": reason})
    return expiry.isoformat()


async def remaining_usage(user: dict[str, Any]) -> str:
    if user.get("plan") == "Ultimate" or is_admin_id(int(user["user_id"])):
        return "Unlimited"
    remaining = max(0, int(user.get("daily_limit") or 0) - int(user.get("daily_used") or 0))
    return str(remaining + int(user.get("bonus_credits") or 0))


async def can_consume(user_id: int) -> bool:
    await expire_user_if_needed(user_id)
    user = await get_user(user_id)
    if user.get("plan") == "Ultimate" or is_admin_id(user_id):
        return True
    return int(user.get("daily_used") or 0) < int(user.get("daily_limit") or 0) or int(user.get("bonus_credits") or 0) > 0


async def consume_usage(user_id: int) -> bool:
    if is_admin_id(user_id):
        return True
    await expire_user_if_needed(user_id)
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute(
                "SELECT plan, daily_limit, daily_used, bonus_credits FROM users WHERE user_id = ?",
                (user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return False
            if row["plan"] == "Ultimate":
                return True
            if row["daily_used"] < row["daily_limit"]:
                await db.execute("UPDATE users SET daily_used=daily_used+1 WHERE user_id=?", (user_id,))
                await db.commit()
                return True
            if row["bonus_credits"] > 0:
                await db.execute("UPDATE users SET bonus_credits=bonus_credits-1 WHERE user_id=?", (user_id,))
                await db.commit()
                return True
            return False


async def daily_reset_job() -> None:
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute("UPDATE users SET daily_used = 0")
            await db.commit()
    log.info("Daily usage counters reset")

# ---------------------------------------------------------------------------
# PAYMENTS / PROMOS / SUPPORT
# ---------------------------------------------------------------------------


async def create_invoice(user_id: int, plan: str, duration: str) -> dict[str, Any]:
    if plan not in PLAN_PRICING or duration not in DURATION_DAYS:
        raise ValueError("Invalid plan or duration")
    invoice_id = generate_id("INV", 6)
    amount = int(PLAN_PRICING[plan][duration])
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                """
                INSERT INTO payments(invoice_id, user_id, plan, duration, amount, currency, status, created_at)
                VALUES (?, ?, ?, ?, ?, 'INR', 'awaiting_utr', ?)
                """,
                (invoice_id, user_id, plan, duration, amount, now_iso()),
            )
            await db.commit()
    return {
        "invoice_id": invoice_id,
        "user_id": user_id,
        "plan": plan,
        "duration": duration,
        "amount": amount,
        "currency": "INR",
        "status": "awaiting_utr",
    }


async def get_payment(invoice_id: str) -> Optional[dict[str, Any]]:
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM payments WHERE invoice_id = ?", (invoice_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def latest_awaiting_payment(user_id: int) -> Optional[dict[str, Any]]:
    async with db_conn() as db:
        cur = await db.execute(
            """
            SELECT * FROM payments
            WHERE user_id=? AND status='awaiting_utr'
            ORDER BY id DESC LIMIT 1
            """,
            (user_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def submit_utr(user_id: int, invoice_id: str, utr: str) -> tuple[bool, str]:
    utr = utr.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{10,22}", utr):
        return False, "UTR / transaction reference must be 10–22 letters/numbers."
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM payments WHERE invoice_id=? AND user_id=?", (invoice_id, user_id))
            row = await cur.fetchone()
            if not row:
                return False, "Invoice not found."
            if row["status"] not in {"awaiting_utr", "pending"}:
                return False, f"Invoice is already `{row['status']}`."
            dup = await db.execute("SELECT invoice_id FROM payments WHERE utr=? AND invoice_id != ?", (utr, invoice_id))
            if await dup.fetchone():
                return False, "This UTR has already been submitted for another invoice."
            await db.execute(
                "UPDATE payments SET utr=?, status='pending', submitted_at=? WHERE invoice_id=?",
                (utr, now_iso(), invoice_id),
            )
            await db.commit()
    await audit(user_id, "payment_submitted", invoice_id, {"utr": utr})
    return True, "Payment submitted for verification."


async def approve_payment(invoice_id: str, admin_id: int) -> tuple[bool, str, Optional[dict[str, Any]]]:
    if not await has_admin_role(admin_id, "payments", "manager"):
        return False, "Not authorized", None
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM payments WHERE invoice_id=?", (invoice_id,))
            row = await cur.fetchone()
            if not row:
                return False, "Invoice not found", None
            payment = dict(row)
            if payment["status"] == "approved":
                return False, "Already approved", payment
            if payment["status"] != "pending":
                return False, f"Cannot approve status {payment['status']}", payment
            await db.execute(
                "UPDATE payments SET status='approved', approved_at=?, approved_by=? WHERE invoice_id=?",
                (now_iso(), admin_id, invoice_id),
            )
            await db.commit()
    expiry = await activate_plan(
        int(payment["user_id"]),
        str(payment["plan"]),
        plan_days(str(payment["duration"])),
        actor_id=admin_id,
        reason=f"payment:{invoice_id}",
    )
    payment["plan_expires_at"] = expiry
    await audit(admin_id, "payment_approved", invoice_id, payment)
    return True, "Approved", payment


async def reject_payment(invoice_id: str, admin_id: int, note: str = "") -> tuple[bool, str, Optional[dict[str, Any]]]:
    if not await has_admin_role(admin_id, "payments", "manager"):
        return False, "Not authorized", None
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM payments WHERE invoice_id=?", (invoice_id,))
            row = await cur.fetchone()
            if not row:
                return False, "Invoice not found", None
            payment = dict(row)
            if payment["status"] == "approved":
                return False, "Approved payments cannot be rejected from the bot.", payment
            await db.execute(
                "UPDATE payments SET status='rejected', rejected_at=?, rejected_by=?, note=? WHERE invoice_id=?",
                (now_iso(), admin_id, note[:500], invoice_id),
            )
            await db.commit()
    await audit(admin_id, "payment_rejected", invoice_id, {"note": note})
    return True, "Rejected", payment


async def redeem_promo(user_id: int, code: str) -> tuple[bool, str]:
    code = code.strip().upper()
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
            row = await cur.fetchone()
            if not row:
                return False, "Invalid promo code."
            promo = dict(row)
            expiry = parse_dt(promo.get("expires_at"))
            if expiry and now_dt() >= expiry:
                return False, "This promo code has expired."
            if int(promo.get("uses_left") or 0) <= 0:
                return False, "This promo code has no uses left."
            cur2 = await db.execute(
                "SELECT COUNT(*) FROM promo_redemptions WHERE code=? AND user_id=?",
                (code, user_id),
            )
            redeemed = int((await cur2.fetchone())[0])
            if redeemed >= int(promo.get("max_per_user") or 1):
                return False, "You already used this promo code."
            if int(promo.get("first_purchase_only") or 0):
                pc = await db.execute("SELECT COUNT(*) FROM payments WHERE user_id=? AND status='approved'", (user_id,))
                if int((await pc.fetchone())[0]) > 0:
                    return False, "This promo is only for first-time customers."
            plan = str(promo.get("plan") or "")
            days = int(promo.get("days") or 0)
            if plan not in PLAN_LIMITS or plan == "Free" or days <= 0:
                return False, "This promo is not configured for direct activation."
            await db.execute("UPDATE promo_codes SET uses_left=uses_left-1 WHERE code=?", (code,))
            await db.execute(
                "INSERT INTO promo_redemptions(code, user_id, redeemed_at) VALUES (?, ?, ?)",
                (code, user_id, now_iso()),
            )
            await db.commit()
    expiry_iso = await activate_plan(user_id, plan, days, actor_id=user_id, reason=f"promo:{code}")
    return True, f"🎉 `{plan}` activated until **{human_dt(expiry_iso)}**."


async def create_ticket(user_id: int, category: str, message: str) -> str:
    ticket_id = generate_id("SUP", 6)
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                """
                INSERT INTO support_tickets(ticket_id, user_id, category, message, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'open', ?, ?)
                """,
                (ticket_id, user_id, category[:50], message[:3500], now_iso(), now_iso()),
            )
            await db.commit()
    return ticket_id


async def close_ticket(ticket_id: str, admin_id: int, note: str = "") -> bool:
    if not await has_admin_role(admin_id, "support", "manager"):
        return False
    async with DB_LOCK:
        async with db_conn() as db:
            cur = await db.execute("SELECT user_id FROM support_tickets WHERE ticket_id=?", (ticket_id,))
            row = await cur.fetchone()
            if not row:
                return False
            await db.execute(
                "UPDATE support_tickets SET status='resolved', admin_note=?, updated_at=? WHERE ticket_id=?",
                (note[:1000], now_iso(), ticket_id),
            )
            await db.commit()
    await audit(admin_id, "ticket_resolved", ticket_id, {"note": note})
    return True

async def clear_non_auth_input_state(user_id: int) -> None:
    """Clear stale payment/batch/support/transfer input state without breaking an active login flow."""
    state = LOGIN_STATES.get(user_id)
    if not state:
        return
    if state.get("step") in {"PHONE", "OTP", "2FA", "SESSION_STRING"}:
        return
    state = LOGIN_STATES.pop(user_id, None)
    cli = state.get("client") if state else None
    if cli:
        try:
            if cli.is_connected:
                await cli.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# JOB QUEUE
# ---------------------------------------------------------------------------


async def user_queue_count(user_id: int) -> int:
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM jobs WHERE user_id=? AND status IN ('queued','running','paused')",
            (user_id,),
        )
        return int((await cur.fetchone())[0])


async def enqueue_job(
    user_id: int,
    job_type: str,
    source: str,
    start_id: Optional[int] = None,
    end_id: Optional[int] = None,
    destination: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> str:
    global JOB_SEQ
    if await user_queue_count(user_id) >= MAX_QUEUE_PER_USER and not is_admin_id(user_id):
        raise RuntimeError(f"Maximum {MAX_QUEUE_PER_USER} queued/running jobs allowed per user.")
    user = await get_user(user_id)
    priority = PLAN_PRIORITY.get(str(user.get("plan") or "Free"), 4)
    job_id = generate_id("JOB", 6)
    total = max(0, (end_id or 0) - (start_id or 0) + 1) if start_id and end_id else 0
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                """
                INSERT INTO jobs(
                    job_id,user_id,job_type,source,start_id,end_id,destination,status,priority,
                    progress_total,payload_json,created_at,updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    user_id,
                    job_type,
                    source,
                    start_id,
                    end_id,
                    destination,
                    priority,
                    total,
                    json.dumps(payload or {}, ensure_ascii=False),
                    now_iso(),
                    now_iso(),
                ),
            )
            await db.commit()
    JOB_SEQ += 1
    await JOB_QUEUE.put((priority, JOB_SEQ, job_id))
    await audit(user_id, "job_queued", job_id, {"type": job_type, "source": source, "start": start_id, "end": end_id})
    return job_id


async def get_job(job_id: str) -> Optional[dict[str, Any]]:
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def update_job(job_id: str, **kwargs: Any) -> None:
    if not kwargs:
        return
    allowed = {
        "status", "progress_total", "progress_done", "success_count", "failed_count",
        "skipped_count", "retry_count", "checkpoint_id", "scan_complete", "error_code", "status_message_id",
        "started_at", "paused_at", "updated_at", "finished_at", "payload_json",
    }
    safe = {k: v for k, v in kwargs.items() if k in allowed}
    if safe and "updated_at" not in safe:
        safe["updated_at"] = now_iso()
    if not safe:
        return
    parts = ", ".join(f"{k}=?" for k in safe)
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(f"UPDATE jobs SET {parts} WHERE job_id=?", list(safe.values()) + [job_id])
            await db.commit()


async def cancel_job(job_id: str, user_id: int) -> bool:
    job = await get_job(job_id)
    if not job or (job["user_id"] != user_id and not is_admin_id(user_id)):
        return False
    if job["status"] not in {"queued", "running", "paused"}:
        return False
    await update_job(job_id, status="cancelled", finished_at=now_iso())
    await add_job_event(job_id, user_id, "cancelled")
    await audit(user_id, "job_cancelled", job_id)
    return True


async def add_job_event(job_id: str, user_id: int, event: str, details: Any = None) -> None:
    try:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO job_events(job_id,user_id,event,details,created_at) VALUES (?, ?, ?, ?, ?)",
                (job_id, user_id, event, json.dumps(details, ensure_ascii=False, default=str) if details is not None else None, now_iso()),
            )
            await db.commit()
    except Exception:
        pass


async def pause_job(job_id: str, user_id: int) -> bool:
    job = await get_job(job_id)
    if not job or (int(job["user_id"]) != user_id and not is_admin_id(user_id)):
        return False
    if job["status"] not in {"queued", "running"}:
        return False
    await update_job(job_id, status="paused", paused_at=now_iso())
    await add_job_event(job_id, user_id, "paused")
    await audit(user_id, "job_paused", job_id)
    return True


async def resume_job(job_id: str, user_id: int) -> bool:
    global JOB_SEQ
    job = await get_job(job_id)
    if not job or (int(job["user_id"]) != user_id and not is_admin_id(user_id)):
        return False
    if job["status"] not in {"paused", "failed"}:
        return False
    await update_job(job_id, status="queued", paused_at=None, finished_at=None, error_code=None)
    JOB_SEQ += 1
    await JOB_QUEUE.put((int(job.get("priority") or 4), JOB_SEQ, job_id))
    await add_job_event(job_id, user_id, "resumed")
    await audit(user_id, "job_resumed", job_id)
    return True


async def recover_jobs() -> None:
    global JOB_SEQ
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute("UPDATE jobs SET status='queued', started_at=NULL WHERE status='running'")
            cur = await db.execute("SELECT job_id, priority FROM jobs WHERE status='queued' ORDER BY id ASC")
            rows = await cur.fetchall()
            await db.commit()
    for row in rows:
        JOB_SEQ += 1
        await JOB_QUEUE.put((int(row["priority"]), JOB_SEQ, str(row["job_id"])))
    if rows:
        log.info("Recovered %d queued jobs", len(rows))


async def connect_user_client(user: dict[str, Any], tag: str) -> Client:
    encrypted = user.get("encrypted_session")
    session_string = decrypt_text(encrypted)
    if not session_string:
        raise RuntimeError("Telegram account is not connected or session cannot be decrypted.")
    api_id = int(user.get("custom_api_id") or API_ID)
    api_hash = str(user.get("custom_api_hash") or API_HASH)
    client = Client(
        f"user_{user['user_id']}_{tag}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
    )
    await client.connect()
    return client


async def resolve_target_peer(client: Client, chat_id: Any) -> Any:
    try:
        chat = await client.get_chat(chat_id)
        return chat.id
    except Exception:
        pass
    try:
        async for dialog in client.get_dialogs(limit=200):
            if str(dialog.chat.id) == str(chat_id) or str(dialog.chat.username or "").lower() == str(chat_id).lower().lstrip("@"):
                return dialog.chat.id
    except Exception:
        pass
    return chat_id


async def is_chat_admin(client: Client, chat_id: Any) -> bool:
    try:
        me = await client.get_me()
        member = await client.get_chat_member(chat_id, me.id)
        return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
    except Exception:
        return False


async def ensure_source_access(client: Client, chat_id: Any) -> Any:
    """Resolve a source the connected account can legitimately read.

    This does not bypass Telegram private/protected access controls. If Telegram
    denies the connected account, the job is rejected.
    """
    peer = await resolve_target_peer(client, chat_id)
    try:
        chat = await client.get_chat(peer)
        if bool(getattr(chat, "has_protected_content", False)):
            raise PermissionError(
                "This source has Telegram protected-content enabled. The bot does not bypass that control."
            )
        # A tiny history read catches private-channel membership/access failures
        # before a long job is queued into download work.
        async for _ in client.get_chat_history(chat.id, limit=1):
            break
        return chat.id
    except PermissionError:
        raise
    except Exception as exc:
        raise PermissionError(
            "The connected Telegram account cannot read this source chat. "
            "Join/get legitimate access first and retry."
        ) from exc


async def can_post_to_chat(client: Client, chat_id: Any) -> bool:
    """Check whether the connected account can post to the destination."""
    try:
        chat = await client.get_chat(chat_id)
        me = await client.get_me()
        member = await client.get_chat_member(chat.id, me.id)
        if member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}:
            return True
        chat_type = str(getattr(chat, "type", "")).lower()
        # Broadcast channels require admin/owner posting rights.
        if "channel" in chat_type:
            return False
        if member.status == ChatMemberStatus.MEMBER:
            return True
        if member.status == ChatMemberStatus.RESTRICTED:
            perms = getattr(member, "permissions", None)
            return bool(perms and getattr(perms, "can_send_messages", False))
        return False
    except Exception:
        return False


async def progress_tracker(current: int, total: int, status_msg: Message, label: str, last: list[float]) -> None:
    now = time.time()
    if now - last[0] < 3 and current != total:
        return
    last[0] = now
    pct = (current / total * 100) if total else 0
    filled = min(10, int(pct // 10))
    bar = "█" * filled + "░" * (10 - filled)
    try:
        await status_msg.edit_text(
            f"⚙️ **{label}**\n\n[{bar}] {pct:.1f}%\n"
            f"📦 {current / 1048576:.1f} MB / {total / 1048576:.1f} MB"
        )
    except Exception:
        pass


async def deliver_message_media(
    user_client: Client,
    target: Message,
    user: dict[str, Any],
    status_msg: Message,
    job_dir: Path,
    item_label: str,
) -> bool:
    user_id = int(user["user_id"])
    filter_type = str(user.get("file_filter") or "all")

    if not target or getattr(target, "empty", False):
        return False
    if target.service:
        return False
    if not target.media and target.text:
        if filter_type not in {"all", "doc"}:
            return False
        await bot.send_message(user_id, target.text[:4096])
        return True
    if filter_type == "video" and not target.video:
        return False
    if filter_type == "doc" and not target.document:
        return False
    if filter_type == "audio" and not (target.audio or target.voice):
        return False
    if filter_type == "photo" and not target.photo:
        return False

    caption = (user.get("custom_caption") or target.caption or "")[:1024]
    thumb_doc = get_valid_thumb(user.get("doc_thumb"))
    thumb_vid = get_valid_thumb(user.get("vid_thumb"))
    last_d = [0.0]
    f_path = await target.download(
        file_name=str(job_dir) + os.sep,
        progress=progress_tracker,
        progress_args=(status_msg, f"Downloading {item_label}", last_d),
    )
    if not f_path or not os.path.exists(f_path):
        return False

    last_u = [0.0]
    try:
        if target.document:
            await bot.send_document(user_id, f_path, caption=caption, thumb=thumb_doc, progress=progress_tracker, progress_args=(status_msg, f"Uploading {item_label}", last_u))
        elif target.video:
            await bot.send_video(user_id, f_path, caption=caption, thumb=thumb_vid, supports_streaming=True, progress=progress_tracker, progress_args=(status_msg, f"Uploading {item_label}", last_u))
        elif target.audio:
            await bot.send_audio(user_id, f_path, caption=caption, thumb=thumb_doc, progress=progress_tracker, progress_args=(status_msg, f"Uploading {item_label}", last_u))
        elif target.voice:
            await bot.send_voice(user_id, f_path, caption=caption)
        elif target.photo:
            await bot.send_photo(user_id, f_path, caption=caption)
        else:
            return False
        return True
    finally:
        try:
            os.remove(f_path)
        except OSError:
            pass


async def execute_save_job(job: dict[str, Any]) -> None:
    """Run a single/batch save with pause/resume, retry and live progress.

    Source access is based on what the connected Telegram account can legitimately
    read. Owner/admin rights are not required for the source.
    """
    user_id = int(job["user_id"])
    user = await get_user(user_id)
    if user.get("is_banned"):
        raise RuntimeError("User is banned")

    chat_id = chat_ref_to_id(str(job["source"]))
    start_id = int(job["start_id"])
    end_id = int(job["end_id"])
    total = end_id - start_id + 1
    job_id = str(job["job_id"])
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    done_before = max(0, int(job.get("progress_done") or 0))
    success = max(0, int(job.get("success_count") or 0))
    failed = max(0, int(job.get("failed_count") or 0))
    skipped = max(0, int(job.get("skipped_count") or 0))
    next_id = min(end_id + 1, start_id + done_before)
    run_started = time.time()

    status_msg = await bot.send_message(
        user_id,
        f"⏳ **{job_id} started/resumed**\n"
        f"Items: {total} • Resume: {done_before}/{total}\n"
        f"Priority: {PLAN_PRIORITY.get(user.get('plan'), 4)}",
    )
    await update_job(job_id, status_message_id=status_msg.id, progress_total=total)
    await add_job_event(job_id, user_id, "run_started", {"resume_from": next_id})

    u_client: Optional[Client] = None
    try:
        u_client = await connect_user_client(user, job_id[-6:])
        peer = await ensure_source_access(u_client, chat_id)

        if next_id > end_id:
            await status_msg.edit_text(
                f"✅ **Job already complete**\nJob: `{job_id}`\n✅ {success} • ⏭ {skipped} • ❌ {failed}"
            )
            return

        for cur_id in range(next_id, end_id + 1):
            latest = await get_job(job_id)
            if not latest:
                return
            if latest["status"] == "cancelled":
                await status_msg.edit_text(
                    f"🚫 **{job_id} cancelled**\nCompleted: {latest.get('progress_done', 0)}/{total}\n"
                    f"✅ {success} • ⏭ {skipped} • ❌ {failed}"
                )
                return
            if latest["status"] == "paused":
                await status_msg.edit_text(
                    f"⏸ **{job_id} paused**\n"
                    f"{progress_line(int(latest.get('progress_done') or 0), total, run_started)}\n"
                    f"Use `/resume {job_id}` to continue."
                )
                return

            if not await can_consume(user_id):
                await update_job(job_id, status="paused", paused_at=now_iso())
                await add_job_event(job_id, user_id, "paused_limit")
                await status_msg.edit_text(
                    f"⏸ **Daily limit reached**\nJob: `{job_id}`\n"
                    f"Progress saved: {int(latest.get('progress_done') or 0)}/{total}\n"
                    f"Upgrade or resume after your limit resets."
                )
                return

            sent = False
            item_failed = False
            retries_used = 0
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    target = await u_client.get_messages(peer, cur_id)
                    sent = await deliver_message_media(
                        u_client, target, user, status_msg, job_dir, f"#{cur_id}"
                    )
                    break
                except FloodWait as fw:
                    retries_used += 1
                    await update_job(job_id, retry_count=int((await get_job(job_id) or {}).get("retry_count") or 0) + 1)
                    wait_for = int(getattr(fw, "value", 1)) + 1
                    try:
                        await status_msg.edit_text(
                            f"⏳ **Telegram FloodWait**\nJob: `{job_id}`\n"
                            f"Waiting {wait_for}s • retry {attempt}/{MAX_RETRIES}"
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(wait_for)
                except (PeerIdInvalid, ChannelPrivate) as exc:
                    item_failed = True
                    await log_error(user_id, f"save_access:{job_id}:{cur_id}", exc)
                    break
                except Exception as exc:
                    retries_used += 1
                    if attempt >= MAX_RETRIES:
                        item_failed = True
                        await log_error(user_id, f"job_item:{job_id}:{cur_id}", exc)
                        break
                    await asyncio.sleep(min(5, attempt * 1.5))

            if sent:
                await consume_usage(user_id)
                success += 1
            elif item_failed:
                failed += 1
            else:
                skipped += 1

            done = cur_id - start_id + 1
            await update_job(
                job_id,
                progress_done=done,
                success_count=success,
                failed_count=failed,
                skipped_count=skipped,
                checkpoint_id=cur_id,
            )

            if done % 5 == 0 or done == total:
                try:
                    await status_msg.edit_text(
                        f"⚙️ **{job_id}**\n"
                        f"{progress_line(done, total, run_started)}\n"
                        f"✅ {success} • ⏭ {skipped} • ❌ {failed}"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.25)

        latest = await get_job(job_id)
        if latest and latest["status"] == "running":
            await status_msg.edit_text(
                f"✅ **Job completed**\n\nJob: `{job_id}`\n"
                f"Processed: **{total}/{total}**\n"
                f"Success: **{success}**\nSkipped: **{skipped}**\nFailed: **{failed}**"
            )
            await add_job_event(job_id, user_id, "run_completed", {"success": success, "skipped": skipped, "failed": failed})
    finally:
        if u_client and u_client.is_connected:
            await u_client.disconnect()
        shutil.rmtree(job_dir, ignore_errors=True)


async def clone_seen(user_id: int, source: str, destination: str, source_message_id: int) -> bool:
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT 1 FROM clone_dedupe WHERE user_id=? AND source=? AND destination=? AND source_message_id=?",
            (user_id, source, destination, source_message_id),
        )
        return bool(await cur.fetchone())


async def mark_clone_seen(user_id: int, source: str, destination: str, source_message_id: int, job_id: str) -> None:
    async with db_conn() as db:
        await db.execute(
            "INSERT OR IGNORE INTO clone_dedupe(user_id,source,destination,source_message_id,job_id,created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, source, destination, source_message_id, job_id, now_iso()),
        )
        await db.commit()


def clone_message_matches(post: Message, media_filter: str, include_text: bool = True) -> bool:
    media_filter = (media_filter or "all").lower()
    if post.service:
        return False
    if media_filter == "all":
        return bool(post.media or (include_text and post.text))
    if media_filter == "video":
        return bool(post.video)
    if media_filter == "audio":
        return bool(post.audio or post.voice)
    if media_filter in {"doc", "document"}:
        return bool(post.document)
    if media_filter == "photo":
        return bool(post.photo)
    if media_filter == "text":
        return bool(post.text and not post.media)
    return bool(post.media or (include_text and post.text))


async def send_clone_post(
    client: Client,
    dest: Any,
    post: Message,
    user: dict[str, Any],
    job_dir: Path,
    caption_mode: str,
) -> bool:
    caption_mode = (caption_mode or "keep").lower()
    if caption_mode == "remove":
        caption = ""
    elif caption_mode == "custom":
        caption = str(user.get("custom_caption") or post.caption or "")[:1024]
    else:
        caption = str(post.caption or "")[:1024]

    if not post.media and post.text:
        text = post.text if caption_mode != "remove" else post.text
        await client.send_message(dest, text[:4096])
        return True

    # Non-file Telegram message types that can be reproduced without bypassing access controls.
    if getattr(post, "location", None):
        loc = post.location
        await client.send_location(dest, loc.latitude, loc.longitude)
        return True
    if getattr(post, "venue", None):
        venue = post.venue
        loc = venue.location
        await client.send_venue(dest, loc.latitude, loc.longitude, venue.title, venue.address)
        return True
    if getattr(post, "contact", None):
        c = post.contact
        await client.send_contact(
            dest, c.phone_number, c.first_name,
            last_name=getattr(c, "last_name", None), vcard=getattr(c, "vcard", None),
        )
        return True

    f = await post.download(file_name=str(job_dir) + os.sep)
    if not f or not os.path.exists(f):
        return False
    try:
        if post.document:
            await client.send_document(dest, f, caption=caption, thumb=get_valid_thumb(user.get("doc_thumb")))
        elif post.video:
            await client.send_video(dest, f, caption=caption, thumb=get_valid_thumb(user.get("vid_thumb")), supports_streaming=True)
        elif post.audio:
            await client.send_audio(dest, f, caption=caption, thumb=get_valid_thumb(user.get("doc_thumb")))
        elif post.voice:
            await client.send_voice(dest, f, caption=caption)
        elif post.photo:
            await client.send_photo(dest, f, caption=caption)
        elif post.animation:
            await client.send_animation(dest, f, caption=caption)
        elif post.video_note:
            await client.send_video_note(dest, f)
        elif post.sticker:
            await client.send_sticker(dest, f)
        else:
            return False
        return True
    finally:
        try:
            os.remove(f)
        except OSError:
            pass


async def clone_item_count(job_id: str) -> int:
    async with db_conn() as db:
        cur = await db.execute("SELECT COUNT(*) FROM clone_items WHERE job_id=?", (job_id,))
        return int((await cur.fetchone())[0])


async def clone_item_min(job_id: str) -> int:
    async with db_conn() as db:
        cur = await db.execute("SELECT COALESCE(MIN(source_message_id),0) FROM clone_items WHERE job_id=?", (job_id,))
        return int((await cur.fetchone())[0])


async def add_clone_items(job_id: str, ids: list[int]) -> None:
    if not ids:
        return
    async with DB_LOCK:
        async with db_conn() as db:
            await db.executemany(
                "INSERT OR IGNORE INTO clone_items(job_id,source_message_id,created_at) VALUES (?, ?, ?)",
                [(job_id, int(mid), now_iso()) for mid in ids],
            )
            await db.commit()


async def next_clone_ids(job_id: str, checkpoint: int, limit: int = 50) -> list[int]:
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT source_message_id FROM clone_items WHERE job_id=? AND source_message_id>? "
            "ORDER BY source_message_id ASC LIMIT ?",
            (job_id, checkpoint, limit),
        )
        return [int(r[0]) for r in await cur.fetchall()]


async def prepare_clone_index(
    client: Client,
    src: Any,
    job_id: str,
    user_id: int,
    status_msg: Message,
    limit: int,
    min_id: int,
    max_id: int,
) -> Optional[int]:
    """Persist source message IDs so cloning can run oldest -> newest and resume safely."""
    latest = await get_job(job_id)
    if latest and int(latest.get("scan_complete") or 0):
        return await clone_item_count(job_id)

    existing = await clone_item_count(job_id)
    oldest_indexed = await clone_item_min(job_id)
    remaining = 0 if limit == 0 else max(0, limit - existing)
    if limit > 0 and remaining == 0:
        await update_job(job_id, scan_complete=1, progress_total=existing)
        return existing

    kwargs: dict[str, Any] = {"limit": remaining if limit > 0 else 0}
    if oldest_indexed > 0:
        kwargs["offset_id"] = oldest_indexed

    buffer: list[int] = []
    scanned = existing
    last_ui = 0.0
    async for post in client.get_chat_history(src, **kwargs):
        current = await get_job(job_id)
        if not current:
            return None
        if current["status"] in {"paused", "cancelled"}:
            if buffer:
                await add_clone_items(job_id, buffer)
            return None

        # History arrives newest -> oldest. We only index the requested ID window.
        if max_id and post.id > max_id:
            continue
        if min_id and post.id < min_id:
            break

        buffer.append(int(post.id))
        scanned += 1
        if len(buffer) >= 300:
            await add_clone_items(job_id, buffer)
            buffer.clear()

        if time.time() - last_ui >= 2.5:
            last_ui = time.time()
            try:
                await status_msg.edit_text(
                    f"🧭 **{job_id} indexing source**\n"
                    f"Indexed: **{scanned}** messages\n"
                    "This one-time index preserves chronological order and enables restart/resume."
                )
            except Exception:
                pass

    if buffer:
        await add_clone_items(job_id, buffer)
    total = await clone_item_count(job_id)
    await update_job(job_id, scan_complete=1, progress_total=total)
    await add_job_event(job_id, user_id, "clone_index_complete", {"messages": total})
    return total


async def execute_transfer_job(job: dict[str, Any]) -> None:
    """Chronological, resumable full-channel clone/transfer for Ultimate users.

    The source only needs legitimate read access for the connected account. The
    destination must allow that account to post. Telegram protected-content and
    private-access controls are never bypassed.
    """
    user_id = int(job["user_id"])
    user = await get_user(user_id)
    if user.get("plan") != "Ultimate" and not is_admin_id(user_id):
        raise RuntimeError("Full channel clone/transfer requires Ultimate plan")

    source_raw = str(job["source"])
    dest_raw = str(job["destination"])
    src = chat_ref_to_id(source_raw)
    dest = chat_ref_to_id(dest_raw)
    job_id = str(job["job_id"])
    payload = json.loads(job.get("payload_json") or "{}") or {}

    limit = int(payload.get("limit", 0) or 0)  # 0 = all accessible history
    media_filter = str(payload.get("media_filter") or "all").lower()
    caption_mode = str(payload.get("caption_mode") or "keep").lower()
    skip_duplicates = bool(payload.get("skip_duplicates", True))
    include_text = bool(payload.get("include_text", True))
    min_id = max(0, int(payload.get("min_id", 0) or 0))
    max_id = max(0, int(payload.get("max_id", 0) or 0))

    done_before = max(0, int(job.get("progress_done") or 0))
    success = max(0, int(job.get("success_count") or 0))
    failed = max(0, int(job.get("failed_count") or 0))
    skipped = max(0, int(job.get("skipped_count") or 0))
    checkpoint = max(0, int(job.get("checkpoint_id") or 0))
    run_started = time.time()

    status_msg = await bot.send_message(
        user_id,
        f"🔄 **{job_id} validating access…**\n"
        f"Mode: {'FULL CHANNEL' if limit == 0 else f'Last {limit} messages'}\n"
        f"Filter: `{media_filter}` • Caption: `{caption_mode}` • Order: `oldest → newest`",
    )
    await update_job(job_id, status_message_id=status_msg.id)
    await add_job_event(job_id, user_id, "clone_validating", payload)

    u_client: Optional[Client] = None
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    try:
        u_client = await connect_user_client(user, f"transfer_{job_id[-6:]}")
        src = await ensure_source_access(u_client, src)
        dest = await resolve_target_peer(u_client, dest)
        if not await can_post_to_chat(u_client, dest):
            raise PermissionError(
                "The connected Telegram account cannot post to the destination. "
                "Grant the required destination posting/admin permission and retry."
            )

        total = await prepare_clone_index(
            u_client, src, job_id, user_id, status_msg, limit, min_id, max_id
        )
        if total is None:
            latest = await get_job(job_id)
            if latest and latest["status"] == "paused":
                await status_msg.edit_text(
                    f"⏸ **{job_id} paused while indexing**\nUse `/resume {job_id}` to continue."
                )
            return
        if total <= 0:
            await status_msg.edit_text(f"ℹ️ **{job_id}** — no matching source messages found.")
            return

        # Refresh persisted counters in case this is a resume after restart.
        fresh = await get_job(job_id) or job
        done_before = max(0, int(fresh.get("progress_done") or 0))
        success = max(0, int(fresh.get("success_count") or 0))
        failed = max(0, int(fresh.get("failed_count") or 0))
        skipped = max(0, int(fresh.get("skipped_count") or 0))
        checkpoint = max(0, int(fresh.get("checkpoint_id") or 0))

        await status_msg.edit_text(
            f"🚀 **{job_id} clone started/resumed**\n"
            f"{progress_line(done_before, total, run_started)}\n"
            f"Source access: ✅ • Destination post: ✅ • Order: oldest → newest"
        )
        await add_job_event(job_id, user_id, "clone_started", {"checkpoint": checkpoint, "total": total})

        while True:
            latest = await get_job(job_id)
            if not latest:
                return
            if latest["status"] == "cancelled":
                await status_msg.edit_text(
                    f"🚫 **{job_id} cancelled**\n✅ {success} • ⏭ {skipped} • ❌ {failed}"
                )
                return
            if latest["status"] == "paused":
                await status_msg.edit_text(
                    f"⏸ **{job_id} paused**\n"
                    f"{progress_line(int(latest.get('progress_done') or 0), total, run_started)}\n"
                    f"Use `/resume {job_id}` to continue."
                )
                return

            ids = await next_clone_ids(job_id, checkpoint, 50)
            if not ids:
                break

            for source_message_id in ids:
                latest = await get_job(job_id)
                if not latest or latest["status"] in {"paused", "cancelled"}:
                    break

                post = await u_client.get_messages(src, source_message_id)
                done = int(latest.get("progress_done") or 0) + 1
                item_skipped = False
                item_failed = False
                item_sent = False

                if not post or getattr(post, "empty", False):
                    item_skipped = True
                elif not clone_message_matches(post, media_filter, include_text=include_text):
                    item_skipped = True
                elif skip_duplicates and await clone_seen(user_id, source_raw, dest_raw, source_message_id):
                    item_skipped = True
                else:
                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            item_sent = await send_clone_post(
                                u_client, dest, post, user, job_dir, caption_mode
                            )
                            if not item_sent:
                                item_skipped = True
                            break
                        except FloodWait as fw:
                            wait_for = int(getattr(fw, "value", 1)) + 1
                            retry_total = int((await get_job(job_id) or {}).get("retry_count") or 0) + 1
                            await update_job(job_id, retry_count=retry_total)
                            try:
                                await status_msg.edit_text(
                                    f"⏳ **Telegram FloodWait**\nJob: `{job_id}`\n"
                                    f"Waiting {wait_for}s • retry {attempt}/{MAX_RETRIES}"
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(wait_for)
                        except Exception as exc:
                            if attempt >= MAX_RETRIES:
                                item_failed = True
                                await log_error(user_id, f"clone_item:{job_id}:{source_message_id}", exc)
                                break
                            await asyncio.sleep(min(5, attempt * 1.5))

                if item_sent:
                    success += 1
                    if skip_duplicates:
                        await mark_clone_seen(user_id, source_raw, dest_raw, source_message_id, job_id)
                elif item_failed:
                    failed += 1
                else:
                    skipped += 1

                checkpoint = int(source_message_id)
                await update_job(
                    job_id,
                    progress_done=done,
                    success_count=success,
                    failed_count=failed,
                    skipped_count=skipped,
                    checkpoint_id=checkpoint,
                )

                if done % CLONE_PROGRESS_EVERY == 0 or done >= total:
                    try:
                        await status_msg.edit_text(
                            f"🔄 **{job_id} LIVE**\n"
                            f"{progress_line(done, total, run_started)}\n"
                            f"✅ {success} • ⏭ {skipped} • ❌ {failed}\n"
                            f"Checkpoint: `#{checkpoint}`"
                        )
                    except Exception:
                        pass
                await asyncio.sleep(0.25)

            latest = await get_job(job_id)
            if latest and latest["status"] in {"paused", "cancelled"}:
                continue

        latest = await get_job(job_id)
        if latest and latest["status"] == "running":
            final_done = int(latest.get("progress_done") or 0)
            await status_msg.edit_text(
                f"✅ **Clone complete**\n\nJob: `{job_id}`\n"
                f"Processed: **{final_done}/{total}**\nSuccess: **{success}**\n"
                f"Skipped: **{skipped}**\nFailed: **{failed}**"
            )
            await add_job_event(job_id, user_id, "clone_completed", {"success": success, "skipped": skipped, "failed": failed})
    finally:
        if u_client and u_client.is_connected:
            await u_client.disconnect()
        shutil.rmtree(job_dir, ignore_errors=True)


async def job_worker(worker_no: int) -> None:
    log.info("Worker %d started", worker_no)
    while not SHUTDOWN_EVENT.is_set():
        try:
            priority, seq, job_id = await asyncio.wait_for(JOB_QUEUE.get(), timeout=2)
        except asyncio.TimeoutError:
            continue
        try:
            job = await get_job(job_id)
            if not job or job["status"] != "queued":
                continue
            await update_job(job_id, status="running", started_at=now_iso())
            try:
                if job["job_type"] in {"single", "batch"}:
                    await asyncio.wait_for(execute_save_job(job), timeout=JOB_TIMEOUT_SECONDS)
                elif job["job_type"] == "transfer":
                    if CLONE_JOB_TIMEOUT_SECONDS > 0:
                        await asyncio.wait_for(execute_transfer_job(job), timeout=CLONE_JOB_TIMEOUT_SECONDS)
                    else:
                        await execute_transfer_job(job)
                else:
                    raise RuntimeError(f"Unknown job type: {job['job_type']}")
                latest = await get_job(job_id)
                if latest and latest["status"] == "running":
                    await update_job(job_id, status="completed", finished_at=now_iso())
            except asyncio.TimeoutError as exc:
                code = await log_error(int(job["user_id"]), f"job_timeout:{job_id}", exc)
                await update_job(job_id, status="failed", error_code=code, finished_at=now_iso())
                try:
                    await bot.send_message(int(job["user_id"]), f"❌ Job `{job_id}` timed out. Error ID: `{code}`")
                except Exception:
                    pass
            except Exception as exc:
                code = await log_error(int(job["user_id"]), f"job:{job_id}", exc)
                await update_job(job_id, status="failed", error_code=code, finished_at=now_iso())
                try:
                    reason = str(exc).strip() or type(exc).__name__
                    await bot.send_message(
                        int(job["user_id"]),
                        f"❌ Job `{job_id}` failed.\nReason: {reason[:350]}\nError ID: `{code}`\n"
                        f"After fixing the issue, use `/resume {job_id}` to retry from the saved checkpoint.",
                    )
                except Exception:
                    pass
        finally:
            JOB_QUEUE.task_done()

# ---------------------------------------------------------------------------
# UI BUILDERS
# ---------------------------------------------------------------------------


def home_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📥 New Save", callback_data="home_save"), InlineKeyboardButton("📋 My Jobs", callback_data="home_jobs")],
        [InlineKeyboardButton("💎 Plans", callback_data="home_plans"), InlineKeyboardButton("📊 My Plan", callback_data="home_myplan")],
        [InlineKeyboardButton("🔐 Connect", callback_data="home_login"), InlineKeyboardButton("🚪 Logout", callback_data="home_logout")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="home_settings"), InlineKeyboardButton("🎁 Rewards", callback_data="home_rewards")],
        [InlineKeyboardButton("🆘 Support", callback_data="home_support"), InlineKeyboardButton("📖 Help", callback_data="home_help")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("👑 Admin Dashboard", callback_data="admin_home")])
    return InlineKeyboardMarkup(rows)


def language_keyboard(back_callback: str = "home_settings") -> InlineKeyboardMarkup:
    order = ["ta", "en", "hi", "te", "ml", "kn", "bn", "mr", "gu", "pa", "ur", "ar", "es", "fr", "de", "id"]
    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(order), 2):
        row = [InlineKeyboardButton(LANG_META[code], callback_data=f"lang:{code}") for code in order[i:i+2]]
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=back_callback)])
    return InlineKeyboardMarkup(rows)


def settings_keyboard(user: dict[str, Any]) -> InlineKeyboardMarkup:
    marketing = "ON" if int(user.get("marketing_opt_in") or 0) else "OFF"
    notify = "ON" if int(user.get("notify_jobs") or 0) else "OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Language", callback_data="settings_lang"), InlineKeyboardButton("🎯 Media Filter", callback_data="settings_filter")],
        [InlineKeyboardButton("🖼 Doc Thumb", callback_data="settings_doc_thumb"), InlineKeyboardButton("🎬 Video Thumb", callback_data="settings_vid_thumb")],
        [InlineKeyboardButton(f"🔔 Job Alerts: {notify}", callback_data="toggle_job_notify")],
        [InlineKeyboardButton(f"📢 Marketing: {marketing}", callback_data="toggle_marketing")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Overview", callback_data="adm_overview"), InlineKeyboardButton("💳 Payments", callback_data="adm_payments")],
        [InlineKeyboardButton("👥 Customers", callback_data="adm_customers"), InlineKeyboardButton("🆘 Support", callback_data="adm_support")],
        [InlineKeyboardButton("💰 Revenue", callback_data="adm_revenue"), InlineKeyboardButton("📢 Marketing", callback_data="adm_marketing")],
        [InlineKeyboardButton("🛠 System", callback_data="adm_system"), InlineKeyboardButton("📜 Audit", callback_data="adm_audit")],
        [InlineKeyboardButton("⬅️ User Home", callback_data="home")],
    ])


async def maintenance_blocked(user_id: int) -> Optional[str]:
    if await has_admin_role(user_id, *ADMIN_ROLES):
        return None
    if await get_setting("maintenance_mode", "0") == "1":
        return await get_setting("maintenance_message", "🛠 Maintenance in progress.")
    return None


async def render_home(user_id: int, message: Message, edit: bool = False, display_name: str = "User") -> None:
    user = await get_user(user_id, display_name)
    lang = str(user.get("lang") or "ta")
    t = LANG.get(lang, LANG["en"])
    expiry = human_dt(user.get("plan_expires_at"))
    qcount = await user_queue_count(user_id)
    session_ok = bool(decrypt_text(user.get("encrypted_session")))
    text = t["welcome"].format(
        name=display_name,
        app=APP_NAME,
        plan=user.get("plan", "Free"),
        expiry=expiry,
        session=t["connected"] if session_ok else t["not_connected"],
        filter=str(user.get("file_filter") or "all").upper(),
        queue=qcount,
    )
    markup = home_keyboard(bool(await get_admin_role(user_id)))
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_plans(message: Message, edit: bool = False) -> None:
    text = (
        "💎 **PLANS & PRICING**\n\n"
        "⚡ **Basic** — 10 jobs/day\nWeekly ₹30 | Monthly ₹100 | Yearly ₹840\n\n"
        "🥈 **Standard** — 50 jobs/day + batch\nWeekly ₹50 | Monthly ₹180 | Yearly ₹1500\n\n"
        "🥇 **Premium** — 100 jobs/day + priority batch\nWeekly ₹80 | Monthly ₹280 | Yearly ₹2350\n\n"
        "🔷 **Ultimate** — Unlimited usage + highest priority + full accessible-channel clone/transfer\n"
        "Weekly ₹130 | Monthly ₹500 | Yearly ₹4200\n\n"
        "✅ Every paid plan has a real expiry date."
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Basic", callback_data="plan:Basic"), InlineKeyboardButton("🥈 Standard", callback_data="plan:Standard")],
        [InlineKeyboardButton("🥇 Premium", callback_data="plan:Premium"), InlineKeyboardButton("🔷 Ultimate", callback_data="plan:Ultimate")],
        [InlineKeyboardButton("🎁 24h Free Trial", callback_data="trial_activate")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_myplan(user_id: int, message: Message, edit: bool = False) -> None:
    user = await get_user(user_id)
    remaining = await remaining_usage(user)
    text = (
        "📊 **MY SUBSCRIPTION**\n\n"
        f"🏷 Plan: **{user.get('plan', 'Free')}**\n"
        f"⏳ Expiry: **{human_dt(user.get('plan_expires_at'))}**\n"
        f"📥 Used today: **{user.get('daily_used', 0)}**\n"
        f"🎯 Remaining: **{remaining}**\n"
        f"🎁 Bonus credits: **{user.get('bonus_credits', 0)}**\n"
        f"🧪 Trial used: **{'Yes' if user.get('trial_used') else 'No'}**"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬆️ Upgrade / Renew", callback_data="home_plans")],
        [InlineKeyboardButton("💳 Payment History", callback_data="payment_history")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_jobs(user_id: int, message: Message, edit: bool = False) -> None:
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT job_id,job_type,status,progress_done,progress_total,success_count,failed_count,"
            "skipped_count,retry_count,checkpoint_id,created_at,finished_at,error_code "
            "FROM jobs WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (user_id,),
        )
        rows = await cur.fetchall()

    status_icon = {
        "queued": "🕒", "running": "▶️", "paused": "⏸", "completed": "✅",
        "failed": "❌", "cancelled": "🚫",
    }
    controls: list[list[InlineKeyboardButton]] = []
    if not rows:
        text = "📋 **MY JOBS**\n\nNo jobs yet. Send a Telegram post link, use `/batch`, or use `/clone`."
    else:
        lines = ["📋 **MY JOB HISTORY**", ""]
        for r in rows:
            total = int(r["progress_total"] or 0)
            done = int(r["progress_done"] or 0)
            progress = f"{done}/{total}" if total else str(done)
            icon = status_icon.get(str(r["status"]), "•")
            lines.append(
                f"{icon} `{r['job_id']}` • **{r['job_type']}** • {r['status']}\n"
                f"   {progress} • ✅{r['success_count']} ⏭{r['skipped_count']} ❌{r['failed_count']} • retry {r['retry_count']}"
            )
            if r["status"] in {"queued", "running"}:
                controls.append([
                    InlineKeyboardButton(f"⏸ {r['job_id'][-6:]}", callback_data=f"job:pause:{r['job_id']}"),
                    InlineKeyboardButton("🚫 Cancel", callback_data=f"job:cancel:{r['job_id']}"),
                ])
            elif r["status"] == "paused":
                controls.append([
                    InlineKeyboardButton(f"▶️ {r['job_id'][-6:]}", callback_data=f"job:resume:{r['job_id']}"),
                    InlineKeyboardButton("🚫 Cancel", callback_data=f"job:cancel:{r['job_id']}"),
                ])
            elif r["status"] == "failed":
                controls.append([
                    InlineKeyboardButton(f"🔁 Retry {r['job_id'][-6:]}", callback_data=f"job:resume:{r['job_id']}")
                ])
        text = "\n".join(lines)

    markup_rows = controls[:4]
    markup_rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="home_jobs")])
    markup_rows.append([InlineKeyboardButton("⬅️ Home", callback_data="home")])
    markup = InlineKeyboardMarkup(markup_rows)
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_settings(user_id: int, message: Message, edit: bool = False) -> None:
    user = await get_user(user_id)
    text = (
        "⚙️ **SETTINGS**\n\n"
        f"🌐 Language: `{user.get('lang', 'ta')}`\n"
        f"🎯 Media filter: `{str(user.get('file_filter') or 'all').upper()}`\n"
        f"🖼 Document thumb: {'✅' if get_valid_thumb(user.get('doc_thumb')) else '❌'}\n"
        f"🎬 Video thumb: {'✅' if get_valid_thumb(user.get('vid_thumb')) else '❌'}\n"
        f"📝 Custom caption: {'✅' if user.get('custom_caption') else '❌'}"
    )
    if edit:
        try:
            await message.edit_text(text, reply_markup=settings_keyboard(user))
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=settings_keyboard(user))


async def render_admin(message: Message, edit: bool = False) -> None:
    text = "👑 **ADMIN BUSINESS DASHBOARD**\n\nPayments • Customers • Support • Revenue • Marketing • System • Audit"
    if edit:
        try:
            await message.edit_text(text, reply_markup=admin_keyboard())
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=admin_keyboard())

# ---------------------------------------------------------------------------
# COMMAND HANDLERS
# ---------------------------------------------------------------------------


@bot.on_message(filters.command("start") & filters.private)
async def start_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user_id = msg.from_user.id
    if rate_limited(user_id):
        await msg.reply_text("⏳ Too many requests. Please retry in a few seconds.")
        return
    user = await get_user(user_id, msg.from_user.first_name or "User", msg.from_user.username, msg.from_user.language_code)
    if user.get("is_banned"):
        await msg.reply_text("⛔ Your access to this bot has been disabled. Contact support if you believe this is an error.")
        return
    args = msg.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("ref_") and not user.get("referred_by"):
        raw = args[1][4:]
        if raw.isdigit() and int(raw) != user_id:
            ref_id = int(raw)
            try:
                ref_user = await get_user(ref_id)
                await update_user(user_id, referred_by=ref_id)
                async with DB_LOCK:
                    async with db_conn() as db:
                        await db.execute("UPDATE users SET ref_count=ref_count+1, bonus_credits=bonus_credits+2 WHERE user_id=?", (ref_id,))
                        await db.commit()
                await audit(user_id, "referral_join", ref_id)
                try:
                    await bot.send_message(ref_id, "🎉 New referral! +2 bonus credits added.")
                except Exception:
                    pass
            except Exception:
                pass
    await render_home(user_id, msg, display_name=msg.from_user.first_name or "User")


@bot.on_message(filters.command(["help", "manual"]) & filters.private)
async def help_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    await msg.reply_text(manual_for(str(user.get("lang") or "en")))


@bot.on_message(filters.command("login") & filters.private)
async def login_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    block = await maintenance_blocked(msg.from_user.id)
    if block:
        await msg.reply_text(block)
        return
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Session String", callback_data="login:session"), InlineKeyboardButton("📲 Phone + OTP", callback_data="login:phone")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    await msg.reply_text(
        "🔐 **Connect your Telegram account**\n\n"
        "Your session is encrypted before storage. For best security, use a dedicated SESSION_ENCRYPTION_KEY on the server.\n\n"
        "Choose a method:",
        reply_markup=markup,
    )


@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    await update_user(msg.from_user.id, encrypted_session=None, session=None)
    state = LOGIN_STATES.pop(msg.from_user.id, None)
    cli = state.get("client") if state else None
    if cli:
        try:
            if cli.is_connected:
                await cli.disconnect()
        except Exception:
            pass
    await audit(msg.from_user.id, "logout")
    await msg.reply_text("🚪 Account disconnected and stored session removed.")


@bot.on_message(filters.command("plans") & filters.private)
async def plans_handler(_: Client, msg: Message) -> None:
    await render_plans(msg)


@bot.on_message(filters.command("myplan") & filters.private)
async def myplan_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await render_myplan(msg.from_user.id, msg)


@bot.on_message(filters.command("trial") & filters.private)
async def trial_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    if user.get("trial_used"):
        await msg.reply_text("🎁 Your one-time free trial has already been used.")
        return
    if user.get("plan") not in {"Free"}:
        await msg.reply_text("You already have an active plan. Use the trial later only if eligible.")
        return
    expiry = now_dt() + timedelta(hours=24)
    await update_user(
        msg.from_user.id,
        plan="Trial",
        daily_limit=TRIAL_DAILY_LIMIT,
        daily_used=0,
        trial_used=1,
        plan_started_at=now_iso(),
        plan_expires_at=expiry.isoformat(),
    )
    await audit(msg.from_user.id, "trial_activated", msg.from_user.id)
    await msg.reply_text(f"🎁 **24-hour Trial activated**\nExpires: **{human_dt(expiry.isoformat())}**")


@bot.on_message(filters.command("redeem") & filters.private)
async def redeem_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.reply_text("Usage: `/redeem YOUR_CODE`")
        return
    ok, text = await redeem_promo(msg.from_user.id, args[1])
    await msg.reply_text(("✅ " if ok else "❌ ") + text)


@bot.on_message(filters.command("paymenthistory") & filters.private)
async def payment_history_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT invoice_id, plan, duration, amount, status, created_at FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (msg.from_user.id,),
        )
        rows = await cur.fetchall()
    if not rows:
        await msg.reply_text("💳 No payment history yet.")
        return
    lines = ["💳 **PAYMENT HISTORY**", ""]
    for r in rows:
        lines.append(f"`{r['invoice_id']}` • {r['plan']} {r['duration']} • ₹{r['amount']} • **{r['status']}**")
    await msg.reply_text("\n".join(lines))


@bot.on_message(filters.command(["jobs", "history"]) & filters.private)
async def jobs_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await render_jobs(msg.from_user.id, msg)


@bot.on_message(filters.command("job") & filters.private)
async def job_details_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply_text("Usage: `/job JOB-xxxxxx-xxxxxx`")
        return
    job_id = parts[1].strip().upper()
    job = await get_job(job_id)
    if not job or (int(job["user_id"]) != msg.from_user.id and not is_admin_id(msg.from_user.id)):
        await msg.reply_text("❌ Job not found.")
        return
    payload = json.loads(job.get("payload_json") or "{}") or {}
    text = (
        f"🧾 **JOB DETAILS**\n\n"
        f"ID: `{job_id}`\nType: **{job['job_type']}**\nStatus: **{job['status']}**\n"
        f"Progress: **{job.get('progress_done', 0)}/{job.get('progress_total', 0) or '?'}**\n"
        f"✅ Success: **{job.get('success_count', 0)}**\n"
        f"⏭ Skipped: **{job.get('skipped_count', 0)}**\n"
        f"❌ Failed: **{job.get('failed_count', 0)}**\n"
        f"🔁 Retries: **{job.get('retry_count', 0)}**\n"
        f"📍 Checkpoint: **{job.get('checkpoint_id', 0)}**\n"
        f"Created: **{human_dt(job.get('created_at'))}**\n"
        f"Updated: **{human_dt(job.get('updated_at'))}**\n"
        f"Error ID: `{job.get('error_code') or '-'}`"
    )
    rows: list[list[InlineKeyboardButton]] = []
    if job["status"] in {"queued", "running"}:
        rows.append([
            InlineKeyboardButton("⏸ Pause", callback_data=f"job:pause:{job_id}"),
            InlineKeyboardButton("🚫 Cancel", callback_data=f"job:cancel:{job_id}"),
        ])
    elif job["status"] == "paused":
        rows.append([
            InlineKeyboardButton("▶️ Resume", callback_data=f"job:resume:{job_id}"),
            InlineKeyboardButton("🚫 Cancel", callback_data=f"job:cancel:{job_id}"),
        ])
    elif job["status"] == "failed":
        rows.append([InlineKeyboardButton("🔁 Retry from checkpoint", callback_data=f"job:resume:{job_id}")])
    rows.append([InlineKeyboardButton("📋 My Jobs", callback_data="home_jobs")])
    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))


@bot.on_message(filters.command("pause") & filters.private)
async def pause_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply_text("Usage: `/pause JOB-xxxxxx-xxxxxx`")
        return
    job_id = parts[1].strip().upper()
    ok = await pause_job(job_id, msg.from_user.id)
    await msg.reply_text(
        f"⏸ `{job_id}` pause requested. The current Telegram transfer may finish before it pauses."
        if ok else "❌ Job not found or cannot be paused."
    )


@bot.on_message(filters.command("resume") & filters.private)
async def resume_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply_text("Usage: `/resume JOB-xxxxxx-xxxxxx`")
        return
    job_id = parts[1].strip().upper()
    ok = await resume_job(job_id, msg.from_user.id)
    await msg.reply_text(f"▶️ `{job_id}` re-queued from its saved checkpoint." if ok else "❌ Job is not paused/failed or was not found.")


@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) > 1 and args[1].strip().upper().startswith("JOB-"):
        job_id = args[1].strip().upper()
        ok = await cancel_job(job_id, msg.from_user.id)
        await msg.reply_text(f"🚫 `{job_id}` cancelled." if ok else "❌ Job not found or cannot be cancelled.")
        return
    state = LOGIN_STATES.pop(msg.from_user.id, None)
    if state and state.get("client"):
        try:
            if state["client"].is_connected:
                await state["client"].disconnect()
        except Exception:
            pass
    await msg.reply_text("✅ Current input flow cancelled. To cancel a job: `/cancel JOB-xxxxxx-xxxxxx`")


@bot.on_message(filters.command("language") & filters.private)
async def language_handler(_: Client, msg: Message) -> None:
    await msg.reply_text(
        "🌐 **Choose your language**\n\n16 built-in UI languages are available. Unsupported Telegram locale codes safely fall back to English.",
        reply_markup=language_keyboard("home"),
    )


@bot.on_message(filters.command("clonehelp") & filters.private)
async def clone_help_handler(_: Client, msg: Message) -> None:
    await msg.reply_text(
        "🧬 **ADVANCED CLONE GUIDE**\n\n"
        "Full accessible history:\n`/clone @source @destination all`\n\n"
        "Limited clone:\n`/clone @source @destination 1000`\n\n"
        "Filters:\n"
        "`--type=all|video|audio|doc|photo|text`\n"
        "`--caption=keep|remove|custom`\n"
        "`--min=message_id` • `--max=message_id`\n"
        "`--duplicates=allow` (default is skip duplicates)\n"
        "`--text=off`\n\n"
        "Example:\n`/clone @source @backup all --type=video --caption=keep`\n\n"
        "Source must be legitimately readable by the connected account; destination must allow posting. Telegram protected/access controls are not bypassed."
    )


@bot.on_message(filters.command("settings") & filters.private)
async def settings_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await render_settings(msg.from_user.id, msg)


@bot.on_message(filters.command("setcaption") & filters.private)
async def setcaption_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    await clear_non_auth_input_state(msg.from_user.id)
    caption = msg.text.partition(" ")[2].strip()
    if not caption:
        await msg.reply_text("Usage: `/setcaption Your caption`")
        return
    await update_user(msg.from_user.id, custom_caption=caption[:1024])
    await msg.reply_text("✅ Custom caption saved.")


@bot.on_message(filters.command("delcaption") & filters.private)
async def delcaption_handler(_: Client, msg: Message) -> None:
    if msg.from_user:
        await update_user(msg.from_user.id, custom_caption=None)
        await msg.reply_text("🗑 Custom caption removed.")


@bot.on_message(filters.command("setthumb") & filters.private)
async def setthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with `/setthumb`.")
        return
    path = THUMB_DIR / f"doc_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=str(path))
    await update_user(msg.from_user.id, doc_thumb=str(path))
    await msg.reply_text("✅ Document/audio thumbnail saved.")


@bot.on_message(filters.command("setvthumb") & filters.private)
async def setvthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with `/setvthumb`.")
        return
    path = THUMB_DIR / f"video_{msg.from_user.id}.jpg"
    await msg.reply_to_message.download(file_name=str(path))
    await update_user(msg.from_user.id, vid_thumb=str(path))
    await msg.reply_text("✅ Video thumbnail saved.")


@bot.on_message(filters.command("delthumb") & filters.private)
async def delthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    path = user.get("doc_thumb")
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    await update_user(msg.from_user.id, doc_thumb=None)
    await msg.reply_text("🗑 Document/audio thumbnail removed.")


@bot.on_message(filters.command("delvthumb") & filters.private)
async def delvthumb_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user = await get_user(msg.from_user.id)
    path = user.get("vid_thumb")
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    await update_user(msg.from_user.id, vid_thumb=None)
    await msg.reply_text("🗑 Video thumbnail removed.")


@bot.on_message(filters.command("batch") & filters.private)
async def batch_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    await clear_non_auth_input_state(msg.from_user.id)
    block = await maintenance_blocked(msg.from_user.id)
    if block:
        await msg.reply_text(block)
        return
    user = await get_user(msg.from_user.id)
    if user.get("plan") not in {"Standard", "Premium", "Ultimate"} and not is_admin_id(msg.from_user.id):
        await msg.reply_text("🔒 Batch is available on Standard, Premium and Ultimate plans.")
        return
    if not decrypt_text(user.get("encrypted_session")):
        await msg.reply_text("🔐 Connect your Telegram account first with `/login`.")
        return

    per_job_limit = batch_limit_for(user)
    links = re.findall(r"https?://t\.me/(?:c/)?[A-Za-z0-9_]+/[0-9]+", msg.text or "")

    # /batch <first-link> <last-link>
    if len(links) >= 2:
        a = parse_tg_link(links[0])
        b = parse_tg_link(links[1])
        if not a or not b or a[0] != b[0]:
            await msg.reply_text("❌ Both batch links must be from the same source chat.")
            return
        start_id, end_id = sorted((a[1], b[1]))
        count = end_id - start_id + 1
        if per_job_limit and count > per_job_limit:
            await msg.reply_text(f"❌ Your plan allows up to {per_job_limit} items in one batch job.")
            return
        job_id = await enqueue_job(msg.from_user.id, "batch", a[0], start_id, end_id)
        await msg.reply_text(
            f"✅ Batch queued: `{job_id}`\nRange: `#{start_id}` → `#{end_id}`\n"
            "Source only needs legitimate read access; owner/admin is not required."
        )
        return

    # /batch <first-link> <count> OR /batch <first-link> then ask count
    if len(links) == 1:
        a = parse_tg_link(links[0])
        if not a:
            await msg.reply_text("❌ Invalid Telegram post link.")
            return
        remainder = (msg.text or "").replace(links[0], " ")
        nums = [int(x) for x in re.findall(r"(?<![0-9])[0-9]{1,9}(?![0-9])", remainder)]
        count = nums[-1] if nums else None
        if count is not None:
            if count < 1 or (per_job_limit and count > per_job_limit):
                label = f"1–{per_job_limit}" if per_job_limit else "a positive number"
                await msg.reply_text(f"❌ Count must be {label}.")
                return
            end_id = a[1] + count - 1
            job_id = await enqueue_job(msg.from_user.id, "batch", a[0], a[1], end_id)
            await msg.reply_text(f"✅ Batch queued: `{job_id}`\nRange: `#{a[1]}` → `#{end_id}`")
            return
        LOGIN_STATES[msg.from_user.id] = {
            "step": "BATCH_COUNT",
            "chat_raw": a[0],
            "start_id": a[1],
            "batch_limit": per_job_limit,
            "updated_at": time.time(),
        }
        label = f"1–{per_job_limit}" if per_job_limit else "any positive count"
        await msg.reply_text(f"✅ Start saved: `#{a[1]}`\nNow send item count ({label}).")
        return

    LOGIN_STATES[msg.from_user.id] = {"step": "BATCH_START", "updated_at": time.time()}
    await msg.reply_text(
        "📦 Send the first Telegram post link.\n"
        "Or use: `/batch <first-link> <count>`\n"
        "Or: `/batch <first-link> <last-link>`\n"
        "Source can be a public/private chat your connected account can legitimately read.\n"
        "Use `/cancel` to exit."
    )


@bot.on_message(filters.command(["transfer", "clone"]) & filters.private)
async def transfer_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    await clear_non_auth_input_state(msg.from_user.id)
    user = await get_user(msg.from_user.id)
    if user.get("plan") != "Ultimate" and not is_admin_id(msg.from_user.id):
        await msg.reply_text("🔒 Full channel clone/transfer is available on Ultimate plan.")
        return
    if not decrypt_text(user.get("encrypted_session")):
        await msg.reply_text("🔐 Connect your Telegram account first with `/login`.")
        return

    parts = (msg.text or "").split()
    if len(parts) < 2:
        await msg.reply_text(
            "Usage: `/clone <source> <destination> [all|limit] [options]`\n"
            "Example full: `/clone @source @backup all`\n"
            "Example filtered: `/clone @source @backup 1000 --type=video --caption=keep`\n\n"
            "Source: connected account needs legitimate read access/member access.\n"
            "Destination: connected account needs permission to post.\n"
            "Use `/clonehelp` for all filters."
        )
        return

    src = chat_ref_from_input(parts[1])
    if len(parts) < 3:
        LOGIN_STATES[msg.from_user.id] = {
            "step": "TRANSFER_DEST",
            "source": src,
            "updated_at": time.time(),
        }
        await msg.reply_text(
            "📤 Source saved. Now send the destination chat as `@username`, numeric chat ID, or t.me link.\n"
            "The source only needs legitimate read access. The destination must allow your connected account to post."
        )
        return

    dest = chat_ref_from_input(parts[2])
    limit = 0  # 0 = full accessible history
    option_start = 3
    if len(parts) >= 4 and not parts[3].startswith("--"):
        raw_limit = parts[3].lower()
        if raw_limit not in {"all", "full", "unlimited", "0"}:
            if not raw_limit.isdigit() or int(raw_limit) < 1:
                await msg.reply_text("❌ Limit must be a positive number or `all`.")
                return
            limit = int(raw_limit)
        option_start = 4

    payload: dict[str, Any] = {
        "limit": limit,
        "media_filter": "all",
        "caption_mode": "keep",
        "skip_duplicates": True,
        "include_text": True,
        "min_id": 0,
        "max_id": 0,
    }
    for token in parts[option_start:]:
        if token.startswith("--type="):
            val = token.split("=", 1)[1].lower()
            if val in {"all", "video", "audio", "doc", "document", "photo", "text"}:
                payload["media_filter"] = "doc" if val == "document" else val
        elif token.startswith("--caption="):
            val = token.split("=", 1)[1].lower()
            if val in {"keep", "remove", "custom"}:
                payload["caption_mode"] = val
        elif token.startswith("--min=") and token.split("=", 1)[1].isdigit():
            payload["min_id"] = int(token.split("=", 1)[1])
        elif token.startswith("--max=") and token.split("=", 1)[1].isdigit():
            payload["max_id"] = int(token.split("=", 1)[1])
        elif token == "--duplicates=allow":
            payload["skip_duplicates"] = False
        elif token == "--text=off":
            payload["include_text"] = False

    job_id = await enqueue_job(msg.from_user.id, "transfer", src, destination=dest, payload=payload)
    mode = "FULL CHANNEL" if limit == 0 else f"Last {limit} messages"
    await msg.reply_text(
        f"✅ Clone queued: `{job_id}`\n"
        f"Mode: **{mode}**\nFilter: `{payload['media_filter']}` • Caption: `{payload['caption_mode']}`\n"
        f"Controls: `/pause {job_id}` • `/resume {job_id}` • `/cancel {job_id}`"
    )


@bot.on_message(filters.command("support") & filters.private)
async def support_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Payment", callback_data="support:payment"), InlineKeyboardButton("🔐 Login", callback_data="support:login")],
        [InlineKeyboardButton("📥 Job", callback_data="support:job"), InlineKeyboardButton("💎 Subscription", callback_data="support:subscription")],
        [InlineKeyboardButton("🐞 Bug", callback_data="support:bug"), InlineKeyboardButton("💡 Feature", callback_data="support:feature")],
        [InlineKeyboardButton("⬅️ Home", callback_data="home")],
    ])
    await msg.reply_text("🆘 Choose a support category:", reply_markup=markup)

# ---------------------------------------------------------------------------
# ADMIN COMMANDS
# ---------------------------------------------------------------------------


@bot.on_message(filters.command(["admin", "adminsetting"]) & filters.private)
async def admin_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await get_admin_role(msg.from_user.id):
        return
    await render_admin(msg)


@bot.on_message(filters.command("stats") & filters.private)
async def stats_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager", "developer", "marketing", "payments", "support"):
        return
    async with db_conn() as db:
        total = int((await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0])
        paid = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE plan NOT IN ('Free','Trial')")).fetchone())[0])
        active = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE last_seen >= ?", ((now_dt()-timedelta(days=7)).isoformat(),))).fetchone())[0])
        open_tickets = int((await (await db.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'")).fetchone())[0])
        pending = int((await (await db.execute("SELECT COUNT(*) FROM payments WHERE status='pending'")).fetchone())[0])
        queued = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running')")).fetchone())[0])
    await msg.reply_text(
        f"📊 **BOT OVERVIEW**\n\nUsers: `{total}`\n7-day active: `{active}`\nPaid: `{paid}`\nPending payments: `{pending}`\nOpen tickets: `{open_tickets}`\nQueued/running jobs: `{queued}`"
    )


@bot.on_message(filters.command("revenue") & filters.private)
async def revenue_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager", "payments"):
        return
    today = now_dt().date().isoformat()
    month = now_dt().strftime("%Y-%m")
    async with db_conn() as db:
        total = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved'")).fetchone())[0])
        today_rev = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved' AND substr(approved_at,1,10)=?", (today,))).fetchone())[0])
        month_rev = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved' AND substr(approved_at,1,7)=?", (month,))).fetchone())[0])
    await msg.reply_text(f"💰 **REVENUE**\n\nToday: **₹{today_rev}**\nThis month: **₹{month_rev}**\nAll time: **₹{total}**")


@bot.on_message(filters.command("user") & filters.private)
async def user_lookup_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager", "payments", "support"):
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.reply_text("Usage: `/user <telegram_user_id>`")
        return
    uid = int(args[1])
    user = await get_user(uid)
    async with db_conn() as db:
        pcount = int((await (await db.execute("SELECT COUNT(*) FROM payments WHERE user_id=? AND status='approved'", (uid,))).fetchone())[0])
        spent = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE user_id=? AND status='approved'", (uid,))).fetchone())[0])
        jcount = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE user_id=?", (uid,))).fetchone())[0])
    await msg.reply_text(
        "👤 **CUSTOMER PROFILE**\n\n"
        f"Name: {user.get('name')}\nID: `{uid}`\nUsername: @{user.get('username') or '-'}\n"
        f"Plan: **{user.get('plan')}**\nExpiry: **{human_dt(user.get('plan_expires_at'))}**\n"
        f"Paid orders: `{pcount}`\nLifetime revenue: `₹{spent}`\nJobs: `{jcount}`\n"
        f"Referrals: `{user.get('ref_count', 0)}`\nBanned: `{bool(user.get('is_banned'))}`"
    )


@bot.on_message(filters.command("ap") & filters.private)
async def admin_plan_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 3 or not args[1].isdigit():
        await msg.reply_text("Usage: `/ap <user_id> <Basic|Standard|Premium|Ultimate> [days]`")
        return
    uid = int(args[1])
    plan = args[2].capitalize()
    days = int(args[3]) if len(args) >= 4 and args[3].isdigit() else 30
    if plan not in PLAN_PRICING:
        await msg.reply_text("Invalid plan.")
        return
    expiry = await activate_plan(uid, plan, days, msg.from_user.id, "admin_manual")
    await msg.reply_text(f"✅ `{uid}` → **{plan}** until **{human_dt(expiry)}**")
    try:
        await bot.send_message(uid, f"🎉 Your **{plan}** plan is active until **{human_dt(expiry)}**.")
    except Exception:
        pass


@bot.on_message(filters.command("rp") & filters.private)
async def reset_plan_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.reply_text("Usage: `/rp <user_id>`")
        return
    uid = int(args[1])
    await get_user(uid)
    await update_user(uid, plan="Free", daily_limit=FREE_DAILY_LIMIT, daily_used=0, plan_started_at=None, plan_expires_at=None)
    await audit(msg.from_user.id, "plan_reset", uid)
    await msg.reply_text(f"✅ `{uid}` reset to Free.")


@bot.on_message(filters.command(["ban", "unban"]) & filters.private)
async def ban_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.reply_text(f"Usage: `/{msg.command[0]} <user_id>`")
        return
    uid = int(args[1])
    if uid == OWNER_ID:
        await msg.reply_text("Owner cannot be banned.")
        return
    value = 1 if msg.command[0] == "ban" else 0
    await get_user(uid)
    await update_user(uid, is_banned=value)
    await audit(msg.from_user.id, msg.command[0], uid)
    await msg.reply_text(f"✅ User `{uid}` {'banned' if value else 'unbanned'}.")


@bot.on_message(filters.command("addcredits") & filters.private)
async def addcredits_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "manager"):
        return
    args = msg.text.split()
    if len(args) < 3 or not args[1].isdigit() or not args[2].lstrip("-").isdigit():
        await msg.reply_text("Usage: `/addcredits <user_id> <amount>`")
        return
    uid, amount = int(args[1]), int(args[2])
    await get_user(uid)
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute("UPDATE users SET bonus_credits=MAX(0, bonus_credits + ?) WHERE user_id=?", (amount, uid))
            await db.commit()
    await audit(msg.from_user.id, "credits_changed", uid, {"amount": amount})
    await msg.reply_text(f"✅ Credits changed by {amount} for `{uid}`.")


@bot.on_message(filters.command("addpromo") & filters.private)
async def addpromo_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "marketing", "manager"):
        return
    args = msg.text.split()
    if len(args) < 5:
        await msg.reply_text("Usage: `/addpromo <CODE> <Plan> <Days> <Uses> [expiry_days]`\nExample: `/addpromo VIP30 Premium 30 25 14`")
        return
    code = args[1].upper()
    plan = args[2].capitalize()
    if plan not in PLAN_PRICING or not args[3].isdigit() or not args[4].isdigit():
        await msg.reply_text("Invalid plan/days/uses.")
        return
    days, uses = int(args[3]), int(args[4])
    expiry = None
    if len(args) >= 6 and args[5].isdigit():
        expiry = (now_dt() + timedelta(days=int(args[5]))).isoformat()
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO promo_codes(code,plan,days,uses_left,expires_at,max_per_user) VALUES (?,?,?,?,?,1) "
                "ON CONFLICT(code) DO UPDATE SET plan=excluded.plan, days=excluded.days, uses_left=excluded.uses_left, expires_at=excluded.expires_at",
                (code, plan, days, uses, expiry),
            )
            await db.commit()
    await audit(msg.from_user.id, "promo_created", code, {"plan": plan, "days": days, "uses": uses})
    await msg.reply_text(f"✅ Promo `{code}` created: {plan} / {days} days / {uses} uses.")


@bot.on_message(filters.command("tickets") & filters.private)
async def tickets_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "support", "manager"):
        return
    async with db_conn() as db:
        cur = await db.execute("SELECT ticket_id,user_id,category,message,created_at FROM support_tickets WHERE status='open' ORDER BY id ASC LIMIT 15")
        rows = await cur.fetchall()
    if not rows:
        await msg.reply_text("🆘 No open support tickets.")
        return
    for r in rows:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Resolve", callback_data=f"ticket:close:{r['ticket_id']}")]])
        await msg.reply_text(
            f"🆘 `{r['ticket_id']}`\nUser: `{r['user_id']}`\nCategory: **{r['category']}**\n\n{r['message'][:2500]}",
            reply_markup=markup,
        )


@bot.on_message(filters.command("broadcast") & filters.private)
async def broadcast_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "marketing", "manager"):
        return
    if not msg.reply_to_message:
        await msg.reply_text("Reply to a message with `/broadcast [all|free|paid|marketing|lang:ta]`.")
        return
    args = msg.text.split(maxsplit=1)
    segment = args[1].strip().lower() if len(args) > 1 else "all"
    query = "SELECT user_id FROM users WHERE is_banned=0"
    params: list[Any] = []
    if segment == "free":
        query += " AND plan IN ('Free','Trial')"
    elif segment == "paid":
        query += " AND plan NOT IN ('Free','Trial')"
    elif segment == "marketing":
        query += " AND marketing_opt_in=1"
    elif segment.startswith("lang:"):
        query += " AND lang=?"
        params.append(segment.split(":", 1)[1][:5])
    async with db_conn() as db:
        rows = await (await db.execute(query, params)).fetchall()
    sent = blocked = failed = 0
    status = await msg.reply_text(f"📢 Broadcasting to {len(rows)} users…")
    for idx, r in enumerate(rows, start=1):
        try:
            await msg.reply_to_message.copy(int(r["user_id"]))
            sent += 1
        except Exception as exc:
            text = str(exc).lower()
            if "blocked" in text or "deactivated" in text:
                blocked += 1
            else:
                failed += 1
        if idx % 40 == 0:
            try:
                await status.edit_text(f"📢 Broadcast progress {idx}/{len(rows)}\n✅ {sent} | 🚫 {blocked} | ❌ {failed}")
            except Exception:
                pass
        await asyncio.sleep(0.04)
    await audit(msg.from_user.id, "broadcast", segment, {"sent": sent, "blocked": blocked, "failed": failed})
    await status.edit_text(f"✅ **Broadcast complete**\nSent: {sent}\nBlocked: {blocked}\nFailed: {failed}")


@bot.on_message(filters.command("maintenance") & filters.private)
async def maintenance_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or not await has_admin_role(msg.from_user.id, "developer", "manager"):
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2 or args[1].lower() not in {"on", "off"}:
        current = await get_setting("maintenance_mode", "0")
        await msg.reply_text(f"Maintenance: {'ON' if current == '1' else 'OFF'}\nUsage: `/maintenance on|off`")
        return
    value = "1" if args[1].lower() == "on" else "0"
    await set_setting("maintenance_mode", value)
    await audit(msg.from_user.id, "maintenance", value)
    await msg.reply_text(f"🛠 Maintenance {'enabled' if value == '1' else 'disabled'}.")


@bot.on_message(filters.command("addadmin") & filters.private)
async def addadmin_handler(_: Client, msg: Message) -> None:
    if not msg.from_user or await get_admin_role(msg.from_user.id) != "owner":
        return
    args = msg.text.split()
    if len(args) < 3 or not args[1].isdigit() or args[2].lower() not in ADMIN_ROLES - {"owner"}:
        await msg.reply_text("Usage: `/addadmin <user_id> <manager|payments|support|marketing|developer>`")
        return
    uid, role = int(args[1]), args[2].lower()
    async with DB_LOCK:
        async with db_conn() as db:
            await db.execute(
                "INSERT INTO admins(user_id,role,added_by,created_at) VALUES (?,?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET role=excluded.role",
                (uid, role, msg.from_user.id, now_iso()),
            )
            await db.commit()
    await audit(msg.from_user.id, "admin_added", uid, {"role": role})
    await msg.reply_text(f"✅ `{uid}` added as **{role}**.")

# ---------------------------------------------------------------------------
# CALLBACK ROUTER
# ---------------------------------------------------------------------------


@bot.on_callback_query()
async def callback_router(_: Client, q: CallbackQuery) -> None:
    if not q.from_user or not q.message:
        return
    user_id = q.from_user.id
    data = q.data or ""
    if rate_limited(user_id):
        await safe_answer(q, "Too many requests. Try again shortly.", True)
        return

    try:
        user = await get_user(user_id, q.from_user.first_name or "User", q.from_user.username)
        if user.get("is_banned"):
            await safe_answer(q, "Access disabled.", True)
            return

        if data == "home":
            await render_home(user_id, q.message, edit=True, display_name=q.from_user.first_name or "User")

        elif data == "home_save":
            await q.message.edit_text(
                "📥 **NEW SAVE**\n\nSend a Telegram post link that your connected account is authorized to access.\n\n"
                "For permitted ranges use `/batch`."
            )

        elif data == "home_jobs":
            await render_jobs(user_id, q.message, edit=True)

        elif data in {"home_plans", "plan_back"}:
            await render_plans(q.message, edit=True)

        elif data == "home_myplan":
            await render_myplan(user_id, q.message, edit=True)

        elif data == "home_login":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ Session String", callback_data="login:session"), InlineKeyboardButton("📲 Phone + OTP", callback_data="login:phone")],
                [InlineKeyboardButton("⬅️ Home", callback_data="home")],
            ])
            await q.message.edit_text("🔐 Choose a secure account connection method:", reply_markup=markup)

        elif data == "home_logout":
            await update_user(user_id, encrypted_session=None, session=None)
            state = LOGIN_STATES.pop(user_id, None)
            cli = state.get("client") if state else None
            if cli:
                try:
                    if cli.is_connected:
                        await cli.disconnect()
                except Exception:
                    pass
            await audit(user_id, "logout")
            await q.message.edit_text("🚪 Account disconnected.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]))

        elif data == "home_settings":
            await render_settings(user_id, q.message, edit=True)

        elif data == "home_rewards":
            bot_name = BOT_USERNAME or (await bot.get_me()).username or "YourBot"
            link = f"https://t.me/{bot_name}?start=ref_{user_id}"
            text = (
                "🎁 **REWARDS & REFERRALS**\n\n"
                f"Referrals: **{user.get('ref_count', 0)}**\n"
                f"Bonus credits: **{user.get('bonus_credits', 0)}**\n\n"
                f"Your referral link:\n`{link}`\n\n"
                "Current reward: +2 bonus credits for a valid new referral."
            )
            await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]))

        elif data == "home_support":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Payment", callback_data="support:payment"), InlineKeyboardButton("🔐 Login", callback_data="support:login")],
                [InlineKeyboardButton("📥 Job", callback_data="support:job"), InlineKeyboardButton("💎 Subscription", callback_data="support:subscription")],
                [InlineKeyboardButton("🐞 Bug", callback_data="support:bug"), InlineKeyboardButton("💡 Feature", callback_data="support:feature")],
                [InlineKeyboardButton("⬅️ Home", callback_data="home")],
            ])
            await q.message.edit_text("🆘 Choose a support category:", reply_markup=markup)

        elif data == "home_help":
            await q.message.edit_text(
                manual_for(str(user.get("lang") or "en")),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]),
            )

        elif data.startswith("job:pause:"):
            job_id = data.split(":", 2)[2].upper()
            ok = await pause_job(job_id, user_id)
            await safe_answer(q, "Pause requested" if ok else "Cannot pause this job", not ok)
            await render_jobs(user_id, q.message, edit=True)

        elif data.startswith("job:resume:"):
            job_id = data.split(":", 2)[2].upper()
            ok = await resume_job(job_id, user_id)
            await safe_answer(q, "Job resumed" if ok else "Cannot resume this job", not ok)
            await render_jobs(user_id, q.message, edit=True)

        elif data.startswith("job:cancel:"):
            job_id = data.split(":", 2)[2].upper()
            ok = await cancel_job(job_id, user_id)
            await safe_answer(q, "Job cancelled" if ok else "Cannot cancel this job", not ok)
            await render_jobs(user_id, q.message, edit=True)

        elif data == "trial_activate":
            if user.get("trial_used"):
                await q.message.reply_text("🎁 Your one-time trial has already been used.")
            elif user.get("plan") != "Free":
                await q.message.reply_text("You already have an active plan.")
            else:
                expiry = now_dt() + timedelta(hours=24)
                await update_user(
                    user_id,
                    plan="Trial",
                    daily_limit=TRIAL_DAILY_LIMIT,
                    daily_used=0,
                    trial_used=1,
                    plan_started_at=now_iso(),
                    plan_expires_at=expiry.isoformat(),
                )
                await audit(user_id, "trial_activated", user_id)
                await q.message.edit_text(
                    f"🎁 **24-hour Trial activated**\nExpires: **{human_dt(expiry.isoformat())}**",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Home", callback_data="home")]]),
                )

        elif data.startswith("plan:"):
            plan = data.split(":", 1)[1]
            if plan not in PLAN_PRICING:
                await safe_answer(q, "Invalid plan", True)
                return
            p = PLAN_PRICING[plan]
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"Weekly — ₹{p['Weekly']}", callback_data=f"buy:{plan}:Weekly")],
                [InlineKeyboardButton(f"Monthly — ₹{p['Monthly']}", callback_data=f"buy:{plan}:Monthly")],
                [InlineKeyboardButton(f"Yearly — ₹{p['Yearly']}", callback_data=f"buy:{plan}:Yearly")],
                [InlineKeyboardButton("⬅️ Back", callback_data="plan_back")],
            ])
            await q.message.edit_text(f"💎 **{plan}**\n{p['access']}\n\nChoose a duration:", reply_markup=markup)

        elif data.startswith("buy:"):
            _, plan, duration = data.split(":", 2)
            invoice = await create_invoice(user_id, plan, duration)
            upi_id = await get_setting("upi_id", UPI_ID_ENV)
            upi_name = await get_setting("upi_name", UPI_NAME_ENV)
            if not upi_id or not upi_name:
                await q.message.reply_text("⚠️ Payment configuration is not ready. Please contact support.")
                return
            payload = (
                f"upi://pay?pa={quote(upi_id)}&pn={quote(upi_name)}&am={invoice['amount']}"
                f"&cu=INR&tn={quote(invoice['invoice_id'])}"
            )
            qr = create_qr_bytes(payload)
            caption = (
                "💳 **PAYMENT INVOICE**\n\n"
                f"Invoice: `{invoice['invoice_id']}`\n"
                f"Plan: **{plan}**\nDuration: **{duration}**\nAmount: **₹{invoice['amount']}**\n"
                f"UPI ID: `{upi_id}`\nPayee: **{upi_name}**\n\n"
                "After payment, send the UTR / transaction reference in this chat.\n"
                "The selected plan, duration and amount are locked to this invoice."
            )
            LOGIN_STATES[user_id] = {
                "step": "PAYMENT_UTR",
                "invoice_id": invoice["invoice_id"],
                "updated_at": time.time(),
            }
            try:
                await q.message.reply_photo(qr, caption=caption)
            except Exception:
                await q.message.reply_text(caption)

        elif data == "payment_history":
            async with db_conn() as db:
                rows = await (await db.execute(
                    "SELECT invoice_id,plan,duration,amount,status FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 10",
                    (user_id,),
                )).fetchall()
            lines = ["💳 **PAYMENT HISTORY**", ""]
            if rows:
                for r in rows:
                    lines.append(f"`{r['invoice_id']}` • {r['plan']} {r['duration']} • ₹{r['amount']} • **{r['status']}**")
            else:
                lines.append("No payment history yet.")
            await q.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ My Plan", callback_data="home_myplan")]]))

        elif data == "login:session":
            LOGIN_STATES[user_id] = {"step": "SESSION_STRING", "updated_at": time.time()}
            await q.message.edit_text(
                "⚡ Send your **Kurigram/Pyrogram session string** now.\n\n"
                "It will be validated and encrypted before database storage.\n"
                "Use `/cancel` to stop."
            )

        elif data == "login:phone":
            LOGIN_STATES[user_id] = {"step": "PHONE", "updated_at": time.time()}
            await q.message.edit_text("📲 Send your phone number with country code, e.g. `+919876543210`.\nUse `/cancel` to stop.")

        elif data == "settings_lang":
            await q.message.edit_text(
                "🌐 **Choose language**\n\n16 built-in UI languages. Unsupported Telegram locale codes fall back to English.",
                reply_markup=language_keyboard("home_settings"),
            )

        elif data.startswith("lang:"):
            lang = data.split(":", 1)[1]
            if lang in VALID_LANGS:
                await update_user(user_id, lang=lang)
            await render_settings(user_id, q.message, edit=True)

        elif data == "settings_filter":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("All", callback_data="filter:all"), InlineKeyboardButton("Video", callback_data="filter:video")],
                [InlineKeyboardButton("Audio", callback_data="filter:audio"), InlineKeyboardButton("Document", callback_data="filter:doc")],
                [InlineKeyboardButton("Photo", callback_data="filter:photo")],
                [InlineKeyboardButton("⬅️ Settings", callback_data="home_settings")],
            ])
            await q.message.edit_text("🎯 Choose media filter:", reply_markup=markup)

        elif data.startswith("filter:"):
            flt = data.split(":", 1)[1]
            if flt in VALID_FILTERS:
                await update_user(user_id, file_filter=flt)
            await render_settings(user_id, q.message, edit=True)

        elif data == "toggle_job_notify":
            new = 0 if int(user.get("notify_jobs") or 0) else 1
            await update_user(user_id, notify_jobs=new)
            await render_settings(user_id, q.message, edit=True)

        elif data == "toggle_marketing":
            new = 0 if int(user.get("marketing_opt_in") or 0) else 1
            await update_user(user_id, marketing_opt_in=new)
            await render_settings(user_id, q.message, edit=True)

        elif data in {"settings_doc_thumb", "settings_vid_thumb"}:
            cmd = "/setthumb" if data.endswith("doc_thumb") else "/setvthumb"
            await q.message.reply_text(f"Reply to a photo with `{cmd}`. Use `/delthumb` or `/delvthumb` to remove it.")

        elif data.startswith("support:"):
            category = data.split(":", 1)[1]
            LOGIN_STATES[user_id] = {"step": "SUPPORT_MESSAGE", "category": category, "updated_at": time.time()}
            await q.message.edit_text(f"🆘 Category: **{category.title()}**\n\nDescribe the issue in one message. Use `/cancel` to stop.")

        elif data == "admin_home":
            if await get_admin_role(user_id):
                await render_admin(q.message, edit=True)

        elif data == "adm_overview":
            if not await get_admin_role(user_id):
                return
            async with db_conn() as db:
                total = int((await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0])
                paid = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE plan NOT IN ('Free','Trial')")).fetchone())[0])
                pending = int((await (await db.execute("SELECT COUNT(*) FROM payments WHERE status='pending'")).fetchone())[0])
                open_t = int((await (await db.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'")).fetchone())[0])
                jobs = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running')")).fetchone())[0])
                paused = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status='paused'")).fetchone())[0])
                failed_jobs = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status='failed'")).fetchone())[0])
                clones = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE job_type='transfer'")).fetchone())[0])
                ultimate = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE plan='Ultimate'")).fetchone())[0])
            await q.message.edit_text(
                f"📊 **BUSINESS OVERVIEW**\n\nUsers: `{total}` • Paid: `{paid}` • Ultimate: `{ultimate}`\n"
                f"Pending payments: `{pending}` • Open tickets: `{open_t}`\n"
                f"Active jobs: `{jobs}` • Paused: `{paused}` • Failed: `{failed_jobs}`\n"
                f"Clone jobs (all time): `{clones}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_payments":
            if not await has_admin_role(user_id, "payments", "manager"):
                await safe_answer(q, "Not authorized", True)
                return
            async with db_conn() as db:
                rows = await (await db.execute(
                    "SELECT invoice_id,user_id,plan,duration,amount,utr FROM payments WHERE status='pending' ORDER BY id ASC LIMIT 10"
                )).fetchall()
            if not rows:
                await q.message.edit_text("💳 No pending payments.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))
            else:
                await q.message.edit_text("💳 **Pending payments are listed below.**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))
                for r in rows:
                    markup = InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅ Approve", callback_data=f"pay:ok:{r['invoice_id']}"),
                        InlineKeyboardButton("❌ Reject", callback_data=f"pay:no:{r['invoice_id']}"),
                    ]])
                    await q.message.reply_text(
                        f"💳 `{r['invoice_id']}`\nUser: `{r['user_id']}`\n{r['plan']} • {r['duration']} • ₹{r['amount']}\nUTR: `{r['utr']}`",
                        reply_markup=markup,
                    )

        elif data.startswith("pay:ok:"):
            invoice_id = data.split(":", 2)[2]
            ok, text, payment = await approve_payment(invoice_id, user_id)
            if ok and payment:
                await q.message.edit_text(f"✅ `{invoice_id}` approved for user `{payment['user_id']}`.")
                try:
                    await bot.send_message(
                        int(payment["user_id"]),
                        f"🎉 **SUBSCRIPTION ACTIVATED**\n\nInvoice: `{invoice_id}`\n"
                        f"Plan: **{payment['plan']}**\nDuration: **{payment['duration']}**\n"
                        f"Expires: **{human_dt(payment.get('plan_expires_at'))}**",
                    )
                except Exception:
                    pass
            else:
                await safe_answer(q, text, True)

        elif data.startswith("pay:no:"):
            invoice_id = data.split(":", 2)[2]
            ok, text, payment = await reject_payment(invoice_id, user_id, "Rejected by admin")
            if ok and payment:
                await q.message.edit_text(f"❌ `{invoice_id}` rejected.")
                try:
                    await bot.send_message(int(payment["user_id"]), f"❌ Payment `{invoice_id}` was not approved. Please contact support if needed.")
                except Exception:
                    pass
            else:
                await safe_answer(q, text, True)

        elif data == "adm_customers":
            if not await has_admin_role(user_id, "manager", "payments", "support"):
                return
            await q.message.edit_text(
                "👥 **CUSTOMER CRM**\n\nUse:\n`/user <id>` — profile\n`/ap <id> <plan> [days]` — activate\n`/rp <id>` — reset\n`/addcredits <id> <amount>`\n`/ban <id>` / `/unban <id>`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_support":
            if not await has_admin_role(user_id, "support", "manager"):
                return
            await q.message.edit_text("🆘 Use `/tickets` to open the support queue.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))

        elif data == "adm_revenue":
            if not await has_admin_role(user_id, "payments", "manager"):
                return
            month = now_dt().strftime("%Y-%m")
            async with db_conn() as db:
                total = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved'")).fetchone())[0])
                monthly = int((await (await db.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved' AND substr(approved_at,1,7)=?", (month,))).fetchone())[0])
            await q.message.edit_text(f"💰 **REVENUE**\n\nThis month: **₹{monthly}**\nAll time: **₹{total}**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))

        elif data == "adm_marketing":
            if not await has_admin_role(user_id, "marketing", "manager"):
                return
            await q.message.edit_text(
                "📢 **MARKETING**\n\n`/addpromo CODE Plan Days Uses [ExpiryDays]`\n"
                "Reply to a message: `/broadcast all|free|paid|marketing|lang:ta`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_system":
            if not await has_admin_role(user_id, "developer", "manager"):
                return
            maintenance = await get_setting("maintenance_mode", "0")
            uptime = int(time.time() - START_TS)
            disk = shutil.disk_usage(DATA_DIR)
            async with db_conn() as db:
                running = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'")).fetchone())[0])
                paused = int((await (await db.execute("SELECT COUNT(*) FROM jobs WHERE status='paused'")).fetchone())[0])
                errors_24h = int((await (await db.execute("SELECT COUNT(*) FROM errors WHERE created_at >= ?", ((now_dt()-timedelta(days=1)).isoformat(),))).fetchone())[0])
            await q.message.edit_text(
                f"🛠 **SYSTEM HEALTH**\n\nMaintenance: **{'ON' if maintenance == '1' else 'OFF'}**\n"
                f"Workers: `{WORKER_COUNT}` • Queue: `{JOB_QUEUE.qsize()}` • Running: `{running}` • Paused: `{paused}`\n"
                f"Disk: `{disk.used/1073741824:.2f} / {disk.total/1073741824:.2f} GB`\n"
                f"Errors (24h): `{errors_24h}`\n"
                f"Uptime: `{uptime//3600}h {(uptime%3600)//60}m`\n\n"
                "Use `/maintenance on|off`.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]),
            )

        elif data == "adm_audit":
            if not await has_admin_role(user_id, "developer", "manager"):
                return
            async with db_conn() as db:
                rows = await (await db.execute("SELECT actor_id,action,target_id,created_at FROM audit_logs ORDER BY id DESC LIMIT 12")).fetchall()
            lines = ["📜 **RECENT AUDIT LOG**", ""]
            for r in rows:
                lines.append(f"`{str(r['created_at'])[:16]}` • `{r['actor_id']}` • **{r['action']}** • `{r['target_id'] or '-'}`")
            await q.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin_home")]]))

        elif data.startswith("ticket:close:"):
            ticket_id = data.split(":", 2)[2]
            if await close_ticket(ticket_id, user_id, "Resolved from admin panel"):
                await q.message.edit_text(f"✅ `{ticket_id}` resolved.")
            else:
                await safe_answer(q, "Unable to resolve ticket", True)

        await safe_answer(q)

    except Exception as exc:
        code = await log_error(user_id, f"callback:{data}", exc)
        await safe_answer(q, f"Error ID: {code}", True)

# ---------------------------------------------------------------------------
# TEXT / STATE HANDLER
# ---------------------------------------------------------------------------


@bot.on_message(filters.text & filters.private)
async def text_handler(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    user_id = msg.from_user.id
    text = (msg.text or "").strip()
    if text.startswith("/"):
        return
    if rate_limited(user_id):
        await msg.reply_text("⏳ Too many requests. Please retry shortly.")
        return
    user = await get_user(user_id, msg.from_user.first_name or "User", msg.from_user.username)
    if user.get("is_banned"):
        return

    # Expire stale interactive states after 15 minutes.
    state = LOGIN_STATES.get(user_id)
    if state and time.time() - float(state.get("updated_at", time.time())) > 900:
        cli = state.get("client")
        if cli:
            try:
                if cli.is_connected:
                    await cli.disconnect()
            except Exception:
                pass
        LOGIN_STATES.pop(user_id, None)
        state = None

    if state:
        step = state.get("step")
        state["updated_at"] = time.time()

        if step == "SESSION_STRING":
            status = await msg.reply_text("🔎 Validating session…")
            test_client: Optional[Client] = None
            try:
                test_client = Client(
                    f"validate_{user_id}_{secrets.token_hex(2)}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=text,
                    in_memory=True,
                    no_updates=True,
                )
                await test_client.connect()
                me = await test_client.get_me()
                await update_user(user_id, encrypted_session=encrypt_text(text), session=None)
                LOGIN_STATES.pop(user_id, None)
                await audit(user_id, "login_session", me.id)
                await status.edit_text(f"✅ **Account connected**\n{me.first_name} (`{me.id}`)\nSession encrypted at rest.")
            except Exception as exc:
                code = await log_error(user_id, "login_session", exc)
                await status.edit_text(f"❌ Session validation failed. Error ID: `{code}`")
            finally:
                if test_client and test_client.is_connected:
                    await test_client.disconnect()
            return

        if step == "PHONE":
            phone = re.sub(r"[\s-]+", "", text)
            if not re.fullmatch(r"\+[1-9][0-9]{7,14}", phone):
                await msg.reply_text("❌ Enter a valid international phone number, e.g. `+919876543210`.")
                return
            u_client = Client(
                f"otp_{user_id}_{secrets.token_hex(2)}",
                api_id=API_ID,
                api_hash=API_HASH,
                in_memory=True,
                no_updates=True,
            )
            try:
                await u_client.connect()
                sent = await u_client.send_code(phone)
                LOGIN_STATES[user_id] = {
                    "step": "OTP",
                    "client": u_client,
                    "phone": phone,
                    "phone_code_hash": sent.phone_code_hash,
                    "updated_at": time.time(),
                }
                await msg.reply_text("📩 Send the OTP digits. Spaces are allowed. Never share OTP with anyone else.")
            except Exception as exc:
                try:
                    if u_client.is_connected:
                        await u_client.disconnect()
                except Exception:
                    pass
                LOGIN_STATES.pop(user_id, None)
                code = await log_error(user_id, "login_phone", exc)
                await msg.reply_text(f"❌ Could not send OTP. Error ID: `{code}`")
            return

        if step == "OTP":
            otp = re.sub(r"\D", "", text)
            u_client: Client = state["client"]
            try:
                await u_client.sign_in(state["phone"], state["phone_code_hash"], otp)
                session_string = await u_client.export_session_string()
                me = await u_client.get_me()
                await update_user(user_id, encrypted_session=encrypt_text(session_string), session=None)
                LOGIN_STATES.pop(user_id, None)
                await audit(user_id, "login_otp", me.id)
                await msg.reply_text(f"✅ **Account connected:** {me.first_name}\nSession encrypted at rest.")
                if u_client.is_connected:
                    await u_client.disconnect()
            except SessionPasswordNeeded:
                LOGIN_STATES[user_id]["step"] = "2FA"
                await msg.reply_text("🔐 2-step verification is enabled. Send your Telegram 2FA password now.")
            except (PhoneCodeInvalid, PhoneCodeExpired) as exc:
                await msg.reply_text(f"❌ OTP failed: {type(exc).__name__}. Use `/cancel` and `/login` to retry.")
            except Exception as exc:
                code = await log_error(user_id, "login_otp", exc)
                await msg.reply_text(f"❌ Login failed. Error ID: `{code}`")
            return

        if step == "2FA":
            u_client: Client = state["client"]
            try:
                await u_client.check_password(text)
                session_string = await u_client.export_session_string()
                me = await u_client.get_me()
                await update_user(user_id, encrypted_session=encrypt_text(session_string), session=None)
                LOGIN_STATES.pop(user_id, None)
                await audit(user_id, "login_2fa", me.id)
                await msg.reply_text(f"✅ **Account connected:** {me.first_name}\nSession encrypted at rest.")
                if u_client.is_connected:
                    await u_client.disconnect()
            except PasswordHashInvalid:
                await msg.reply_text("❌ Incorrect 2FA password. Try again or `/cancel`.")
            except Exception as exc:
                code = await log_error(user_id, "login_2fa", exc)
                await msg.reply_text(f"❌ 2FA login failed. Error ID: `{code}`")
            return

        if step == "BATCH_START":
            parsed = parse_tg_link(text)
            if not parsed:
                await msg.reply_text("❌ Send a valid Telegram post link.")
                return
            per_job_limit = batch_limit_for(user)
            LOGIN_STATES[user_id] = {
                "step": "BATCH_COUNT",
                "chat_raw": parsed[0],
                "start_id": parsed[1],
                "batch_limit": per_job_limit,
                "updated_at": time.time(),
            }
            label = f"1–{per_job_limit}" if per_job_limit else "any positive count"
            await msg.reply_text(f"✅ Start saved: `#{parsed[1]}`\nNow send item count ({label}).")
            return

        if step == "BATCH_COUNT":
            per_job_limit = state.get("batch_limit")
            if not text.isdigit() or int(text) < 1 or (per_job_limit and int(text) > int(per_job_limit)):
                label = f"1–{per_job_limit}" if per_job_limit else "a positive number"
                await msg.reply_text(f"❌ Enter {label}.")
                return
            count = int(text)
            chat_raw = state["chat_raw"]
            start_id = int(state["start_id"])
            end_id = start_id + count - 1
            LOGIN_STATES.pop(user_id, None)
            try:
                job_id = await enqueue_job(user_id, "batch", chat_raw, start_id, end_id)
                await msg.reply_text(f"✅ Batch queued: `{job_id}`\nRange: `#{start_id}` → `#{end_id}`")
            except Exception as exc:
                await msg.reply_text(f"❌ Could not queue batch: {exc}")
            return

        if step == "TRANSFER_DEST":
            dest = chat_ref_from_input(text)
            if not dest:
                await msg.reply_text("❌ Send a valid destination `@username`, chat ID, or t.me link.")
                return
            src = str(state.get("source") or "").strip()
            if not src:
                LOGIN_STATES.pop(user_id, None)
                await msg.reply_text("❌ Transfer source was lost. Start again with `/clone`.")
                return
            LOGIN_STATES.pop(user_id, None)
            try:
                payload = {
                    "limit": 0, "media_filter": "all", "caption_mode": "keep",
                    "skip_duplicates": True, "include_text": True, "min_id": 0, "max_id": 0,
                }
                job_id = await enqueue_job(user_id, "transfer", src, destination=dest, payload=payload)
                await msg.reply_text(
                    f"✅ Full channel clone queued: `{job_id}`\n"
                    f"Controls: `/pause {job_id}` • `/resume {job_id}` • `/cancel {job_id}`"
                )
            except Exception as exc:
                await msg.reply_text(f"❌ Could not queue transfer: {exc}")
            return

        if step == "PAYMENT_UTR":
            if parse_tg_link(text) or "t.me/" in text:
                await msg.reply_text(
                    "💳 A payment verification input is still active. Use `/cancel` first, then start `/batch` or `/clone`."
                )
                return
            invoice_id = str(state["invoice_id"])
            utr = re.sub(r"\s+", "", text).upper()
            ok, result = await submit_utr(user_id, invoice_id, utr)
            if not ok:
                await msg.reply_text(f"❌ {result}\nPlease send the correct transaction reference or `/cancel`.")
                return
            LOGIN_STATES.pop(user_id, None)
            payment = await get_payment(invoice_id)
            await msg.reply_text(f"✅ `{invoice_id}` submitted. Payment is pending admin verification.")
            if payment:
                async with db_conn() as db:
                    admins = await (await db.execute("SELECT user_id FROM admins WHERE role IN ('owner','manager','payments')")).fetchall()
                markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Approve", callback_data=f"pay:ok:{invoice_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"pay:no:{invoice_id}"),
                ]])
                alert = (
                    f"💳 **NEW PAYMENT**\n\nInvoice: `{invoice_id}`\nUser: `{user_id}`\n"
                    f"Plan: **{payment['plan']}**\nDuration: **{payment['duration']}**\n"
                    f"Amount: **₹{payment['amount']}**\nUTR: `{payment['utr']}`"
                )
                for a in {int(r["user_id"]) for r in admins}:
                    try:
                        await bot.send_message(a, alert, reply_markup=markup)
                    except Exception:
                        pass
            return

        if step == "SUPPORT_MESSAGE":
            ticket_id = await create_ticket(user_id, str(state.get("category") or "general"), text)
            LOGIN_STATES.pop(user_id, None)
            await msg.reply_text(f"✅ Support ticket created: `{ticket_id}`\nOur team can track this ticket until resolution.")
            async with db_conn() as db:
                admins = await (await db.execute("SELECT user_id FROM admins WHERE role IN ('owner','manager','support')")).fetchall()
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Resolve", callback_data=f"ticket:close:{ticket_id}")]])
            for a in {int(r["user_id"]) for r in admins}:
                try:
                    await bot.send_message(a, f"🆘 **NEW TICKET** `{ticket_id}`\nUser: `{user_id}`\nCategory: **{state.get('category')}**\n\n{text[:2500]}", reply_markup=markup)
                except Exception:
                    pass
            return

    # Direct single/range link flow.
    if "t.me/" in text:
        block = await maintenance_blocked(user_id)
        if block:
            await msg.reply_text(block)
            return
        if not decrypt_text(user.get("encrypted_session")):
            await msg.reply_text("🔐 Connect your Telegram account first with `/login`.")
            return
        # Optional compact range syntax: https://t.me/channel/100-120
        rm = re.search(r"https?://t\.me/(?:c/)?([A-Za-z0-9_]+)/([0-9]+)-([0-9]+)", text)
        try:
            if rm:
                if user.get("plan") not in {"Standard", "Premium", "Ultimate"} and not is_admin_id(user_id):
                    await msg.reply_text("🔒 Range processing requires Standard or above.")
                    return
                start_id, end_id = sorted((int(rm.group(2)), int(rm.group(3))))
                per_job_limit = batch_limit_for(user)
                count = end_id - start_id + 1
                if per_job_limit and count > per_job_limit:
                    await msg.reply_text(f"❌ Your plan allows up to {per_job_limit} items in one batch job.")
                    return
                job_id = await enqueue_job(user_id, "batch", rm.group(1), start_id, end_id)
                await msg.reply_text(f"✅ Range queued: `{job_id}`")
                return
            parsed = parse_tg_link(text)
            if parsed:
                job_id = await enqueue_job(user_id, "single", parsed[0], parsed[1], parsed[1])
                await msg.reply_text(f"✅ Save queued: `{job_id}`")
                return
        except Exception as exc:
            await msg.reply_text(f"❌ Could not queue job: {exc}")
            return

# ---------------------------------------------------------------------------
# SCHEDULED JOBS
# ---------------------------------------------------------------------------


async def subscription_reminder_job() -> None:
    now = now_dt()
    async with db_conn() as db:
        rows = await (await db.execute(
            "SELECT user_id,plan,plan_expires_at,notify_expiry FROM users WHERE plan NOT IN ('Free') AND plan_expires_at IS NOT NULL"
        )).fetchall()
    for row in rows:
        if not int(row["notify_expiry"] or 0):
            continue
        expiry = parse_dt(row["plan_expires_at"])
        if not expiry:
            continue
        delta = expiry - now
        reminder_type: Optional[str] = None
        if timedelta(hours=23) <= delta <= timedelta(hours=25):
            reminder_type = "1day"
        elif timedelta(days=2, hours=23) <= delta <= timedelta(days=3, hours=1):
            reminder_type = "3day"
        elif -timedelta(hours=2) <= delta <= timedelta(minutes=0):
            reminder_type = "expired"
        if not reminder_type:
            continue
        async with DB_LOCK:
            async with db_conn() as db:
                cur = await db.execute(
                    "SELECT 1 FROM subscription_reminders WHERE user_id=? AND expiry=? AND reminder_type=?",
                    (row["user_id"], row["plan_expires_at"], reminder_type),
                )
                if await cur.fetchone():
                    continue
                await db.execute(
                    "INSERT INTO subscription_reminders(user_id,expiry,reminder_type,sent_at) VALUES (?,?,?,?)",
                    (row["user_id"], row["plan_expires_at"], reminder_type, now_iso()),
                )
                await db.commit()
        try:
            if reminder_type == "expired":
                await expire_user_if_needed(int(row["user_id"]))
                await bot.send_message(int(row["user_id"]), "⏰ Your subscription has expired. Use `/plans` to renew.")
            else:
                days = 1 if reminder_type == "1day" else 3
                await bot.send_message(int(row["user_id"]), f"⏰ Your **{row['plan']}** plan expires in about {days} day(s). Use `/plans` to renew.")
        except Exception:
            pass


async def cleanup_temp_job() -> None:
    cutoff = time.time() - 24 * 3600
    for child in TEMP_DIR.glob("*"):
        try:
            if child.stat().st_mtime < cutoff:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
        except OSError:
            pass

# ---------------------------------------------------------------------------
# HEALTH SERVER
# ---------------------------------------------------------------------------


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/", "/health", "/healthz"}:
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps({
            "status": "ok",
            "app": APP_NAME,
            "uptime_seconds": int(time.time() - START_TS),
            "queue_size": JOB_QUEUE.qsize(),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def run_health_server() -> None:
    try:
        server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
        log.info("Health server listening on port %s", PORT)
        server.serve_forever()
    except Exception as exc:
        log.warning("Health server failed: %s", exc)

# ---------------------------------------------------------------------------
# BOOTSTRAP / SHUTDOWN
# ---------------------------------------------------------------------------


async def register_commands() -> None:
    commands = [
        BotCommand("start", "Home dashboard"),
        BotCommand("login", "Connect Telegram account"),
        BotCommand("logout", "Disconnect account"),
        BotCommand("help", "User manual"),
        BotCommand("language", "Change language"),
        BotCommand("batch", "Batch accessible posts"),
        BotCommand("clone", "Full/filtered channel clone (Ultimate)"),
        BotCommand("clonehelp", "Clone filters and examples"),
        BotCommand("jobs", "Live jobs and history"),
        BotCommand("job", "Job details"),
        BotCommand("pause", "Pause a job"),
        BotCommand("resume", "Resume a job"),
        BotCommand("cancel", "Cancel input or a job"),
        BotCommand("settings", "User settings"),
        BotCommand("plans", "Plans and pricing"),
        BotCommand("myplan", "Subscription status"),
        BotCommand("trial", "One-time 24h trial"),
        BotCommand("redeem", "Redeem promo code"),
        BotCommand("paymenthistory", "Payment history"),
        BotCommand("support", "Open support ticket"),
        BotCommand("admin", "Admin dashboard"),
    ]
    await bot.set_bot_commands(commands)


async def start_services() -> None:
    global SCHEDULER
    await init_db()
    await recover_jobs()

    workers = [asyncio.create_task(job_worker(i + 1), name=f"worker-{i+1}") for i in range(WORKER_COUNT)]

    SCHEDULER = AsyncIOScheduler(timezone=TZ)
    SCHEDULER.add_job(daily_reset_job, "cron", hour=0, minute=0, id="daily_reset", replace_existing=True)
    SCHEDULER.add_job(subscription_reminder_job, "cron", hour=9, minute=0, id="subscription_reminders", replace_existing=True)
    SCHEDULER.add_job(cleanup_temp_job, "interval", hours=6, id="temp_cleanup", replace_existing=True)
    SCHEDULER.start()

    await bot.start()
    me = await bot.get_me()
    log.info("Bot started: @%s (%s)", me.username, me.id)
    try:
        await register_commands()
    except Exception as exc:
        log.warning("Could not register commands: %s", exc)

    try:
        await idle()
    finally:
        SHUTDOWN_EVENT.set()
        if SCHEDULER:
            SCHEDULER.shutdown(wait=False)
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await bot.stop()


def main() -> None:
    if not API_ID or not API_HASH or not BOT_TOKEN or not OWNER_ID:
        print("FATAL: API_ID, API_HASH, BOT_TOKEN and OWNER_ID must be configured.")
        raise SystemExit(2)

    thread = threading.Thread(target=run_health_server, daemon=True, name="health-server")
    thread.start()

    try:
        # Run all async services on the exact loop that existed when `bot` was
        # created.  Do not use asyncio.run() here (it creates another loop), and
        # do not pass a coroutine to Client.run() because Kurigram 2.2.26's
        # public run() API is intended as `bot.run()` in this release.
        asyncio.set_event_loop(APP_LOOP)
        APP_LOOP.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    finally:
        if not APP_LOOP.is_closed():
            pending = asyncio.all_tasks(APP_LOOP)
            for task in pending:
                task.cancel()
            if pending:
                APP_LOOP.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            APP_LOOP.run_until_complete(APP_LOOP.shutdown_asyncgens())
            APP_LOOP.close()


if __name__ == "__main__":
    main()
