# Save Restricted Pro V2

Production-oriented Telegram subscription utility bot built with **Kurigram**, SQLite, APScheduler and encrypted session storage.

> Use the bot only for Telegram content/chats/accounts you are authorized to access and process. Respect Telegram rules and applicable rights.

## Main features

- Professional home dashboard
- One-time 24-hour trial with real expiry
- Weekly / monthly / yearly subscriptions with real expiry
- Invoice-based UPI payment flow
- UTR duplicate protection
- Admin Approve / Reject payment actions
- Encrypted Telegram user-session storage
- Persistent priority job queue with restart recovery
- Single save and plan-gated batch processing
- Ultimate owned-channel transfer with admin/owner checks in both chats
- User job history and cancellation
- Referral bonus credits
- Promo codes with days, uses, expiry and per-user redemption protection
- Support tickets
- Customer CRM commands
- Revenue analytics
- Segmented broadcast
- Maintenance mode
- Audit logs and error IDs
- Subscription expiry reminders
- Temp-file cleanup
- JSON health endpoint
- Dockerfile and GitHub Actions syntax check

## Why Kurigram?

The original Pyrogram repository is archived. Kurigram is an actively maintained Pyrogram-compatible fork, so the familiar `pyrogram` imports are retained while using a maintained package.

## Required environment variables

Copy `.env.example` to `.env` and set at least:

```env
API_ID=...
API_HASH=...
BOT_TOKEN=...
OWNER_ID=...
SESSION_ENCRYPTION_KEY=...
UPI_ID=...
UPI_NAME=...
```

Generate an encryption key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Never commit `.env`, session files or your database to GitHub.** `.gitignore` already excludes them.

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
```

Load the environment variables using your hosting platform, shell, Docker environment, or a secret manager, then:

```bash
python bot.py
```

The script automatically loads a local `.env` file using `python-dotenv`. On production hosts, prefer the platform's encrypted environment/secrets settings.

## Docker

```bash
docker build -t savepro-v2 .
docker run --env-file .env -p 8080:8080 -v $(pwd)/data:/app/data -v $(pwd)/bot_data.db:/app/bot_data.db savepro-v2
```

For a new deployment where `bot_data.db` does not yet exist, mount a directory instead or let your hosting platform provide persistent storage.

## User commands

- `/start` — dashboard
- `/login` — connect account
- `/logout` — disconnect account
- `/plans` — plans/pricing
- `/myplan` — subscription/expiry
- `/trial` — one-time 24h trial
- `/batch` — authorized batch processing
- `/transfer` — owned-channel transfer (Ultimate; admin/owner in both chats required)
- `/jobs` — recent jobs
- `/cancel JOB-...` — cancel job
- `/settings` — preferences
- `/setthumb`, `/setvthumb` — thumbnails
- `/setcaption` — custom caption
- `/redeem CODE` — promo
- `/paymenthistory` — payment records
- `/support` — ticket system

## Admin commands

- `/admin` — dashboard
- `/stats` — overview
- `/revenue` — revenue totals
- `/user <id>` — CRM profile
- `/ap <id> <Plan> [days]` — activate plan
- `/rp <id>` — reset to Free
- `/ban <id>` / `/unban <id>`
- `/addcredits <id> <amount>`
- `/addpromo CODE Plan Days Uses [ExpiryDays]`
- `/tickets`
- Reply to a message with `/broadcast all|free|paid|marketing|lang:ta`
- `/maintenance on|off`
- Owner: `/addadmin <id> <role>`

Roles: `manager`, `payments`, `support`, `marketing`, `developer`.

## Existing database migration

On startup, V2 creates missing tables/columns without deleting existing data. If an older `users.session` plaintext column contains session strings, V2 encrypts them into `encrypted_session` and clears the legacy plaintext value. Legacy V1 trial labels with no expiry are reset to Free so an old 24-hour trial cannot accidentally become lifetime access. Existing legacy paid plans with no recorded expiry are left unchanged; review them with `/user <id>` and set the intended expiry using `/ap`.

**Before first production migration, take a copy of `bot_data.db`.**

## Scaling path

SQLite + local priority queue is suitable for one instance / moderate traffic. For very large international traffic, the next upgrade should be:

1. PostgreSQL for primary data
2. Redis for distributed queues/rate limits
3. Multiple worker instances
4. Object storage for temporary media where appropriate
5. Centralized logs/metrics and alerting
6. Automated DB backups

The V2 tables and job abstraction are designed to make that migration easier.

## GitHub upload

Create a new private repository first while testing. Upload all files from this project folder **except** `.env`, `bot_data.db`, sessions and `data/`.

Recommended first release tag: `v2.0.0`.
