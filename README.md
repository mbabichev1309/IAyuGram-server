# iayugram-server

Companion capture server for [IAyuGram](../client). A **second authorized
session** of the user's own Telegram account (a userbot, running with Ghost mode
semantics) that receives `updateDeleteMessages` / `updateEditMessage` in real time
and is the single authoritative source of deleted / edited message content — it
sees everything even while the phone is open.

Architecture (topology + the 3 capture scenarios A/B/C + view-once) is documented
in the **client** repo: `docs/ayugram-features.md`, section
"Delivery architecture — companion capture server". That doc is the source of
truth; this repo implements it.

## Why a separate repo
- Different toolchain: Python vs the client's Swift/Bazel/Xcode monorepo.
- Different lifecycle / deploy: this runs 24/7 on an Ubuntu box via systemd.
- Sensitive: it stores deleted content **and** a Telegram session string.
  Secrets stay out of the public client fork; content is encrypted at rest.

## Stack
- **Telethon** — the MTProto client (`events.MessageDeleted` / `events.MessageEdited`).
- **FastAPI + uvicorn** — REST gap-sync + WebSocket live stream, one asyncio loop.
- **SQLite (aiosqlite)** — rolling content store, append-only event log, persisted pts.
- **cryptography (Fernet)** — encrypt-at-rest for stored message content.

## Hard caveats baked into the design
1. A delete update carries **only message IDs, no content** → we store content on
   every `on_message` and look it up on delete. The rolling content store is
   mandatory, not optional.
2. Long downtime → `getDifference` returns `differenceTooLong` and does **not**
   replay individual deletes. Missed deletes must be found by diffing the stored
   ID set against a refetched slice. **Uptime is critical** — hence systemd
   `Restart=always` + pts persisted to disk.

## Quick start (Ubuntu)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env          # fill in API_ID / API_HASH / secrets
python scripts/login.py       # interactive: produces the session string once
python -m iayugram_server     # run capture + api together
```

## Deploy
See `deploy/iayugram-server.service`. Copy to `/etc/systemd/system/`,
`systemctl enable --now iayugram-server`.
