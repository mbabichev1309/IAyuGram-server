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

## Getting the session string (do this once, on a machine with Telegram Desktop)

The server needs a **separate** authorized session of your account. Interactive
login-code auth (`scripts/login.py`) is unreliable in practice: if your account
already has an active session, Telegram delivers the code only in-app
(`SentCodeTypeApp`) with no SMS/call fallback, and it may never arrive. So the
supported path extracts a session from an existing **Telegram/AyuGram Desktop**
install, with **no login code**:

```powershell
# on the desktop machine, inside this repo, with a venv that has deps installed
#   1. fill API_ID / API_HASH / CONTENT_KEY / CLIENT_TOKEN in .env (copy .env.example)
#   2. log Telegram/AyuGram Desktop into the target account (QR is fine)
#   3. fully quit Telegram/AyuGram Desktop, then:
$env:TG_2FA = "<your cloud password>"     # needed: new-session QR login triggers 2FA
.venv\Scripts\python.exe scripts\tdata_to_session.py
Remove-Item Env:\TG_2FA
```

This writes `SESSION_STRING=` into `.env` (a fresh, separate session — its own
auth_key, so it actually receives the Updates stream). Edit `TDATA` at the top of
`scripts/tdata_to_session.py` if your tdata folder is elsewhere.

`scripts/login.py` remains for the classic code-based flow but is **not** the
recommended path (see above).

## Client integration (how the phone consumes events)

Two channels, both authenticated with `CLIENT_TOKEN`. Every event has a
monotonic `cursor` — that's the whole synchronization primitive.

- **REST `GET /gap-sync?since=<cursor>&limit=<n>`** (header `Authorization: Bearer <token>`)
  → `{ events: [...], latest_cursor }`. Backfill: what happened while offline,
  starting *after* `since`. Paginate until the last event's `cursor` reaches
  `latest_cursor`.
- **WebSocket `/live?token=<token>`** → pushes each new event as JSON the moment
  it happens.

**Recommended connect order (avoids a gap):**
1. Open the WebSocket **first** and start buffering incoming events (don't apply yet).
2. Call `/gap-sync` from your last-seen cursor, paginating to `latest_cursor`.
3. Apply the gap-sync events, then drain the buffered live events.
4. **Dedup by `cursor`** — drop any cursor already applied. Cursors are strictly
   increasing and unique per server, so this makes the whole flow idempotent even
   if backfill and live overlap.

Persist the highest applied `cursor` on the client; that's your `since` next launch.

Event shape (`models.py::MessageEvent`): `cursor, kind (deleted|edited), chat_id,
message_id, text, old_text, date`. For `deleted`, `text` is the pre-delete
content; for `edited`, `text` is the new content and `old_text` the previous.

## Multiple accounts (one isolated instance each)

For a handful of accounts (you + trusted friends), run **one process per
account** — full isolation: each gets its own session, port, database and
encryption key, and one account's ban/flood/crash can't affect the others.

Each friend generates **their own** `SESSION_STRING` on their desktop
(`scripts/tdata_to_session.py`) and sends it to you — note a session string is
**full access** to that account, so only do this with people who trust you as the
operator (with symmetric keys, the operator *can* read the stored archive; a
future "sealed" key mode would change that — see `content_key_type`).

Onboard on the server:
```bash
sudo bash deploy/add-account.sh alice            # prompts for the session string
# or:  sudo bash deploy/add-account.sh alice alice-session.txt
```
This picks a free port, generates a fresh `CONTENT_KEY` + `CLIENT_TOKEN`, writes a
root-only `/etc/iayugram/alice.env`, installs the `iayugram-server@.service`
template, and starts `iayugram-server@alice`. It prints the `host:port` + token to
hand to that account's client. Manage with `systemctl … iayugram-server@alice`.

> Remote friends: the client must reach the server. On a home box behind NAT,
> don't port-forward plain HTTP — put it on a private tunnel (e.g. Tailscale) or
> a TLS reverse proxy so the token and traffic are encrypted (`ws://`+token in the
> clear is LAN-only safe).

## Deploy (Ubuntu, 24/7)

```bash
git clone https://github.com/mbabichev1309/IAyuGram-server.git
cd IAyuGram-server
cp /path/to/your/.env .env      # transfer manually — secrets never go through git
bash deploy/setup-ubuntu.sh     # venv + deps + renders & starts the systemd unit
```

`deploy/setup-ubuntu.sh` installs the venv, `pip install -e .`, renders
`deploy/iayugram-server.service` with the correct paths/user into
`/etc/systemd/system/`, and `systemctl enable --now`s it. Then:

```bash
journalctl -u iayugram-server -f     # follow logs
```
