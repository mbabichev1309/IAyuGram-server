"""Diagnostic login with full logging: shows WHERE Telegram sent the code,
surfaces flood-waits with exact seconds, logs the MTProto exchange, then
completes sign-in (incl. 2FA cloud password).

Run in a REAL terminal (needs interactive input):

    .venv\\Scripts\\python.exe scripts\\login_debug.py

A full transcript is also written to scripts\\login_debug.log so we can
inspect what Telegram actually returned even after the window closes.
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path


def _load_env() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


_load_env()

# --- Logging setup --------------------------------------------------------
LOG_PATH = Path(__file__).resolve().parent / "login_debug.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),          # console
        logging.FileHandler(LOG_PATH, encoding="utf-8"),  # persistent transcript
    ],
)
# Telethon is very chatty at DEBUG (logs every MTProto request/response) —
# keep our own logger at DEBUG but tame Telethon to INFO so the console stays
# readable. Bump the next line to DEBUG if you want the raw packet-level trace.
logging.getLogger("telethon").setLevel(logging.INFO)

log = logging.getLogger("login")
# -------------------------------------------------------------------------

from telethon import TelegramClient  # noqa: E402
from telethon.errors import (  # noqa: E402
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession  # noqa: E402

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]


async def main() -> None:
    log.info("Using API_ID=%s (api_hash length=%d)", API_ID, len(API_HASH))
    log.info("Log file: %s", LOG_PATH)

    client = TelegramClient(StringSession(), API_ID, API_HASH)

    log.info("Connecting to Telegram...")
    await client.connect()
    log.info("Connected. DC=%s", client.session.dc_id)

    phone = input("Phone (+48...): ").strip()
    log.info("Requesting login code for %s (single request, no retries)", phone)

    try:
        sent = await client.send_code_request(phone)
    except FloodWaitError as e:
        log.error("FLOOD_WAIT: Telegram is rate-limiting code requests.")
        log.error("  Wait %d seconds (~%.1f hours) before trying again.",
                  e.seconds, e.seconds / 3600)
        log.error("  Every new attempt RESETS this timer — do NOT retry sooner.")
        await client.disconnect()
        return
    except PhoneNumberBannedError:
        log.error("PHONE_NUMBER_BANNED: this number is banned by Telegram.")
        await client.disconnect()
        return
    except PhoneNumberInvalidError:
        log.error("PHONE_NUMBER_INVALID: check the number format (+48...).")
        await client.disconnect()
        return
    except Exception:
        log.exception("send_code_request FAILED with an unexpected error:")
        await client.disconnect()
        return

    log.info("--- Telegram accepted the request ---")
    log.info("  code delivery type : %s", type(sent.type).__name__)
    log.info("  next type (fallback): %s",
             type(sent.next_type).__name__ if sent.next_type else None)
    log.info("  phone_code_hash     : %s", sent.phone_code_hash)
    # SentCodeTypeApp usually carries a `length` (number of digits expected).
    length = getattr(sent.type, "length", None)
    if length is not None:
        log.info("  expected code length: %s digits", length)
    log.info("  full sent object    : %r", sent)
    log.info("-------------------------------------")
    log.info("SentCodeTypeApp  -> code is in the 'Telegram' service chat, in-app")
    log.info("SentCodeTypeSms  -> code is an SMS to your phone")
    log.info("SentCodeTypeCall -> Telegram will CALL and read digits")
    log.info("-------------------------------------")

    code = input("Enter the code you received (blank to abort): ").strip()
    if not code:
        log.warning("No code entered — aborting without consuming the code.")
        await client.disconnect()
        return

    try:
        log.info("Submitting code...")
        await client.sign_in(phone=phone, code=code,
                             phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        log.info("2FA enabled — cloud password required.")
        pw = input("2FA password: ")
        try:
            await client.sign_in(password=pw)
        except Exception:
            log.exception("2FA sign-in FAILED:")
            await client.disconnect()
            return
    except PhoneCodeInvalidError:
        log.error("PHONE_CODE_INVALID: wrong code. Use the NEWEST code, do not "
                  "forward it in any chat (forwarding auto-expires it).")
        await client.disconnect()
        return
    except PhoneCodeExpiredError:
        log.error("PHONE_CODE_EXPIRED: code timed out. Run this script again for "
                  "a fresh code and enter it promptly.")
        await client.disconnect()
        return
    except Exception:
        log.exception("sign_in FAILED with an unexpected error:")
        await client.disconnect()
        return

    me = await client.get_me()
    log.info("Authorized as: %s (id=%s)",
             getattr(me, "username", None) or getattr(me, "first_name", "?"),
             getattr(me, "id", "?"))

    session_string = client.session.save()
    log.info("=== Authorized. Copy this into .env as SESSION_STRING ===")
    print("\n" + session_string + "\n")
    log.info("=== (also written to the log file above) ===")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
