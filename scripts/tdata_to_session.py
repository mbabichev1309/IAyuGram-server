"""Extract a Telethon session string from an existing Telegram/AyuGram Desktop
tdata folder via opentele — NO login code needed (reuses the current auth).

opentele 1.15.1 can't parse the account "map" from newer tdesktop builds
(AyuGram is tdesktop 6.7.x): the map now contains key type 23 =
lskCustomEmojiKeys, which opentele doesn't know, so it raises "Unknown key
type" and silently fails to load the account. But the map (drafts, stickers,
custom-emoji refs) is irrelevant to us — the MTP auth key we need lives in a
SEPARATE file. So we monkeypatch readMapWith to ignore the map-parse failure
and still read the MTP authorization.

Writes the result straight into ../.env as SESSION_STRING= (never prints the
credential to the console). Run:

    .venv\\Scripts\\python.exe scripts\\tdata_to_session.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path

from PyQt5.QtCore import QByteArray

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("tdata")

import opentele.td.account as _acct  # noqa: E402
from opentele.td import TDesktop  # noqa: E402
from opentele.api import API, CreateNewSession, UseCurrentSession  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402


def _readMapWith_tolerant(self, localKey, legacyPasscode=QByteArray()):
    """Like opentele's readMapWith, but if the map won't parse (e.g. an unknown
    newer key type like lskCustomEmojiKeys=23) we still proceed to read the MTP
    auth — the only part we need for a session string. Body kept byte-identical
    to the proven-working diagnostic."""
    import sys as _sys
    try:
        self._StorageAccount__mapData.read(localKey, legacyPasscode)
    except BaseException as e:
        print(">>> map parse skipped:", repr(e), file=_sys.stderr, flush=True)
    self.readMtpData()


# Apply the patch AFTER all opentele imports are resolved.
_acct.StorageAccount.readMapWith = _readMapWith_tolerant

TDATA = r"C:\Users\mbabi\AppData\Local\AyuGram\tdata"
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _write_session_to_env(session_string: str) -> None:
    text = ENV_PATH.read_text(encoding="utf-8")
    if re.search(r"(?m)^SESSION_STRING=.*$", text):
        text = re.sub(r"(?m)^SESSION_STRING=.*$",
                      f"SESSION_STRING={session_string}", text)
    else:
        text = text.rstrip("\n") + f"\nSESSION_STRING={session_string}\n"
    ENV_PATH.write_text(text, encoding="utf-8")


async def main() -> None:
    log.info("Loading tdata from: %s", TDATA)
    tdesk = TDesktop(TDATA)
    log.info("isLoaded: %s | accounts: %s", tdesk.isLoaded(), len(tdesk.accounts))
    if not tdesk.isLoaded():
        log.error("tdata still not loaded — unexpected after the patch.")
        return

    # CreateNewSession = use the desktop auth to spin up a BRAND-NEW, SEPARATE
    # session (its own auth_key) via internal QR login — no login code needed.
    # This is what we actually want: a true 2nd session that receives the Updates
    # stream. UseCurrentSession would reuse the desktop's auth_key (same session),
    # which does NOT reliably get pushed updates and risks a ban.
    #
    # QR login of a new device triggers 2FA when a cloud password is set — pass it
    # via the TG_2FA env var (never hard-code it). We create a fresh API identity
    # so the new session isn't tied to Telegram Desktop's api_id.
    password = os.environ.get("TG_2FA") or None
    new_api = API.TelegramDesktop.Generate()
    client = await tdesk.ToTelethon(
        session=StringSession(), flag=CreateNewSession, api=new_api,
        password=password,
    )
    await client.connect()

    # opentele 1.15.1 bug: after 2FA it calls `newClient._on_login(...)` WITHOUT
    # awaiting it, so the client object never flips its local _authorized flag —
    # but the session IS authorized server-side (the QR login + 2FA succeeded).
    # So we don't trust this client's is_user_authorized(); instead we extract the
    # session string and re-verify with a FRESH client (which re-checks via
    # GetState against the server).
    string_sess = StringSession()
    string_sess.set_dc(client.session.dc_id, client.session.server_address,
                       client.session.port)
    string_sess.auth_key = client.session.auth_key
    session_string = string_sess.save()
    await client.disconnect()

    # Re-verify with a clean client using the new session + new API identity.
    from telethon import TelegramClient  # noqa: E402
    verify = TelegramClient(StringSession(session_string), new_api.api_id,
                            new_api.api_hash)
    await verify.connect()
    if not await verify.is_user_authorized():
        log.error("New session failed verification — not authorized server-side.")
        await verify.disconnect()
        return
    me = await verify.get_me()
    log.info("Authorized as: %s (id=%s, phone=+%s)",
             getattr(me, "first_name", "?"), me.id, getattr(me, "phone", "?"))
    await verify.disconnect()

    _write_session_to_env(session_string)
    log.info("SESSION_STRING written to %s (%d chars). Not printed for safety.",
             ENV_PATH, len(session_string))


if __name__ == "__main__":
    asyncio.run(main())
