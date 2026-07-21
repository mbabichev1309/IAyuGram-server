#!/usr/bin/env python3
"""Standalone session-string extractor — for a FRIEND to run on their own PC.

It creates a NEW, separate Telegram session from your already-logged-in
Telegram/AyuGram Desktop (no login code needed) and prints a SESSION_STRING.
Send that string to the person running the capture server.

WHAT YOU NEED
  * Python 3.8+
  * One dependency:   pip install opentele
  * Telegram Desktop OR AyuGram Desktop installed and logged into your account.

HOW TO RUN
  1. Fully QUIT Telegram/AyuGram Desktop (tray -> Quit, not just close window).
  2. Run:   python friend_extract_session.py
     (optional: point at a custom folder)  python friend_extract_session.py --tdata "C:\\path\\to\\tdata"
  3. If you have a 2-step (cloud) password, type it when asked.
  4. Copy the printed SESSION_STRING and send it to the server operator.

SECURITY: a SESSION_STRING grants FULL access to your account. Only send it to
someone you trust to run the server. Creating it triggers a "new login" alert in
your Telegram — that is this session; don't terminate it or the server stops
working. You can revoke it anytime in Settings -> Devices.
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys
from pathlib import Path

try:
    import opentele.td.account as _acct
    from opentele.api import API, CreateNewSession
    from opentele.td import TDesktop
    from telethon.sessions import StringSession
    from telethon import TelegramClient
    from PyQt5.QtCore import QByteArray
except ImportError:
    print("Missing dependency. Install it first:\n\n    pip install opentele\n")
    sys.exit(1)


# --- opentele 1.15.1 compatibility patch ----------------------------------
# Newer tdesktop (6.7+) writes map key type 23 (lskCustomEmojiKeys) that opentele
# doesn't know, so it fails to load the account. The map (drafts/stickers/emoji)
# isn't needed; the MTP auth we want lives in a separate file. So swallow the map
# parse error and still read the MTP auth. NOTE: opentele's exceptions subclass
# BaseException (not Exception), so we MUST catch BaseException here.
def _readMapWith_tolerant(self, localKey, legacyPasscode=QByteArray()):
    try:
        self._StorageAccount__mapData.read(localKey, legacyPasscode)
    except BaseException:
        pass
    self.readMtpData()


_acct.StorageAccount.readMapWith = _readMapWith_tolerant


def _candidate_tdata_paths() -> list[Path]:
    home = Path.home()
    c: list[Path] = []
    if sys.platform == "win32":
        roaming = Path(os.environ.get("APPDATA", home / "AppData/Roaming"))
        local = Path(os.environ.get("LOCALAPPDATA", home / "AppData/Local"))
        for base in (roaming, local):
            c += [base / "Telegram Desktop" / "tdata",
                  base / "AyuGram" / "tdata",
                  base / "AyuGram Desktop" / "tdata"]
    elif sys.platform == "darwin":
        appsup = home / "Library" / "Application Support"
        c += [appsup / "Telegram Desktop" / "tdata", appsup / "AyuGram" / "tdata"]
    else:
        share = home / ".local" / "share"
        c += [share / "TelegramDesktop" / "tdata",
              share / "AyuGram" / "tdata",
              share / "Telegram Desktop" / "tdata"]
    return [p for p in c if p.is_dir()]


async def _extract(tdata: Path, password: str | None) -> str:
    print(f"Loading tdata: {tdata}")
    tdesk = TDesktop(str(tdata))
    if not tdesk.isLoaded():
        raise SystemExit("Could not load tdata. Is Desktop fully closed? Right "
                         "folder? Try --tdata <path>.")

    # CreateNewSession: use the desktop auth to spin up a brand-new, separate
    # session via internal QR login (no code). session=StringSession() so the
    # result is portable. A fresh API identity keeps it independent of Desktop.
    new_api = API.TelegramDesktop.Generate()
    client = await tdesk.ToTelethon(
        session=StringSession(), flag=CreateNewSession, api=new_api,
        password=password,
    )
    await client.connect()

    # opentele bug: after 2FA it calls _on_login WITHOUT awaiting, so the client's
    # local auth flag never flips even though the session IS authorized server
    # side. So extract the session and re-verify with a fresh client.
    ss = StringSession()
    ss.set_dc(client.session.dc_id, client.session.server_address, client.session.port)
    ss.auth_key = client.session.auth_key
    session_string = ss.save()
    await client.disconnect()

    verify = TelegramClient(StringSession(session_string), new_api.api_id, new_api.api_hash)
    await verify.connect()
    ok = await verify.is_user_authorized()
    me = await verify.get_me() if ok else None
    await verify.disconnect()
    if not ok:
        raise SystemExit("New session failed verification (not authorized).")
    who = getattr(me, "username", None) or getattr(me, "first_name", "?")
    print(f"Authorized as: {who} (id={getattr(me, 'id', '?')})")
    return session_string


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract a Telegram session string from tdata.")
    ap.add_argument("--tdata", help="path to the tdata folder (auto-detected if omitted)")
    args = ap.parse_args()

    if args.tdata:
        tdata = Path(args.tdata)
        if not tdata.is_dir():
            raise SystemExit(f"No such folder: {tdata}")
    else:
        found = _candidate_tdata_paths()
        if not found:
            raise SystemExit("Could not auto-find tdata. Pass it: --tdata <path>\n"
                             "(usually inside your Telegram/AyuGram Desktop data folder).")
        if len(found) > 1:
            print("Multiple tdata folders found:")
            for i, p in enumerate(found):
                print(f"  [{i}] {p}")
            sel = input("Pick number (default 0): ").strip() or "0"
            tdata = found[int(sel)]
        else:
            tdata = found[0]

    print("Make sure Telegram/AyuGram Desktop is FULLY closed (tray -> Quit).")
    password = getpass.getpass("2-step (cloud) password [Enter if none]: ") or None

    session_string = asyncio.run(_extract(tdata, password))

    print("\n" + "=" * 64)
    print("YOUR SESSION STRING (send this to the server operator, keep private):")
    print("=" * 64)
    print(session_string)
    print("=" * 64)
    print("Full account access. Revoke anytime in Settings -> Devices.")


if __name__ == "__main__":
    main()
