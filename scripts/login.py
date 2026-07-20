"""One-time interactive login: authorize the 2nd session and print a session string.

Run on a machine where you can receive the Telegram login code:

    python scripts/login.py

Paste the resulting string into SESSION_STRING in your .env. The string IS a
credential — treat it like a password (it grants full account access).
"""
from __future__ import annotations

import os

from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]


def main() -> None:
    with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        print("\n=== Authorized. Copy this into .env as SESSION_STRING ===\n")
        print(client.session.save())
        print("\n=========================================================\n")


if __name__ == "__main__":
    main()
