"""One-time interactive login: authorize the 2nd session and print a session string.

Run on a machine where you can receive the Telegram login code:

    python scripts/login.py

Fill API_ID and API_HASH in .env first, then run this. Paste the resulting
string into SESSION_STRING in your .env. The string IS a credential — treat it
like a password (it grants full account access).
"""
from __future__ import annotations

import os
from pathlib import Path


def _load_env() -> None:
    """Minimal .env loader so API_ID/API_HASH come from the same file as the rest."""
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

from telethon import TelegramClient  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]


def main() -> None:
    with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        print("\n=== Authorized. Copy this into .env as SESSION_STRING ===\n")
        print(client.session.save())
        print("\n=========================================================\n")


if __name__ == "__main__":
    main()
