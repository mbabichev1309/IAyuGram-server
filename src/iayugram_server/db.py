"""SQLite persistence: rolling content store, append-only event log, pts state.

Three responsibilities, one file so a home-server backup is a single copy:

  content   — every seen message, content encrypted at rest. Delete updates carry
              only IDs, so we MUST have stored content beforehand (caveat #1).
  events    — append-only log with a monotonic `cursor`; the client pulls from
              its last cursor forward (WebSocket live + REST gap-sync).
  kv        — persisted pts/qts/date etc. so we survive restarts (caveat #2).
"""
from __future__ import annotations

import time

import aiosqlite

from .config import settings
from .crypto import decrypt, encrypt
from .models import EventKind, MessageEvent

_SCHEMA = """
CREATE TABLE IF NOT EXISTS content (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    body        BLOB,               -- Fernet-encrypted text
    date        INTEGER,            -- original send date (unix s)
    seen_at     INTEGER NOT NULL,
    PRIMARY KEY (chat_id, message_id)
);
CREATE TABLE IF NOT EXISTS events (
    cursor      INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    body        BLOB,               -- Fernet-encrypted snapshot text
    old_body    BLOB,
    date        INTEGER,
    created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self._path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Store not opened"
        return self._db

    # --- content store -----------------------------------------------------
    async def put_content(self, chat_id: int, message_id: int, text: str, date: int) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO content(chat_id, message_id, body, date, seen_at) "
            "VALUES (?,?,?,?,?)",
            (chat_id, message_id, encrypt(text), date, int(time.time())),
        )
        await self.db.commit()

    async def get_content(self, chat_id: int, message_id: int) -> tuple[str | None, int | None]:
        async with self.db.execute(
            "SELECT body, date FROM content WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
        if not row or row[0] is None:
            return None, (row[1] if row else None)
        return decrypt(row[0]), row[1]

    async def prune_content(self) -> int:
        cutoff = int(time.time()) - settings.content_retention_hours * 3600
        cur = await self.db.execute("DELETE FROM content WHERE seen_at < ?", (cutoff,))
        await self.db.commit()
        return cur.rowcount

    # --- event log ---------------------------------------------------------
    async def append_event(
        self,
        kind: EventKind,
        chat_id: int,
        message_id: int,
        text: str | None,
        old_text: str | None,
        date: int | None,
    ) -> int:
        cur = await self.db.execute(
            "INSERT INTO events(kind, chat_id, message_id, body, old_body, date, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                kind.value,
                chat_id,
                message_id,
                encrypt(text) if text is not None else None,
                encrypt(old_text) if old_text is not None else None,
                date,
                int(time.time()),
            ),
        )
        await self.db.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def events_after(self, cursor: int, limit: int = 500) -> list[MessageEvent]:
        async with self.db.execute(
            "SELECT cursor, kind, chat_id, message_id, body, old_body, date "
            "FROM events WHERE cursor > ? ORDER BY cursor ASC LIMIT ?",
            (cursor, limit),
        ) as c:
            rows = await c.fetchall()
        return [
            MessageEvent(
                cursor=r[0],
                kind=EventKind(r[1]),
                chat_id=r[2],
                message_id=r[3],
                text=decrypt(r[4]) if r[4] is not None else None,
                old_text=decrypt(r[5]) if r[5] is not None else None,
                date=r[6],
            )
            for r in rows
        ]

    async def latest_cursor(self) -> int:
        async with self.db.execute("SELECT COALESCE(MAX(cursor), 0) FROM events") as c:
            row = await c.fetchone()
        return row[0] if row else 0

    # --- kv (pts persistence) ---------------------------------------------
    async def get_state(self, key: str) -> str | None:
        async with self.db.execute("SELECT value FROM kv WHERE key=?", (key,)) as c:
            row = await c.fetchone()
        return row[0] if row else None

    async def set_state(self, key: str, value: str) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO kv(key, value) VALUES (?,?)", (key, value)
        )
        await self.db.commit()


store = Store(settings.db_path)
