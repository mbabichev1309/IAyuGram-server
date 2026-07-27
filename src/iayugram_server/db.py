"""SQLite persistence: rolling content store, append-only event log, pts state.

Three responsibilities, one file so a home-server backup is a single copy:

  content   — every seen message, content encrypted at rest. Delete updates carry
              only IDs, so we MUST have stored content beforehand (caveat #1).
  events    — append-only log with a monotonic `cursor`; the client pulls from
              its last cursor forward (WebSocket live + REST gap-sync).
  kv        — persisted pts/qts/date etc. so we survive restarts (caveat #2).
"""
from __future__ import annotations

import asyncio
import os
import time

import aiosqlite

from .config import settings
from .crypto import decrypt, encrypt, encrypt_file
from .models import EventKind, MediaMeta, MessageEvent

_SCHEMA = """
CREATE TABLE IF NOT EXISTS content (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    body        BLOB,               -- Fernet-encrypted text
    date        INTEGER,            -- original send date (unix s)
    out         INTEGER NOT NULL DEFAULT 0,  -- 1 = sent by the account owner
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
    out         INTEGER NOT NULL DEFAULT 0,  -- 1 = sent by the account owner
    created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS media (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    kind        TEXT NOT NULL,      -- photo|sticker|voice|round|video|gif|audio|document
    mime        TEXT,
    size        INTEGER NOT NULL,   -- plaintext byte size
    width       INTEGER,
    height      INTEGER,
    duration    INTEGER,
    view_once   INTEGER NOT NULL DEFAULT 0,
    path        TEXT NOT NULL,      -- relative path of the encrypted file
    seen_at     INTEGER NOT NULL,
    file_name   TEXT,               -- original document name, when there is one
    PRIMARY KEY (chat_id, message_id)
);
"""


class Store:
    def __init__(self, path: str) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        # sqlite won't create the parent dir (e.g. data/); ensure it exists.
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._db = await aiosqlite.connect(self._path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.executescript(_SCHEMA)
        # Migration: add content.out to DBs created before it existed.
        for table in ("content", "events"):
            try:
                await self._db.execute(
                    f"ALTER TABLE {table} ADD COLUMN out INTEGER NOT NULL DEFAULT 0"
                )
            except Exception:  # noqa: BLE001 — column already exists
                pass
        # Migration: phase 2 records the original file name for documents.
        try:
            await self._db.execute("ALTER TABLE media ADD COLUMN file_name TEXT")
        except Exception:  # noqa: BLE001 — column already exists
            pass
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Store not opened"
        return self._db

    # --- content store -----------------------------------------------------
    async def put_content(
        self, chat_id: int, message_id: int, text: str, date: int, out: bool = False
    ) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO content(chat_id, message_id, body, date, out, seen_at) "
            "VALUES (?,?,?,?,?,?)",
            (chat_id, message_id, encrypt(text), date, 1 if out else 0, int(time.time())),
        )
        await self.db.commit()

    async def get_content(
        self, chat_id: int, message_id: int
    ) -> tuple[str | None, int | None, bool]:
        async with self.db.execute(
            "SELECT body, date, out FROM content WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None, None, False
        text = decrypt(row[0]) if row[0] is not None else None
        return text, row[1], bool(row[2])

    async def resolve_by_mid(
        self, message_id: int
    ) -> tuple[int | None, str | None, int | None, bool]:
        """Look up content by message_id alone — for DM/cloud-chat deletes where
        Telethon can't give us chat_id (UpdateDeleteMessages carries only IDs, and
        non-channel message IDs are unique across all of a user's cloud dialogs).
        Returns (chat_id, text, date, out) or (None, None, None, False) if unknown."""
        async with self.db.execute(
            "SELECT chat_id, body, date, out FROM content WHERE message_id=? "
            "ORDER BY seen_at DESC LIMIT 1",
            (message_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None, None, None, False
        chat_id, body, date, out = row
        return chat_id, (decrypt(body) if body is not None else None), date, bool(out)

    async def candidates_for_reconcile(
        self, limit: int
    ) -> list[tuple[int, int, str | None, int | None, bool]]:
        """Stored messages that have NO recorded delete yet — the set to verify
        against the server on launch. Returns (chat_id, message_id, text, date, out),
        newest first, capped at `limit`. A delete event may have been stored with
        chat_id=0 (DM limitation), so match on message_id OR the exact chat."""
        async with self.db.execute(
            "SELECT c.chat_id, c.message_id, c.body, c.date, c.out "
            "FROM content c "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM events e "
            "  WHERE e.kind='deleted' AND e.message_id=c.message_id "
            "    AND (e.chat_id=c.chat_id OR e.chat_id=0)"
            ") "
            "ORDER BY c.seen_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [
            (r[0], r[1], decrypt(r[2]) if r[2] is not None else None, r[3], bool(r[4]))
            for r in rows
        ]

    async def has_delete_event(self, chat_id: int, message_id: int) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM events WHERE kind='deleted' AND message_id=? "
            "AND (chat_id=? OR chat_id=0) LIMIT 1",
            (message_id, chat_id),
        ) as cur:
            return await cur.fetchone() is not None

    async def prune_content(self) -> int:
        cutoff = int(time.time()) - settings.content_retention_hours * 3600
        cur = await self.db.execute("DELETE FROM content WHERE seen_at < ?", (cutoff,))
        await self.db.commit()
        return cur.rowcount

    # --- media store -------------------------------------------------------
    async def put_media_file(
        self,
        chat_id: int,
        message_id: int,
        kind: str,
        mime: str | None,
        width: int | None,
        height: int | None,
        duration: int | None,
        view_once: bool,
        src_path: str,
        file_name: str | None = None,
    ) -> int:
        """Register media that was streamed to `src_path`, encrypting it into the
        media dir chunk-by-chunk. Returns the plaintext size. Used for everything in
        phase 2 — nothing is ever held whole in memory."""
        os.makedirs(settings.media_dir, exist_ok=True)
        rel = f"{chat_id}_{message_id}.enc"
        full = os.path.join(settings.media_dir, rel)
        size = await asyncio.to_thread(
            encrypt_file, src_path, full, settings.media_chunk_bytes
        )
        await self.db.execute(
            "INSERT OR REPLACE INTO media(chat_id, message_id, kind, mime, size, "
            "width, height, duration, view_once, path, seen_at, file_name) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (chat_id, message_id, kind, mime, size, width, height, duration,
             1 if view_once else 0, rel, int(time.time()), file_name),
        )
        await self.db.commit()
        return size

    def media_full_path(self, meta: MediaMeta) -> str:
        return os.path.join(settings.media_dir, meta.path)

    async def get_media(self, chat_id: int, message_id: int) -> MediaMeta | None:
        async with self.db.execute(
            "SELECT kind, mime, size, width, height, duration, view_once, path, "
            "file_name FROM media WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return MediaMeta(
            kind=row[0], mime=row[1], size=row[2], width=row[3], height=row[4],
            # duration may have been stored as a float (round/voice seconds).
            duration=int(row[5]) if row[5] is not None else None,
            view_once=bool(row[6]), path=row[7], file_name=row[8],
        )

    async def prune_media(self) -> int:
        cutoff = int(time.time()) - settings.media_retention_hours * 3600
        async with self.db.execute(
            "SELECT path FROM media WHERE seen_at < ?", (cutoff,)
        ) as cur:
            rows = await cur.fetchall()
        for (rel,) in rows:
            try:
                os.remove(os.path.join(settings.media_dir, rel))
            except OSError:
                pass
        await self.db.execute("DELETE FROM media WHERE seen_at < ?", (cutoff,))
        await self.db.commit()
        return len(rows)

    # --- event log ---------------------------------------------------------
    async def append_event(
        self,
        kind: EventKind,
        chat_id: int,
        message_id: int,
        text: str | None,
        old_text: str | None,
        date: int | None,
        out: bool = False,
    ) -> int:
        cur = await self.db.execute(
            "INSERT INTO events(kind, chat_id, message_id, body, old_body, date, out, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                kind.value,
                chat_id,
                message_id,
                encrypt(text) if text is not None else None,
                encrypt(old_text) if old_text is not None else None,
                date,
                1 if out else 0,
                int(time.time()),
            ),
        )
        await self.db.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def events_after(self, cursor: int, limit: int = 500) -> list[MessageEvent]:
        # LEFT JOIN media so replayed events carry media metadata too (derived from
        # the media table, not duplicated into events; gone if the media was pruned).
        async with self.db.execute(
            "SELECT e.cursor, e.kind, e.chat_id, e.message_id, e.body, e.old_body, e.date, e.out, "
            "       m.kind, m.mime, m.size, m.width, m.height, m.duration, m.view_once "
            "FROM events e "
            "LEFT JOIN media m ON m.chat_id = e.chat_id AND m.message_id = e.message_id "
            "WHERE e.cursor > ? ORDER BY e.cursor ASC LIMIT ?",
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
                from_me=bool(r[7]),
                media_kind=r[8],
                media_mime=r[9],
                media_size=r[10],
                media_width=r[11],
                media_height=r[12],
                media_duration=int(r[13]) if r[13] is not None else None,
                media_view_once=bool(r[14]) if r[14] is not None else False,
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
