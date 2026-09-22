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
from .models import EventKind, EventMediaItem, MediaMeta, MessageEvent

_SCHEMA = """
CREATE TABLE IF NOT EXISTS content (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    body        BLOB,               -- Fernet-encrypted text
    date        INTEGER,            -- original send date (unix s)
    out         INTEGER NOT NULL DEFAULT 0,  -- 1 = sent by the account owner
    sender_id   INTEGER,            -- marked peer id of the sender (NULL if unknown)
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
    sender_id   INTEGER,            -- marked peer id of the sender (NULL if unknown)
    created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- When the recipient FIRST played one of our own voice/round messages. Telegram
-- itself only exposes when a message was read (messages.getOutboxReadDate), which
-- for other message types is a different fact and for these two is not the one the
-- user wants. The signal is UpdateReadMessagesContents, whose timestamp nothing
-- persists — the client's ConsumableContentMessageAttribute keeps a bare Bool.
-- First only: INSERT OR IGNORE, never updated, so a replay cannot move the time.
CREATE TABLE IF NOT EXISTS listened (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    listened_at INTEGER NOT NULL,   -- unix seconds, from the update when it carries one
    PRIMARY KEY (chat_id, message_id)
);
CREATE TABLE IF NOT EXISTS media (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    idx         INTEGER NOT NULL DEFAULT 0,  -- position within the message (paid albums)
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
    PRIMARY KEY (chat_id, message_id, idx)
);
-- Paid posts (messageMediaPaidMedia) seen while still locked. Before purchase the
-- server sends only messageExtendedMediaPreview -- a blurred thumbnail with no file
-- reference -- so there is nothing to capture yet. The row is the reminder to look
-- again: unlocking arrives as UpdateMessageExtendedMedia, and a purchase made while
-- this server was down produces no update at all, so the launch sweep re-checks
-- these and drops the row once the media is stored (or the post is gone).
CREATE TABLE IF NOT EXISTS paid_pending (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    seen_at     INTEGER NOT NULL,
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
        # Migration: record who sent each message, so the client can attribute a
        # preserved group message to its actual author instead of to the group. Rows
        # captured before this stay NULL — their sender is not recoverable, since the
        # original message is gone by the time anyone asks.
        for table in ("content", "events"):
            try:
                await self._db.execute(f"ALTER TABLE {table} ADD COLUMN sender_id INTEGER")
            except Exception:  # noqa: BLE001 — column already exists
                pass
        # Migration: media is keyed by (chat, message, idx) since paid albums --
        # one message, up to 10 files. sqlite cannot alter a primary key, so the
        # table is rebuilt; existing rows are their message's only file, i.e. idx 0,
        # and their encrypted files keep their names (idx 0 has no suffix).
        async with self._db.execute("PRAGMA table_info(media)") as cur:
            media_columns = {row[1] for row in await cur.fetchall()}
        if media_columns and "idx" not in media_columns:
            await self._db.executescript(
                """
                CREATE TABLE media_v2 (
                    chat_id     INTEGER NOT NULL,
                    message_id  INTEGER NOT NULL,
                    idx         INTEGER NOT NULL DEFAULT 0,
                    kind        TEXT NOT NULL,
                    mime        TEXT,
                    size        INTEGER NOT NULL,
                    width       INTEGER,
                    height      INTEGER,
                    duration    INTEGER,
                    view_once   INTEGER NOT NULL DEFAULT 0,
                    path        TEXT NOT NULL,
                    seen_at     INTEGER NOT NULL,
                    file_name   TEXT,
                    PRIMARY KEY (chat_id, message_id, idx)
                );
                INSERT INTO media_v2(chat_id, message_id, idx, kind, mime, size, width,
                                     height, duration, view_once, path, seen_at, file_name)
                SELECT chat_id, message_id, 0, kind, mime, size, width, height, duration,
                       view_once, path, seen_at, file_name FROM media;
                DROP TABLE media;
                ALTER TABLE media_v2 RENAME TO media;
                """
            )
            # The rebuild dropped the table's indexes with it; the loop below recreates
            # them, which is why this has to run before it.

        # Indexes. Without them the launch reconcile is a nested full scan: its
        # NOT EXISTS correlates content against events, and with no index on either
        # side that is rows(content) x rows(events) comparisons — measured at 24k x
        # 165k, which pinned a core for minutes on every single start. The API is
        # unresponsive for that whole time (aiosqlite serializes on one worker
        # thread) while redeploy.sh happily reports "healthy", because /healthz is
        # the one endpoint that never touches the database.
        #
        # events(message_id, kind) serves that subquery; content(message_id) serves
        # resolve_by_mid, which every delete and every consumed-media update calls,
        # and which the composite primary key cannot answer because message_id is
        # its second column. The seen_at pair is for the hourly prune.
        for statement in (
            "CREATE INDEX IF NOT EXISTS idx_events_mid_kind ON events(message_id, kind)",
            # Replaying a time window (the client's forced re-sync) filters on
            # created_at across the whole log, which is otherwise a full scan.
            "CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_content_mid ON content(message_id)",
            "CREATE INDEX IF NOT EXISTS idx_content_seen_at ON content(seen_at)",
            "CREATE INDEX IF NOT EXISTS idx_media_seen_at ON media(seen_at)",
        ):
            await self._db.execute(statement)
        await self._db.commit()

    async def checkpoint(self) -> None:
        """Fold the WAL back into the database and truncate the file.

        SQLite's automatic checkpoint is PASSIVE: it reuses the WAL from the start
        but never shrinks it, so the file sits at its high-water mark — measured at
        375 MiB against a 19 MB database. A clean close would collapse it, but the
        service never gets one (uvicorn waits on the live WebSocket until systemd
        SIGKILLs it), so it has to be done explicitly.
        """
        await self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Store not opened"
        return self._db

    # --- content store -----------------------------------------------------
    async def put_content(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        date: int,
        out: bool = False,
        sender_id: int | None = None,
    ) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO content(chat_id, message_id, body, date, out, sender_id, seen_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                chat_id,
                message_id,
                encrypt(text),
                date,
                1 if out else 0,
                sender_id,
                int(time.time()),
            ),
        )
        await self.db.commit()

    async def get_content(
        self, chat_id: int, message_id: int
    ) -> tuple[str | None, int | None, bool, int | None]:
        async with self.db.execute(
            "SELECT body, date, out, sender_id FROM content WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None, None, False, None
        text = decrypt(row[0]) if row[0] is not None else None
        return text, row[1], bool(row[2]), row[3]

    async def resolve_by_mid(
        self, message_id: int
    ) -> tuple[int | None, str | None, int | None, bool, int | None]:
        """Look up content by message_id alone — for DM/cloud-chat deletes where
        Telethon can't give us chat_id (UpdateDeleteMessages carries only IDs, and
        non-channel message IDs are unique across all of a user's cloud dialogs).
        Returns (chat_id, text, date, out, sender_id), or all-empty if unknown."""
        async with self.db.execute(
            "SELECT chat_id, body, date, out, sender_id FROM content WHERE message_id=? "
            "ORDER BY seen_at DESC LIMIT 1",
            (message_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None, None, None, False, None
        chat_id, body, date, out, sender_id = row
        return (
            chat_id,
            (decrypt(body) if body is not None else None),
            date,
            bool(out),
            sender_id,
        )

    async def candidates_for_reconcile(
        self, limit: int
    ) -> list[tuple[int, int, str | None, int | None, bool, int | None]]:
        """Stored messages that have NO recorded delete yet — the set to verify
        against the server on launch. Returns (chat_id, message_id, text, date, out,
        sender_id), newest first, capped at `limit`. A delete event may have been stored
        with chat_id=0 (DM limitation), so match on message_id OR the exact chat."""
        async with self.db.execute(
            "SELECT c.chat_id, c.message_id, c.body, c.date, c.out, c.sender_id "
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
            (r[0], r[1], decrypt(r[2]) if r[2] is not None else None, r[3], bool(r[4]), r[5])
            for r in rows
        ]

    async def has_delete_event(self, chat_id: int, message_id: int) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM events WHERE kind='deleted' AND message_id=? "
            "AND (chat_id=? OR chat_id=0) LIMIT 1",
            (message_id, chat_id),
        ) as cur:
            return await cur.fetchone() is not None

    # --- listened marks -----------------------------------------------------
    async def put_listened(self, chat_id: int, message_id: int, listened_at: int) -> bool:
        """Record the FIRST time our own media message was played. Returns whether
        this was new — OR IGNORE means a later update for the same message cannot
        overwrite the original moment, which is the whole point of the feature."""
        cur = await self.db.execute(
            "INSERT OR IGNORE INTO listened(chat_id, message_id, listened_at) VALUES (?,?,?)",
            (chat_id, message_id, listened_at),
        )
        await self.db.commit()
        return cur.rowcount > 0

    async def get_listened(self, chat_id: int, message_id: int) -> int | None:
        async with self.db.execute(
            "SELECT listened_at FROM listened WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else None

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
        idx: int = 0,
    ) -> int:
        """Register media that was streamed to `src_path`, encrypting it into the
        media dir chunk-by-chunk. Returns the plaintext size. Used for everything in
        phase 2 — nothing is ever held whole in memory.

        `idx` is the file's position in the message; only a purchased paid album ever
        goes past 0. Item 0 keeps the historical file name, so media captured before
        albums existed stays reachable."""
        os.makedirs(settings.media_dir, exist_ok=True)
        rel = f"{chat_id}_{message_id}.enc" if idx == 0 else f"{chat_id}_{message_id}_{idx}.enc"
        full = os.path.join(settings.media_dir, rel)
        size = await asyncio.to_thread(
            encrypt_file, src_path, full, settings.media_chunk_bytes
        )
        await self.db.execute(
            "INSERT OR REPLACE INTO media(chat_id, message_id, idx, kind, mime, size, "
            "width, height, duration, view_once, path, seen_at, file_name) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (chat_id, message_id, idx, kind, mime, size, width, height, duration,
             1 if view_once else 0, rel, int(time.time()), file_name),
        )
        await self.db.commit()
        return size

    def media_full_path(self, meta: MediaMeta) -> str:
        return os.path.join(settings.media_dir, meta.path)

    _MEDIA_COLUMNS = (
        "kind, mime, size, width, height, duration, view_once, path, file_name, idx"
    )

    @staticmethod
    def _media_meta(row: tuple) -> MediaMeta:
        return MediaMeta(
            kind=row[0], mime=row[1], size=row[2], width=row[3], height=row[4],
            # duration may have been stored as a float (round/voice seconds).
            duration=int(row[5]) if row[5] is not None else None,
            view_once=bool(row[6]), path=row[7], file_name=row[8], idx=row[9],
        )

    async def get_media(
        self, chat_id: int, message_id: int, idx: int = 0
    ) -> MediaMeta | None:
        async with self.db.execute(
            f"SELECT {self._MEDIA_COLUMNS} FROM media "
            "WHERE chat_id=? AND message_id=? AND idx=?",
            (chat_id, message_id, idx),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return self._media_meta(row)

    async def get_media_items(self, chat_id: int, message_id: int) -> list[MediaMeta]:
        """Every captured file of one message, in album order. One element for
        ordinary media; several only for a purchased paid post."""
        async with self.db.execute(
            f"SELECT {self._MEDIA_COLUMNS} FROM media "
            "WHERE chat_id=? AND message_id=? ORDER BY idx ASC",
            (chat_id, message_id),
        ) as cur:
            rows = await cur.fetchall()
        return [self._media_meta(row) for row in rows]

    # --- paid posts awaiting purchase --------------------------------------
    async def put_paid_pending(self, chat_id: int, message_id: int) -> None:
        await self.db.execute(
            "INSERT OR IGNORE INTO paid_pending(chat_id, message_id, seen_at) VALUES (?,?,?)",
            (chat_id, message_id, int(time.time())),
        )
        await self.db.commit()

    async def drop_paid_pending(self, chat_id: int, message_id: int) -> None:
        await self.db.execute(
            "DELETE FROM paid_pending WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        )
        await self.db.commit()

    async def paid_pending_rows(self, limit: int) -> list[tuple[int, int]]:
        """Locked paid posts to re-check, newest first."""
        async with self.db.execute(
            "SELECT chat_id, message_id FROM paid_pending ORDER BY seen_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [(row[0], row[1]) for row in await cur.fetchall()]

    async def prune_paid_pending(self) -> int:
        """Drop reminders older than the media retention window. A post bought that
        long after it appeared is not what this table is for, and the sweep should not
        keep re-checking it forever."""
        cutoff = int(time.time()) - settings.media_retention_hours * 3600
        cur = await self.db.execute("DELETE FROM paid_pending WHERE seen_at < ?", (cutoff,))
        await self.db.commit()
        return cur.rowcount

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
        sender_id: int | None = None,
    ) -> int:
        cur = await self.db.execute(
            "INSERT INTO events(kind, chat_id, message_id, body, old_body, date, out, sender_id, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                kind.value,
                chat_id,
                message_id,
                encrypt(text) if text is not None else None,
                encrypt(old_text) if old_text is not None else None,
                date,
                1 if out else 0,
                sender_id,
                int(time.time()),
            ),
        )
        await self.db.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def _extra_media_items(
        self, pairs: set[tuple[int, int]]
    ) -> dict[tuple[int, int], list[EventMediaItem]]:
        """The 2nd..Nth files of the given messages, keyed by (chat, message).

        Only a purchased paid album ever has any, so the common case is an empty dict
        and one cheap query. Matching on the (chat_id, message_id) pair keeps the
        media primary key usable as an index; a bare `message_id IN (…)` could not.
        """
        if not pairs:
            return {}
        placeholders = ",".join("(?,?)" for _ in pairs)
        params: list[int] = []
        for chat_id, message_id in pairs:
            params.extend((chat_id, message_id))
        async with self.db.execute(
            f"SELECT chat_id, message_id, {self._MEDIA_COLUMNS} FROM media "
            f"WHERE idx > 0 AND (chat_id, message_id) IN (VALUES {placeholders}) "
            "ORDER BY idx ASC",
            params,
        ) as c:
            rows = await c.fetchall()
        extras: dict[tuple[int, int], list[EventMediaItem]] = {}
        for row in rows:
            meta = self._media_meta(row[2:])
            extras.setdefault((row[0], row[1]), []).append(
                EventMediaItem(
                    idx=meta.idx, kind=meta.kind, mime=meta.mime, size=meta.size,
                    width=meta.width, height=meta.height, duration=meta.duration,
                    file_name=meta.file_name,
                )
            )
        return extras

    async def events_after(
        self, cursor: int, limit: int = 500, since_ts: int | None = None
    ) -> list[MessageEvent]:
        # LEFT JOIN media so replayed events carry media metadata too (derived from
        # the media table, not duplicated into events; gone if the media was pruned).
        # Pinned to idx 0: a message can now hold several files (paid album), and an
        # unrestricted join would return the same event once per file.
        async with self.db.execute(
            "SELECT e.cursor, e.kind, e.chat_id, e.message_id, e.body, e.old_body, e.date, e.out, "
            "       m.kind, m.mime, m.size, m.width, m.height, m.duration, m.view_once, "
            "       m.file_name, e.sender_id, m.idx "
            "FROM events e "
            "LEFT JOIN media m ON m.chat_id = e.chat_id AND m.message_id = e.message_id "
            "                 AND m.idx = 0 "
            "WHERE e.cursor > ? AND (? IS NULL OR e.created_at >= ?) "
            "ORDER BY e.cursor ASC LIMIT ?",
            (cursor, since_ts, since_ts, limit),
        ) as c:
            rows = await c.fetchall()
        extras = await self._extra_media_items(
            {(r[2], r[3]) for r in rows if r[8] is not None}
        )
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
                media_file_name=r[15],
                # Appended at the end of the SELECT on purpose: inserting it next to the
                # other event columns would shift every media index below it.
                sender_id=r[16],
                media_items=(
                    [
                        EventMediaItem(
                            idx=0, kind=r[8], mime=r[9], size=r[10], width=r[11],
                            height=r[12],
                            duration=int(r[13]) if r[13] is not None else None,
                            file_name=r[15],
                        )
                    ]
                    + extras[(r[2], r[3])]
                    if (r[2], r[3]) in extras and r[8] is not None
                    else None
                ),
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
