"""Telethon capture: the 2nd authorized session.

Subscribes to the Updates stream and turns raw Telegram updates into append-only
events in the store, then fans them out to live WebSocket subscribers.

Skeleton status: the happy-path handlers (on_message / on_delete / on_edit) are
wired. The gap-sync-on-launch reconciliation (diff stored ID set vs refetched
slice to recover deletes lost to `differenceTooLong`) is stubbed with TODOs —
that is the delicate part called out in docs/ayugram-features.md.
"""
from __future__ import annotations

import asyncio
import logging

from telethon import TelegramClient, events
from telethon.errors import AuthKeyUnregisteredError, FloodWaitError
from telethon.sessions import StringSession

from .config import settings
from .db import store
from .models import EventKind, MessageEvent

log = logging.getLogger("capture")


class Capture:
    def __init__(self) -> None:
        self.client = TelegramClient(
            StringSession(settings.session_string),
            settings.api_id,
            settings.api_hash,
        )
        # Live subscribers (WebSocket). Populated by the API layer.
        self.subscribers: set[asyncio.Queue[MessageEvent]] = set()

    async def _publish(self, event: MessageEvent) -> None:
        for q in list(self.subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow client: it will catch up via REST gap-sync on reconnect.
                log.warning("subscriber queue full, dropping live push")

    def _register_handlers(self) -> None:
        @self.client.on(events.NewMessage)
        async def _on_message(ev: events.NewMessage.Event) -> None:
            # Caveat #1: store content NOW; the delete update won't carry it.
            text = ev.message.message or ""
            await store.put_content(
                ev.chat_id, ev.message.id, text, int(ev.message.date.timestamp())
            )

        @self.client.on(events.MessageEdited)
        async def _on_edit(ev: events.MessageEdited.Event) -> None:
            old_text, date = await store.get_content(ev.chat_id, ev.message.id)
            new_text = ev.message.message or ""
            await store.put_content(
                ev.chat_id, ev.message.id, new_text, int(ev.message.date.timestamp())
            )
            cursor = await store.append_event(
                EventKind.EDITED, ev.chat_id, ev.message.id, new_text, old_text, date
            )
            await self._publish(
                MessageEvent(
                    cursor=cursor, kind=EventKind.EDITED, chat_id=ev.chat_id,
                    message_id=ev.message.id, text=new_text, old_text=old_text, date=date,
                )
            )

        @self.client.on(events.MessageDeleted)
        async def _on_delete(ev: events.MessageDeleted.Event) -> None:
            # For DMs Telethon often can't resolve chat_id here (ev.chat_id is
            # None) — a known limitation, and UpdateDeleteMessages carries only
            # IDs. Non-channel message IDs are unique across a user's cloud
            # dialogs, so when chat_id is unknown we resolve BOTH the chat_id and
            # the text from the content store by message_id alone. Without this
            # the deleted text is lost — which defeats the whole feature.
            for mid in ev.deleted_ids:
                if ev.chat_id:
                    chat_id = ev.chat_id
                    text, date = await store.get_content(chat_id, mid)
                else:
                    chat_id, text, date = await store.resolve_by_mid(mid)
                    chat_id = chat_id or 0
                cursor = await store.append_event(
                    EventKind.DELETED, chat_id, mid, text, None, date
                )
                await self._publish(
                    MessageEvent(
                        cursor=cursor, kind=EventKind.DELETED, chat_id=chat_id,
                        message_id=mid, text=text, date=date,
                    )
                )

    async def _reconcile_on_launch(self) -> None:
        """Recover deletes missed during downtime.

        `getDifference` returns differenceTooLong after long gaps and does NOT
        replay individual deletes — only a new pts + current slice. So we must
        diff our stored ID set against a refetched slice per dialog and emit
        synthetic DELETED events for the gap.

        TODO: implement per-dialog slice refetch + ID-set diff. This is the
        scenario-C path from docs/ayugram-features.md and is the riskiest piece.
        """
        last_pts = await store.get_state("pts")
        log.info("launch reconcile: persisted pts=%s (diff-recovery not yet implemented)", last_pts)

    async def run(self) -> None:
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError(
                "Session not authorized. Run `python scripts/login.py` to create one."
            )
        self._register_handlers()
        await self._reconcile_on_launch()
        me = await self.client.get_me()
        log.info("capture authorized as id=%s; catching up on updates", getattr(me, "id", "?"))
        try:
            await self.client.catch_up()
        except Exception as e:  # noqa: BLE001
            log.warning("catch_up failed: %s", e)
        log.info("capture running; listening for deletes/edits")

        while True:
            try:
                await self.client.run_until_disconnected()
            except FloodWaitError as e:
                log.warning("FLOOD_WAIT: sleeping %ss", e.seconds)
                await asyncio.sleep(e.seconds)
            except AuthKeyUnregisteredError:
                # Session killed (likelier on datacenter IPs). Cannot self-heal.
                log.error("AUTH_KEY_UNREGISTERED — session revoked; re-run login.py")
                raise
            else:
                log.warning("disconnected; reconnecting")
                await asyncio.sleep(3)


capture = Capture()
