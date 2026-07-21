"""Telethon capture: the 2nd authorized session.

Subscribes to the Updates stream and turns raw Telegram updates into append-only
events in the store, then fans them out to live WebSocket subscribers.

Live handlers (on_message / on_delete / on_edit) capture everything while the
server is up. `_reconcile_on_launch` recovers deletes that happened while it was
DOWN — mandatory because StringSession doesn't persist Telethon's update pts, so
catch_up() can't replay a gap. See docs/ayugram-features.md.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

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
        """Recover deletes missed while the server was down.

        StringSession doesn't persist Telethon's update pts across restarts, so
        catch_up() can't replay a gap — this scan is the only recovery path. We
        take every stored message with no recorded delete and ask the server
        whether it still exists: `messages.getMessages` by ID returns None for
        deleted/absent messages. Anything gone → emit a synthetic DELETED event
        from the stored content. Verifying by ID (rather than diffing a refetched
        recent slice) avoids false positives from messages that merely scrolled
        out of a window.
        """
        if not settings.reconcile_on_launch:
            log.info("launch reconcile: disabled by config")
            return

        cap = settings.reconcile_max_messages
        candidates = await store.candidates_for_reconcile(cap)
        if not candidates:
            log.info("launch reconcile: nothing to verify")
            return
        if len(candidates) >= cap:
            log.warning("launch reconcile: capped at %d candidates — older stored "
                        "messages NOT checked this run", cap)

        by_chat: dict[int, list[tuple[int, str | None, int | None]]] = defaultdict(list)
        for chat_id, mid, text, date in candidates:
            by_chat[chat_id].append((mid, text, date))

        checked = recovered = skipped_chats = 0
        for chat_id, items in by_chat.items():
            if not chat_id:  # chat_id=0: DM delete we couldn't attribute; no entity
                skipped_chats += 1
                continue
            try:
                entity = await self.client.get_input_entity(chat_id)
            except Exception as e:  # noqa: BLE001 — entity may be unresolvable
                log.warning("reconcile: can't resolve chat %s (%s); skipping %d msgs",
                            chat_id, e, len(items))
                skipped_chats += 1
                continue

            info = {mid: (text, date) for mid, text, date in items}
            ids = list(info)
            for i in range(0, len(ids), 100):  # getMessages accepts <=100 ids
                batch = ids[i:i + 100]
                try:
                    msgs = await self.client.get_messages(entity, ids=batch)
                except FloodWaitError as e:
                    log.warning("reconcile FLOOD_WAIT %ss — stopping early "
                                "(checked=%d recovered=%d)", e.seconds, checked, recovered)
                    return
                except Exception as e:  # noqa: BLE001
                    log.warning("reconcile getMessages failed for chat %s: %s", chat_id, e)
                    break
                checked += len(batch)
                for mid, msg in zip(batch, msgs):
                    if msg is not None:
                        continue  # still exists
                    if await store.has_delete_event(chat_id, mid):
                        continue  # already recorded
                    text, date = info[mid]
                    cursor = await store.append_event(
                        EventKind.DELETED, chat_id, mid, text, None, date
                    )
                    await self._publish(MessageEvent(
                        cursor=cursor, kind=EventKind.DELETED, chat_id=chat_id,
                        message_id=mid, text=text, date=date,
                    ))
                    recovered += 1

        log.info("launch reconcile done: checked=%d recovered_deletes=%d skipped_chats=%d",
                 checked, recovered, skipped_chats)

    async def run(self) -> None:
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError(
                "Session not authorized. Regenerate it with "
                "`python scripts/tdata_to_session.py` (see README)."
            )
        self._register_handlers()
        me = await self.client.get_me()
        log.info("capture authorized as id=%s", getattr(me, "id", "?"))

        # StringSession persists no entity cache — warm it so reconcile's
        # get_input_entity / get_messages can resolve chats by id.
        try:
            dialogs = await self.client.get_dialogs()
            log.info("warmed entity cache: %d dialogs", len(dialogs))
        except Exception as e:  # noqa: BLE001
            log.warning("get_dialogs (cache warm) failed: %s", e)

        await self._reconcile_on_launch()
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
