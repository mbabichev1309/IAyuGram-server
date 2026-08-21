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
import os
from collections import defaultdict

import time
from datetime import datetime

from telethon import TelegramClient, events, utils
from telethon.errors import AuthKeyUnregisteredError, FloodWaitError
from telethon.sessions import StringSession
from telethon.tl import types
from telethon.tl.custom.file import File

from .config import settings
from .db import store
from .models import EventKind, EventMediaItem, MediaMeta, MessageEvent

log = logging.getLogger("capture")


def _media_event_fields(
    media: MediaMeta | None, items: list[MediaMeta] | None = None
) -> dict:
    """Flatten stored media metadata into MessageEvent's media_* fields.

    `items` is every file of the message; it is carried separately only when there is
    more than one, which today means a purchased paid album. The flattened fields stay
    the first item either way, so a client that predates media_items shows the album's
    first photo rather than nothing.
    """
    if media is None:
        return {}
    fields: dict = {
        "media_kind": media.kind,
        "media_mime": media.mime,
        "media_size": media.size,
        "media_width": media.width,
        "media_height": media.height,
        "media_duration": media.duration,
        "media_view_once": media.view_once,
        "media_file_name": media.file_name,
    }
    if items and len(items) > 1:
        fields["media_items"] = [
            EventMediaItem(
                idx=item.idx, kind=item.kind, mime=item.mime, size=item.size,
                width=item.width, height=item.height, duration=item.duration,
                file_name=item.file_name,
            )
            for item in items
        ]
    return fields


class Capture:
    def __init__(self) -> None:
        self.client = TelegramClient(
            StringSession(settings.session_string),
            settings.api_id,
            settings.api_hash,
        )
        # Live subscribers (WebSocket). Populated by the API layer.
        self.subscribers: set[asyncio.Queue[MessageEvent]] = set()
        # Background launch-reconcile task (kept referenced so it isn't GC'd).
        self._reconcile_task: asyncio.Task | None = None
        # Same, for the launch re-check of paid posts that were locked when seen.
        self._paid_sweep_task: asyncio.Task | None = None
        # Media downloads still running, by (chat_id, message_id) — a delete that
        # lands mid-download waits on these. See _await_inflight_media.
        self._media_inflight: dict[tuple[int, int], asyncio.Task] = {}
        # Whether Telegram still accepts this session. Surfaced through /healthz so the
        # phone can tell "server is down" from "server is up and capturing nothing" —
        # the second looks healthy from outside and is the more dangerous of the two.
        self.session_authorized = False

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
                ev.chat_id, ev.message.id, text,
                int(ev.message.date.timestamp()), bool(ev.message.out),
                ev.message.sender_id,
            )
            if settings.media_capture:
                await self._capture_media_tracked(ev.message, ev.chat_id)

        @self.client.on(events.MessageEdited)
        async def _on_edit(ev: events.MessageEdited.Event) -> None:
            old_text, date, _, _ = await store.get_content(ev.chat_id, ev.message.id)
            new_text = ev.message.message or ""
            out = bool(ev.message.out)
            sender_id = ev.message.sender_id
            await store.put_content(
                ev.chat_id, ev.message.id, new_text,
                int(ev.message.date.timestamp()), out, sender_id,
            )
            cursor = await store.append_event(
                EventKind.EDITED, ev.chat_id, ev.message.id, new_text, old_text, date, out,
                sender_id,
            )
            media_fields = await self._media_fields(ev.chat_id, ev.message.id)
            await self._publish(
                MessageEvent(
                    cursor=cursor, kind=EventKind.EDITED, chat_id=ev.chat_id,
                    message_id=ev.message.id, text=new_text, old_text=old_text, date=date,
                    from_me=out, sender_id=sender_id, **media_fields,
                )
            )

        @self.client.on(events.Raw(types.UpdateReadMessagesContents))
        async def _on_contents_read(update: types.UpdateReadMessagesContents) -> None:
            """The recipient played one of our voice/round messages.

            This is the only signal that carries WHEN media was actually consumed.
            Telegram's own messages.getOutboxReadDate answers a different question —
            when the message was read — and for voice and round video that is not what
            anyone means by "listened". The iOS client throws the timestamp away
            entirely (ConsumableContentMessageAttribute stores one Bool), which is why
            it has to be captured here.

            The update carries message ids and no peer: non-channel ids are unique
            across a user's cloud dialogs, so the chat is resolved from the content
            store, the same way deletes already do it. `out` is what filters the two
            directions apart — the same update fires when WE consume someone else's
            media, and that is not interesting.

            `date` is optional in the schema; falling back to now is honest here,
            because the update is delivered as it happens on a live connection.
            """
            # Telethon hands `date` over already parsed into a datetime, not the raw TL
            # int — int() on it raises, which took the whole handler down until this was
            # caught in the journal. The field is also optional in the schema, and
            # falling back to now is honest: the update is delivered as it happens.
            raw_date = getattr(update, "date", None)
            if isinstance(raw_date, datetime):
                listened_at = int(raw_date.timestamp())
            elif raw_date:
                listened_at = int(raw_date)
            else:
                listened_at = int(time.time())
            for mid in update.messages:
                try:
                    chat_id, _, _, out, _ = await store.resolve_by_mid(mid)
                    if not chat_id:
                        # content is pruned after content_retention_hours (7 days), so a
                        # message played long after it was sent has no row left to
                        # resolve the chat from, and the mark would be dropped silently.
                        chat_id, out = await self._resolve_peer_from_telegram(mid)
                    if not chat_id or not out:
                        continue
                    if await store.put_listened(chat_id, mid, listened_at):
                        log.info("listened: chat=%s message=%s at=%s", chat_id, mid, listened_at)
                except Exception:  # noqa: BLE001 — one bad id must not drop the rest
                    log.exception("failed to record listened mark for %s", mid)

        @self.client.on(events.Raw(types.UpdateMessageExtendedMedia))
        async def _on_extended_media(update: types.UpdateMessageExtendedMedia) -> None:
            """A paid post was just unlocked — this is the one moment its files exist.

            Before purchase Telegram sends `messageExtendedMediaPreview`: width, height,
            a blurred thumbnail and nothing else, so there is nothing to capture and no
            way to fabricate it. The purchase happens on the phone, but it is recorded
            against the ACCOUNT, so this session gets the update too and can then
            download the real files. Missing this moment is not fatal — the launch sweep
            re-checks paid_pending — but it is the only path that captures instantly.

            The same update also carries bot-invoice extended media, hence the check
            that something in it is actually unlocked before spending an API call.
            """
            if not settings.media_capture:
                return
            unlocked = any(
                isinstance(item, types.MessageExtendedMedia)
                for item in (update.extended_media or [])
            )
            if not unlocked:
                return
            try:
                chat_id = utils.get_peer_id(update.peer)
                message = await self.client.get_messages(update.peer, ids=update.msg_id)
                if message is None or not getattr(message, "media", None):
                    log.info("paid unlock for msg %s: message no longer available",
                             update.msg_id)
                    return
                # The post may predate this server, or its content row may have been
                # pruned; store it now so a later delete still has text and a date.
                await store.put_content(
                    chat_id, message.id, message.message or "",
                    int(message.date.timestamp()), bool(message.out), message.sender_id,
                )
                log.info("paid post unlocked: chat=%s msg=%s", chat_id, message.id)
                await self._capture_media_tracked(message, chat_id)
            except Exception as e:  # noqa: BLE001 — never break the update stream
                log.warning("paid unlock handling failed for msg %s: %s", update.msg_id, e)

        @self.client.on(events.MessageDeleted)
        async def _on_delete(ev: events.MessageDeleted.Event) -> None:
            # For DMs Telethon often can't resolve chat_id here (ev.chat_id is
            # None) — a known limitation, and UpdateDeleteMessages carries only
            # IDs. Non-channel message IDs are unique across a user's cloud
            # dialogs, so when chat_id is unknown we resolve BOTH the chat_id and
            # the text from the content store by message_id alone. Without this
            # the deleted text is lost — which defeats the whole feature.
            for mid in ev.deleted_ids:
                # Guard each id so one bad message can't abort the whole delete batch.
                try:
                    if ev.chat_id:
                        chat_id = ev.chat_id
                        text, date, out, sender_id = await store.get_content(chat_id, mid)
                    else:
                        chat_id, text, date, out, sender_id = await store.resolve_by_mid(mid)
                        chat_id = chat_id or 0
                    if chat_id:
                        await self._await_inflight_media(chat_id, mid)
                    media_fields = (
                        await self._media_fields(chat_id, mid) if chat_id else {}
                    )
                    cursor = await store.append_event(
                        EventKind.DELETED, chat_id, mid, text, None, date, out, sender_id
                    )
                    await self._publish(
                        MessageEvent(
                            cursor=cursor, kind=EventKind.DELETED, chat_id=chat_id,
                            message_id=mid, text=text, date=date, from_me=out,
                            sender_id=sender_id, **media_fields,
                        )
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("delete handler failed for msg %s: %s", mid, e)

    async def _resolve_peer_from_telegram(self, message_id: int) -> tuple[int | None, bool]:
        """Which chat a bare message id belongs to, and whether we sent it.

        The fallback for when the content store has been pruned. messages.getMessages
        accepts an id with no peer for non-channel messages — the same uniqueness the
        delete path already leans on. Costs one API call, and only on a miss.
        """
        try:
            message = await self.client.get_messages(None, ids=message_id)
        except Exception as e:  # noqa: BLE001 — never let a lookup break the handler
            log.warning("could not resolve peer for msg %s: %s", message_id, e)
            return None, False
        if message is None:
            return None, False
        # A deleted or otherwise unavailable message comes back as MessageEmpty, which
        # has neither attribute.
        chat_id = getattr(message, "chat_id", None)
        out = bool(getattr(message, "out", False))
        return chat_id, out

    @staticmethod
    def _media_kind(message) -> str | None:
        """Classify a message's media. Order matters: a sticker and a round video are
        both documents, so the specific cases must come first."""
        if message.sticker:
            return "sticker"
        if message.photo:
            return "photo"
        if message.voice:
            return "voice"
        if message.video_note:
            return "round"
        if message.gif:  # animation (silent looping mp4)
            return "gif"
        if message.video:
            return "video"
        if message.audio:  # music, as opposed to a voice note
            return "audio"
        if message.document:
            return "document"
        return None

    @staticmethod
    def _paid_extended_media(message) -> list | None:
        """The extended-media list of a paid ("stars") post, or None if the message is
        not one.

        Telethon does not unwrap MessageMediaPaidMedia at all — message.photo,
        .video and .document all see straight past it — so _media_kind returns None
        for a paid post and, before this existed, even a post the account had PAID for
        was silently never captured.
        """
        media = getattr(message, "media", None)
        if isinstance(media, types.MessageMediaPaidMedia):
            return list(media.extended_media or [])
        return None

    @staticmethod
    def _purchased_inner(item):
        """The Photo/Document inside one paid-album item, or None while it is still
        locked.

        An unpurchased item is a `messageExtendedMediaPreview`: width, height, a
        blurred thumbnail and a duration. No document, no photo, no file reference —
        so there is literally nothing to download until the post is bought. That gate
        is Telegram's, not a client flag, and nothing here can work around it.
        """
        if not isinstance(item, types.MessageExtendedMedia):
            return None
        inner = getattr(item, "media", None)
        if isinstance(inner, types.MessageMediaPhoto) and isinstance(inner.photo, types.Photo):
            return inner.photo
        if isinstance(inner, types.MessageMediaDocument) and isinstance(
            inner.document, types.Document
        ):
            return inner.document
        return None

    @staticmethod
    def _raw_media_kind(inner) -> str:
        """Classify a bare Photo/Document the way _media_kind classifies a message.

        Same order and the same reason: a sticker and a round video are documents too.
        Paid posts carry photos and videos in practice, but the rest costs nothing.
        """
        if isinstance(inner, types.Photo):
            return "photo"
        attributes = list(getattr(inner, "attributes", None) or [])

        def attribute(cls):
            return next((a for a in attributes if isinstance(a, cls)), None)

        if attribute(types.DocumentAttributeSticker):
            return "sticker"
        audio = attribute(types.DocumentAttributeAudio)
        if audio is not None and getattr(audio, "voice", False):
            return "voice"
        video = attribute(types.DocumentAttributeVideo)
        if video is not None and getattr(video, "round_message", False):
            return "round"
        if attribute(types.DocumentAttributeAnimated):
            return "gif"
        if video is not None:
            return "video"
        if audio is not None:
            return "audio"
        return "document"

    @staticmethod
    async def _media_fields(chat_id: int, message_id: int) -> dict:
        """The media_* fields for an event: the message's first file, plus the whole
        album when it holds more than one (a purchased paid post)."""
        items = await store.get_media_items(chat_id, message_id)
        if not items:
            return {}
        return _media_event_fields(items[0], items)

    @staticmethod
    def _remove_temp(path: str | None) -> None:
        """Delete a plaintext temp file. It must never outlive the capture — on any
        path, including every failure."""
        if not path:
            return
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass

    async def _capture_media_tracked(self, message, chat_id: int) -> None:
        """Run the capture as a tracked task, so a delete for this same message can
        find and wait for the download instead of racing it."""
        key = (chat_id, message.id)
        task = asyncio.create_task(self._maybe_capture_media(message, chat_id))
        self._media_inflight[key] = task
        try:
            await task
        finally:
            self._media_inflight.pop(key, None)

    async def _await_inflight_media(self, chat_id: int, message_id: int) -> None:
        """Wait for this message's media download, if one is still running.

        A delete routinely lands mid-download: a 90 MB video takes far longer to
        fetch than it takes to hit "delete". Publishing the delete then loses the
        media metadata PERMANENTLY — the bytes do land moments later and gap-sync's
        LEFT JOIN would replay a corrected event, but the client dedups deletes by
        message_id and ignores it, leaving an empty bubble where the video was.
        """
        task = self._media_inflight.get((chat_id, message_id))
        if task is None or task.done():
            return
        log.info("delete for msg %s arrived mid-download; waiting for the media",
                 message_id)
        try:
            # shield: this waiter timing out must not cancel the download itself.
            await asyncio.wait_for(
                asyncio.shield(task), timeout=settings.media_download_timeout + 30
            )
        except asyncio.TimeoutError:
            log.warning("media for msg %s not stored in time; publishing the delete "
                        "without media metadata", message_id)
        except Exception as e:  # noqa: BLE001 — never block the delete on this
            log.warning("media capture for msg %s failed while a delete waited: %s",
                        message_id, e)

    async def _maybe_capture_media(self, message, chat_id: int) -> None:
        """Strategy A: download the media on receipt, encrypt and store it.
        Downloading never sends a read/consumed update, so view-once media is
        grabbed silently before it's opened on the phone. Wrapped so a media
        failure never breaks the capture stream.

        Phase 2: the file is streamed to a temp file and encrypted chunk-by-chunk,
        so a large video costs disk rather than server memory."""
        tmp_path: str | None = None
        try:
            paid_items = self._paid_extended_media(message)
            if paid_items is not None:
                # A paid post is one message holding an album, so it needs its own
                # loop and its own store rows — and, while it is still locked, nothing
                # to download at all.
                await self._capture_paid_media(message, chat_id, paid_items)
                return

            kind = self._media_kind(message)
            if kind is None:
                return

            f = message.file
            size = getattr(f, "size", None)
            if size is not None and size > settings.media_max_bytes:
                log.info("media msg %s (%s) skipped: %s bytes > limit",
                         message.id, kind, size)
                return

            tmp_path = await self._download_to_temp(
                message, chat_id, message.id, 0, kind, size
            )
            if tmp_path is None:
                return

            view_once = getattr(message.media, "ttl_seconds", None) is not None
            # duration comes as a float (seconds) for round/voice — store as int.
            raw_duration = getattr(f, "duration", None)
            duration = int(raw_duration) if raw_duration is not None else None
            stored = await store.put_media_file(
                chat_id, message.id, kind,
                getattr(f, "mime_type", None),
                getattr(f, "width", None), getattr(f, "height", None),
                duration, view_once, tmp_path,
                file_name=getattr(f, "name", None),
            )
            log.info("captured %s media for msg %s (%d bytes, view_once=%s)",
                     kind, message.id, stored, view_once)

            if view_once:
                # View-once media never fires MessageDeleted when consumed (that's a
                # read-contents update, not a delete), so the client would never learn
                # to materialize it. Since it WILL vanish and we already have the
                # bytes, preserve it immediately: emit a synthetic DELETED now so the
                # client inserts a permanent copy — before the original is even opened,
                # so the sender is never notified. The client dedups deletes by
                # message_id, so a later real delete can't duplicate it.
                media_fields = await self._media_fields(chat_id, message.id)
                caption = message.message or ""
                date = int(message.date.timestamp())
                out = bool(message.out)
                sender_id = message.sender_id
                cursor = await store.append_event(
                    EventKind.DELETED, chat_id, message.id, caption, None, date, out,
                    sender_id,
                )
                await self._publish(MessageEvent(
                    cursor=cursor, kind=EventKind.DELETED, chat_id=chat_id,
                    message_id=message.id, text=caption, date=date, from_me=out,
                    sender_id=sender_id, **media_fields,
                ))
                log.info("view-once %s preserved immediately (msg %s)", kind, message.id)
        except Exception as e:  # noqa: BLE001 — must never break the capture stream
            log.warning("media capture failed for msg %s: %s",
                        getattr(message, "id", "?"), e)
        finally:
            # The plaintext temp file must never outlive the capture, even on failure.
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    async def _download_to_temp(
        self, target, chat_id: int, message_id: int, idx: int, kind: str, size
    ) -> str | None:
        """Stream one media to a plaintext temp file and return where it actually
        landed, or None — having cleaned up after itself — if nothing usable did.

        `target` is the Message for ordinary media, or a bare Photo/Document for one
        item of a purchased paid album; download_media accepts both.
        """
        os.makedirs(settings.media_dir, exist_ok=True)
        # The ".part" matters: Telethon appends the media's own extension when the
        # name has none, so an extensionless temp lands somewhere we didn't name —
        # and then a download that times out or throws leaks a PLAINTEXT file the
        # cleanup can't see. With an extension the name is kept as given.
        suffix = "" if idx == 0 else f"_{idx}"
        tmp_path = os.path.join(
            settings.media_dir, f".incoming_{chat_id}_{message_id}{suffix}.part"
        )
        # Telethon streams to a path without buffering the whole file.
        # A cross-DC download goes through a borrowed sender, which has been seen
        # to connect and then never transfer anything — so it must be bounded, or
        # the media is lost with nothing in the log to say why.
        log.info("downloading %s media for msg %s[%d] (%s bytes) -> %s",
                 kind, message_id, idx, size, tmp_path)
        try:
            written = await asyncio.wait_for(
                self.client.download_media(target, file=tmp_path),
                timeout=settings.media_download_timeout,
            )
        except asyncio.TimeoutError:
            log.warning("media download timed out after %ss: msg %s[%d] (%s, %s bytes)",
                        settings.media_download_timeout, message_id, idx, kind, size)
            self._remove_temp(tmp_path)
            return None
        # Telethon returns where it actually wrote, which is not always the name we
        # asked for: it appends an extension when there is none, and side-steps to
        # "name (1).part" if our temp already exists. Adopt the returned path, both
        # to read it and so the cleanup deletes the right (plaintext) file.
        if isinstance(written, str):
            tmp_path = written
        if not written or not os.path.exists(tmp_path):
            log.warning("media download produced nothing: msg %s[%d] (%s) "
                        "returned=%r exists=%s",
                        message_id, idx, kind, written, os.path.exists(tmp_path))
            self._remove_temp(tmp_path)
            return None
        return tmp_path

    async def _capture_paid_media(self, message, chat_id: int, items: list) -> None:
        """Store the files of a paid post, one media row per album position.

        Only what the account has PAID for can be stored: a locked item carries a
        blurred preview and no file reference, so there is nothing to fetch and no
        client-side trick that changes that. An unpurchased post is therefore only
        remembered in paid_pending and looked at again when it unlocks — which is
        what makes the bought post survive the channel deleting it later.
        """
        purchased: list[tuple[int, object]] = []
        for idx, item in enumerate(items):
            inner = self._purchased_inner(item)
            if inner is not None:
                purchased.append((idx, inner))

        if not purchased:
            await store.put_paid_pending(chat_id, message.id)
            log.info("paid post msg %s is locked (%d items); remembered for re-check",
                     message.id, len(items))
            return

        for idx, inner in purchased:
            if await store.get_media(chat_id, message.id, idx) is not None:
                continue  # already captured: a re-check, or a repeated unlock update
            f = File(inner)
            size = f.size
            if size is not None and size > settings.media_max_bytes:
                log.info("paid media msg %s[%d] skipped: %s bytes > limit",
                         message.id, idx, size)
                continue
            kind = self._raw_media_kind(inner)
            tmp_path = await self._download_to_temp(
                inner, chat_id, message.id, idx, kind, size
            )
            if tmp_path is None:
                continue
            try:
                raw_duration = f.duration
                stored = await store.put_media_file(
                    chat_id, message.id, kind, f.mime_type, f.width, f.height,
                    int(raw_duration) if raw_duration is not None else None,
                    False, tmp_path, file_name=f.name, idx=idx,
                )
                log.info("captured paid %s media for msg %s[%d] (%d bytes)",
                         kind, message.id, idx, stored)
            finally:
                self._remove_temp(tmp_path)

        if len(purchased) == len(items):
            # Nothing in this post is locked any more, so there is nothing left to
            # come back for. Items skipped for size stay skipped — the re-check exists
            # to catch a purchase, not to retry a download the config refused.
            await store.drop_paid_pending(chat_id, message.id)
        else:
            await store.put_paid_pending(chat_id, message.id)

    async def _sweep_paid_pending(self) -> None:
        """Re-check paid posts that were still locked when we saw them.

        Covers the two cases the live unlock update cannot: a purchase made while this
        server was down (StringSession persists no pts, so nothing is ever replayed),
        and an update lost to a reconnect. Runs at launch beside the delete reconcile,
        bounded by paid_recheck_max refetches.
        """
        if not settings.media_capture:
            return
        rows = await store.paid_pending_rows(settings.paid_recheck_max)
        if not rows:
            log.info("paid sweep: nothing pending")
            return

        by_chat: dict[int, list[int]] = defaultdict(list)
        for chat_id, message_id in rows:
            by_chat[chat_id].append(message_id)

        checked = captured = 0
        for chat_id, ids in by_chat.items():
            try:
                entity = await self.client.get_input_entity(chat_id)
            except Exception as e:  # noqa: BLE001 — entity may be unresolvable
                log.warning("paid sweep: can't resolve chat %s (%s); skipping %d posts",
                            chat_id, e, len(ids))
                continue
            for i in range(0, len(ids), 100):  # getMessages accepts <=100 ids
                batch = ids[i:i + 100]
                try:
                    messages = await self.client.get_messages(entity, ids=batch)
                except FloodWaitError as e:
                    log.warning("paid sweep FLOOD_WAIT %ss — stopping early "
                                "(checked=%d captured=%d)", e.seconds, checked, captured)
                    return
                except Exception as e:  # noqa: BLE001
                    log.warning("paid sweep getMessages failed for chat %s: %s", chat_id, e)
                    break
                for mid, message in zip(batch, messages):
                    checked += 1
                    try:
                        paid_items = (
                            self._paid_extended_media(message)
                            if message is not None else None
                        )
                        if paid_items is None:
                            # Deleted, or no longer a paid post: nothing to wait for.
                            await store.drop_paid_pending(chat_id, mid)
                            continue
                        before = len(await store.get_media_items(chat_id, mid))
                        await self._capture_paid_media(message, chat_id, paid_items)
                        if len(await store.get_media_items(chat_id, mid)) > before:
                            captured += 1
                    except Exception as e:  # noqa: BLE001 — one post can't stop the sweep
                        log.warning("paid sweep failed for msg %s: %s", mid, e)

        log.info("paid sweep done: checked=%d newly_captured=%d", checked, captured)

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

        by_chat: dict[int, list[tuple[int, str | None, int | None, bool, int | None]]] = defaultdict(list)
        for chat_id, mid, text, date, out, sender_id in candidates:
            by_chat[chat_id].append((mid, text, date, out, sender_id))

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

            info = {
                mid: (text, date, out, sender_id)
                for mid, text, date, out, sender_id in items
            }
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
                    text, date, out, sender_id = info[mid]
                    media_fields = await self._media_fields(chat_id, mid)
                    cursor = await store.append_event(
                        EventKind.DELETED, chat_id, mid, text, None, date, out, sender_id
                    )
                    await self._publish(MessageEvent(
                        cursor=cursor, kind=EventKind.DELETED, chat_id=chat_id,
                        message_id=mid, text=text, date=date, from_me=out,
                        sender_id=sender_id, **media_fields,
                    ))
                    recovered += 1

        log.info("launch reconcile done: checked=%d recovered_deletes=%d skipped_chats=%d",
                 checked, recovered, skipped_chats)

    async def run(self) -> None:
        await self.client.connect()
        if not await self.client.is_user_authorized():
            # Deliberately NOT fatal. Raising here takes down the whole process (the
            # entrypoint gathers this with uvicorn), systemd restarts it, and it dies
            # the same way — a crash loop in which /healthz never answers, so the phone
            # sees only "unreachable" and cannot tell that capture is the thing that
            # broke. Stay up, serve the stored archive, and report the real reason.
            log.error(
                "Session not authorized — capture is DOWN. Regenerate it with "
                "`python scripts/tdata_to_session.py` (see README)."
            )
            self.session_authorized = False
            return
        self.session_authorized = True
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

        # Run the launch reconcile in the BACKGROUND so live capture starts
        # immediately. The scan does getMessages over up to reconcile_max_messages
        # and can flood-wait for minutes; blocking on it would delay live deletes/
        # media capture on every restart. It shares the client, yielding on its
        # awaits/sleeps so the update stream keeps flowing.
        self._reconcile_task = asyncio.create_task(self._reconcile_on_launch())

        def _reconcile_done(task: asyncio.Task) -> None:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:  # noqa: BLE001
                log.warning("background reconcile failed: %s", e)

        self._reconcile_task.add_done_callback(_reconcile_done)

        # Same treatment for the paid-post re-check: it refetches messages and can
        # flood-wait, and live capture must not wait behind it.
        self._paid_sweep_task = asyncio.create_task(self._sweep_paid_pending())

        def _paid_sweep_done(task: asyncio.Task) -> None:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:  # noqa: BLE001
                log.warning("background paid sweep failed: %s", e)

        self._paid_sweep_task.add_done_callback(_paid_sweep_done)

        log.info("capture running; listening for deletes/edits (reconcile in background)")

        while True:
            try:
                await self.client.run_until_disconnected()
            except FloodWaitError as e:
                log.warning("FLOOD_WAIT: sleeping %ss", e.seconds)
                await asyncio.sleep(e.seconds)
            except AuthKeyUnregisteredError:
                # Session killed (likelier on datacenter IPs). Cannot self-heal — but
                # stay up rather than crash-looping, so /healthz can tell the phone that
                # the session is what died. See the note in the authorization check.
                log.error("AUTH_KEY_UNREGISTERED — session revoked; re-run login.py")
                self.session_authorized = False
                return
            else:
                log.warning("disconnected; reconnecting")
                await asyncio.sleep(3)


capture = Capture()
