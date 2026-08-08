"""Protocol contract shared with the iOS client.

These schemas ARE the wire format for both the WebSocket live stream and the REST
gap-sync endpoints. Keep them in sync with the client side (see the client repo's
docs/ayugram-features.md). If this grows, extract to a shared schema package.
"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class EventKind(str, Enum):
    DELETED = "deleted"
    EDITED = "edited"


class MediaMeta(BaseModel):
    """Metadata for a captured media file. `path` is server-internal (never sent
    to the client) — the client fetches bytes from GET /media by chat+message id."""

    kind: str  # photo|sticker|voice|round|video|gif|audio|document
    mime: str | None = None
    size: int
    width: int | None = None
    height: int | None = None
    duration: int | None = None
    view_once: bool = False
    path: str = ""
    file_name: str | None = None  # original document name, when there is one


class MessageEvent(BaseModel):
    """One append-only event in the log. `cursor` is monotonic per server."""

    cursor: int
    kind: EventKind
    chat_id: int
    message_id: int
    # Snapshot of content as the server last knew it (decrypted for the client).
    # For DELETED this is the pre-delete content; for EDITED, the new content.
    text: str | None = None
    # Original send date (unix seconds), so scenario-C synthetic inserts land in
    # the right place in the client's Postbox timeline.
    date: int | None = None
    # Prior text for edits, if we had it stored.
    old_text: str | None = None
    # True if the message was sent by the account owner (outgoing) — so the client
    # renders the preserved copy on the correct side.
    from_me: bool = False
    # Who actually sent it, as a marked peer id (Telethon's convention: positive for
    # users, -100… for channels). from_me alone only says "mine or not", which is enough
    # for a DM but leaves a group message with no author, so the client had to attribute
    # it to the group itself. None for rows captured before this field existed.
    sender_id: int | None = None
    # Media metadata (if the message carried captured photo/voice/round). The
    # client fetches the bytes from GET /media?chat_id=..&message_id=.. . These
    # are flattened (not a nested object) to keep the client's Codable simple.
    media_kind: str | None = None
    media_mime: str | None = None
    media_size: int | None = None
    media_width: int | None = None
    media_height: int | None = None
    media_duration: int | None = None
    media_view_once: bool = False
    media_file_name: str | None = None


class GapSyncResponse(BaseModel):
    """Response to REST gap-sync: everything after the client's last cursor."""

    events: list[MessageEvent]
    latest_cursor: int
