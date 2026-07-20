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


class GapSyncResponse(BaseModel):
    """Response to REST gap-sync: everything after the client's last cursor."""

    events: list[MessageEvent]
    latest_cursor: int
