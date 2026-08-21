"""Client-facing API: REST gap-sync (on launch) + WebSocket live stream.

Auth is a shared CLIENT_TOKEN — this server is single-user (the account owner)
and lives on a home LAN / behind a tunnel, not a public multi-tenant service.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
from urllib.parse import quote

from fastapi import (
    Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect,
)
from fastapi.responses import Response, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .capture import capture
from .config import settings
from .crypto import decrypt_file_range
from .db import store
from .httprange import parse_range
from .models import GapSyncResponse

log = logging.getLogger("api")
app = FastAPI(title="iayugram-server")
_bearer = HTTPBearer(auto_error=False)


def _auth(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> None:
    if creds is None or creds.credentials != settings.client_token:
        raise HTTPException(status_code=401, detail="bad client token")


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    # session_authorized is the field that matters: the process can be perfectly healthy
    # while Telegram has revoked the companion session, in which case nothing is being
    # captured at all. The client treats a false here as a hard warning.
    return {"status": "ok", "session_authorized": capture.session_authorized}


def _storage_free_bytes() -> int | None:
    """Free space on the volume that holds captured media.

    Measured against media_dir rather than the process's cwd: that directory is what
    fills, and it is the one that may sit on a different volume. Best effort — a
    failure here must not take gap-sync down with it, since sync matters and the
    warning does not.
    """
    try:
        target = settings.media_dir if os.path.isdir(settings.media_dir) else "."
        return shutil.disk_usage(target).free
    except OSError:
        log.warning("could not read free space for %s", settings.media_dir, exc_info=True)
        return None


@app.get("/gap-sync", response_model=GapSyncResponse, dependencies=[Depends(_auth)])
async def gap_sync(since: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=2000)) -> GapSyncResponse:
    """Everything the client missed while offline, from its last cursor forward."""
    events = await store.events_after(since, limit)
    return GapSyncResponse(
        events=events,
        latest_cursor=await store.latest_cursor(),
        storage_free_bytes=_storage_free_bytes(),
    )


@app.get("/listened", dependencies=[Depends(_auth)])
async def get_listened(
    chat_id: int = Query(...), message_id: int = Query(...)
) -> dict[str, object]:
    """When the recipient first played our own voice/round message.

    Answered on demand rather than pushed through the event log: the client asks
    exactly once, when the message's context menu is opened, so there is nothing to
    stream and no client-side store to keep in sync. Query params (not a path) so
    negative chat_ids work, same as /media.

    listened_at is null when we never saw the update — including every message sent
    before this was deployed. The client falls back to Telegram's own read date
    there rather than claiming the message was never played.
    """
    return {"listened_at": await store.get_listened(chat_id, message_id)}


@app.get("/media", dependencies=[Depends(_auth)])
async def get_media(
    request: Request,
    chat_id: int = Query(...),
    message_id: int = Query(...),
    idx: int = Query(0, ge=0),
) -> Response:
    """Stream the decrypted bytes of a captured media file. Query params (not path)
    so negative chat_ids (channels/groups) work. TLS + token guard the transport.

    Phase 2: the response is streamed a chunk at a time and honours Range, so a
    large video neither loads into server memory nor has to be refetched from the
    start when a transfer is interrupted.

    `idx` picks a file within the message. It is 0 for everything except a purchased
    paid post, which is a single message carrying an album; the event's media_items
    lists which indices exist."""
    meta = await store.get_media(chat_id, message_id, idx)
    if meta is None:
        raise HTTPException(status_code=404, detail="media not found")

    path = store.media_full_path(meta)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="media file missing")

    mime = meta.mime or "application/octet-stream"
    rng = parse_range(request.headers.get("range"), meta.size)
    start, end = rng if rng else (0, meta.size - 1)

    def body():
        # Decryption is CPU work on a sync generator; StarletteResponse runs it in a
        # threadpool, so it doesn't block the event loop.
        yield from decrypt_file_range(path, start, end)

    headers = {
        "Content-Length": str(end - start + 1),
        "Accept-Ranges": "bytes",
    }
    if meta.file_name:
        headers["X-IAyu-File-Name"] = quote(meta.file_name)
    if rng:
        headers["Content-Range"] = f"bytes {start}-{end}/{meta.size}"

    return StreamingResponse(
        body(), status_code=206 if rng else 200, media_type=mime, headers=headers
    )


@app.websocket("/live")
async def live(ws: WebSocket) -> None:
    # WebSocket can't use the HTTP bearer dependency; check the token param.
    if ws.query_params.get("token") != settings.client_token:
        await ws.close(code=4401)
        return
    await ws.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    capture.subscribers.add(queue)
    log.info("live subscriber connected (%d total)", len(capture.subscribers))

    async def pump() -> None:
        """Forward captured events to this subscriber."""
        while True:
            event = await queue.get()
            await ws.send_text(event.model_dump_json())

    async def watch_peer() -> None:
        """Read from the socket purely to notice when it goes away.

        The client only ever sends WebSocket pings, which the protocol layer answers
        by itself, so this never yields an application message — but without someone
        awaiting receive(), a client's close frame is never observed. /live is silent
        between events, so pump() may not write for hours and would not fail either:
        the subscriber then leaks for the lifetime of the process (seen climbing to
        11 dead sockets, whose failed writes were the endless socket.send() spam).
        """
        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                return

    tasks = {asyncio.create_task(pump()), asyncio.create_task(watch_peer())}
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                log.warning("live subscriber ended with %r", exc)
    finally:
        capture.subscribers.discard(queue)
        log.info("live subscriber disconnected (%d left)", len(capture.subscribers))
