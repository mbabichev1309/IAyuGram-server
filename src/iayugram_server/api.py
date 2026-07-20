"""Client-facing API: REST gap-sync (on launch) + WebSocket live stream.

Auth is a shared CLIENT_TOKEN — this server is single-user (the account owner)
and lives on a home LAN / behind a tunnel, not a public multi-tenant service.
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .capture import capture
from .config import settings
from .db import store
from .models import GapSyncResponse

log = logging.getLogger("api")
app = FastAPI(title="iayugram-server")
_bearer = HTTPBearer(auto_error=False)


def _auth(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> None:
    if creds is None or creds.credentials != settings.client_token:
        raise HTTPException(status_code=401, detail="bad client token")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/gap-sync", response_model=GapSyncResponse, dependencies=[Depends(_auth)])
async def gap_sync(since: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=2000)) -> GapSyncResponse:
    """Everything the client missed while offline, from its last cursor forward."""
    events = await store.events_after(since, limit)
    return GapSyncResponse(events=events, latest_cursor=await store.latest_cursor())


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
    try:
        while True:
            event = await queue.get()
            await ws.send_text(event.model_dump_json())
    except WebSocketDisconnect:
        pass
    finally:
        capture.subscribers.discard(queue)
        log.info("live subscriber disconnected (%d left)", len(capture.subscribers))
