"""Entrypoint: run the Telethon capture loop and the uvicorn API in one asyncio loop."""
from __future__ import annotations

import asyncio
import logging

import uvicorn

from .api import app
from .capture import capture
from .config import settings
from .db import store


async def _prune_loop() -> None:
    while True:
        removed = await store.prune_content()
        if removed:
            logging.getLogger("prune").info("pruned %d stale content rows", removed)
        await asyncio.sleep(3600)


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    await store.open()
    config = uvicorn.Config(app, host=settings.host, port=settings.port, log_level="info")
    server = uvicorn.Server(config)
    try:
        await asyncio.gather(
            capture.run(),
            server.serve(),
            _prune_loop(),
        )
    finally:
        await store.close()


def run() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    run()
