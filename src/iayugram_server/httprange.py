"""HTTP Range header parsing.

Kept in its own module, free of config and Telethon imports, so it can be tested
without booting the capture client.
"""
from __future__ import annotations


def parse_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Parse a single `bytes=start-end` range into inclusive offsets.

    Returns None for a missing, malformed or unsatisfiable header, in which case the
    caller should serve the whole file. Only the first range of a multi-range request
    is honoured — enough for media playback, which asks for one span at a time.
    """
    if not header or not header.startswith("bytes="):
        return None
    spec = header[len("bytes="):].split(",")[0].strip()
    first, sep, last = spec.partition("-")
    if not sep:
        return None
    try:
        if not first:  # suffix range: bytes=-500 → the final 500 bytes
            length = int(last)
            if length <= 0:
                return None
            return max(0, size - length), size - 1
        start = int(first)
        end = int(last) if last else size - 1
    except ValueError:
        return None
    if start >= size or start > end:
        return None
    return start, min(end, size - 1)
