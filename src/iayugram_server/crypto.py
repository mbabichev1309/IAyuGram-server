"""Encrypt-at-rest for stored message content.

Deleted-message content sitting on an external server is the most sensitive data
in the system, so it is never stored in plaintext. Fernet = AES-128-CBC + HMAC.
"""
from __future__ import annotations

from cryptography.fernet import Fernet

from .config import settings

_f = Fernet(settings.content_key.encode())


def encrypt(plaintext: str) -> bytes:
    return _f.encrypt(plaintext.encode("utf-8"))


def decrypt(token: bytes) -> str:
    return _f.decrypt(token).decode("utf-8")


def encrypt_bytes(data: bytes) -> bytes:
    """Encrypt raw media bytes at rest (same Fernet key as text)."""
    return _f.encrypt(data)


def decrypt_bytes(token: bytes) -> bytes:
    return _f.decrypt(token)


# --- Chunked media container -------------------------------------------------
#
# Fernet is all-or-nothing: encrypting a file means holding the plaintext AND the
# token in memory at once, which rules out the large videos and documents phase 2
# is about. So media is stored as a framed container instead:
#
#     b"IAYUMED2" | uint32 plaintext-chunk-size | (uint32 token-length | token)*
#
# Every frame is an independent Fernet token over a fixed-size plaintext chunk, so
# the file can be written and read incrementally, and a byte range can be served by
# skipping whole frames without decrypting them. The chunk size is recorded in the
# header so changing the default later can't break existing files.
#
# Files written before this format are a single bare Fernet token; readers detect
# the missing magic and fall back to whole-file decryption.

_MAGIC = b"IAYUMED2"
_HEADER_LEN = len(_MAGIC) + 4


def encrypt_file(src_path: str, dst_path: str, chunk_size: int) -> int:
    """Encrypt src into the framed container at dst. Returns plaintext byte count.

    Never holds more than one chunk in memory, so file size is bounded by disk,
    not RAM.
    """
    total = 0
    with open(src_path, "rb") as src, open(dst_path, "wb") as dst:
        dst.write(_MAGIC)
        dst.write(chunk_size.to_bytes(4, "big"))
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            total += len(chunk)
            token = _f.encrypt(chunk)
            dst.write(len(token).to_bytes(4, "big"))
            dst.write(token)
    return total


def decrypt_file_range(path: str, start: int = 0, end: int | None = None):
    """Yield decrypted plaintext for byte range [start, end] inclusive.

    Frames whose plaintext lies entirely before `start` are skipped by seeking past
    their ciphertext — they are never decrypted. `end` of None means to the end.
    """
    with open(path, "rb") as f:
        header = f.read(_HEADER_LEN)
        if not header.startswith(_MAGIC):
            # Legacy single-token file: decrypt whole, then slice.
            f.seek(0)
            data = _f.decrypt(f.read())
            stop = len(data) if end is None else min(end + 1, len(data))
            if start < stop:
                yield data[start:stop]
            return

        chunk_size = int.from_bytes(header[len(_MAGIC):], "big")
        pos = 0  # plaintext offset of the current frame
        while True:
            raw_len = f.read(4)
            if len(raw_len) < 4:
                return
            token_len = int.from_bytes(raw_len, "big")
            frame_end = pos + chunk_size  # exclusive, upper bound

            if end is not None and pos > end:
                return
            if frame_end <= start:
                # Entirely before the requested range — skip without decrypting.
                f.seek(token_len, 1)
                pos = frame_end
                continue

            plain = _f.decrypt(f.read(token_len))
            lo = max(0, start - pos)
            hi = len(plain) if end is None else min(len(plain), end - pos + 1)
            if lo < hi:
                yield plain[lo:hi]
            pos += len(plain)
