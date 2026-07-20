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
