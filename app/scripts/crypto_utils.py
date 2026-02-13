"""
Shared AES-256-CBC Encryption Utilities
========================================
Single source of truth for all encryption/decryption across the application.

Functions:
    load_key()                            → Load and cache the AES-256 key from env
    encrypt(data, key)                    → Returns [IV (16 B) || ciphertext]
    decrypt(package, key)                 → Decrypts [IV || ciphertext] → plaintext
    create_encrypted_package(data, key)   → Alias for encrypt (backward compat)
    extract_and_decrypt_package(pkg, key) → Alias for decrypt (backward compat)
"""

import os
import base64
import secrets
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

_cached_key: bytes | None = None


def load_key() -> bytes:
    """Load and cache the AES-256 key from the ENCRYPTION_KEY env var."""
    global _cached_key
    if _cached_key is None:
        _cached_key = base64.b64decode(
            os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs=")
        )
    return _cached_key


def encrypt(data: bytes, key: bytes) -> bytes:
    """
    Encrypt data with AES-256-CBC.
    Returns: [IV (16 bytes) || ciphertext]
    """
    iv = secrets.token_bytes(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    padder = padding.PKCS7(128).padder()
    padded = padder.update(data) + padder.finalize()

    ciphertext = encryptor.update(padded) + encryptor.finalize()
    return iv + ciphertext


def decrypt(package: bytes, key: bytes) -> bytes:
    """
    Decrypt an [IV (16 bytes) || ciphertext] package with AES-256-CBC.
    """
    iv = package[:16]
    ciphertext = package[16:]

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    padded = decryptor.update(ciphertext) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


# Backward-compatible aliases used by app.py
create_encrypted_package = encrypt
extract_and_decrypt_package = decrypt
