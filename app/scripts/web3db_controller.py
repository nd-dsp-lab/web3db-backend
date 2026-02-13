"""
Web3DB Storage Controller
=========================
  - POST /web3db/store  — upload a file, encrypt (AES-256-CBC), store on IPFS, return CID
  - GET  /web3db/fetch/{cid} — fetch from IPFS, decrypt, return the original file
"""

import json
import os
import logging
import base64
import secrets
import requests as http_requests
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import Response
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/web3db", tags=["Web3DB Storage"])

# ── Config ────────────────────────────────────────────────────────────────────

IPFS_API = "http://localhost:5001/api/v0"

_encryption_key: bytes | None = None


def _get_key() -> bytes:
    """Return the AES-256 key, loading once from the same env var as app.py."""
    global _encryption_key
    if _encryption_key is None:
        _encryption_key = base64.b64decode(
            os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs=")
        )
    return _encryption_key


# ── Crypto helpers (mirror app.py's AES-256-CBC scheme) ───────────────────────

def _encrypt(data: bytes) -> bytes:
    """Encrypt with AES-256-CBC. Returns [IV (16 B) || ciphertext]."""
    key = _get_key()
    iv = secrets.token_bytes(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    padder = padding.PKCS7(128).padder()
    padded = padder.update(data) + padder.finalize()

    ciphertext = encryptor.update(padded) + encryptor.finalize()
    return iv + ciphertext


def _decrypt(package: bytes) -> bytes:
    """Decrypt [IV (16 B) || ciphertext] with AES-256-CBC."""
    key = _get_key()
    iv = package[:16]
    ciphertext = package[16:]

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    padded = decryptor.update(ciphertext) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


# ── IPFS helpers ──────────────────────────────────────────────────────────────

def _ipfs_add(data: bytes, filename: str) -> str:
    """Upload bytes to IPFS via /api/v0/add (pins by default). Returns CID."""
    resp = http_requests.post(
        f"{IPFS_API}/add",
        files={"file": (filename, data)},
        stream=True,
    )
    resp.raise_for_status()
    # IPFS Kubo uses chunked transfer encoding and keeps the connection open.
    # Read only the first line (the JSON result) then close immediately.
    line = resp.raw.readline()
    resp.close()
    return json.loads(line)["Hash"]


def _ipfs_cat(cid: str) -> bytes:
    """Fetch raw bytes from IPFS via /api/v0/cat."""
    resp = http_requests.post(
        f"{IPFS_API}/cat",
        params={"arg": cid},
        stream=True,
        timeout=60,
    )
    if resp.status_code != 200:
        resp.close()
        raise HTTPException(status_code=404, detail=f"CID not found on IPFS: {cid}")
    content = resp.content
    resp.close()
    return content


# ── Content-type guessing ────────────────────────────────────────────────────

_EXT_TO_MEDIA = {
    ".csv":     "text/csv",
    ".parquet": "application/octet-stream",
    ".sql":     "application/sql",
    ".json":    "application/json",
    ".txt":     "text/plain",
}


def _guess_media_type(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    return _EXT_TO_MEDIA.get(ext, "application/octet-stream")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/store")
def store_content(file: UploadFile = File(...)):
    """
    Accept a file (CSV, Parquet, SQL dump, etc.), encrypt it with
    AES-256-CBC, upload to IPFS, and return the CID.
    """
    raw = file.file.read()
    original_size = len(raw)
    filename = file.filename or "upload.bin"

    logger.info(f"[store] Received {filename} ({original_size:,} bytes)")

    try:
        encrypted = _encrypt(raw)
    except Exception as e:
        logger.error(f"[store] Encryption failed: {e}")
        raise HTTPException(status_code=500, detail="Encryption failed")

    try:
        cid = _ipfs_add(encrypted, f"{filename}.enc")
    except Exception as e:
        logger.error(f"[store] IPFS upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Failed to upload to IPFS: {e}")

    logger.info(f"[store] Stored {filename} → CID {cid}")

    return {
        "status": "success",
        "cid": cid,
        "filename": filename,
        "original_size_bytes": original_size,
        "encrypted_size_bytes": len(encrypted),
    }


@router.get("/fetch/{cid}")
def fetch_content(cid: str):
    """
    Fetch encrypted content from IPFS by CID, decrypt it, and
    return the original file bytes.
    """
    logger.info(f"[fetch] Fetching CID {cid}")

    try:
        encrypted = _ipfs_cat(cid)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[fetch] IPFS fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch from IPFS")

    try:
        decrypted = _decrypt(encrypted)
    except Exception as e:
        logger.error(f"[fetch] Decryption failed: {e}")
        raise HTTPException(status_code=500, detail="Decryption failed — wrong key or corrupted data")

    logger.info(f"[fetch] Returning {len(decrypted):,} bytes for CID {cid}")

    return Response(
        content=decrypted,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{cid}"',
            "X-Original-Size": str(len(decrypted)),
        },
    )
