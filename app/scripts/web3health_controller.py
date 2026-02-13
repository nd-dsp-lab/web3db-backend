"""
Web3Health Storage Controller
==============================
  - POST /web3health/store      — upload a file with a segment_id, encrypt, store on IPFS, persist mapping
  - GET  /web3health/fetch/{segment_id} — look up CID by segment_id, fetch from IPFS, decrypt, return file
"""

import json
import os
import sqlite3
import logging
import base64
import secrets
import requests as http_requests
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import Response
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/web3health", tags=["Web3Health Storage"])

# ── Config ────────────────────────────────────────────────────────────────────

IPFS_API = "http://localhost:5001/api/v0"

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH = os.path.join(_SCRIPT_DIR, "web3health_segments.db")
_OLD_JSON_PATH = os.path.join(_SCRIPT_DIR, "web3health_segments.json")

_encryption_key: bytes | None = None


def _get_key() -> bytes:
    """Return the AES-256 key, loading once from the same env var as app.py."""
    global _encryption_key
    if _encryption_key is None:
        _encryption_key = base64.b64decode(
            os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs=")
        )
    return _encryption_key


# ── SQLite segment mapping ────────────────────────────────────────────────────

def _get_db() -> sqlite3.Connection:
    """Get a thread-local SQLite connection (sqlite3 handles concurrency)."""
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent read performance
    return conn


def _init_db() -> None:
    """Create the table if it doesn't exist and migrate any old JSON data."""
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS segment_mappings (
            segment_id  TEXT PRIMARY KEY,
            cid         TEXT NOT NULL,
            filename    TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    # Migrate existing JSON file if present
    if os.path.exists(_OLD_JSON_PATH):
        try:
            with open(_OLD_JSON_PATH, "r") as f:
                old_data = json.load(f)
            if old_data:
                for seg_id, entry in old_data.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO segment_mappings (segment_id, cid, filename) VALUES (?, ?, ?)",
                        (seg_id, entry.get("cid", ""), entry.get("filename", ""))
                    )
                conn.commit()
                logger.info(f"Migrated {len(old_data)} entries from JSON to SQLite")
                # Rename old file so we don't re-migrate
                os.rename(_OLD_JSON_PATH, _OLD_JSON_PATH + ".migrated")
        except Exception as e:
            logger.warning(f"Failed to migrate JSON mapping: {e}")
    conn.close()


# Initialize DB on module load
_init_db()


def _set_segment(segment_id: str, cid: str, filename: str) -> None:
    """Persist a segment_id → CID entry."""
    conn = _get_db()
    conn.execute(
        "INSERT OR REPLACE INTO segment_mappings (segment_id, cid, filename) VALUES (?, ?, ?)",
        (segment_id, cid, filename)
    )
    conn.commit()
    conn.close()


def _get_segment(segment_id: str) -> dict | None:
    """Look up a segment_id. Returns {"cid": ..., "filename": ...} or None."""
    conn = _get_db()
    row = conn.execute(
        "SELECT cid, filename FROM segment_mappings WHERE segment_id = ?",
        (segment_id,)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return {"cid": row["cid"], "filename": row["filename"]}


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


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/store")
def store_content(segment_id: str = Form(...), file: UploadFile = File(...)):
    """
    Accept a segment_id and a file (JSON, CSV, Parquet, SQL dump, etc.),
    encrypt & upload it to IPFS. Persist the mapping segment_id → CID for later retrieval.
    """
    raw = file.file.read()
    original_size = len(raw)
    filename = file.filename or "upload.bin"

    logger.info(f"[web3health/store] segment_id={segment_id}, file={filename} ({original_size:,} bytes)")

    # Encrypt
    try:
        encrypted = _encrypt(raw)
    except Exception as e:
        logger.error(f"[web3health/store] Encryption failed: {e}")
        raise HTTPException(status_code=500, detail="Encryption failed")

    # Upload to IPFS
    try:
        cid = _ipfs_add(encrypted, f"{filename}.enc")
    except Exception as e:
        logger.error(f"[web3health/store] IPFS upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Failed to upload to IPFS: {e}")

    # Persist mapping
    _set_segment(segment_id, cid, filename)

    logger.info(f"[web3health/store] Stored segment_id={segment_id} → CID {cid}")

    return {
        "status": "success",
        "segment_id": segment_id,
        "cid": cid,
        "filename": filename,
        "original_size_bytes": original_size,
        "encrypted_size_bytes": len(encrypted),
    }


@router.get("/fetch/{segment_id}")
def fetch_content(segment_id: str):
    """
    Look up the CID for the segment_id, fetch encrypted content from IPFS,
    decrypt and return the original file.
    """
    logger.info(f"[web3health/fetch] segment_id={segment_id}")

    # Look up mapping
    entry = _get_segment(segment_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"segment_id not found: {segment_id}")

    cid = entry["cid"]
    filename = entry.get("filename", segment_id)

    # Fetch from IPFS
    try:
        encrypted = _ipfs_cat(cid)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[web3health/fetch] IPFS fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch from IPFS")

    # Decrypt
    try:
        decrypted = _decrypt(encrypted)
    except Exception as e:
        logger.error(f"[web3health/fetch] Decryption failed: {e}")
        raise HTTPException(status_code=500, detail="Decryption failed — wrong key or corrupted data")

    logger.info(f"[web3health/fetch] Returning {len(decrypted):,} bytes for segment_id={segment_id} (CID {cid})")

    return Response(
        content=decrypted,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Segment-Id": segment_id,
            "X-CID": cid,
            "X-Original-Size": str(len(decrypted)),
        },
    )
