"""
Web3Health Storage Controller
==============================
  - POST /web3health/store      — upload a file with a segment_id, encrypt, store on IPFS, persist mapping
  - GET  /web3health/fetch/{segment_id} — look up CID by segment_id, fetch from IPFS, decrypt, return file
"""

import json
import os
import logging
import base64
import secrets
import threading
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

# Persistent mapping file (segment_id → CID)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_MAPPING_FILE = os.path.join(_SCRIPT_DIR, "web3health_segments.json")
_mapping_lock = threading.Lock()

_encryption_key: bytes | None = None


def _get_key() -> bytes:
    """Return the AES-256 key, loading once from the same env var as app.py."""
    global _encryption_key
    if _encryption_key is None:
        _encryption_key = base64.b64decode(
            os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs=")
        )
    return _encryption_key


# ── Segment mapping (persistent JSON) ────────────────────────────────────────

def _load_mapping() -> dict:
    """Load the segment_id → CID mapping from disk."""
    if not os.path.exists(_MAPPING_FILE):
        return {}
    try:
        with open(_MAPPING_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        logger.warning(f"Corrupt mapping file {_MAPPING_FILE}, starting fresh")
        return {}


def _save_mapping(mapping: dict) -> None:
    """Atomically write the mapping to disk."""
    tmp = _MAPPING_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(mapping, f, indent=2)
    os.replace(tmp, _MAPPING_FILE)


def _set_segment(segment_id: str, cid: str, filename: str) -> None:
    """Persist a segment_id → CID entry."""
    with _mapping_lock:
        mapping = _load_mapping()
        mapping[segment_id] = {"cid": cid, "filename": filename}
        _save_mapping(mapping)


def _get_segment(segment_id: str) -> dict | None:
    """Look up a segment_id. Returns {"cid": ..., "filename": ...} or None."""
    with _mapping_lock:
        mapping = _load_mapping()
    return mapping.get(segment_id)


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
