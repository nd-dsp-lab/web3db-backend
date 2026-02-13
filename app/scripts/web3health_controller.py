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
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import Response

from crypto import load_key, encrypt, decrypt
from ipfs_utils import ipfs_add, ipfs_cat

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/web3health", tags=["Web3Health Storage"])

# ── Config ────────────────────────────────────────────────────────────────────

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_SQLITE_DIR = os.path.join(os.path.dirname(_SCRIPT_DIR), "sqlite")
os.makedirs(_SQLITE_DIR, exist_ok=True)
_DB_PATH = os.path.join(_SQLITE_DIR, "web3health_segments.db")
_OLD_JSON_PATH = os.path.join(_SCRIPT_DIR, "web3health_segments.json")


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
        encrypted = encrypt(raw, load_key())
    except Exception as e:
        logger.error(f"[web3health/store] Encryption failed: {e}")
        raise HTTPException(status_code=500, detail="Encryption failed")

    # Upload to IPFS
    try:
        cid = ipfs_add(encrypted, f"{filename}.enc")
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
        encrypted = ipfs_cat(cid)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[web3health/fetch] IPFS fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch from IPFS")

    # Decrypt
    try:
        decrypted = decrypt(encrypted, load_key())
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
