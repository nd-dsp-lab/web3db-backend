"""
Web3DB Storage Controller
=========================
  - POST /web3db/store  — upload a file, encrypt & upload it to IPFS, return CID
  - GET  /web3db/fetch/{cid} — fetch from IPFS, decrypt & return the original file
"""

import os
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import Response

from crypto_utils import load_key, encrypt, decrypt
from ipfs_utils import ipfs_add, ipfs_cat

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/web3db", tags=["Web3DB Storage"])


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
    Accept a file (CSV, Parquet, SQL dump, etc.), encryp & upload it to IPFS, and return the CID.
    """
    raw = file.file.read()
    original_size = len(raw)
    filename = file.filename or "upload.bin"

    logger.info(f"[store] Received {filename} ({original_size:,} bytes)")

    try:
        encrypted = encrypt(raw, load_key())
    except Exception as e:
        logger.error(f"[store] Encryption failed: {e}")
        raise HTTPException(status_code=500, detail="Encryption failed")

    try:
        cid = ipfs_add(encrypted, f"{filename}.enc")
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
    Fetch encrypted content from IPFS by CID, decrypt & return the original file bytes.
    """
    logger.info(f"[fetch] Fetching CID {cid}")

    try:
        encrypted = ipfs_cat(cid)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[fetch] IPFS fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch from IPFS")

    try:
        decrypted = decrypt(encrypted, load_key())
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
