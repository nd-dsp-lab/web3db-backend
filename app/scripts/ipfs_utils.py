"""
Shared IPFS Utilities
======================
Single source of truth for all IPFS operations.

Functions:
    ipfs_add(data, filename)  → Upload bytes to IPFS, return CID
    ipfs_cat(cid)             → Fetch bytes from IPFS by CID
    fetch_from_ipfs(cid)      → Safe wrapper returning bytes or None
"""

import json
import logging
from typing import Optional

import requests as http_requests

logger = logging.getLogger(__name__)

IPFS_API = "http://localhost:5001/api/v0"
IPFS_TIMEOUT = 300  # seconds


def ipfs_add(data: bytes, filename: str) -> str:
    """Upload bytes to IPFS via /api/v0/add (pins by default). Returns CID."""
    resp = http_requests.post(
        f"{IPFS_API}/add",
        files={"file": (filename, data)},
        stream=True,
        timeout=IPFS_TIMEOUT,
    )
    resp.raise_for_status()
    # IPFS Kubo uses chunked transfer encoding and keeps the connection open.
    # Read only the first line (the JSON result) then close immediately.
    line = resp.raw.readline()
    resp.close()
    return json.loads(line)["Hash"]


def ipfs_cat(cid: str) -> bytes:
    """
    Fetch raw bytes from IPFS via /api/v0/cat.
    Raises an exception if the CID is not found.
    """
    resp = http_requests.post(
        f"{IPFS_API}/cat",
        params={"arg": cid},
        stream=True,
        timeout=IPFS_TIMEOUT,
    )
    if resp.status_code != 200:
        resp.close()
        raise RuntimeError(f"CID not found on IPFS: {cid} (status {resp.status_code})")
    content = resp.content
    resp.close()
    return content


def fetch_from_ipfs(cid: str) -> Optional[bytes]:
    """
    Safe wrapper around ipfs_cat — returns bytes or None on failure.
    Used by app.py for endpoints that return None instead of raising.
    """
    try:
        return ipfs_cat(cid)
    except Exception as e:
        logger.error(f"Error fetching CID {cid}: {e}")
        return None
