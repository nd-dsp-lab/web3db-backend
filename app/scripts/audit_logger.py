import os
import sqlite3
import logging
import uuid
import json
import time
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_SQLITE_DIR = os.path.join(os.path.dirname(_SCRIPT_DIR), "sqlite")
os.makedirs(_SQLITE_DIR, exist_ok=True)
DB_PATH = os.path.join(_SQLITE_DIR, "audit_logs.db")


class LogEntry(BaseModel):
    log_id: str
    timestamp: str
    action: str
    api_endpoint: str
    status: str
    wallet_address: Optional[str] = None
    target_table: Optional[List[str]] = None
    query: Optional[str] = None
    details: Optional[dict] = None
    ip_address: Optional[str] = None
    blockchain_log_id: Optional[str] = None
    sk2: Optional[str] = None


class PaginatedLogs(BaseModel):
    total: int
    page: int
    page_size: int
    logs: List[LogEntry]


class AuditLogger:
    def __init__(self, db_path: str = DB_PATH, contract=None, server_address: Optional[str] = None):
        self.db_path = db_path
        self.contract = contract
        self.server_address = server_address
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    log_id            TEXT PRIMARY KEY,
                    timestamp         TEXT NOT NULL,
                    wallet_address    TEXT,
                    action            TEXT NOT NULL,
                    api_endpoint      TEXT NOT NULL,
                    target_table      TEXT,
                    query             TEXT,
                    status            TEXT NOT NULL,
                    details           TEXT,
                    ip_address        TEXT,
                    blockchain_log_id TEXT,
                    sk2               TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def log(
        self,
        audit_ctx: dict,
        api_endpoint: str,
        ip_address: Optional[str],
        status: str,
    ) -> None:
        log_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()

        target_table = audit_ctx.get("target_table")
        details = audit_ctx.get("details")

        log_json = json.dumps({
            "log_id": log_id,
            "timestamp": timestamp,
            "wallet_address": audit_ctx.get("wallet_address"),
            "action": audit_ctx.get("action", "UNKNOWN"),
            "api_endpoint": api_endpoint,
            "target_table": target_table,
            "query": audit_ctx.get("query"),
            "status": status,
            "details": details,
            "ip_address": ip_address,
        }, sort_keys=True, default=str)

        blockchain_log_id, sk2_hex = self._commit_to_chain(log_json)

        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO audit_logs (
                    log_id, timestamp, wallet_address, action, api_endpoint,
                    target_table, query, status, details, ip_address,
                    blockchain_log_id, sk2
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    log_id,
                    timestamp,
                    audit_ctx.get("wallet_address"),
                    audit_ctx.get("action", "UNKNOWN"),
                    api_endpoint,
                    json.dumps(target_table) if target_table is not None else None,
                    audit_ctx.get("query"),
                    status,
                    json.dumps(details) if details is not None else None,
                    ip_address,
                    blockchain_log_id,
                    sk2_hex,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _commit_to_chain(self, log_json: str):
        """Returns (blockchain_log_id_hex, sk2_hex) or (None, None) on failure."""
        if not self.contract or not self.server_address:
            return None, None
        try:
            from eth_keys import keys
            from web3 import Web3

            sk2_obj = keys.PrivateKey(os.urandom(32))
            # to_bytes() returns 64 bytes: x(32) || y(32), no 0x04 prefix
            pub_bytes = sk2_obj.public_key.to_bytes()
            sk1x = int.from_bytes(pub_bytes[:32], 'big')
            sk1y = int.from_bytes(pub_bytes[32:64], 'big')

            message_hash = Web3.keccak(text=log_json)
            timelock = int(time.time()) + 365 * 24 * 3600  # 1 year from now

            success, log_id_bytes = self.contract.new_log(
                self.server_address, sk1x, sk1y, message_hash, timelock
            )
            if success and log_id_bytes:
                sk2_int = int.from_bytes(sk2_obj.to_bytes(), 'big')
                return log_id_bytes.hex(), hex(sk2_int)

            logger.warning("SecLog new_log returned failure")
            return None, None
        except Exception as e:
            logger.warning(f"Blockchain audit commitment failed: {e}")
            return None, None
