import json
import logging
import sqlite3
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from audit_logger import DB_PATH, LogEntry, PaginatedLogs

logger = logging.getLogger(__name__)

_DB_PATH = DB_PATH

router = APIRouter(prefix="/audit", tags=["Audit Logs"])


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _row_to_entry(row: sqlite3.Row) -> LogEntry:
    return LogEntry(
        log_id=row["log_id"],
        timestamp=row["timestamp"],
        wallet_address=row["wallet_address"],
        action=row["action"],
        api_endpoint=row["api_endpoint"],
        target_table=json.loads(row["target_table"]) if row["target_table"] else None,
        query=row["query"],
        status=row["status"],
        details=json.loads(row["details"]) if row["details"] else None,
        ip_address=row["ip_address"],
        blockchain_log_id=row["blockchain_log_id"],
        sk2=row["sk2"],
    )


@router.get("/logs", response_model=PaginatedLogs)
def get_logs(
    wallet_address: str,
    action: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = Query(default=50, le=200),
):
    filters = ["wallet_address = ?"]
    params: list = [wallet_address]

    if action:
        filters.append("action = ?")
        params.append(action)
    if status:
        filters.append("status = ?")
        params.append(status)
    if date_from:
        filters.append("timestamp >= ?")
        params.append(date_from)
    if date_to:
        filters.append("timestamp <= ?")
        params.append(date_to + "T23:59:59")

    where = " AND ".join(filters)
    offset = (page - 1) * page_size

    conn = _get_conn()
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM audit_logs WHERE {where}", params).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM audit_logs WHERE {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()
    finally:
        conn.close()

    return PaginatedLogs(
        total=total,
        page=page,
        page_size=page_size,
        logs=[_row_to_entry(r) for r in rows],
    )


@router.get("/logs/{log_id}", response_model=LogEntry)
def get_log(log_id: str, wallet_address: str):
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM audit_logs WHERE log_id = ? AND wallet_address = ?",
            (log_id, wallet_address),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Log entry not found")
    return _row_to_entry(row)
