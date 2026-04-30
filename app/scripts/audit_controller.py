"""Audit log read endpoints.

All reads go through `eth_getLogs` filtered by the indexed wallet topic
on the AuditEntry event. No local storage.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from audit_logger import LogEntry, PaginatedLogs

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/audit", tags=["Audit Logs"])


def _get_contract(request: Request):
    contract = getattr(request.app.state, "index_storage", None)
    if contract is None:
        raise HTTPException(status_code=503, detail="Contract not available")
    return contract


def _parse_ts(value: str, field: str) -> int:
    """Accept either a unix-seconds int or an ISO-8601 timestamp.
    Returns unix seconds."""
    from datetime import datetime
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        # fromisoformat accepts "2026-04-30", "2026-04-30T12:34:56",
        # "2026-04-30T12:34:56+00:00", and (since 3.11) trailing "Z".
        v = value.replace("Z", "+00:00") if value.endswith("Z") else value
        dt = datetime.fromisoformat(v)
        return int(dt.timestamp())
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be unix seconds or ISO-8601 (e.g. 2026-04-30T12:00:00Z)",
        )


@router.get("/logs", response_model=PaginatedLogs)
def get_logs(
    request: Request,
    wallet_address: str,
    action: Optional[str] = None,
    from_block: int = 0,
    from_timestamp: Optional[str] = Query(
        default=None,
        description="Lower bound (inclusive). Unix seconds or ISO-8601 (e.g. 2026-04-30T12:00:00Z).",
    ),
    to_timestamp: Optional[str] = Query(
        default=None,
        description="Upper bound (inclusive). Unix seconds or ISO-8601.",
    ),
    limit: int = Query(default=100, le=500),
):
    ts_lo = _parse_ts(from_timestamp, "from_timestamp") if from_timestamp else None
    ts_hi = _parse_ts(to_timestamp, "to_timestamp") if to_timestamp else None
    if ts_lo is not None and ts_hi is not None and ts_lo > ts_hi:
        raise HTTPException(status_code=400, detail="from_timestamp must be <= to_timestamp")

    contract = _get_contract(request)
    try:
        entries = contract.get_audit_logs(wallet_address, from_block=from_block)
    except Exception as e:
        logger.error(f"get_audit_logs failed: {e}")
        raise HTTPException(status_code=503, detail="Failed to read on-chain logs")

    if action:
        entries = [e for e in entries if e["action"] == action]
    if ts_lo is not None:
        entries = [e for e in entries if e["timestamp"] >= ts_lo]
    if ts_hi is not None:
        entries = [e for e in entries if e["timestamp"] <= ts_hi]

    entries = entries[:limit]
    logs = [LogEntry(**e) for e in entries]
    return PaginatedLogs(total=len(logs), logs=logs)


@router.get("/logs/{entry_id}", response_model=LogEntry)
def get_log(entry_id: str, wallet_address: str, request: Request):
    if "-" not in entry_id:
        raise HTTPException(
            status_code=400,
            detail="entry_id must be {tx_hash}-{log_index}",
        )
    tx_hash, _, idx_str = entry_id.rpartition("-")
    try:
        log_index = int(idx_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid log_index in entry_id")

    contract = _get_contract(request)
    try:
        entries = contract.get_audit_logs(wallet_address)
    except Exception as e:
        logger.error(f"get_audit_logs failed: {e}")
        raise HTTPException(status_code=503, detail="Failed to read on-chain logs")

    tx_norm = tx_hash.lower().removeprefix("0x")
    for e in entries:
        e_tx = e["tx_hash"].lower().removeprefix("0x")
        if e_tx == tx_norm and e["log_index"] == log_index:
            return LogEntry(**e)

    raise HTTPException(status_code=404, detail="Log entry not found")
