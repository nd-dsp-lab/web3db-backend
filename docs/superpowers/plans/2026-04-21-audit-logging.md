# Audit Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a secure, blockchain-backed audit log that records every API call with a cryptographic commitment on Ethereum Sepolia via the existing SecLog contract.

**Architecture:** FastAPI middleware initialises `request.state.audit = {}` before every request and writes to SQLite + SecLog after the response. Endpoints enrich the dict with wallet, table, and query context. A new router serves wallet-scoped read endpoints.

**Tech Stack:** FastAPI `BaseHTTPMiddleware`, SQLite (WAL mode), `eth_keys` (bundled with `web3`), existing `Web3dbContract.new_log()`.

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `app/scripts/audit_logger.py` | Create | SQLite schema, `LogEntry` model, `AuditLogger.log()` |
| `app/scripts/audit_middleware.py` | Create | `AuditMiddleware` — wraps every request |
| `app/scripts/audit_controller.py` | Create | `GET /audit/logs`, `GET /audit/logs/{log_id}` |
| `app/tests/test_audit.py` | Create | Unit + integration tests |
| `app/scripts/app.py` | Modify | Wire middleware, router, logger |

---

## Task 1: audit_logger.py — SQLite init and Pydantic models

**Files:**
- Create: `app/scripts/audit_logger.py`
- Create: `app/tests/test_audit.py`

- [ ] **Step 1: Write the failing test**

```python
# app/tests/test_audit.py
import os, sys, sqlite3, tempfile, pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from audit_logger import AuditLogger, LogEntry

def test_init_db_creates_table():
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    try:
        AuditLogger(db_path=db_path, contract=None, server_address=None)
        conn = sqlite3.connect(db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_logs'"
        ).fetchall()
        conn.close()
        assert len(tables) == 1
    finally:
        os.unlink(db_path)

def test_log_entry_model_defaults():
    entry = LogEntry(
        log_id="abc",
        timestamp="2026-04-21T00:00:00",
        action="QUERY",
        api_endpoint="/query",
        status="SUCCESS",
    )
    assert entry.wallet_address is None
    assert entry.target_table is None
    assert entry.query is None
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/shady/develop/Github/web3db-backend
python -m pytest app/tests/test_audit.py::test_init_db_creates_table app/tests/test_audit.py::test_log_entry_model_defaults -v
```

Expected: `ImportError` — `audit_logger` not found.

- [ ] **Step 3: Create audit_logger.py with SQLite init and models**

```python
# app/scripts/audit_logger.py
import os
import sqlite3
import logging
from typing import Optional, List
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_SQLITE_DIR = os.path.join(os.path.dirname(_SCRIPT_DIR), "sqlite")
os.makedirs(_SQLITE_DIR, exist_ok=True)
DB_PATH = os.path.join(_SQLITE_DIR, "audit_logs.db")


class LogEntry(BaseModel):
    log_id: str
    timestamp: str
    wallet_address: Optional[str] = None
    action: str
    api_endpoint: str
    target_table: Optional[List[str]] = None
    query: Optional[str] = None
    status: str
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
        conn.close()
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python -m pytest app/tests/test_audit.py::test_init_db_creates_table app/tests/test_audit.py::test_log_entry_model_defaults -v
```

Expected: both `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/scripts/audit_logger.py app/tests/test_audit.py
git commit -m "feat: add audit_logger SQLite schema and Pydantic models"
```

---

## Task 2: AuditLogger.log() — SQLite write path

**Files:**
- Modify: `app/scripts/audit_logger.py`
- Modify: `app/tests/test_audit.py`

- [ ] **Step 1: Add the failing test**

```python
# append to app/tests/test_audit.py
import json

def _make_logger(tmp_path):
    db_path = str(tmp_path / "audit.db")
    return AuditLogger(db_path=db_path, contract=None, server_address=None)

def test_log_writes_sqlite_row(tmp_path):
    al = _make_logger(tmp_path)
    al.log(
        audit_ctx={
            "action": "QUERY",
            "wallet_address": "0xABC",
            "target_table": ["patient_data"],
            "query": "SELECT * FROM patient_data",
        },
        api_endpoint="/query",
        ip_address="127.0.0.1",
        status="SUCCESS",
    )
    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    row = conn.execute("SELECT * FROM audit_logs").fetchone()
    conn.close()
    assert row is not None
    assert row[2] == "0xABC"          # wallet_address
    assert row[3] == "QUERY"          # action
    assert row[4] == "/query"         # api_endpoint
    assert json.loads(row[5]) == ["patient_data"]  # target_table
    assert row[7] == "SUCCESS"        # status
    assert row[9] == "127.0.0.1"      # ip_address

def test_log_unknown_action_defaults(tmp_path):
    al = _make_logger(tmp_path)
    al.log(audit_ctx={}, api_endpoint="/health", ip_address=None, status="SUCCESS")
    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    row = conn.execute("SELECT action FROM audit_logs").fetchone()
    conn.close()
    assert row[0] == "UNKNOWN"
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest app/tests/test_audit.py::test_log_writes_sqlite_row app/tests/test_audit.py::test_log_unknown_action_defaults -v
```

Expected: `AttributeError: 'AuditLogger' object has no attribute 'log'`.

- [ ] **Step 3: Add log() method (SQLite path, no blockchain yet)**

Add these imports at the top of `audit_logger.py`:

```python
import uuid
import json
import time
from datetime import datetime, timezone
```

Add this method inside the `AuditLogger` class (after `_init_db`):

```python
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
        conn.close()

    def _commit_to_chain(self, log_json: str):
        """Returns (blockchain_log_id_hex, sk2_hex) or (None, None) on failure."""
        return None, None
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python -m pytest app/tests/test_audit.py::test_log_writes_sqlite_row app/tests/test_audit.py::test_log_unknown_action_defaults -v
```

Expected: both `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/scripts/audit_logger.py app/tests/test_audit.py
git commit -m "feat: implement AuditLogger.log() SQLite write path"
```

---

## Task 3: SecLog blockchain commitment

**Files:**
- Modify: `app/scripts/audit_logger.py`
- Modify: `app/tests/test_audit.py`

- [ ] **Step 1: Add the failing test**

```python
# append to app/tests/test_audit.py
from unittest.mock import MagicMock, patch

def test_commit_to_chain_calls_new_log(tmp_path):
    fake_log_id = b'\x01' * 32
    contract = MagicMock()
    contract.new_log.return_value = (True, fake_log_id)
    contract.address = "0xSERVER"

    al = AuditLogger(
        db_path=str(tmp_path / "audit.db"),
        contract=contract,
        server_address="0xSERVER",
    )
    al.log(
        audit_ctx={"action": "QUERY", "wallet_address": "0xABC"},
        api_endpoint="/query",
        ip_address="127.0.0.1",
        status="SUCCESS",
    )

    assert contract.new_log.called
    call_args = contract.new_log.call_args[0]
    assert call_args[0] == "0xSERVER"   # receiver
    assert isinstance(call_args[1], int)  # sk1x
    assert isinstance(call_args[2], int)  # sk1y
    assert len(call_args[3]) == 32        # message_hash bytes
    assert call_args[4] > int(time.time())  # timelock in future

    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    row = conn.execute("SELECT blockchain_log_id, sk2 FROM audit_logs").fetchone()
    conn.close()
    assert row[0] == fake_log_id.hex()
    assert row[1] is not None  # sk2_hex stored

def test_commit_to_chain_failure_does_not_raise(tmp_path):
    contract = MagicMock()
    contract.new_log.side_effect = Exception("network error")
    contract.address = "0xSERVER"

    al = AuditLogger(
        db_path=str(tmp_path / "audit.db"),
        contract=contract,
        server_address="0xSERVER",
    )
    # Must not raise
    al.log(
        audit_ctx={"action": "UPLOAD"},
        api_endpoint="/upload/patient_data",
        ip_address=None,
        status="SUCCESS",
    )
    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    row = conn.execute("SELECT blockchain_log_id FROM audit_logs").fetchone()
    conn.close()
    assert row[0] is None  # chain failed, still written to SQLite
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest app/tests/test_audit.py::test_commit_to_chain_calls_new_log app/tests/test_audit.py::test_commit_to_chain_failure_does_not_raise -v
```

Expected: `test_commit_to_chain_calls_new_log` FAILED (new_log not called), second test PASSED.

- [ ] **Step 3: Implement _commit_to_chain with SecLog**

Replace the stub `_commit_to_chain` in `audit_logger.py` with:

```python
    def _commit_to_chain(self, log_json: str):
        """Returns (blockchain_log_id_hex, sk2_hex) or (None, None) on failure."""
        if not self.contract or not self.server_address:
            return None, None
        try:
            from eth_keys import keys
            from web3 import Web3

            sk2_obj = keys.PrivateKey(os.urandom(32))
            pub = sk2_obj.public_key.to_uncompressed_bytes()  # 65 bytes: 0x04 || x || y
            sk1x = int.from_bytes(pub[1:33], 'big')
            sk1y = int.from_bytes(pub[33:65], 'big')

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
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python -m pytest app/tests/test_audit.py -v
```

Expected: all 6 tests `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/scripts/audit_logger.py app/tests/test_audit.py
git commit -m "feat: add SecLog blockchain commitment to AuditLogger"
```

---

## Task 4: audit_middleware.py

**Files:**
- Create: `app/scripts/audit_middleware.py`
- Modify: `app/tests/test_audit.py`

- [ ] **Step 1: Add the failing test**

```python
# append to app/tests/test_audit.py
from fastapi import FastAPI
from fastapi.testclient import TestClient
from audit_middleware import AuditMiddleware

def _make_test_app(tmp_path):
    app = FastAPI()
    al = AuditLogger(db_path=str(tmp_path / "audit.db"), contract=None, server_address=None)
    app.add_middleware(AuditMiddleware, audit_logger=al)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/query")
    def query(req_data: dict):
        return {"result": "ok"}

    return app, al

def test_middleware_logs_every_request(tmp_path):
    app, al = _make_test_app(tmp_path)
    client = TestClient(app)
    client.get("/health")
    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    rows = conn.execute("SELECT api_endpoint, status FROM audit_logs").fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0][0] == "/health"
    assert rows[0][1] == "SUCCESS"

def test_middleware_sets_error_status_on_4xx(tmp_path):
    app, al = _make_test_app(tmp_path)
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/nonexistent")
    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    row = conn.execute("SELECT status FROM audit_logs").fetchone()
    conn.close()
    assert row[0] == "ERROR"

def test_middleware_uses_audit_ctx_from_endpoint(tmp_path):
    app = FastAPI()
    al = AuditLogger(db_path=str(tmp_path / "audit.db"), contract=None, server_address=None)
    app.add_middleware(AuditMiddleware, audit_logger=al)
    from fastapi import Request

    @app.get("/enriched")
    def enriched(request: Request):
        request.state.audit["action"] = "QUERY"
        request.state.audit["wallet_address"] = "0xDEAD"
        return {"ok": True}

    client = TestClient(app)
    client.get("/enriched")
    conn = sqlite3.connect(str(tmp_path / "audit.db"))
    row = conn.execute("SELECT action, wallet_address FROM audit_logs").fetchone()
    conn.close()
    assert row[0] == "QUERY"
    assert row[1] == "0xDEAD"
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest app/tests/test_audit.py::test_middleware_logs_every_request app/tests/test_audit.py::test_middleware_sets_error_status_on_4xx app/tests/test_audit.py::test_middleware_uses_audit_ctx_from_endpoint -v
```

Expected: `ImportError` — `audit_middleware` not found.

- [ ] **Step 3: Create audit_middleware.py**

```python
# app/scripts/audit_middleware.py
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

_PATH_ACTION_MAP = [
    ("POST",   "/query",            "QUERY"),
    ("POST",   "/upload/",          "UPLOAD"),
    ("POST",   "/delete",           "DELETE"),
    ("POST",   "/update",           "UPDATE"),
    ("POST",   "/schemas",          "SCHEMA_CREATE"),
    ("DELETE", "/schemas/",         "SCHEMA_DELETE"),
    ("GET",    "/schemas",          "SCHEMA_READ"),
    ("POST",   "/access-policies",  "POLICY_CREATE"),
    ("DELETE", "/access-policies",  "POLICY_DELETE"),
    ("GET",    "/access-policies/", "POLICY_READ"),
    ("PUT",    "/index-cids",       "INDEX_UPDATE"),
    ("DELETE", "/index-cids",       "INDEX_DELETE"),
    ("POST",   "/tables/config",    "TABLE_CONFIG"),
    ("GET",    "/tables/config",    "TABLE_CONFIG"),
    ("GET",    "/ipfs/fetch/",      "IPFS_FETCH"),
    ("GET",    "/health",           "HEALTH_CHECK"),
    ("POST",   "/web3db/store",     "WEB3DB_STORE"),
    ("GET",    "/web3db/fetch/",    "WEB3DB_FETCH"),
    ("POST",   "/web3health/store", "WEB3HEALTH_STORE"),
    ("GET",    "/web3health/fetch/","WEB3HEALTH_FETCH"),
]


def _resolve_action(method: str, path: str) -> str:
    for m, p, action in _PATH_ACTION_MAP:
        if method == m and path.startswith(p):
            return action
    return "UNKNOWN"


class AuditMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, audit_logger):
        super().__init__(app)
        self.audit_logger = audit_logger

    async def dispatch(self, request: Request, call_next) -> Response:
        request.state.audit = {}
        ip = request.client.host if request.client else None

        response = await call_next(request)

        audit_ctx = request.state.audit
        if "action" not in audit_ctx:
            audit_ctx["action"] = _resolve_action(request.method, request.url.path)

        if response.status_code in (401, 403):
            status = "UNAUTHORIZED"
        elif response.status_code >= 400:
            status = "ERROR"
        else:
            status = "SUCCESS"

        try:
            self.audit_logger.log(
                audit_ctx=audit_ctx,
                api_endpoint=request.url.path,
                ip_address=ip,
                status=status,
            )
        except Exception as e:
            logger.error(f"Audit logging failed: {e}")

        return response
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python -m pytest app/tests/test_audit.py -v
```

Expected: all 9 tests `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/scripts/audit_middleware.py app/tests/test_audit.py
git commit -m "feat: add AuditMiddleware for automatic request logging"
```

---

## Task 5: audit_controller.py — read endpoints

**Files:**
- Create: `app/scripts/audit_controller.py`
- Modify: `app/tests/test_audit.py`

- [ ] **Step 1: Add the failing tests**

```python
# append to app/tests/test_audit.py
from fastapi.testclient import TestClient
from audit_controller import router as audit_router

def _make_audit_app(tmp_path):
    """App with real AuditLogger + audit router wired together."""
    app = FastAPI()
    al = AuditLogger(db_path=str(tmp_path / "audit.db"), contract=None, server_address=None)
    app.add_middleware(AuditMiddleware, audit_logger=al)

    import audit_controller
    audit_controller._DB_PATH = str(tmp_path / "audit.db")
    app.include_router(audit_router)

    # Write a couple of logs directly so we have data
    al.log({"action": "QUERY", "wallet_address": "0xAAA", "target_table": ["t1"]}, "/query", "1.1.1.1", "SUCCESS")
    al.log({"action": "UPLOAD", "wallet_address": "0xAAA"}, "/upload/t1", "1.1.1.1", "ERROR")
    al.log({"action": "QUERY", "wallet_address": "0xBBB"}, "/query", "2.2.2.2", "SUCCESS")
    return app

def test_get_logs_returns_only_own_wallet(tmp_path):
    client = TestClient(_make_audit_app(tmp_path))
    resp = client.get("/audit/logs?wallet_address=0xAAA")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert all(log["wallet_address"] == "0xAAA" for log in data["logs"])

def test_get_logs_filter_by_action(tmp_path):
    client = TestClient(_make_audit_app(tmp_path))
    resp = client.get("/audit/logs?wallet_address=0xAAA&action=QUERY")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1

def test_get_logs_filter_by_status(tmp_path):
    client = TestClient(_make_audit_app(tmp_path))
    resp = client.get("/audit/logs?wallet_address=0xAAA&status=ERROR")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1

def test_get_log_by_id_wrong_wallet_returns_404(tmp_path):
    app = _make_audit_app(tmp_path)
    # Get a real log_id for 0xAAA
    import audit_controller
    conn = sqlite3.connect(audit_controller._DB_PATH)
    log_id = conn.execute("SELECT log_id FROM audit_logs WHERE wallet_address='0xAAA' LIMIT 1").fetchone()[0]
    conn.close()

    client = TestClient(app)
    resp = client.get(f"/audit/logs/{log_id}?wallet_address=0xBBB")
    assert resp.status_code == 404

def test_get_log_by_id_correct_wallet(tmp_path):
    app = _make_audit_app(tmp_path)
    import audit_controller
    conn = sqlite3.connect(audit_controller._DB_PATH)
    log_id = conn.execute("SELECT log_id FROM audit_logs WHERE wallet_address='0xAAA' LIMIT 1").fetchone()[0]
    conn.close()

    client = TestClient(app)
    resp = client.get(f"/audit/logs/{log_id}?wallet_address=0xAAA")
    assert resp.status_code == 200
    assert resp.json()["log_id"] == log_id

def test_get_logs_pagination(tmp_path):
    client = TestClient(_make_audit_app(tmp_path))
    resp = client.get("/audit/logs?wallet_address=0xAAA&page=1&page_size=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert len(data["logs"]) == 1
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest app/tests/test_audit.py::test_get_logs_returns_only_own_wallet -v
```

Expected: `ImportError` — `audit_controller` not found.

- [ ] **Step 3: Create audit_controller.py**

```python
# app/scripts/audit_controller.py
import json
import logging
import os
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
    total = conn.execute(f"SELECT COUNT(*) FROM audit_logs WHERE {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM audit_logs WHERE {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        params + [page_size, offset],
    ).fetchall()
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
    row = conn.execute(
        "SELECT * FROM audit_logs WHERE log_id = ? AND wallet_address = ?",
        (log_id, wallet_address),
    ).fetchone()
    conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Log entry not found")
    return _row_to_entry(row)
```

- [ ] **Step 4: Run all tests**

```bash
python -m pytest app/tests/test_audit.py -v
```

Expected: all 15 tests `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/scripts/audit_controller.py app/tests/test_audit.py
git commit -m "feat: add audit read endpoints GET /audit/logs and GET /audit/logs/{id}"
```

---

## Task 6: Wire into app.py

**Files:**
- Modify: `app/scripts/app.py`

- [ ] **Step 1: Add imports**

In `app/scripts/app.py`, find the existing import block:

```python
from web3db_controller import router as web3db_router
from web3health_controller import router as web3health_router
```

Change it to:

```python
from web3db_controller import router as web3db_router
from web3health_controller import router as web3health_router
from audit_logger import AuditLogger
from audit_middleware import AuditMiddleware
from audit_controller import router as audit_router
```

- [ ] **Step 2: Add AuditMiddleware after CORSMiddleware**

Find:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
```

Change it to:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
app.add_middleware(AuditMiddleware, audit_logger=None)  # placeholder; replaced below
```

Wait — middleware is instantiated at `add_middleware` call time in some Starlette versions. To safely pass the `AuditLogger` (which depends on the contract being ready), use a startup event instead:

Replace the placeholder approach. Find:

```python
# Register sub-routers
app.include_router(web3db_router)
app.include_router(web3health_router)
```

Change it to:

```python
# Register sub-routers
app.include_router(web3db_router)
app.include_router(web3health_router)
app.include_router(audit_router)
```

Then find (after the contract is initialized, around line 72):

```python
    logger.info("Smart contract connection initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize smart contract connection: {e}")
    raise Exception("Smart contract connection is required but failed to initialize")
```

Add immediately after the `except` block (before the `app.state.default_table` line):

```python
# Initialize audit logger (depends on contract being ready)
_audit_db = os.path.join(script_dir, '..', 'sqlite', 'audit_logs.db')
app.state.audit_logger = AuditLogger(
    db_path=_audit_db,
    contract=app.state.index_storage,
    server_address=app.state.index_storage.address,
)
app.add_middleware(AuditMiddleware, audit_logger=app.state.audit_logger)
logger.info("Audit logger initialized")
```

- [ ] **Step 3: Verify the server starts without error**

```bash
cd app/scripts && python -c "import app; print('app imported OK')"
```

Expected: `app imported OK` (may also print contract connection output).

- [ ] **Step 4: Commit**

```bash
git add app/scripts/app.py
git commit -m "feat: wire AuditMiddleware and audit router into FastAPI app"
```

---

## Task 7: Annotate key endpoints with audit context

**Files:**
- Modify: `app/scripts/app.py`

- [ ] **Step 1: Add Request import**

Find:

```python
from fastapi import FastAPI, UploadFile, File
```

Change it to:

```python
from fastapi import FastAPI, Request, UploadFile, File
```

- [ ] **Step 2: Annotate POST /upload/{table_name}**

Find:

```python
@app.post("/upload/{table_name}")
async def upload_data(table_name: str, file: UploadFile = File(...)):
```

Change it to:

```python
@app.post("/upload/{table_name}")
async def upload_data(table_name: str, file: UploadFile = File(...), req: Request = None):
```

Find the first line inside `upload_data` (after the docstring):

```python
    logger.info(f"POST /upload/{table_name} -
```

Add before that logger line:

```python
    if req:
        req.state.audit["action"] = "UPLOAD"
        req.state.audit["target_table"] = [table_name]
```

- [ ] **Step 3: Annotate POST /query**

Find:

```python
@app.post("/query")
async def query(request: QueryRequest):
    logger.info(f"POST /query - Processing query for table '{request.table_name}' with access control")
```

Change it to:

```python
@app.post("/query")
async def query(request: QueryRequest, req: Request = None):
    logger.info(f"POST /query - Processing query for table '{request.table_name}' with access control")
    if req:
        req.state.audit["action"] = "QUERY"
        req.state.audit["wallet_address"] = request.wallet_address
        req.state.audit["target_table"] = [request.table_name]
        req.state.audit["query"] = request.query
```

- [ ] **Step 4: Annotate POST /delete**

Find:

```python
@app.post("/delete")
async def delete_records(request: DeleteRequest):
    """
```

Change it to:

```python
@app.post("/delete")
async def delete_records(request: DeleteRequest, req: Request = None):
    """
```

Find the first `logger.info` line inside `delete_records`:

```python
    logger.info(f"POST /delete - Processing DELETE query for wallet: {request.wallet_address}")
```

Add after it:

```python
    if req:
        req.state.audit["action"] = "DELETE"
        req.state.audit["wallet_address"] = request.wallet_address
        req.state.audit["query"] = request.delete_query
```

- [ ] **Step 5: Annotate POST /update**

Find:

```python
@app.post("/update")
async def update_records(request: UpdateRequest):
```

Change it to:

```python
@app.post("/update")
async def update_records(request: UpdateRequest, req: Request = None):
```

Find the first `logger.info` line inside `update_records` (around line 1711):

```python
    logger.info(f"POST /update - Processing UPDATE query for wallet: {request.wallet_address}")
```

Add after it:

```python
    if req:
        req.state.audit["action"] = "UPDATE"
        req.state.audit["wallet_address"] = request.wallet_address
        req.state.audit["query"] = request.update_query
```

- [ ] **Step 6: Verify server still imports cleanly**

```bash
cd app/scripts && python -c "import app; print('app imported OK')"
```

Expected: `app imported OK`.

- [ ] **Step 7: Commit**

```bash
git add app/scripts/app.py
git commit -m "feat: annotate query/upload/delete/update endpoints with audit context"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** LogEntry model ✓, middleware ✓, SecLog commitment ✓, SQLite storage ✓, GET /audit/logs with filters ✓, GET /audit/logs/{id} ✓, wallet-scoped reads ✓, null wallet for unauthenticated endpoints ✓, failure handling (chain down ≠ request failure) ✓, action table from spec ✓
- [x] **No placeholders:** All steps contain full code
- [x] **Type consistency:** `LogEntry`, `PaginatedLogs`, `AuditLogger`, `AuditMiddleware`, `DB_PATH` — consistent across all tasks
- [x] **timelock:** `int(time.time()) + 365 * 24 * 3600` ensures it's always in the future (satisfies `futureTimelock` modifier which requires `_time > block.timestamp`)
- [x] **sk2 storage:** stored as `hex(int)` string so it can be retrieved and passed as `int` to `verify_log` via `int(sk2_hex, 16)`
