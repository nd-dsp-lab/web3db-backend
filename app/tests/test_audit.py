import os
import sys
import sqlite3
import tempfile
import pytest
import json

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


from unittest.mock import MagicMock, patch
import time


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


from fastapi import FastAPI, Request
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


# Audit Controller Tests
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
