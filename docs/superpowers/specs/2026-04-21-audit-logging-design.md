# Web3DB Secure Audit Logging — Design Spec

**Date:** 2026-04-21
**Status:** Approved

---

## Overview

Implement a secure, immutable audit-logging system that monitors all data and API access across the Web3DB application. Every API call is automatically captured by FastAPI middleware; per-entry immutability is provided by committing each log's content hash to the Ethereum Sepolia testnet via the existing SecLog contract functions.

---

## Architecture

Three new files, following the existing controller pattern:

```
app/scripts/
├── audit_logger.py       # SQLite schema, AuditLogger class, SecLog integration
├── audit_middleware.py   # FastAPI middleware (hooks into every request/response)
├── audit_controller.py   # GET /audit/logs, GET /audit/logs/{id} router
```

`app.py` changes:
- Import and include `audit_controller.router`
- Add `audit_middleware` via `app.add_middleware`
- Pass `app.state` reference to `AuditLogger` so it can reach the contract

### Why not pure middleware for body fields?

FastAPI middleware cannot read the request body without consuming the stream. Instead, a two-part pattern is used:

1. **Middleware** initialises `request.state.audit = {}` on every request, captures IP and start timestamp. After the response it reads `request.state.audit`, records the HTTP status code, and calls `AuditLogger.log()`.
2. **Endpoints** write rich fields to `request.state.audit` as they process — `wallet_address`, `action`, `target_table`, `query`. New endpoints get IP / timestamp / status automatically; richer fields require a single one-liner.

This satisfies the spec requirement that new endpoints are logged automatically.

---

## Data Model

### SQLite table

File: `app/sqlite/audit_logs.db` (same directory as `web3health_segments.db`)

```sql
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id            TEXT PRIMARY KEY,   -- UUID v4
    timestamp         TEXT NOT NULL,      -- ISO 8601
    wallet_address    TEXT,               -- NULL for unauthenticated endpoints
    action            TEXT NOT NULL,      -- see Action Values below
    api_endpoint      TEXT NOT NULL,      -- e.g. /query
    target_table      TEXT,               -- JSON array, e.g. ["patient_data"]
    query             TEXT,               -- raw SQL; NULL if not applicable
    status            TEXT NOT NULL,      -- SUCCESS | ERROR | UNAUTHORIZED
    details           TEXT,               -- arbitrary JSON blob
    ip_address        TEXT,
    blockchain_log_id TEXT,               -- SecLog logId (bytes32 hex); NULL on chain failure
    sk2               TEXT               -- ephemeral private key hex for verifyLog
)
```

### Pydantic response model

```python
class LogEntry(BaseModel):
    log_id: str
    timestamp: str
    wallet_address: Optional[str]
    action: str
    api_endpoint: str
    target_table: Optional[List[str]]
    query: Optional[str]
    status: str
    details: Optional[dict]
    ip_address: Optional[str]
    blockchain_log_id: Optional[str]
    sk2: Optional[str]
```

### Action values

| Action | Trigger |
|---|---|
| `QUERY` | POST /query |
| `UPLOAD` | POST /upload/{table_name} |
| `DELETE` | POST /delete |
| `UPDATE` | POST /update |
| `SCHEMA_CREATE` | POST /schemas |
| `SCHEMA_DELETE` | DELETE /schemas/{table_name} |
| `SCHEMA_READ` | GET /schemas, GET /schemas/{table_name} |
| `POLICY_CREATE` | POST /access-policies |
| `POLICY_DELETE` | DELETE /access-policies, DELETE /access-policies/{addr}/all |
| `POLICY_READ` | GET /access-policies/{addr} |
| `INDEX_UPDATE` | PUT /index-cids |
| `INDEX_DELETE` | DELETE /index-cids |
| `TABLE_CONFIG` | POST/GET /tables/config |
| `IPFS_FETCH` | GET /ipfs/fetch/{cid} |
| `WEB3DB_STORE` | POST /web3db/store |
| `WEB3DB_FETCH` | GET /web3db/fetch/{cid} |
| `WEB3HEALTH_STORE` | POST /web3health/store |
| `WEB3HEALTH_FETCH` | GET /web3health/fetch/{segment_id} |
| `HEALTH_CHECK` | GET /health |

---

## SecLog Integration

On every audit event, `AuditLogger.log()` executes these steps synchronously (matching the existing blockchain write pattern):

1. Serialize the completed `LogEntry` to JSON with `sort_keys=True` for determinism.
2. Generate an ephemeral secp256k1 key pair via `eth_keys` (already in the `web3` dependency):
   ```python
   from eth_keys import keys
   sk2_obj = keys.PrivateKey(os.urandom(32))
   pub = sk2_obj.public_key.to_uncompressed_bytes()  # 65 bytes: 0x04 || x || y
   sk1x = int.from_bytes(pub[1:33], 'big')   # uint256 for newLog
   sk1y = int.from_bytes(pub[33:65], 'big')  # uint256 for newLog
   sk2_hex = sk2_obj.to_hex()                # stored in SQLite
   ```
3. Compute `messageHash = Web3.keccak(text=log_json)`.
4. Call `contract.new_log(receiver=server_address, sk1x, sk1y, messageHash, timelock=0)` — uses `wait_for_transaction_receipt`, returns `logId`.
5. Write the full log entry + `blockchain_log_id` + `sk2` to SQLite.

### Verification

Given any SQLite row, anyone can independently verify on-chain:
```python
contract.verify_log(logId, sk2, log_json)
# → True if content matches the original commitment
```

### Failure handling

If the blockchain write fails (network error, gas issue), the log is still committed to SQLite with `blockchain_log_id = NULL`. The original API request is never failed due to an audit logging error. Blockchain immutability is best-effort; SQLite durability is guaranteed.

---

## Read API

Both endpoints require `wallet_address` — responses are scoped to that wallet's own log entries only. No admin override.

### GET /audit/logs

```
Query params:
  wallet_address  string   required
  action          string   optional
  status          string   optional
  date_from       string   optional  ISO date (inclusive)
  date_to         string   optional  ISO date (inclusive)
  page            int      default 1
  page_size       int      default 50, max 200

Response 200:
{
  "total": 142,
  "page": 1,
  "page_size": 50,
  "logs": [ { ...LogEntry }, ... ]
}
```

### GET /audit/logs/{log_id}

```
Query params:
  wallet_address  string   required

Response 200: { ...LogEntry }
Response 404: log not found or wallet mismatch
```

`blockchain_log_id` and `sk2` are included in all responses so callers can verify on-chain independently.

---

## What Is NOT in Scope

- Admin role / multi-wallet log access
- Log retention or expiry policy
- Async/background blockchain writes (synchronous, matching existing pattern)
- Encryption of log content at rest in SQLite
- Exposing a `verifyLog` endpoint (verification is an offline/manual operation)
