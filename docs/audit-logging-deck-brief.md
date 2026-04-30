# Deck Design Brief: MtDB Audit Logging Feature

## What you are building

A technical presentation deck for a live demo session with research collaborators. The subject is a newly implemented audit logging system for **MtDB** — a decentralized multi-tenant database built on Intel SGX, IPFS, and Ethereum.

The audience is technical (PhD-level researchers and engineers). They understand distributed systems and cryptography but are not familiar with the MtDB codebase. The tone should be precise and confident, not salesy. No fluff. Every slide should teach something.

---

## Project context

MtDB is a research prototype for secure data sharing across organizations. It uses:
- **Intel SGX (via Gramine)** — code runs inside a hardware enclave; the operator cannot read data in memory
- **IPFS** — distributed storage for encrypted data partitions; data never stored in plaintext
- **Ethereum (Sepolia testnet)** — smart contract stores index CIDs, table schemas, and access policies on-chain
- **DuckDB** — in-memory SQL engine for querying decrypted parquet files inside the enclave
- **FastAPI** — REST API server, runs on port 8001

The security model: the SGX enclave is the trust boundary. The smart contract has no access control — security is enforced by the enclave, not the chain.

---

## The feature being presented: Audit Logging

### The problem

MtDB had no audit trail. A query ran, data was returned — but there was no tamper-evident record of:
- Who queried what
- When
- Whether the enclave was operating correctly
- Whether a log could be retroactively altered

In a multi-tenant system where one organization's data can be accessed by another (under policy), this is a trust gap.

### The solution: two-layer immutability

Every API action produces an audit log entry that exists in two places simultaneously:

**Layer 1 — SQLite (operational)**
- Fast, queryable, local
- Wallet-scoped reads: a querier can only see their own logs
- Filterable by action, status, date range
- Paginated

**Layer 2 — Ethereum SecLog (tamper-evident)**
- For each log entry, the enclave generates an ephemeral secp256k1 keypair (`sk2`)
- Computes `(sk1x, sk1y) = sk2 × G` off-chain (EC scalar multiplication on secp256k1 generator)
- Calls `newLog(receiver, sk1x, sk1y, keccak256(message), timelock)` on the smart contract
- The contract stores the public key commitment and message hash — not the secret
- Anyone with `sk2` can later call `verifyLog(logId, sk2, message)` — the contract recomputes `sk2 × G` and checks equality
- If the log was tampered with, `sk2` will not verify

`sk2` never leaves the SGX enclave in production. In the audit database it is stored alongside the `blockchain_log_id` (the on-chain log identifier).

### Architecture: three files

| File | Role |
|---|---|
| `audit_logger.py` | Core: writes SQLite row + fires SecLog blockchain commitment |
| `audit_middleware.py` | FastAPI middleware: intercepts every request/response automatically |
| `audit_controller.py` | Read API: `GET /audit/logs` and `GET /audit/logs/{id}` |

**Key design decision:** endpoints do not call the logger. The `AuditMiddleware` wraps every request. Endpoints only write rich context (wallet address, table name, query text) to `request.state.audit` — the middleware reads this after the response and fires the log. This means logging is automatic and cannot be bypassed by endpoint code.

**Failure isolation:** if the blockchain write fails, the SQLite row is still written. The system degrades gracefully — operational logs survive even if chain commits fail.

---

## The full workflow (use this as the live demo script)

### Step 1 — Upload data

```bash
curl -X POST "http://localhost:8001/upload/demo_table" \
  -F "file=@demo.csv"
```

CSV content:
```
PatientID,OwnerID,Name,Diagnosis,Age
10,0x6d7144648A233919a386a8D54D9F4b3705F96C9C,Diana,Migraine,34
11,0x6d7144648A233919a386a8D54D9F4b3705F96C9C,Evan,Arthritis,52
12,0x6d7144648A233919a386a8D54D9F4b3705F96C9C,Fiona,Diabetes,29
```

Response (actual output from live test):
```json
{
  "table_name": "demo_table",
  "data_cid": "QmaX3MWGd5AefVfGyKeeAqT7KcbYtJ1fzr2yKV5AVTWi3r",
  "index_cids": {
    "demo_table.PatientID": "QmdJoAYUwSGeq666hB8U4YYtRVo2x3zBRpxnd4htqCrCbQ"
  },
  "rows_processed": 3,
  "indexed_attributes": ["PatientID"]
}
```

What happened internally:
1. CSV → Pandas DataFrame → Parquet bytes
2. AES-256-CBC encrypt → IPFS add → `data_cid`
3. Build B+tree index over `PatientID` → serialize → AES-256 encrypt → IPFS add → `index_cid`
4. `batchUpdateIndexCIDs(["demo_table.PatientID"], [index_cid])` → single Sepolia transaction

### Step 2 — Register access policy

```bash
curl -X POST "http://localhost:8001/access-policies" \
  -H "Content-Type: application/json" \
  -d '{
    "subject_address": "0x6d7144648A233919a386a8D54D9F4b3705F96C9C",
    "object_address":  "0x6d7144648A233919a386a8D54D9F4b3705F96C9C",
    "table_name": "demo_table",
    "policy_sql": "SELECT * FROM demo_table WHERE PatientID > 0"
  }'
```

Policy is stored on-chain via `addAccessPolicy()`. Default-deny: a wallet with no policy gets zero rows.

### Step 3 — Query

```bash
curl -X POST "http://localhost:8001/query" \
  -H "Content-Type: application/json" \
  -d '{
    "wallet_address": "0x6d7144648A233919a386a8D54D9F4b3705F96C9C",
    "table_name": "demo_table",
    "index_attribute": "PatientID",
    "query": "SELECT * FROM demo_table WHERE PatientID = 11"
  }'
```

Response (actual output from live test):
```json
{
  "policy_count": 1,
  "rewritten_query": "WITH accessible_part AS (SELECT * FROM demo_table WHERE (OwnerID = '0x6d7144648A233919a386a8D54D9F4b3705F96C9C' AND PatientID > 0)) SELECT * FROM accessible_part WHERE PatientID = 11",
  "cids": 1,
  "records": 1,
  "results": [
    {
      "PatientID": 11,
      "OwnerID": "0x6d7144648A233919a386a8D54D9F4b3705F96C9C",
      "Name": "Evan",
      "Diagnosis": "Arthritis",
      "Age": 52
    }
  ]
}
```

What happened internally:
1. `getAccessPolicies(wallet)` → Sepolia view call (free, no gas)
2. SQL rewritten: `OwnerID = '0x6d...' AND (PatientID > 0)` prepended
3. `getIndexCID("demo_table.PatientID")` → Sepolia → IPFS fetch → AES decrypt → B+tree
4. B+tree lookup: PatientID=11 → matching CIDs
5. ThreadPoolExecutor: parallel IPFS fetch + AES decrypt → parquet files in `/tmp`
6. DuckDB executes rewritten query → result
7. **AuditMiddleware fires**: SQLite write + `newLog()` on Sepolia

### Step 4 — Read the audit log

```bash
curl "http://localhost:8001/audit/logs?wallet_address=0x6d7144648A233919a386a8D54D9F4b3705F96C9C"
```

Response (actual output from live test):
```json
{
  "total": 3,
  "logs": [
    {
      "log_id": "545974b8-0220-4f27-99ca-1c9101741764",
      "timestamp": "2026-04-27T20:01:31.149436+00:00",
      "action": "QUERY",
      "api_endpoint": "/query",
      "status": "SUCCESS",
      "wallet_address": "0x6d7144648A233919a386a8D54D9F4b3705F96C9C",
      "target_table": ["demo_table"],
      "query": "SELECT * FROM demo_table WHERE PatientID = 11",
      "blockchain_log_id": "8914cc4bc87987e6411d39d9d58741ba1c991929425f1aa6a9b1064c349e1e77",
      "sk2": "0x8e0bea04c1b470658d0735c398b5241231f14e7ae82f464a0d0e114246cccff8"
    }
  ]
}
```

Point out to the audience:
- `blockchain_log_id` — look this up on Sepolia Etherscan to see the on-chain commitment
- `sk2` — the secret scalar; pass this to `verifyLog(logId, sk2, message)` on-chain to cryptographically prove this log was never altered

### Step 5 — Fetch a single log by ID

```bash
curl "http://localhost:8001/audit/logs/545974b8-0220-4f27-99ca-1c9101741764?wallet_address=0x6d7144648A233919a386a8D54D9F4b3705F96C9C"
```

Returns the same entry. A different wallet gets `404` — wallet isolation is enforced at the query layer.

---

## Architecture diagram (for a slide)

```
  ┌─────────────────────────────────────────────────────────┐
  │                    SGX Enclave                          │
  │                                                         │
  │  POST /upload     POST /query     GET /audit/logs       │
  │       │                │                │               │
  │       ▼                ▼                ▼               │
  │  ┌─────────────────────────────────────────────┐        │
  │  │            AuditMiddleware                  │        │
  │  │  (wraps every request — fires after resp)   │        │
  │  └──────────────────────┬──────────────────────┘        │
  │                         │                               │
  │                         ▼                               │
  │               ┌─────────────────┐                       │
  │               │  AuditLogger    │                       │
  │               │                 │                       │
  │               │  1. SQLite row  │                       │
  │               │  2. newLog() ───┼──▶ Sepolia testnet    │
  │               └─────────────────┘      (SecLog)         │
  │                                                         │
  └─────────────────────────────────────────────────────────┘
```

---

## Slide structure suggestion

1. **Title** — "Tamper-Evident Audit Logging for a Decentralized Multi-Tenant Database"
2. **The problem** — no central audit trail; data stored on IPFS, logic on Ethereum; who watched the watchers?
3. **The solution** — two-layer immutability: SQLite (operational) + Ethereum SecLog (tamper-evident)
4. **SecLog cryptographic protocol** — ephemeral keypair, EC commitment, `verifyLog` proof
5. **Architecture** — three-file design, middleware intercept pattern, failure isolation
6. **Live demo** — run steps 1–5 above in a terminal
7. **Verification** — show `blockchain_log_id` on Sepolia Etherscan; explain `sk2` as the receipt
8. **Properties gained** — non-repudiation, tamper evidence, SGX confidentiality, graceful degradation

---

## Tone and visual notes

- Dark background preferred (this is a systems/crypto demo, not a product pitch)
- Use monospace font for all CIDs, hashes, addresses, and code blocks
- Diagrams: boxes and arrows, not icons
- Keep slides sparse — the terminal and Etherscan are the visual proof; slides are just anchors
- No marketing language. "Tamper-evident" not "secure". "Committed to the chain" not "blockchain-powered".
- Audience will ask: "what happens if the enclave is compromised?" Answer: in production, `sk2` never leaves the enclave. The attacker would need to break SGX to forge a log commitment.

---

## Key numbers (from live test run on 2026-04-27)

| Metric | Value |
|---|---|
| Blockchain log IDs confirmed on Sepolia | 3 |
| SecLog gas limit | 3,000,000 (EC scalar multiplication is expensive in pure Solidity) |
| Audit log entries per request | 1 (middleware fires once per HTTP response) |
| SQLite schema | `audit_logs` table, WAL mode |
| Log read latency | Local SQLite — sub-millisecond |
| Log write latency | Dominated by Sepolia tx confirmation (~15s) |
| Blockchain write failure behaviour | SQLite row written, `blockchain_log_id = NULL` |
