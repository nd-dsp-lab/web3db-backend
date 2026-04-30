# End-to-End Audit Test — `app/tests/e2e_audit.py`

## What it is

A single Python script that exercises the entire MtDB audit logging feature end-to-end by making real HTTP requests to a live server. It runs 6 steps in sequence, checks specific conditions at each step, and prints PASS/FAIL for every check. It exits with code 0 if everything passed, 1 if anything failed — so it can be used in CI.

**Run it:**
```bash
python3 app/tests/e2e_audit.py

# Against a different host:
python3 app/tests/e2e_audit.py --host http://your-server:8001
```

Requires the server (`app/scripts/app.py`) and IPFS (`app/ipfs/docker-compose.yml`) to be running.

---

## Setup (top of file)

```python
WALLET = "0x6d7144648A233919a386a8D54D9F4b3705F96C9C"
OTHER_WALLET = "0x000000000000000000000000000000000000dEaD"
TABLE = "e2e_audit_test"
```

`WALLET` is the test identity — a real Ethereum address that owns the data and runs the queries. `OTHER_WALLET` is a dummy address used to verify that one wallet can't read another's logs. `TABLE` is the name given to the test dataset so it doesn't collide with real data.

```python
DEMO_CSV = "PatientID,OwnerID,Name,Diagnosis,Age\n101,...\n102,...\n103,..."
```

Three rows of synthetic patient data built inline — no file needed on disk. The `OwnerID` column in each row is set to the test wallet address, which is how MtDB enforces row-level access control.

---

## Step 1 — Health check

Hits `GET /health`. If the server doesn't respond at all (connection refused or timeout), the script exits immediately with a helpful message rather than running 5 more steps that would all fail for the same reason.

---

## Step 2 — Upload

Posts the demo CSV to `POST /upload/e2e_audit_test`. This triggers the full upload pipeline: CSV → Parquet → AES-256 encrypt → IPFS → blockchain index update. The script checks:

- HTTP 200
- `rows_processed == 3` — all three rows made it in
- `data_cid` is present — the encrypted data landed on IPFS
- `index_cids` is present — the blockchain index was updated

120-second timeout because a Sepolia transaction confirmation takes ~15 seconds and IPFS can be slow.

---

## Step 3 — Access policy

Posts to `POST /access-policies` to grant the test wallet permission to read its own data. Without this, the query in step 4 would return zero rows — MtDB is default-deny. The policy SQL here is `SELECT * FROM e2e_audit_test WHERE PatientID > 0`, which allows all rows for this wallet. This also fires a blockchain transaction (storing the policy on-chain), so same 120-second timeout.

---

## Step 4 — Query

Posts to `POST /query` asking for `PatientID = 102` (Bob's row). This triggers the full query pipeline: blockchain index lookup → IPFS fetch → AES decrypt → DuckDB SQL execution. The script checks:

- HTTP 200
- At least 1 result row came back
- The returned row actually has `PatientID = 102` — verifying the index lookup returned the right data, not just any data

This step also fires the audit middleware in the background, which writes a log entry to SQLite and starts a `newLog()` blockchain transaction.

---

## Step 5 — Audit log

Waits 3 seconds first to give the blockchain write from step 4 time to land, then hits `GET /audit/logs?wallet_address=<WALLET>`. This is the audit read API. The script checks:

- HTTP 200
- At least 1 log entry exists (the query from step 4 should have generated one)
- The entry has a `log_id` (UUID assigned at write time)
- The `wallet_address` on the entry matches the test wallet — confirms wallet scoping is working
- The `action` is either `QUERY` or `UPLOAD` — confirms the middleware correctly identified what kind of request it was
- `blockchain_log_id` is present — the `newLog()` call succeeded and the log was committed to Sepolia
- `sk2` is present — the ephemeral private key scalar stored alongside the log; pass this to `verifyLog()` on-chain to prove the log was never tampered with

It also prints the `blockchain_log_id` so you can look it up on Etherscan or use it to call `getLog()` on the contract.

---

## Step 6 — Single entry + wallet isolation

Two checks using the `log_id` captured in step 5:

**Ownership check:** `GET /audit/logs/{log_id}?wallet_address=WALLET` — fetches that specific entry by ID and verifies the returned `log_id` matches what was requested.

**Isolation check:** same URL but with `OTHER_WALLET` instead. Expects a `404`. This proves the audit API won't return another user's log entries, even if they know the exact log ID.

---

## Summary

At the end the script prints how many steps failed and exits with the appropriate code. The whole thing takes 30–90 seconds depending on Sepolia congestion — the blockchain writes dominate the runtime.

The script tests three distinct layers simultaneously: the FastAPI endpoints, the SQLite audit database, and the Ethereum blockchain commitment — all in one run against a real live system.

---

## What the `sk2` field means

Every audit log entry includes an `sk2` value — a 32-byte secret scalar generated fresh for that log entry. When the log was written, the enclave computed `(sk1x, sk1y) = sk2 × G` (elliptic curve scalar multiplication on secp256k1) and called `newLog()` on the smart contract, which stored the public key point and a hash of the log message on-chain.

To verify a log was never altered:
1. Take `sk2` from the audit entry
2. Take the original log message (the JSON that was hashed)
3. Call `verifyLog(logId, sk2, message)` on the contract
4. The contract recomputes `sk2 × G` on-chain and checks it matches the stored public key — if it does, and the message hash matches, the log is proven authentic

In production, `sk2` never leaves the SGX enclave. An attacker would need to break SGX to forge a log commitment.
