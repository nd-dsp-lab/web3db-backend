#!/usr/bin/env python3
"""
End-to-end test for the audit logging integration.
Requires the server and IPFS to be running.

Usage:
    python3 app/tests/e2e_audit.py [--host http://localhost:8001]
"""

import sys
import io
import csv
import json
import time
import argparse
import requests

WALLET = "0x6d7144648A233919a386a8D54D9F4b3705F96C9C"
OTHER_WALLET = "0x000000000000000000000000000000000000dEaD"
TABLE = "e2e_audit_test"

DEMO_CSV = (
    "PatientID,OwnerID,Name,Diagnosis,Age\n"
    f"101,{WALLET},Alice,Migraine,34\n"
    f"102,{WALLET},Bob,Arthritis,52\n"
    f"103,{WALLET},Carol,Diabetes,29\n"
)

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
SKIP = "\033[93mSKIP\033[0m"


_check_state = {"failed": 0}


def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    print(f"  [{status}] {label}")
    if not condition:
        _check_state["failed"] += 1
        if detail:
            print(f"         {detail}")
    return condition


def section(title):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


def run(host):
    failures = 0
    entry_id = None
    latest = None

    # ── Step 1: Health check ─────────────────────────────
    section("1 / 6  Health check")
    try:
        r = requests.get(f"{host}/health", timeout=15)
        ok = check("Server reachable", r.status_code == 200, f"HTTP {r.status_code}")
        if not ok:
            print("\nServer is not responding. Start it with: cd app/scripts && python3 app.py")
            sys.exit(1)
    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        print(f"\n  [{FAIL}] Cannot connect to {host}")
        print("  Start the server: cd app/scripts && python3 app.py")
        sys.exit(1)

    # ── Step 2: Upload ───────────────────────────────────
    section("2 / 6  Upload CSV")
    csv_file = io.BytesIO(DEMO_CSV.encode())
    r = requests.post(
        f"{host}/upload/{TABLE}",
        files={"file": ("demo.csv", csv_file, "text/csv")},
        timeout=120,
    )
    upload_ok = check("Upload returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
    if upload_ok:
        body = r.json()
        check("rows_processed == 3", body.get("rows_processed") == 3, str(body.get("rows_processed")))
        check("data_cid present", bool(body.get("data_cid")), str(body))
        check("index_cids present", bool(body.get("index_cids")), str(body))
    else:
        failures += 1

    # ── Step 3: Access policy ────────────────────────────
    section("3 / 6  Register access policy")
    r = requests.post(
        f"{host}/access-policies",
        json={
            "subject_address": WALLET,
            "object_address":  WALLET,
            "table_name":      TABLE,
            "policy_sql":      f"SELECT * FROM {TABLE} WHERE PatientID > 0",
        },
        timeout=120,
    )
    policy_ok = check("Policy returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
    if not policy_ok:
        failures += 1

    # ── Step 4: Query ────────────────────────────────────
    section("4 / 6  Query")
    r = requests.post(
        f"{host}/query",
        json={
            "wallet_address":   WALLET,
            "table_name":       TABLE,
            "index_attribute":  "PatientID",
            "query":            f"SELECT * FROM {TABLE} WHERE PatientID = 102",
        },
        timeout=120,
    )
    query_ok = check("Query returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
    if query_ok:
        body = r.json()
        results = body.get("results", [])
        check("At least 1 result row", len(results) >= 1, f"got {len(results)} rows")
        if results:
            check(
                "Correct row returned (PatientID=102)",
                any(str(row.get("PatientID")) == "102" for row in results),
                str(results),
            )
    else:
        failures += 1

    # ── Step 5: Audit log list ───────────────────────────
    section("5 / 6  Audit log")
    # Audit emits run on a background thread; Sepolia confirmations take ~15s.
    # Wait long enough for the QUERY tx from step 4 to land in a block.
    print("  (waiting 30s for blockchain write to settle...)")
    time.sleep(30)

    r = requests.get(
        f"{host}/audit/logs",
        params={"wallet_address": WALLET},
        timeout=30,
    )
    audit_ok = check("GET /audit/logs returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
    if audit_ok:
        body = r.json()
        logs = body.get("logs", [])
        # Pick the newest entry whose action matches what this run should
        # have produced — skips unrelated entries (e.g. leftover TEST writes
        # from earlier verification, or HEALTH_CHECK / SCHEMA_READ entries).
        relevant = [l for l in logs if l.get("action") in ("QUERY", "UPLOAD")]
        check("At least 1 QUERY/UPLOAD log entry", len(relevant) >= 1,
              f"total_returned={len(logs)}, relevant={len(relevant)}")

        if relevant:
            latest = relevant[0]
            tx_hash = latest.get("tx_hash")
            log_index = latest.get("log_index")
            if tx_hash is not None and log_index is not None:
                entry_id = f"{tx_hash}-{log_index}"

            check("tx_hash present", bool(tx_hash), str(latest))
            check("log_index present (int)", isinstance(log_index, int), str(log_index))
            check("block_number present", isinstance(latest.get("block_number"), int), str(latest.get("block_number")))
            check("wallet matches", latest.get("wallet") == WALLET, latest.get("wallet"))
            check("action is QUERY or UPLOAD", latest.get("action") in ("QUERY", "UPLOAD"), latest.get("action"))
            check("content present", bool(latest.get("content")), str(latest.get("content")))
            check("timestamp present (int)", isinstance(latest.get("timestamp"), int), str(latest.get("timestamp")))

            if tx_hash:
                tx_clean = tx_hash if tx_hash.startswith("0x") else f"0x{tx_hash}"
                print(f"\n  On-chain tx (Sepolia Etherscan):")
                print(f"  https://sepolia.etherscan.io/tx/{tx_clean}")
    else:
        failures += 1

    # ── Step 6: Single entry + wallet isolation ──────────
    section("6 / 6  Single entry & wallet isolation")
    if entry_id:
        try:
            r = requests.get(
                f"{host}/audit/logs/{entry_id}",
                params={"wallet_address": WALLET},
                timeout=30,
            )
            single_ok = check("GET /audit/logs/{entry_id} returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
            if single_ok:
                body = r.json()
                check(
                    "Returned correct entry",
                    body.get("tx_hash") == latest.get("tx_hash") and body.get("log_index") == latest.get("log_index"),
                    str(body),
                )
            else:
                failures += 1
        except requests.exceptions.ReadTimeout:
            check("GET /audit/logs/{entry_id} returns 200", False, "Request timed out after 30s")
            failures += 1

        # Small spacing — back-to-back eth_getLogs calls can trip Infura rate limits.
        time.sleep(1)
        try:
            r = requests.get(
                f"{host}/audit/logs/{entry_id}",
                params={"wallet_address": OTHER_WALLET},
                timeout=30,
            )
            check("Different wallet gets 404", r.status_code == 404, f"HTTP {r.status_code}")
        except requests.exceptions.ReadTimeout:
            check("Different wallet gets 404", False, "Request timed out after 30s")
            failures += 1
    else:
        print(f"  [{SKIP}] No entry_id captured — skipping single-entry checks")

    # ── Summary ──────────────────────────────────────────
    total_failed = failures + _check_state["failed"]
    print(f"\n{'═' * 50}")
    if total_failed == 0:
        print(f"  All checks passed.")
    else:
        print(f"  {total_failed} check(s) failed — see above.")
    print(f"{'═' * 50}\n")

    sys.exit(0 if total_failed == 0 else 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="http://localhost:8001")
    args = parser.parse_args()
    run(args.host)
