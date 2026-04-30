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


def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    print(f"  [{status}] {label}")
    if not condition and detail:
        print(f"         {detail}")
    return condition


def section(title):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


def run(host):
    failures = 0
    log_id = None

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
    print("  (waiting 3s for blockchain write to settle...)")
    time.sleep(3)

    r = requests.get(
        f"{host}/audit/logs",
        params={"wallet_address": WALLET},
        timeout=30,
    )
    audit_ok = check("GET /audit/logs returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
    if audit_ok:
        body = r.json()
        logs = body.get("logs", [])
        check("At least 1 log entry", len(logs) >= 1, f"total={body.get('total')}")

        if logs:
            latest = logs[0]
            log_id = latest.get("log_id")
            check("log_id present", bool(log_id), str(latest))
            check("wallet matches", latest.get("wallet_address") == WALLET, latest.get("wallet_address"))
            check("action is QUERY or UPLOAD", latest.get("action") in ("QUERY", "UPLOAD"), latest.get("action"))
            check("blockchain_log_id present", bool(latest.get("blockchain_log_id")), str(latest.get("blockchain_log_id")))
            check("sk2 present", bool(latest.get("sk2")), str(latest.get("sk2")))

            if latest.get("blockchain_log_id"):
                print(f"\n  On-chain log ID (Sepolia Etherscan):")
                print(f"  {latest['blockchain_log_id']}")
    else:
        failures += 1

    # ── Step 6: Single entry + wallet isolation ──────────
    section("6 / 6  Single entry & wallet isolation")
    if log_id:
        try:
            r = requests.get(
                f"{host}/audit/logs/{log_id}",
                params={"wallet_address": WALLET},
                timeout=30,
            )
            single_ok = check("GET /audit/logs/{id} returns 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")
            if single_ok:
                body = r.json()
                check("Returned correct log_id", body.get("log_id") == log_id, body.get("log_id"))
            else:
                failures += 1
        except requests.exceptions.ReadTimeout:
            check("GET /audit/logs/{id} returns 200", False, "Request timed out after 30s")
            failures += 1

        try:
            r = requests.get(
                f"{host}/audit/logs/{log_id}",
                params={"wallet_address": OTHER_WALLET},
                timeout=30,
            )
            check("Different wallet gets 404", r.status_code == 404, f"HTTP {r.status_code}")
        except requests.exceptions.ReadTimeout:
            check("Different wallet gets 404", False, "Request timed out after 30s")
            failures += 1
    else:
        print(f"  [{SKIP}] No log_id captured — skipping single-entry checks")

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'═' * 50}")
    if failures == 0:
        print(f"  All checks passed.")
    else:
        print(f"  {failures} step(s) failed — see above.")
    print(f"{'═' * 50}\n")

    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="http://localhost:8001")
    args = parser.parse_args()
    run(args.host)
