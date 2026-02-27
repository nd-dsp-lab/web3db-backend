#!/usr/bin/env python3
"""
Benchmark: Branching Factor Sensitivity Sweep
==============================================

Uploads the same dataset at a fixed shard size (128 KB) with varying
branching factors {2, 4, 8, 16, 32} and measures:
  (a) DAG traversal time
  (b) Total query latency
  (c) Pruning effectiveness

Config: 100K rows, 128 KB target shard size, 3 partition attributes.

Usage:
    python3 benchmark_branching_factor_sweep.py [--host HOST] [--runs N]
"""

import os
import sys
import time
import json
import argparse
import requests
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────────────────

BASE = "http://localhost:8002"

BRANCHING_FACTORS = [2, 4, 8, 16, 32]
TARGET_SHARD_SIZE_KB = 64
PARTITION_ATTRS = ["age", "salary_usd", "experience_years"]

DATASET_FILE = "../dataset/employee_100k.csv"

QUERIES = [
    {"id": "Q1",  "desc": "Point (age=30, exp=5)",                    "sql": "SELECT * FROM employee WHERE age = 30 AND experience_years = 5",                                                                                              "selectivity": "point"},
    {"id": "Q2",  "desc": "3D corner (age>60, sal>150K, exp>20)",     "sql": "SELECT * FROM employee WHERE age > 60 AND salary_usd > 150000 AND experience_years > 20",                                                                      "selectivity": "point"},
    {"id": "Q3",  "desc": "Salary > 150K",                            "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 150000",                                                                                              "selectivity": "narrow"},
    {"id": "Q4",  "desc": "Young + junior (age<25, exp<3)",           "sql": "SELECT * FROM employee WHERE age < 25 AND experience_years < 3",                                                                                                "selectivity": "narrow"},
    {"id": "Q5",  "desc": "Senior + high pay (age>55, sal>120K)",     "sql": "SELECT * FROM employee WHERE age > 55 AND salary_usd > 120000",                                                                                                 "selectivity": "narrow"},
    {"id": "Q6",  "desc": "3D box (age 25-35, sal 50K-90K, exp 3-8)","sql": "SELECT * FROM employee WHERE age BETWEEN 25 AND 35 AND salary_usd BETWEEN 50000 AND 90000 AND experience_years BETWEEN 3 AND 8",                                "selectivity": "medium"},
    {"id": "Q7",  "desc": "Mid-career (exp 8-15, age 30-50)",        "sql": "SELECT department, AVG(salary_usd) AS avg_sal FROM employee WHERE experience_years BETWEEN 8 AND 15 AND age BETWEEN 30 AND 50 GROUP BY department ORDER BY avg_sal DESC", "selectivity": "medium"},
    {"id": "Q8",  "desc": "Experienced (exp >= 10)",                  "sql": "SELECT COUNT(*) AS total FROM employee WHERE experience_years >= 10",                                                                                            "selectivity": "medium"},
    {"id": "Q9",  "desc": "Salary > 50K (wide)",                     "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 50000",                                                                                                "selectivity": "wide"},
    {"id": "Q10", "desc": "Age >= 25 (wide)",                        "sql": "SELECT COUNT(*) AS total FROM employee WHERE age >= 25",                                                                                                         "selectivity": "wide"},
    {"id": "Q11", "desc": "Full scan (no pushdown)",                  "sql": "SELECT COUNT(*) AS total FROM employee",                                                                                                                         "selectivity": "full"},
]


# ─── Data structures ────────────────────────────────────────────────────────

@dataclass
class UploadResult:
    branching_factor: int
    root_cid: str
    total_shards: int
    dag_levels: int
    total_data_bytes: int
    avg_shard_kb: float
    min_shard_kb: float
    max_shard_kb: float
    partition_ms: float
    dag_build_ms: float
    total_ms: float


@dataclass
class QueryResult:
    query_id: str
    desc: str
    selectivity: str
    result_rows: int
    total_shards: int
    shards_matched: int
    shards_pruned: int
    prune_pct: float
    nodes_visited: int
    nodes_pruned: int
    node_prune_pct: float
    total_data_kb: float
    fetched_data_kb: float
    io_saved_pct: float
    traversal_ms: float
    fetch_ms: float
    duckdb_ms: float
    total_ms: float


# ─── API helpers ─────────────────────────────────────────────────────────────

def upload_dataset(filepath: str, bf: int) -> Optional[UploadResult]:
    if not os.path.exists(filepath):
        print(f"    ✗ File not found: {filepath}")
        return None

    print(f"    Uploading with branching_factor={bf}, shard_size={TARGET_SHARD_SIZE_KB}KB ...")
    start = time.time()
    try:
        with open(filepath, "rb") as f:
            resp = requests.post(
                f"{BASE}/upload-semantic/employee",
                files={"file": (os.path.basename(filepath), f, "text/csv")},
                params={
                    "partition_attributes": ",".join(PARTITION_ATTRS),
                    "target_shard_size_kb": TARGET_SHARD_SIZE_KB,
                    "branching_factor": bf,
                },
                timeout=600,
            )
    except requests.ConnectionError:
        print(f"    ✗ Cannot connect to server at {BASE}")
        return None

    wall = time.time() - start
    if resp.status_code != 200:
        print(f"    ✗ HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    r = resp.json()
    if r.get("status") != "success":
        print(f"    ✗ Upload failed: {r.get('error')}")
        return None

    result = UploadResult(
        branching_factor=bf,
        root_cid=r["root_cid"],
        total_shards=r["partition"]["total_shards"],
        dag_levels=r["dag"]["levels"],
        total_data_bytes=r["dag"]["total_data_bytes"],
        avg_shard_kb=r["partition"]["avg_shard_size_kb"],
        min_shard_kb=r["partition"]["min_shard_size_kb"],
        max_shard_kb=r["partition"]["max_shard_size_kb"],
        partition_ms=r["timing"]["partition_ms"],
        dag_build_ms=r["timing"]["dag_build_ms"],
        total_ms=r["timing"]["total_ms"],
    )
    print(f"    ✓ Done in {wall:.1f}s — {result.total_shards} shards, "
          f"DAG levels={result.dag_levels}, avg {result.avg_shard_kb:.1f} KB")
    return result


def run_query(upload: UploadResult, q: dict) -> Optional[QueryResult]:
    try:
        resp = requests.post(
            f"{BASE}/query-semantic",
            json={
                "root_cid": upload.root_cid,
                "query": q["sql"],
                "partition_attributes": PARTITION_ATTRS,
            },
            timeout=300,
        )
    except requests.ConnectionError:
        return None

    if resp.status_code != 200:
        return None

    r = resp.json()
    if r.get("status") != "success":
        return None

    pd_ = r["pushdown"]
    timing = r["timing"]

    shards_matched = pd_["shards_matched"]
    shards_pruned = upload.total_shards - shards_matched
    prune_pct = (shards_pruned / upload.total_shards * 100) if upload.total_shards > 0 else 0

    nodes_visited = pd_["nodes_visited"]
    nodes_pruned = pd_["nodes_pruned"]
    node_prune_pct = (nodes_pruned / nodes_visited * 100) if nodes_visited > 0 else 0

    total_data_kb = upload.total_data_bytes / 1024
    fetched_data_kb = (shards_matched / upload.total_shards * total_data_kb) if upload.total_shards > 0 else total_data_kb
    io_saved_pct = ((total_data_kb - fetched_data_kb) / total_data_kb * 100) if total_data_kb > 0 else 0

    return QueryResult(
        query_id=q["id"], desc=q["desc"], selectivity=q["selectivity"],
        result_rows=r["records"], total_shards=upload.total_shards,
        shards_matched=shards_matched, shards_pruned=shards_pruned,
        prune_pct=prune_pct, nodes_visited=nodes_visited,
        nodes_pruned=nodes_pruned, node_prune_pct=node_prune_pct,
        total_data_kb=total_data_kb, fetched_data_kb=fetched_data_kb,
        io_saved_pct=io_saved_pct,
        traversal_ms=timing.get("traversal_ms", 0),
        fetch_ms=timing.get("fetch_decrypt_ms", 0),
        duckdb_ms=timing.get("duckdb_ms", 0),
        total_ms=timing.get("total_ms", 0),
    )


def run_queries_with_warmup(upload: UploadResult, queries: list, runs: int) -> List[QueryResult]:
    # Warm-up
    print("    Warm-up run ...")
    for q in queries:
        run_query(upload, q)

    accum: Dict[str, List[QueryResult]] = {q["id"]: [] for q in queries}
    for run_i in range(runs):
        if runs > 1:
            print(f"    Run {run_i+1}/{runs} ...")
        for q in queries:
            result = run_query(upload, q)
            if result:
                accum[q["id"]].append(result)

    averaged: List[QueryResult] = []
    for q in queries:
        results = accum[q["id"]]
        if not results:
            continue
        n = len(results)
        avg = QueryResult(
            query_id=q["id"], desc=q["desc"], selectivity=q["selectivity"],
            result_rows=results[0].result_rows, total_shards=results[0].total_shards,
            shards_matched=results[0].shards_matched, shards_pruned=results[0].shards_pruned,
            prune_pct=results[0].prune_pct, nodes_visited=results[0].nodes_visited,
            nodes_pruned=results[0].nodes_pruned, node_prune_pct=results[0].node_prune_pct,
            total_data_kb=results[0].total_data_kb, fetched_data_kb=results[0].fetched_data_kb,
            io_saved_pct=results[0].io_saved_pct,
            traversal_ms=sum(r.traversal_ms for r in results) / n,
            fetch_ms=sum(r.fetch_ms for r in results) / n,
            duckdb_ms=sum(r.duckdb_ms for r in results) / n,
            total_ms=sum(r.total_ms for r in results) / n,
        )
        averaged.append(avg)
    return averaged


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global BASE, DATASET_FILE

    parser = argparse.ArgumentParser(description="Branching Factor Sensitivity Sweep")
    parser.add_argument("--host", default=None, help=f"Server URL (default: {BASE})")
    parser.add_argument("--dataset", default=None, help=f"CSV file (default: {DATASET_FILE})")
    parser.add_argument("--runs", type=int, default=1, help="Query runs to average (default: 1)")
    parser.add_argument("--output-dir", default="../plot", help="Output directory")
    args = parser.parse_args()

    if args.host:
        BASE = args.host
    if args.dataset:
        DATASET_FILE = args.dataset

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("╔" + "═" * 80 + "╗")
    print("║" + "  Branching Factor Sensitivity Sweep".center(80) + "║")
    print("║" + f"  Server: {BASE}".ljust(80) + "║")
    print("║" + f"  Dataset: {DATASET_FILE}".ljust(80) + "║")
    print("║" + f"  Shard size: {TARGET_SHARD_SIZE_KB} KB (fixed)".ljust(80) + "║")
    print("║" + f"  Branching factors: {BRANCHING_FACTORS}".ljust(80) + "║")
    print("║" + f"  Partition attrs: {PARTITION_ATTRS}".ljust(80) + "║")
    print("║" + f"  Query runs: {args.runs}".ljust(80) + "║")
    print("╚" + "═" * 80 + "╝")

    sweep_results = []

    for bf in BRANCHING_FACTORS:
        print(f"\n{'━' * 80}")
        print(f"  BRANCHING FACTOR = {bf}")
        print(f"{'━' * 80}")

        upload = upload_dataset(DATASET_FILE, bf)
        if not upload:
            print(f"    ⚠ Upload failed for BF={bf} — skipping")
            continue

        print(f"    Running queries ({args.runs} run(s)) ...")
        query_results = run_queries_with_warmup(upload, QUERIES, args.runs)

        for qr in query_results:
            print(f"      {qr.query_id}: prune={qr.prune_pct:5.1f}%  "
                  f"traversal={qr.traversal_ms:7.0f}ms  "
                  f"total={qr.total_ms:7.0f}ms")

        sweep_results.append({
            "branching_factor": bf,
            "upload": asdict(upload),
            "queries": [asdict(qr) for qr in query_results],
        })

    if not sweep_results:
        print("\n  ✗ No successful sweep points. Exiting.")
        sys.exit(1)

    # Save JSON
    json_path = os.path.join(output_dir, f"branching_factor_sweep_{timestamp}.json")
    with open(json_path, "w") as f:
        json.dump(sweep_results, f, indent=2)
    print(f"\n  JSON saved: {json_path}")

    # Save text report
    txt_path = os.path.join(output_dir, f"branching_factor_sweep_{timestamp}.txt")
    lines = []
    w = 120
    lines.append("=" * w)
    lines.append("  BRANCHING FACTOR SENSITIVITY SWEEP")
    lines.append(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Dataset: {DATASET_FILE}")
    lines.append(f"  Shard size: {TARGET_SHARD_SIZE_KB} KB (fixed)")
    lines.append(f"  Partition attrs: {PARTITION_ATTRS}")
    lines.append(f"  Branching factors: {BRANCHING_FACTORS}")
    lines.append("=" * w)

    # DAG structure
    lines.append("")
    lines.append("─" * w)
    lines.append("  DAG STRUCTURE")
    lines.append("─" * w)
    lines.append(f"  {'BF':>4} │ {'Shards':>7} │ {'DAG Levels':>10} │ {'Total Nodes':>11} │ {'DAG Build ms':>12} │ {'Upload ms':>10}")
    lines.append("  " + "─" * 70)
    for sp in sweep_results:
        u = sp["upload"]
        # total nodes from full-scan query
        full_scan = next((q for q in sp["queries"] if q["query_id"] == "Q11"), None)
        total_nodes = full_scan["nodes_visited"] if full_scan else "—"
        lines.append(f"  {u['branching_factor']:>4} │ {u['total_shards']:>7} │ {u['dag_levels']:>10} │ {str(total_nodes):>11} │ {u['dag_build_ms']:>12.1f} │ {u['total_ms']:>10.1f}")

    # Pruning
    lines.append("")
    lines.append("─" * w)
    lines.append("  PRUNING % BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for sp in sweep_results:
        hdr += f" │ BF={sp['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for sp in sweep_results:
            qr = next((r for r in sp["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['prune_pct']:>5.1f}%"
            else:
                row += f" │     —"
        lines.append(row)

    # Traversal time
    lines.append("")
    lines.append("─" * w)
    lines.append("  DAG TRAVERSAL TIME (ms) BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for sp in sweep_results:
        hdr += f" │  BF={sp['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for sp in sweep_results:
            qr = next((r for r in sp["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['traversal_ms']:>6.0f}"
            else:
                row += f" │     —"
        lines.append(row)

    # Total latency
    lines.append("")
    lines.append("─" * w)
    lines.append("  TOTAL QUERY LATENCY (ms) BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for sp in sweep_results:
        hdr += f" │  BF={sp['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for sp in sweep_results:
            qr = next((r for r in sp["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['total_ms']:>6.0f}"
            else:
                row += f" │     —"
        lines.append(row)

    # Summary
    lines.append("")
    lines.append("─" * w)
    lines.append("  SUMMARY (averages over selective queries Q1–Q8)")
    lines.append("─" * w)
    for sp in sweep_results:
        bf = sp["upload"]["branching_factor"]
        sel = [q for q in sp["queries"] if q["prune_pct"] > 0]
        if sel:
            avg_prune = sum(q["prune_pct"] for q in sel) / len(sel)
            avg_trav = sum(q["traversal_ms"] for q in sel) / len(sel)
            avg_total = sum(q["total_ms"] for q in sel) / len(sel)
            lines.append(f"  BF={bf:>2}: avg_prune={avg_prune:.1f}%, avg_traversal={avg_trav:.0f}ms, "
                         f"avg_total={avg_total:.0f}ms, dag_levels={sp['upload']['dag_levels']}, "
                         f"total_dag_nodes(fullscan)={next((q['nodes_visited'] for q in sp['queries'] if q['query_id']=='Q11'), '—')}")

    lines.append("")
    lines.append("=" * w)

    report = "\n".join(lines)
    with open(txt_path, "w") as f:
        f.write(report)
    print(f"  Report saved: {txt_path}")
    print(report)

    print("\n" + "═" * 80)
    print("  Sweep complete.")
    print(f"    JSON  : {json_path}")
    print(f"    Report: {txt_path}")
    print("═" * 80)


if __name__ == "__main__":
    main()
