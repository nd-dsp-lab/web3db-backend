#!/usr/bin/env python3
"""
Benchmark: Semantic Content-Addressed Partitioning (SeCAPa)
===========================================================

Measures the benefit of IPLD Merkle DAG predicate pushdown vs. naive full-scan
for two dataset sizes (100K and 1M rows).

Metrics reported per query:
  - Shards pruned (%) and nodes pruned (%)
  - Network I/O saved (KB transferred vs. total data size)
  - Traversal time, fetch time, DuckDB time, total time

Run:
    python3 benchmark_semantic.py [--host HOST] [--skip-upload]
"""

import requests
import json
import time
import sys
import os
import argparse
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# ─── Configuration ───────────────────────────────────────────────────────────

BASE = "http://localhost:8002"

DATASETS = [
    {
        "label": "100K",
        "file": "employee_100k.csv",
        "rows": 100_000,
    },
    {
        "label": "1M",
        "file": "employee_1m.csv",
        "rows": 1_000_000,
    },
]

PARTITION_ATTRS = ["age", "salary_usd"]

# Queries covering a range of selectivities (narrow → wide → full scan)
QUERIES = [
    {
        "id": "Q1",
        "desc": "Point query (age = 30)",
        "sql": "SELECT * FROM employee WHERE age = 30",
    },
    {
        "id": "Q2",
        "desc": "Narrow range (age BETWEEN 25 AND 35)",
        "sql": "SELECT COUNT(*) AS total FROM employee WHERE age BETWEEN 25 AND 35",
    },
    {
        "id": "Q3",
        "desc": "High salary (salary > 150K)",
        "sql": "SELECT * FROM employee WHERE salary_usd > 150000",
    },
    {
        "id": "Q4",
        "desc": "Combined selective (age > 55 AND salary > 120K)",
        "sql": "SELECT * FROM employee WHERE age > 55 AND salary_usd > 120000",
    },
    {
        "id": "Q5",
        "desc": "Dual BETWEEN (age 20-30, salary 40K-70K)",
        "sql": "SELECT * FROM employee WHERE age BETWEEN 20 AND 30 AND salary_usd BETWEEN 40000 AND 70000",
    },
    {
        "id": "Q6",
        "desc": "Medium range (age >= 50)",
        "sql": "SELECT department, AVG(salary_usd) AS avg_sal FROM employee WHERE age >= 50 GROUP BY department ORDER BY avg_sal DESC",
    },
    {
        "id": "Q7",
        "desc": "Wide range (salary > 60K)",
        "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 60000",
    },
    {
        "id": "Q8",
        "desc": "Full scan (no pushdown)",
        "sql": "SELECT COUNT(*) AS total FROM employee",
    },
]


# ─── Data classes ────────────────────────────────────────────────────────────

@dataclass
class UploadResult:
    label: str
    root_cid: str
    total_shards: int
    total_rows: int
    dag_levels: int
    total_data_bytes: int
    avg_shard_kb: float
    partition_ms: float
    dag_build_ms: float
    total_ms: float


@dataclass
class QueryResult:
    query_id: str
    desc: str
    result_rows: int
    # Pushdown stats
    total_shards: int          # from upload
    shards_matched: int
    shards_pruned: int
    prune_pct: float
    nodes_visited: int
    nodes_pruned: int
    node_prune_pct: float
    # I/O
    total_data_kb: float       # all shards
    fetched_data_kb: float     # only matched shards (estimated)
    io_saved_pct: float
    # Timing
    traversal_ms: float
    fetch_ms: float
    duckdb_ms: float
    total_ms: float
    # Rows
    rows_from_shards: int
    rows_after_query: int


# ─── Helpers ─────────────────────────────────────────────────────────────────

def upload_dataset(ds: dict) -> Optional[UploadResult]:
    """Upload a CSV file via /upload-semantic and return stats."""
    filepath = ds["file"]
    label = ds["label"]

    if not os.path.exists(filepath):
        print(f"  ✗ File not found: {filepath}")
        return None

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  Uploading {filepath} ({file_size_mb:.1f} MB, {ds['rows']:,} rows) ...")

    start = time.time()
    try:
        with open(filepath, "rb") as f:
            resp = requests.post(
                f"{BASE}/upload-semantic/employee",
                files={"file": (filepath, f, "text/csv")},
                params={
                    "partition_attributes": ",".join(PARTITION_ATTRS),
                    "target_shard_size_kb": 256,
                    "branching_factor": 4,
                },
                timeout=600,  # 1M rows can take a while
            )
    except requests.ConnectionError:
        print(f"  ✗ Cannot connect to server at {BASE}")
        return None
    wall_time = time.time() - start

    if resp.status_code != 200:
        print(f"  ✗ HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    r = resp.json()
    if r.get("status") != "success":
        print(f"  ✗ Upload failed: {r.get('error')}")
        return None

    result = UploadResult(
        label=label,
        root_cid=r["root_cid"],
        total_shards=r["partition"]["total_shards"],
        total_rows=r["partition"]["total_rows"],
        dag_levels=r["dag"]["levels"],
        total_data_bytes=r["dag"]["total_data_bytes"],
        avg_shard_kb=r["partition"]["avg_shard_size_kb"],
        partition_ms=r["timing"]["partition_ms"],
        dag_build_ms=r["timing"]["dag_build_ms"],
        total_ms=r["timing"]["total_ms"],
    )

    print(f"  ✓ Upload complete in {wall_time:.1f}s")
    print(f"    Root CID    : {result.root_cid[:20]}…")
    print(f"    Shards      : {result.total_shards}")
    print(f"    DAG levels  : {result.dag_levels}")
    print(f"    Data size   : {result.total_data_bytes/1024:.1f} KB ({result.total_data_bytes/1024/1024:.2f} MB)")
    print(f"    Avg shard   : {result.avg_shard_kb:.1f} KB")
    print(f"    Partition   : {result.partition_ms:.0f} ms")
    print(f"    DAG build   : {result.dag_build_ms:.0f} ms")
    return result


def run_query(upload: UploadResult, q: dict) -> Optional[QueryResult]:
    """Run a single query via /query-semantic and return metrics."""
    resp = requests.post(
        f"{BASE}/query-semantic",
        json={
            "root_cid": upload.root_cid,
            "query": q["sql"],
            "partition_attributes": PARTITION_ATTRS,
        },
        timeout=300,
    )

    if resp.status_code != 200:
        print(f"    ✗ HTTP {resp.status_code}")
        return None

    r = resp.json()
    if r.get("status") != "success":
        print(f"    ✗ {r.get('error', 'unknown error')}")
        return None

    pd_ = r["pushdown"]
    timing = r["timing"]
    assembly = r.get("data_assembly", {})

    shards_matched = pd_["shards_matched"]
    shards_pruned = upload.total_shards - shards_matched
    prune_pct = (shards_pruned / upload.total_shards * 100) if upload.total_shards > 0 else 0

    nodes_visited = pd_["nodes_visited"]
    nodes_pruned = pd_["nodes_pruned"]
    node_prune_pct = (nodes_pruned / nodes_visited * 100) if nodes_visited > 0 else 0

    total_data_kb = upload.total_data_bytes / 1024
    # Estimate fetched data: proportional to matched shards
    fetched_data_kb = (shards_matched / upload.total_shards * total_data_kb) if upload.total_shards > 0 else total_data_kb
    io_saved_pct = ((total_data_kb - fetched_data_kb) / total_data_kb * 100) if total_data_kb > 0 else 0

    return QueryResult(
        query_id=q["id"],
        desc=q["desc"],
        result_rows=r["records"],
        total_shards=upload.total_shards,
        shards_matched=shards_matched,
        shards_pruned=shards_pruned,
        prune_pct=prune_pct,
        nodes_visited=nodes_visited,
        nodes_pruned=nodes_pruned,
        node_prune_pct=node_prune_pct,
        total_data_kb=total_data_kb,
        fetched_data_kb=fetched_data_kb,
        io_saved_pct=io_saved_pct,
        traversal_ms=timing.get("traversal_ms", 0),
        fetch_ms=timing.get("fetch_decrypt_ms", 0),
        duckdb_ms=timing.get("duckdb_ms", 0),
        total_ms=timing.get("total_ms", 0),
        rows_from_shards=assembly.get("rows_from_shards", 0),
        rows_after_query=assembly.get("rows_after_query", 0),
    )


# ─── Reporting ───────────────────────────────────────────────────────────────

def print_header(title: str):
    w = 100
    print()
    print("═" * w)
    print(f"  {title}")
    print("═" * w)


def print_upload_comparison(uploads: List[UploadResult]):
    """Side-by-side upload comparison table."""
    print_header("UPLOAD SUMMARY")
    print(f"  {'Metric':<25} ", end="")
    for u in uploads:
        print(f"{'  ' + u.label:>18}", end="")
    print()
    print("  " + "─" * (25 + 18 * len(uploads)))

    rows = [
        ("Total rows",          [f"{u.total_rows:,}" for u in uploads]),
        ("Total shards",        [f"{u.total_shards}" for u in uploads]),
        ("DAG levels",          [f"{u.dag_levels}" for u in uploads]),
        ("Data size (MB)",      [f"{u.total_data_bytes/1024/1024:.2f}" for u in uploads]),
        ("Avg shard (KB)",      [f"{u.avg_shard_kb:.1f}" for u in uploads]),
        ("Partition time (ms)", [f"{u.partition_ms:.0f}" for u in uploads]),
        ("DAG build time (ms)", [f"{u.dag_build_ms:.0f}" for u in uploads]),
        ("Total upload (ms)",   [f"{u.total_ms:.0f}" for u in uploads]),
    ]
    for label, vals in rows:
        print(f"  {label:<25} ", end="")
        for v in vals:
            print(f"{v:>18}", end="")
        print()


def print_query_table(label: str, results: List[QueryResult], total_data_kb: float):
    """Print a detailed results table for one dataset."""
    print_header(f"QUERY RESULTS — {label}")

    # Table 1: Pushdown effectiveness
    print()
    print(f"  ┌─────┬────────────────────────────────────────────┬─────────┬─────────┬──────────┬──────────┐")
    print(f"  │ ID  │ Description                                │ Matched │ Pruned  │ Prune %  │ I/O Saved│")
    print(f"  ├─────┼────────────────────────────────────────────┼─────────┼─────────┼──────────┼──────────┤")
    for r in results:
        desc = r.desc[:42].ljust(42)
        print(f"  │ {r.query_id:<3} │ {desc} │ {r.shards_matched:>4}/{r.total_shards:<3} │ {r.shards_pruned:>4}    │ {r.prune_pct:>6.1f}%  │ {r.io_saved_pct:>6.1f}%  │")
    print(f"  └─────┴────────────────────────────────────────────┴─────────┴─────────┴──────────┴──────────┘")

    # Table 2: Timing breakdown
    print()
    print(f"  ┌─────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────┬──────────────┐")
    print(f"  │ ID  │ Traversal (ms) │ Fetch (ms)     │ DuckDB (ms)    │ Total (ms)     │ Result     │ Rows fetched │")
    print(f"  ├─────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────┼──────────────┤")
    for r in results:
        print(f"  │ {r.query_id:<3} │ {r.traversal_ms:>12.1f}   │ {r.fetch_ms:>12.1f}   │ {r.duckdb_ms:>12.1f}   │ {r.total_ms:>12.1f}   │ {r.result_rows:>8,}   │ {r.rows_from_shards:>10,}   │")
    print(f"  └─────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────┴──────────────┘")

    # Table 3: Network I/O analysis
    print()
    print(f"  Network I/O Analysis (total data = {total_data_kb:.1f} KB = {total_data_kb/1024:.2f} MB):")
    print(f"  ┌─────┬──────────────────────────────────────────┬─────────────┬─────────────┬─────────────┬──────────┐")
    print(f"  │ ID  │ Query                                    │ Naive (KB)  │ SeCAPa (KB) │ Saved (KB)  │ Saved %  │")
    print(f"  ├─────┼──────────────────────────────────────────┼─────────────┼─────────────┼─────────────┼──────────┤")
    for r in results:
        naive = r.total_data_kb
        fetched = r.fetched_data_kb
        saved = naive - fetched
        desc = r.desc[:40].ljust(40)
        print(f"  │ {r.query_id:<3} │ {desc} │ {naive:>9.1f}   │ {fetched:>9.1f}   │ {saved:>9.1f}   │ {r.io_saved_pct:>6.1f}%  │")
    print(f"  └─────┴──────────────────────────────────────────┴─────────────┴─────────────┴─────────────┴──────────┘")


def print_cross_dataset_comparison(all_results: Dict[str, List[QueryResult]]):
    """Compare pruning across dataset sizes for the same queries."""
    labels = list(all_results.keys())
    if len(labels) < 2:
        return

    print_header("CROSS-DATASET COMPARISON")
    print()
    print(f"  Shard pruning % across dataset sizes:")
    print(f"  ┌─────┬────────────────────────────────────────────", end="")
    for _ in labels:
        print("┬──────────", end="")
    print("┐")

    print(f"  │ ID  │ Description                                ", end="")
    for lbl in labels:
        print(f"│ {lbl:>8} ", end="")
    print("│")

    print(f"  ├─────┼────────────────────────────────────────────", end="")
    for _ in labels:
        print("┼──────────", end="")
    print("┤")

    # Match queries across datasets by ID
    query_ids = [r.query_id for r in all_results[labels[0]]]
    for qid in query_ids:
        row_data = {}
        desc = ""
        for lbl in labels:
            for r in all_results[lbl]:
                if r.query_id == qid:
                    row_data[lbl] = r
                    desc = r.desc
                    break
        desc = desc[:42].ljust(42)
        print(f"  │ {qid:<3} │ {desc}", end="")
        for lbl in labels:
            r = row_data.get(lbl)
            if r:
                print(f"│ {r.prune_pct:>6.1f}%  ", end="")
            else:
                print(f"│      -   ", end="")
        print("│")

    print(f"  └─────┴────────────────────────────────────────────", end="")
    for _ in labels:
        print("┴──────────", end="")
    print("┘")

    # I/O savings comparison
    print()
    print(f"  Network I/O saved (%) across dataset sizes:")
    print(f"  ┌─────┬────────────────────────────────────────────", end="")
    for _ in labels:
        print("┬──────────", end="")
    print("┐")

    print(f"  │ ID  │ Description                                ", end="")
    for lbl in labels:
        print(f"│ {lbl:>8} ", end="")
    print("│")

    print(f"  ├─────┼────────────────────────────────────────────", end="")
    for _ in labels:
        print("┼──────────", end="")
    print("┤")

    for qid in query_ids:
        row_data = {}
        desc = ""
        for lbl in labels:
            for r in all_results[lbl]:
                if r.query_id == qid:
                    row_data[lbl] = r
                    desc = r.desc
                    break
        desc = desc[:42].ljust(42)
        print(f"  │ {qid:<3} │ {desc}", end="")
        for lbl in labels:
            r = row_data.get(lbl)
            if r:
                print(f"│ {r.io_saved_pct:>6.1f}%  ", end="")
            else:
                print(f"│      -   ", end="")
        print("│")

    print(f"  └─────┴────────────────────────────────────────────", end="")
    for _ in labels:
        print("┴──────────", end="")
    print("┘")

    # Timing comparison
    print()
    print(f"  Total query time (ms) across dataset sizes:")
    print(f"  ┌─────┬────────────────────────────────────────────", end="")
    for _ in labels:
        print("┬──────────", end="")
    print("┐")

    print(f"  │ ID  │ Description                                ", end="")
    for lbl in labels:
        print(f"│ {lbl:>8} ", end="")
    print("│")

    print(f"  ├─────┼────────────────────────────────────────────", end="")
    for _ in labels:
        print("┼──────────", end="")
    print("┤")

    for qid in query_ids:
        row_data = {}
        desc = ""
        for lbl in labels:
            for r in all_results[lbl]:
                if r.query_id == qid:
                    row_data[lbl] = r
                    desc = r.desc
                    break
        desc = desc[:42].ljust(42)
        print(f"  │ {qid:<3} │ {desc}", end="")
        for lbl in labels:
            r = row_data.get(lbl)
            if r:
                print(f"│ {r.total_ms:>7.0f}ms ", end="")
            else:
                print(f"│      -   ", end="")
        print("│")

    print(f"  └─────┴────────────────────────────────────────────", end="")
    for _ in labels:
        print("┴──────────", end="")
    print("┘")


def print_key_takeaways(all_results: Dict[str, List[QueryResult]], uploads: List[UploadResult]):
    """Print high-level takeaways for the paper."""
    print_header("KEY TAKEAWAYS (for paper)")

    for upload in uploads:
        label = upload.label
        results = all_results.get(label, [])
        if not results:
            continue

        selective = [r for r in results if r.prune_pct > 0]
        if not selective:
            continue

        avg_prune = sum(r.prune_pct for r in selective) / len(selective)
        avg_io = sum(r.io_saved_pct for r in selective) / len(selective)
        best = max(selective, key=lambda r: r.prune_pct)
        fastest = min(selective, key=lambda r: r.total_ms)
        full_scan = next((r for r in results if r.prune_pct == 0), None)

        print(f"\n  {label} rows ({upload.total_shards} shards, {upload.dag_levels} levels, "
              f"{upload.total_data_bytes/1024/1024:.1f} MB):")
        print(f"    • Average shard pruning (selective queries):  {avg_prune:.1f}%")
        print(f"    • Average network I/O saved:                  {avg_io:.1f}%")
        print(f"    • Best pruning: {best.query_id} ({best.desc})")
        print(f"      → {best.shards_pruned}/{best.total_shards} shards pruned ({best.prune_pct:.1f}%), "
              f"I/O saved {best.io_saved_pct:.1f}%")
        print(f"    • Fastest selective query: {fastest.query_id} = {fastest.total_ms:.0f} ms")
        if full_scan:
            print(f"    • Full scan baseline: {full_scan.total_ms:.0f} ms "
                  f"(fetches all {full_scan.total_shards} shards)")
            for r in selective:
                speedup = full_scan.total_ms / r.total_ms if r.total_ms > 0 else 0
                if speedup > 1.2:
                    print(f"    • {r.query_id} is {speedup:.1f}× faster than full scan")

    print()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global BASE

    parser = argparse.ArgumentParser(description="SeCAPa Benchmark")
    parser.add_argument("--host", default=None, help=f"Server URL (default: {BASE})")
    parser.add_argument("--skip-upload", action="store_true", help="Skip upload, use provided root CIDs")
    parser.add_argument("--root-cid-100k", default=None, help="Root CID for 100K dataset (if skipping upload)")
    parser.add_argument("--root-cid-1m", default=None, help="Root CID for 1M dataset (if skipping upload)")
    parser.add_argument("--only", choices=["100k", "1m"], default=None, help="Run only one dataset size")
    args = parser.parse_args()

    if args.host:
        BASE = args.host

    # Must declare global before any use of BASE in this function
    print("╔" + "═" * 98 + "╗")
    print("║" + "  SeCAPa Benchmark: Semantic Content-Addressed Partitioning with Predicate Pushdown".center(98) + "║")
    print("║" + f"  Server: {BASE}".ljust(98) + "║")
    print("║" + f"  Date: {time.strftime('%Y-%m-%d %H:%M:%S')}".ljust(98) + "║")
    print("╚" + "═" * 98 + "╝")

    # Filter datasets
    datasets = DATASETS
    if args.only == "100k":
        datasets = [d for d in datasets if d["label"] == "100K"]
    elif args.only == "1m":
        datasets = [d for d in datasets if d["label"] == "1M"]

    # ── Phase 1: Upload ──────────────────────────────────────────────────────
    uploads: List[UploadResult] = []

    if args.skip_upload:
        # Build fake upload results from provided CIDs
        cid_map = {"100K": args.root_cid_100k, "1M": args.root_cid_1m}
        for ds in datasets:
            cid = cid_map.get(ds["label"])
            if not cid:
                print(f"  ⚠ No root CID for {ds['label']}, skipping")
                continue
            # Fetch DAG summary to recover stats
            print(f"  Using existing root CID for {ds['label']}: {cid[:20]}…")
            resp = requests.post(f"{BASE}/dag-inspect", params={"root_cid": cid}, timeout=60)
            dag = resp.json().get("dag", {})

            def _count(node):
                if node.get("node_type") == "leaf":
                    return 1, node.get("size_bytes", 0)
                total_s, total_b = 0, 0
                for c in node.get("children", []):
                    s, b = _count(c)
                    total_s += s
                    total_b += b
                return total_s, total_b

            n_shards, total_bytes = _count(dag)
            uploads.append(UploadResult(
                label=ds["label"], root_cid=cid,
                total_shards=n_shards, total_rows=ds["rows"],
                dag_levels=dag.get("depth", 0) + 1 if dag.get("node_type") == "internal" else 1,
                total_data_bytes=total_bytes,
                avg_shard_kb=total_bytes / n_shards / 1024 if n_shards else 0,
                partition_ms=0, dag_build_ms=0, total_ms=0,
            ))
    else:
        for ds in datasets:
            print_header(f"UPLOADING: {ds['label']} rows")
            result = upload_dataset(ds)
            if result:
                uploads.append(result)
            else:
                print(f"  ⚠ Skipping {ds['label']} due to upload failure")

    if not uploads:
        print("\n  ✗ No datasets uploaded. Exiting.")
        sys.exit(1)

    if len(uploads) > 1:
        print_upload_comparison(uploads)

    # ── Phase 2: Queries ─────────────────────────────────────────────────────
    all_results: Dict[str, List[QueryResult]] = {}

    for upload in uploads:
        print_header(f"RUNNING QUERIES — {upload.label} ({upload.total_shards} shards, {upload.total_rows:,} rows)")

        query_results = []
        for q in QUERIES:
            sys.stdout.write(f"  {q['id']}: {q['desc']:<45} ")
            sys.stdout.flush()
            result = run_query(upload, q)
            if result:
                query_results.append(result)
                print(f"✓  pruned {result.prune_pct:5.1f}%  I/O saved {result.io_saved_pct:5.1f}%  total {result.total_ms:7.0f}ms")
            else:
                print("✗  FAILED")

        all_results[upload.label] = query_results

        # Print detailed tables for this dataset
        print_query_table(upload.label, query_results, upload.total_data_bytes / 1024)

    # ── Phase 3: Cross-dataset comparison ────────────────────────────────────
    if len(uploads) > 1:
        print_cross_dataset_comparison(all_results)

    # ── Phase 4: Key takeaways ───────────────────────────────────────────────
    print_key_takeaways(all_results, uploads)

    print("═" * 100)
    print("  Benchmark complete.")
    print("═" * 100)


if __name__ == "__main__":
    main()
