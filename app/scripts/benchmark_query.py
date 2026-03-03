#!/usr/bin/env python3
"""
Benchmark Query — Run on Server B (UGA)
========================================

Reads the upload manifest JSON (produced by benchmark_upload.py on Server A),
then runs every query against each config with COLD CACHE:
  - Flush LOCAL semantic cache  (POST localhost:8002/cache/clear)
  - Flush LOCAL IPFS datastore  (POST localhost:5001/api/v0/repo/gc)
  - Run each query exactly ONCE  (no warmup, no averaging)

This ensures every IPFS block is fetched over the network from Server A's
IPFS node, capturing true cross-campus (ND → UGA) network latency.

Usage (on Server B, after SCP'ing the manifest):
    python3 benchmark_query.py --manifest upload_manifest_20260701_120000.json

For NSDI 2027.
"""

import os
import sys
import time
import json
import argparse
import requests
from datetime import datetime

# ─── Defaults ────────────────────────────────────────────────────────────────

DEFAULT_HOST = "http://localhost:8002"
IPFS_API     = "http://localhost:5001/api/v0"

PARTITION_ATTRS = ["age", "salary_usd", "experience_years"]

QUERIES = [
    # ── Point
    {"id": "Q1",  "desc": "Point (age=30, exp=5)",
     "sql": "SELECT * FROM employee WHERE age = 30 AND experience_years = 5",
     "selectivity": "point"},
    {"id": "Q2",  "desc": "3D corner (age>60, sal>150K, exp>20)",
     "sql": "SELECT * FROM employee WHERE age > 60 AND salary_usd > 150000 AND experience_years > 20",
     "selectivity": "point"},
    # ── Narrow
    {"id": "Q3",  "desc": "Salary > 150K",
     "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 150000",
     "selectivity": "narrow"},
    {"id": "Q4",  "desc": "Young + junior (age<25, exp<3)",
     "sql": "SELECT * FROM employee WHERE age < 25 AND experience_years < 3",
     "selectivity": "narrow"},
    {"id": "Q5",  "desc": "Senior + high pay (age>55, sal>120K)",
     "sql": "SELECT * FROM employee WHERE age > 55 AND salary_usd > 120000",
     "selectivity": "narrow"},
    # ── Medium
    {"id": "Q6",  "desc": "3D box (age 25-35, sal 50K-90K, exp 3-8)",
     "sql": "SELECT * FROM employee WHERE age BETWEEN 25 AND 35 AND salary_usd BETWEEN 50000 AND 90000 AND experience_years BETWEEN 3 AND 8",
     "selectivity": "medium"},
    {"id": "Q7",  "desc": "Mid-career (exp 8-15, age 30-50)",
     "sql": "SELECT department, AVG(salary_usd) AS avg_sal FROM employee WHERE experience_years BETWEEN 8 AND 15 AND age BETWEEN 30 AND 50 GROUP BY department ORDER BY avg_sal DESC",
     "selectivity": "medium"},
    {"id": "Q8",  "desc": "Experienced (exp >= 10)",
     "sql": "SELECT COUNT(*) AS total FROM employee WHERE experience_years >= 10",
     "selectivity": "medium"},
    # ── Wide
    {"id": "Q9",  "desc": "Salary > 50K (wide)",
     "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 50000",
     "selectivity": "wide"},
    {"id": "Q10", "desc": "Age >= 25 (wide)",
     "sql": "SELECT COUNT(*) AS total FROM employee WHERE age >= 25",
     "selectivity": "wide"},
    # ── Full scan
    {"id": "Q11", "desc": "Full scan (no pushdown)",
     "sql": "SELECT COUNT(*) AS total FROM employee",
     "selectivity": "full"},
]


# ─── Cache management (LOCAL on Server B) ────────────────────────────────────

def flush_local_caches(host: str):
    """
    Flush ALL local caches on Server B so every query is a cold-cache
    network fetch from Server A's IPFS node.

    1. POST /cache/clear   — app-level semantic cache
    2. POST /repo/gc       — IPFS local blockstore (drops fetched blocks)
    """
    print(f"    Flushing local caches ...")

    # 1. App cache
    try:
        resp = requests.post(f"{host}/cache/clear", timeout=30)
        if resp.status_code == 200:
            r = resp.json()
            print(f"      ✓ Semantic cache cleared ({r.get('entries_cleared', 0)} entries)")
        else:
            print(f"      ⚠ Cache clear → HTTP {resp.status_code}")
    except Exception as e:
        print(f"      ⚠ Cache clear failed: {e}")

    # 2. IPFS repo GC (always localhost because this script runs ON Server B)
    try:
        resp = requests.post(f"{IPFS_API}/repo/gc", timeout=120)
        if resp.status_code == 200:
            print(f"      ✓ IPFS repo GC completed ({IPFS_API})")
        else:
            print(f"      ⚠ IPFS GC → HTTP {resp.status_code}")
    except Exception as e:
        print(f"      ⚠ IPFS GC failed ({IPFS_API}): {e}")

    # Let GC settle
    time.sleep(2)


# ─── Query runner ────────────────────────────────────────────────────────────

def run_query(host: str, root_cid: str, total_shards: int,
              total_data_bytes: int, q: dict) -> dict | None:
    """Run a single query and return a results dict, or None on failure."""
    try:
        resp = requests.post(
            f"{host}/query-semantic",
            json={
                "root_cid": root_cid,
                "query": q["sql"],
                "partition_attributes": PARTITION_ATTRS,
            },
            timeout=600,
        )
    except requests.ConnectionError:
        print(f"        ✗ Cannot connect to {host}")
        return None

    if resp.status_code != 200:
        print(f"        ✗ HTTP {resp.status_code} for {q['id']}")
        return None

    r = resp.json()
    if r.get("status") != "success":
        print(f"        ✗ {q['id']}: {r.get('error', '')}")
        return None

    pd_ = r["pushdown"]
    timing = r["timing"]

    shards_matched = pd_["shards_matched"]
    shards_pruned = total_shards - shards_matched
    prune_pct = (shards_pruned / total_shards * 100) if total_shards > 0 else 0

    nodes_visited = pd_["nodes_visited"]
    nodes_pruned = pd_["nodes_pruned"]
    node_prune_pct = (nodes_pruned / nodes_visited * 100) if nodes_visited > 0 else 0

    total_data_kb = total_data_bytes / 1024
    fetched_data_kb = (shards_matched / total_shards * total_data_kb) if total_shards > 0 else total_data_kb
    io_saved_pct = ((total_data_kb - fetched_data_kb) / total_data_kb * 100) if total_data_kb > 0 else 0

    return {
        "query_id":       q["id"],
        "desc":           q["desc"],
        "selectivity":    q["selectivity"],
        "result_rows":    r["records"],
        "total_shards":   total_shards,
        "shards_matched": shards_matched,
        "shards_pruned":  shards_pruned,
        "prune_pct":      round(prune_pct, 2),
        "nodes_visited":  nodes_visited,
        "nodes_pruned":   nodes_pruned,
        "node_prune_pct": round(node_prune_pct, 2),
        "total_data_kb":  round(total_data_kb, 2),
        "fetched_data_kb": round(fetched_data_kb, 2),
        "io_saved_pct":   round(io_saved_pct, 2),
        "traversal_ms":   timing.get("traversal_ms", 0),
        "fetch_ms":       timing.get("fetch_decrypt_ms", 0),
        "duckdb_ms":      timing.get("duckdb_ms", 0),
        "total_ms":       timing.get("total_ms", 0),
    }


def run_queries_cold(host: str, config: dict, queries: list) -> list:
    """
    Run all queries ONCE with cold cache — no warmup, no averaging.
    Returns list of result dicts.
    """
    results = []
    for q in queries:
        result = run_query(
            host, config["root_cid"],
            config["total_shards"], config["total_data_bytes"], q,
        )
        if result:
            results.append(result)
            print(f"      {result['query_id']}: prune={result['prune_pct']:5.1f}%  "
                  f"fetch={result['fetch_ms']:7.0f}ms  "
                  f"total={result['total_ms']:7.0f}ms")
        else:
            print(f"      {q['id']}: ✗ failed")
    return results


# ─── Text report writers ────────────────────────────────────────────────────

def write_shard_sweep_report(configs: list, manifest: dict, output_path: str):
    """Write detailed text report for shard size sweep results."""
    lines = []
    w = 120
    selective_ids = {q["id"] for q in QUERIES if q["selectivity"] in ("point", "narrow", "medium")}

    lines.append("=" * w)
    lines.append("  DISTRIBUTED SHARD SIZE SWEEP — True Network Latency (Cold Cache)")
    lines.append(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Upload server (A): {manifest['upload_host']}")
    lines.append(f"  Query server  (B): localhost (this machine)")
    lines.append(f"  Dataset: {manifest['dataset']} ({manifest['dataset_rows']:,} rows)")
    lines.append(f"  Partition attrs: {manifest['partition_attrs']}")
    lines.append(f"  Mode: COLD CACHE (single run, no warmup, IPFS GC between configs)")
    lines.append("=" * w)

    # ── Partition summary
    lines.append("")
    lines.append("─" * w)
    lines.append("  PARTITION SUMMARY")
    lines.append("─" * w)
    header = (f"  {'Target KB':>10} │ {'Shards':>7} │ {'DAG Lvl':>7} │ {'Avg KB':>8} │ "
              f"{'Min KB':>8} │ {'Max KB':>8} │ {'Partition ms':>12} │ {'DAG ms':>8} │ {'Upload ms':>10}")
    lines.append(header)
    lines.append("  " + "─" * (len(header) - 2))
    for cfg in configs:
        u = cfg["upload"]
        lines.append(
            f"  {u['shard_size_kb']:>10} │ {u['total_shards']:>7} │ {u['dag_levels']:>7} │ "
            f"{u['avg_shard_kb']:>8.1f} │ {u['min_shard_kb']:>8.1f} │ {u['max_shard_kb']:>8.1f} │ "
            f"{u['partition_ms']:>12.1f} │ {u['dag_build_ms']:>8.1f} │ {u['total_ms']:>10.1f}"
        )

    # ── Pruning %
    lines.append("")
    lines.append("─" * w)
    lines.append("  PRUNING % BY SHARD SIZE (per query)")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<38}"
    for cfg in configs:
        hdr += f" │ {cfg['upload']['shard_size_kb']:>4}KB"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:38]:<38}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['prune_pct']:>5.1f}%"
            else:
                row += f" │     -"
        lines.append(row)

    # ── Total latency
    lines.append("")
    lines.append("─" * w)
    lines.append("  TOTAL QUERY LATENCY — COLD CACHE (ms) BY SHARD SIZE")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<38}"
    for cfg in configs:
        hdr += f" │ {cfg['upload']['shard_size_kb']:>6}KB"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:38]:<38}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['total_ms']:>7.0f}ms"
            else:
                row += f" │       -"
        lines.append(row)

    # ── Timing breakdown per shard size
    lines.append("")
    lines.append("─" * w)
    lines.append("  TIMING BREAKDOWN (ms) — Traversal / Network Fetch / DuckDB")
    lines.append("─" * w)
    for cfg in configs:
        u = cfg["upload"]
        lines.append(f"\n  Shard size = {u['shard_size_kb']} KB "
                     f"({u['total_shards']} shards, {u['dag_levels']} levels):")
        lines.append(f"  {'Query':>5} │ {'Trav ms':>8} │ {'Fetch ms':>9} │ "
                     f"{'DuckDB ms':>10} │ {'Total ms':>9} │ {'Prune%':>7} │ {'I/O Saved%':>10}")
        lines.append("  " + "─" * 80)
        for qr in cfg["queries"]:
            lines.append(
                f"  {qr['query_id']:>5} │ {qr['traversal_ms']:>8.1f} │ {qr['fetch_ms']:>9.1f} │ "
                f"{qr['duckdb_ms']:>10.1f} │ {qr['total_ms']:>9.1f} │ "
                f"{qr['prune_pct']:>6.1f}% │ {qr['io_saved_pct']:>9.1f}%"
            )

    # ── Network I/O savings
    lines.append("")
    lines.append("─" * w)
    lines.append("  NETWORK I/O SAVED (%) BY SHARD SIZE")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<38}"
    for cfg in configs:
        hdr += f" │ {cfg['upload']['shard_size_kb']:>4}KB"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:38]:<38}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['io_saved_pct']:>5.1f}%"
            else:
                row += f" │     -"
        lines.append(row)

    # ── Sweet spot (Q1-Q8)
    lines.append("")
    lines.append("─" * w)
    lines.append("  SWEET SPOT ANALYSIS (averaged over Q1–Q8, cold cache)")
    lines.append("─" * w)
    for cfg in configs:
        u = cfg["upload"]
        sel = [qr for qr in cfg["queries"] if qr["query_id"] in selective_ids]
        if sel:
            avg_prune = sum(qr["prune_pct"] for qr in sel) / len(sel)
            avg_fetch = sum(qr["fetch_ms"] for qr in sel) / len(sel)
            avg_total = sum(qr["total_ms"] for qr in sel) / len(sel)
            avg_io    = sum(qr["io_saved_pct"] for qr in sel) / len(sel)
            lines.append(
                f"  {u['shard_size_kb']:>4} KB: avg_prune={avg_prune:.1f}%, "
                f"avg_fetch={avg_fetch:.0f}ms, avg_total={avg_total:.0f}ms, "
                f"avg_io_saved={avg_io:.1f}%, shards={u['total_shards']}, "
                f"levels={u['dag_levels']}"
            )

    # ── Full-scan baseline
    lines.append("")
    lines.append("─" * w)
    lines.append("  LATENCY SAVINGS vs FULL SCAN (Q11 baseline)")
    lines.append("─" * w)
    for cfg in configs:
        u = cfg["upload"]
        full_scan = next((qr for qr in cfg["queries"] if qr["query_id"] == "Q11"), None)
        if not full_scan:
            continue
        baseline_ms = full_scan["total_ms"]
        sel = [qr for qr in cfg["queries"] if qr["query_id"] in selective_ids]
        if sel:
            avg_sel_ms = sum(qr["total_ms"] for qr in sel) / len(sel)
            speedup = baseline_ms / avg_sel_ms if avg_sel_ms > 0 else 0
            saved_ms = baseline_ms - avg_sel_ms
            lines.append(
                f"  {u['shard_size_kb']:>4} KB: full_scan={baseline_ms:.0f}ms, "
                f"avg_selective={avg_sel_ms:.0f}ms, saved={saved_ms:.0f}ms, "
                f"speedup={speedup:.1f}×"
            )

    lines.append("")
    lines.append("=" * w)

    report = "\n".join(lines)
    with open(output_path, "w") as f:
        f.write(report)
    print(f"\n  Report saved: {output_path}")
    print(report)


def write_bf_sweep_report(configs: list, manifest: dict, output_path: str):
    """Write detailed text report for branching factor sweep results."""
    lines = []
    w = 120
    selective_ids = {q["id"] for q in QUERIES if q["selectivity"] in ("point", "narrow", "medium")}

    lines.append("=" * w)
    lines.append("  DISTRIBUTED BRANCHING FACTOR SWEEP — True Network Latency (Cold Cache)")
    lines.append(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Upload server (A): {manifest['upload_host']}")
    lines.append(f"  Query server  (B): localhost (this machine)")
    lines.append(f"  Dataset: {manifest['dataset']} ({manifest['dataset_rows']:,} rows)")
    lines.append(f"  Partition attrs: {manifest['partition_attrs']}")
    lines.append(f"  Mode: COLD CACHE (single run, no warmup, IPFS GC between configs)")
    lines.append("=" * w)

    # ── DAG structure
    lines.append("")
    lines.append("─" * w)
    lines.append("  DAG STRUCTURE")
    lines.append("─" * w)
    lines.append(f"  {'BF':>4} │ {'Shards':>7} │ {'DAG Levels':>10} │ {'Total Nodes':>11} │ "
                 f"{'DAG Build ms':>12} │ {'Upload ms':>10}")
    lines.append("  " + "─" * 70)
    for cfg in configs:
        u = cfg["upload"]
        full_scan = next((q for q in cfg["queries"] if q["query_id"] == "Q11"), None)
        total_nodes = full_scan["nodes_visited"] if full_scan else "—"
        lines.append(f"  {u['branching_factor']:>4} │ {u['total_shards']:>7} │ "
                     f"{u['dag_levels']:>10} │ {str(total_nodes):>11} │ "
                     f"{u['dag_build_ms']:>12.1f} │ {u['total_ms']:>10.1f}")

    # ── Pruning %
    lines.append("")
    lines.append("─" * w)
    lines.append("  PRUNING % BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for cfg in configs:
        hdr += f" │ BF={cfg['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['prune_pct']:>5.1f}%"
            else:
                row += f" │     —"
        lines.append(row)

    # ── Traversal time
    lines.append("")
    lines.append("─" * w)
    lines.append("  DAG TRAVERSAL TIME — COLD CACHE (ms) BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for cfg in configs:
        hdr += f" │  BF={cfg['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['traversal_ms']:>6.0f}"
            else:
                row += f" │     —"
        lines.append(row)

    # ── Network fetch time
    lines.append("")
    lines.append("─" * w)
    lines.append("  NETWORK FETCH TIME — COLD CACHE (ms) BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for cfg in configs:
        hdr += f" │  BF={cfg['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['fetch_ms']:>6.0f}"
            else:
                row += f" │     —"
        lines.append(row)

    # ── Total latency
    lines.append("")
    lines.append("─" * w)
    lines.append("  TOTAL QUERY LATENCY — COLD CACHE (ms) BY BRANCHING FACTOR")
    lines.append("─" * w)
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<40}"
    for cfg in configs:
        hdr += f" │  BF={cfg['upload']['branching_factor']:>2}"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))
    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:40]:<40}"
        for cfg in configs:
            qr = next((r for r in cfg["queries"] if r["query_id"] == q["id"]), None)
            if qr:
                row += f" │ {qr['total_ms']:>6.0f}"
            else:
                row += f" │     —"
        lines.append(row)

    # ── Summary (Q1-Q8)
    lines.append("")
    lines.append("─" * w)
    lines.append("  SUMMARY (Q1–Q8, cold cache, true network latency)")
    lines.append("─" * w)
    for cfg in configs:
        u = cfg["upload"]
        bf = u["branching_factor"]
        sel = [q for q in cfg["queries"] if q["query_id"] in selective_ids]
        if sel:
            avg_prune = sum(q["prune_pct"] for q in sel) / len(sel)
            avg_trav  = sum(q["traversal_ms"] for q in sel) / len(sel)
            avg_fetch = sum(q["fetch_ms"] for q in sel) / len(sel)
            avg_total = sum(q["total_ms"] for q in sel) / len(sel)
            full_scan = next((q for q in cfg["queries"] if q["query_id"] == "Q11"), None)
            total_nodes = full_scan["nodes_visited"] if full_scan else "—"
            lines.append(
                f"  BF={bf:>2}: avg_prune={avg_prune:.1f}%, "
                f"avg_traversal={avg_trav:.0f}ms, avg_fetch={avg_fetch:.0f}ms, "
                f"avg_total={avg_total:.0f}ms, dag_levels={u['dag_levels']}, "
                f"total_dag_nodes={total_nodes}"
            )

    # ── Full-scan baseline
    lines.append("")
    lines.append("─" * w)
    lines.append("  LATENCY SAVINGS vs FULL SCAN (Q11 baseline)")
    lines.append("─" * w)
    for cfg in configs:
        u = cfg["upload"]
        bf = u["branching_factor"]
        full_scan = next((q for q in cfg["queries"] if q["query_id"] == "Q11"), None)
        if not full_scan:
            continue
        baseline_ms = full_scan["total_ms"]
        baseline_fetch = full_scan["fetch_ms"]
        sel = [q for q in cfg["queries"] if q["query_id"] in selective_ids]
        if sel:
            avg_sel_ms = sum(q["total_ms"] for q in sel) / len(sel)
            avg_sel_fetch = sum(q["fetch_ms"] for q in sel) / len(sel)
            speedup = baseline_ms / avg_sel_ms if avg_sel_ms > 0 else 0
            fetch_saved = baseline_fetch - avg_sel_fetch
            lines.append(
                f"  BF={bf:>2}: full_scan={baseline_ms:.0f}ms "
                f"(fetch={baseline_fetch:.0f}ms), "
                f"avg_selective={avg_sel_ms:.0f}ms "
                f"(fetch={avg_sel_fetch:.0f}ms), "
                f"fetch_saved={fetch_saved:.0f}ms, "
                f"speedup={speedup:.1f}×"
            )

    lines.append("")
    lines.append("=" * w)

    report = "\n".join(lines)
    with open(output_path, "w") as f:
        f.write(report)
    print(f"\n  Report saved: {output_path}")
    print(report)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run cold-cache queries on Server B using upload manifest from Server A")
    parser.add_argument("--manifest", required=True,
                        help="Path to upload_manifest JSON from benchmark_upload.py")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"Local server URL (default: {DEFAULT_HOST})")
    parser.add_argument("--output-dir", default="../results",
                        help="Directory for results (default: ../results)")
    parser.add_argument("--sweep", choices=["shard", "bf", "both"], default=None,
                        help="Which sweep to query (default: auto-detect from manifest)")
    args = parser.parse_args()

    host = args.host
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Load manifest
    if not os.path.exists(args.manifest):
        print(f"  ✗ Manifest not found: {args.manifest}")
        sys.exit(1)

    with open(args.manifest) as f:
        manifest = json.load(f)

    sweep_type = args.sweep or manifest.get("sweep_type", "both")

    has_shard = len(manifest.get("shard_sweep", [])) > 0
    has_bf    = len(manifest.get("bf_sweep", [])) > 0

    if sweep_type in ("shard", "both") and not has_shard:
        print("  ⚠ No shard_sweep configs in manifest — skipping shard sweep")
    if sweep_type in ("bf", "both") and not has_bf:
        print("  ⚠ No bf_sweep configs in manifest — skipping BF sweep")

    # ── Connectivity check
    print(f"\n  Checking local server {host} ...")
    try:
        r = requests.get(f"{host}/health", timeout=10)
        if r.status_code == 200:
            print(f"  ✓ Server B healthy: {host}")
        else:
            print(f"  ⚠ HTTP {r.status_code}")
    except Exception as e:
        print(f"  ✗ Cannot reach {host}: {e}")
        sys.exit(1)

    # ── Banner
    print()
    print("╔" + "═" * 70 + "╗")
    print("║" + "  Benchmark Query — Server B (Cold Cache)".center(70) + "║")
    print("║" + f"  Local host: {host}".ljust(70) + "║")
    print("║" + f"  Manifest: {args.manifest}".ljust(70) + "║")
    print("║" + f"  Upload host (A): {manifest['upload_host']}".ljust(70) + "║")
    print("║" + f"  Dataset: {manifest['dataset']} ({manifest['dataset_rows']:,} rows)".ljust(70) + "║")
    print("║" + f"  Sweep: {sweep_type}".ljust(70) + "║")
    if has_shard and sweep_type in ("shard", "both"):
        sizes = [c["shard_size_kb"] for c in manifest["shard_sweep"]]
        print("║" + f"  Shard configs: {sizes} KB".ljust(70) + "║")
    if has_bf and sweep_type in ("bf", "both"):
        bfs = [c["branching_factor"] for c in manifest["bf_sweep"]]
        print("║" + f"  BF configs: {bfs}".ljust(70) + "║")
    print("║" + f"  Mode: COLD CACHE (single run, local IPFS GC)".ljust(70) + "║")
    print("║" + f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".ljust(70) + "║")
    print("╚" + "═" * 70 + "╝")

    # ──────────────────────────────────────────────────────────────────────
    #  SHARD SIZE SWEEP
    # ──────────────────────────────────────────────────────────────────────
    shard_results = []
    if has_shard and sweep_type in ("shard", "both"):
        print(f"\n{'━' * 70}")
        print(f"  SHARD SIZE SWEEP QUERIES")
        print(f"{'━' * 70}")
        for cfg in manifest["shard_sweep"]:
            sk = cfg["shard_size_kb"]
            print(f"\n  ── Shard = {sk} KB "
                  f"({cfg['total_shards']} shards, "
                  f"DAG levels={cfg['dag_levels']}) ──")

            flush_local_caches(host)

            print(f"    Running queries (cold cache, single run) ...")
            queries = run_queries_cold(host, cfg, QUERIES)

            shard_results.append({
                "upload": cfg,
                "queries": queries,
            })

    # ──────────────────────────────────────────────────────────────────────
    #  BRANCHING FACTOR SWEEP
    # ──────────────────────────────────────────────────────────────────────
    bf_results = []
    if has_bf and sweep_type in ("bf", "both"):
        print(f"\n{'━' * 70}")
        print(f"  BRANCHING FACTOR SWEEP QUERIES")
        print(f"{'━' * 70}")
        for cfg in manifest["bf_sweep"]:
            bf = cfg["branching_factor"]
            print(f"\n  ── BF = {bf} "
                  f"({cfg['total_shards']} shards, "
                  f"DAG levels={cfg['dag_levels']}) ──")

            flush_local_caches(host)

            print(f"    Running queries (cold cache, single run) ...")
            queries = run_queries_cold(host, cfg, QUERIES)

            bf_results.append({
                "upload": cfg,
                "queries": queries,
            })

    # ──────────────────────────────────────────────────────────────────────
    #  SAVE RESULTS
    # ──────────────────────────────────────────────────────────────────────
    if not shard_results and not bf_results:
        print("\n  ✗ No successful query results. Exiting.")
        sys.exit(1)

    # ── JSON
    json_path = os.path.join(output_dir, f"distributed_query_results_{timestamp}.json")
    json_data = {
        "timestamp":    datetime.now().isoformat(),
        "upload_host":  manifest["upload_host"],
        "query_host":   host,
        "dataset":      manifest["dataset"],
        "dataset_rows": manifest["dataset_rows"],
        "mode":         "cold_cache",
        "shard_sweep":  shard_results,
        "bf_sweep":     bf_results,
    }
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"\n  JSON saved: {json_path}")

    # ── Text reports
    if shard_results:
        txt_path = os.path.join(output_dir, f"distributed_shard_sweep_{timestamp}.txt")
        write_shard_sweep_report(shard_results, manifest, txt_path)

    if bf_results:
        txt_path = os.path.join(output_dir, f"distributed_bf_sweep_{timestamp}.txt")
        write_bf_sweep_report(bf_results, manifest, txt_path)

    # ── Final summary
    print(f"\n{'═' * 70}")
    print(f"  ✓ Distributed query benchmark complete.")
    print(f"    JSON: {json_path}")
    if shard_results:
        print(f"    Shard sweep: {len(shard_results)} configs × {len(QUERIES)} queries")
    if bf_results:
        print(f"    BF sweep:    {len(bf_results)} configs × {len(QUERIES)} queries")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
