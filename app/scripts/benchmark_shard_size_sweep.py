#!/usr/bin/env python3
"""
Benchmark: Shard Size Sensitivity Sweep
========================================

Uploads the same dataset at multiple target shard sizes
{32, 64, 128, 256, 512} KB and measures:
  - Partition stats (shard count, avg/min/max shard size)
  - Pruning effectiveness per query (% shards pruned)
  - Query latency breakdown (traversal, fetch, DuckDB, total)
  - Network I/O savings

Produces:
  1. A text report file with all raw results
  2. Publication-ready sensitivity plots (PDF + PNG):
     - Pruning % vs. shard size
     - Query latency vs. shard size
     - I/O savings vs. shard size
     - Shard count & DAG depth vs. shard size

Usage:
    python3 benchmark_shard_size_sweep.py [--host HOST] [--dataset FILE] [--runs N]

For NSDI 2027 — shows the sweet spot where pruning plateaus against per-shard overhead.
"""

import os
import sys
import time
import json
import argparse
import requests
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────────────────

BASE = "http://localhost:8002"

SHARD_SIZES_KB = [32, 64, 128, 256, 512]

PARTITION_ATTRS = ["age", "salary_usd", "experience_years"]
BRANCHING_FACTOR = 4

DATASET_FILE = "../dataset/employee_100k.csv"
DATASET_ROWS = 1_00_000

# Queries covering a range of selectivities across all 3 partition attributes
# Designed for effective benchmarking: point → narrow → medium → wide → full scan
QUERIES = [
    # ── Point / Very Narrow ────────────────────────────────────────────────
    {
        "id": "Q1",
        "desc": "Point (age=30, exp=5)",
        "sql": "SELECT * FROM employee WHERE age = 30 AND experience_years = 5",
        "selectivity": "point",
    },
    {
        "id": "Q2",
        "desc": "3D corner (age>60, sal>150K, exp>20)",
        "sql": "SELECT * FROM employee WHERE age > 60 AND salary_usd > 150000 AND experience_years > 20",
        "selectivity": "point",
    },
    # ── Narrow (single + dual attribute) ──────────────────────────────────
    {
        "id": "Q3",
        "desc": "Salary > 150K",
        "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 150000",
        "selectivity": "narrow",
    },
    {
        "id": "Q4",
        "desc": "Young + junior (age<25, exp<3)",
        "sql": "SELECT * FROM employee WHERE age < 25 AND experience_years < 3",
        "selectivity": "narrow",
    },
    {
        "id": "Q5",
        "desc": "Senior + high pay (age>55, sal>120K)",
        "sql": "SELECT * FROM employee WHERE age > 55 AND salary_usd > 120000",
        "selectivity": "narrow",
    },
    # ── Medium (range on 2-3 attributes) ──────────────────────────────────
    {
        "id": "Q6",
        "desc": "3D box (age 25-35, sal 50K-90K, exp 3-8)",
        "sql": "SELECT * FROM employee WHERE age BETWEEN 25 AND 35 AND salary_usd BETWEEN 50000 AND 90000 AND experience_years BETWEEN 3 AND 8",
        "selectivity": "medium",
    },
    {
        "id": "Q7",
        "desc": "Mid-career (exp 8-15, age 30-50)",
        "sql": "SELECT department, AVG(salary_usd) AS avg_sal FROM employee WHERE experience_years BETWEEN 8 AND 15 AND age BETWEEN 30 AND 50 GROUP BY department ORDER BY avg_sal DESC",
        "selectivity": "medium",
    },
    {
        "id": "Q8",
        "desc": "Experienced (exp >= 10)",
        "sql": "SELECT COUNT(*) AS total FROM employee WHERE experience_years >= 10",
        "selectivity": "medium",
    },
    # ── Wide (covers most data) ───────────────────────────────────────────
    {
        "id": "Q9",
        "desc": "Salary > 50K OR exp > 5",
        "sql": "SELECT COUNT(*) AS total FROM employee WHERE salary_usd > 50000",
        "selectivity": "wide",
    },
    {
        "id": "Q10",
        "desc": "Age >= 25 (wide single-attr)",
        "sql": "SELECT COUNT(*) AS total FROM employee WHERE age >= 25",
        "selectivity": "wide",
    },
    # ── Full scan (baseline) ──────────────────────────────────────────────
    {
        "id": "Q11",
        "desc": "Full scan (no pushdown)",
        "sql": "SELECT COUNT(*) AS total FROM employee",
        "selectivity": "full",
    },
]


# ─── Data structures ────────────────────────────────────────────────────────

@dataclass
class UploadResult:
    shard_size_kb: int
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


@dataclass
class SweepPoint:
    shard_size_kb: int
    upload: UploadResult
    queries: List[QueryResult]


# ─── API helpers ─────────────────────────────────────────────────────────────

def upload_dataset(filepath: str, shard_size_kb: int) -> Optional[UploadResult]:
    """Upload the dataset with a specific target shard size."""
    if not os.path.exists(filepath):
        print(f"    ✗ File not found: {filepath}")
        return None

    print(f"    Uploading with target_shard_size_kb={shard_size_kb} ...")
    start = time.time()
    try:
        with open(filepath, "rb") as f:
            resp = requests.post(
                f"{BASE}/upload-semantic/employee",
                files={"file": (os.path.basename(filepath), f, "text/csv")},
                params={
                    "partition_attributes": ",".join(PARTITION_ATTRS),
                    "target_shard_size_kb": shard_size_kb,
                    "branching_factor": BRANCHING_FACTOR,
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
        shard_size_kb=shard_size_kb,
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
          f"avg {result.avg_shard_kb:.1f} KB, DAG levels={result.dag_levels}")
    return result


def run_query(upload: UploadResult, q: dict) -> Optional[QueryResult]:
    """Run a single query and collect metrics."""
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
    assembly = r.get("data_assembly", {})

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
        query_id=q["id"],
        desc=q["desc"],
        selectivity=q["selectivity"],
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
    )


def run_queries_with_warmup(upload: UploadResult, queries: list, runs: int) -> List[QueryResult]:
    """Run all queries with optional averaging over multiple runs."""
    # Warm-up run (discard results — primes IPFS cache, DuckDB JIT)
    print("    Warm-up run ...")
    for q in queries:
        run_query(upload, q)

    # Actual measured runs
    accum: Dict[str, List[QueryResult]] = {q["id"]: [] for q in queries}

    for run_i in range(runs):
        if runs > 1:
            print(f"    Run {run_i+1}/{runs} ...")
        for q in queries:
            result = run_query(upload, q)
            if result:
                accum[q["id"]].append(result)

    # Average the numeric fields across runs
    averaged: List[QueryResult] = []
    for q in queries:
        results = accum[q["id"]]
        if not results:
            continue
        n = len(results)
        avg = QueryResult(
            query_id=q["id"],
            desc=q["desc"],
            selectivity=q["selectivity"],
            result_rows=results[0].result_rows,
            total_shards=results[0].total_shards,
            shards_matched=results[0].shards_matched,
            shards_pruned=results[0].shards_pruned,
            prune_pct=results[0].prune_pct,
            nodes_visited=results[0].nodes_visited,
            nodes_pruned=results[0].nodes_pruned,
            node_prune_pct=results[0].node_prune_pct,
            total_data_kb=results[0].total_data_kb,
            fetched_data_kb=results[0].fetched_data_kb,
            io_saved_pct=results[0].io_saved_pct,
            # Average the timing fields
            traversal_ms=sum(r.traversal_ms for r in results) / n,
            fetch_ms=sum(r.fetch_ms for r in results) / n,
            duckdb_ms=sum(r.duckdb_ms for r in results) / n,
            total_ms=sum(r.total_ms for r in results) / n,
        )
        averaged.append(avg)

    return averaged


# ─── Text report ─────────────────────────────────────────────────────────────

def write_text_report(sweep_results: List[SweepPoint], output_path: str, runs: int):
    """Write a comprehensive text report of all sweep results."""
    lines = []
    w = 110

    lines.append("=" * w)
    lines.append("  SHARD SIZE SENSITIVITY SWEEP — SeCAPa Benchmark")
    lines.append(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Dataset: {DATASET_FILE} ({DATASET_ROWS:,} rows)")
    lines.append(f"  Partition attrs: {PARTITION_ATTRS}")
    lines.append(f"  Branching factor: {BRANCHING_FACTOR}")
    lines.append(f"  Shard sizes tested: {SHARD_SIZES_KB} KB")
    lines.append(f"  Query runs averaged: {runs}")
    lines.append("=" * w)

    # ── Section 1: Upload / Partition Summary ─────────────────────────────
    lines.append("")
    lines.append("─" * w)
    lines.append("  PARTITION SUMMARY")
    lines.append("─" * w)
    header = f"  {'Target KB':>10} │ {'Shards':>7} │ {'DAG Lvl':>7} │ {'Avg KB':>8} │ {'Min KB':>8} │ {'Max KB':>8} │ {'Partition ms':>12} │ {'DAG ms':>8} │ {'Total ms':>10}"
    lines.append(header)
    lines.append("  " + "─" * (len(header) - 2))
    for sp in sweep_results:
        u = sp.upload
        lines.append(
            f"  {u.shard_size_kb:>10} │ {u.total_shards:>7} │ {u.dag_levels:>7} │ "
            f"{u.avg_shard_kb:>8.1f} │ {u.min_shard_kb:>8.1f} │ {u.max_shard_kb:>8.1f} │ "
            f"{u.partition_ms:>12.1f} │ {u.dag_build_ms:>8.1f} │ {u.total_ms:>10.1f}"
        )

    # ── Section 2: Per-query pruning at each shard size ───────────────────
    lines.append("")
    lines.append("─" * w)
    lines.append("  PRUNING % BY SHARD SIZE (per query)")
    lines.append("─" * w)

    # Header row
    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<38}"
    for sp in sweep_results:
        hdr += f" │ {sp.shard_size_kb:>4}KB"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))

    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:38]:<38}"
        for sp in sweep_results:
            qr = next((r for r in sp.queries if r.query_id == q["id"]), None)
            if qr:
                row += f" │ {qr.prune_pct:>5.1f}%"
            else:
                row += f" │     -"
        lines.append(row)

    # ── Section 3: Per-query total latency at each shard size ─────────────
    lines.append("")
    lines.append("─" * w)
    lines.append("  TOTAL QUERY LATENCY (ms) BY SHARD SIZE")
    lines.append("─" * w)

    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<38}"
    for sp in sweep_results:
        hdr += f" │ {sp.shard_size_kb:>6}KB"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))

    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:38]:<38}"
        for sp in sweep_results:
            qr = next((r for r in sp.queries if r.query_id == q["id"]), None)
            if qr:
                row += f" │ {qr.total_ms:>7.0f}ms"
            else:
                row += f" │       -"
        lines.append(row)

    # ── Section 4: Timing breakdown ───────────────────────────────────────
    lines.append("")
    lines.append("─" * w)
    lines.append("  TIMING BREAKDOWN (ms) — Traversal / Fetch / DuckDB")
    lines.append("─" * w)

    for sp in sweep_results:
        lines.append(f"\n  Shard size = {sp.shard_size_kb} KB ({sp.upload.total_shards} shards, {sp.upload.dag_levels} levels):")
        lines.append(f"  {'Query':>5} │ {'Trav ms':>8} │ {'Fetch ms':>9} │ {'DuckDB ms':>10} │ {'Total ms':>9} │ {'Prune%':>7} │ {'I/O Saved%':>10}")
        lines.append("  " + "─" * 80)
        for qr in sp.queries:
            lines.append(
                f"  {qr.query_id:>5} │ {qr.traversal_ms:>8.1f} │ {qr.fetch_ms:>9.1f} │ "
                f"{qr.duckdb_ms:>10.1f} │ {qr.total_ms:>9.1f} │ {qr.prune_pct:>6.1f}% │ {qr.io_saved_pct:>9.1f}%"
            )

    # ── Section 5: I/O savings ────────────────────────────────────────────
    lines.append("")
    lines.append("─" * w)
    lines.append("  NETWORK I/O SAVED (%) BY SHARD SIZE")
    lines.append("─" * w)

    hdr = f"  {'Query':>5} │ {'Selectivity':>12} │ {'Description':<38}"
    for sp in sweep_results:
        hdr += f" │ {sp.shard_size_kb:>4}KB"
    lines.append(hdr)
    lines.append("  " + "─" * (len(hdr) - 2))

    for q in QUERIES:
        row = f"  {q['id']:>5} │ {q['selectivity']:>12} │ {q['desc'][:38]:<38}"
        for sp in sweep_results:
            qr = next((r for r in sp.queries if r.query_id == q["id"]), None)
            if qr:
                row += f" │ {qr.io_saved_pct:>5.1f}%"
            else:
                row += f" │     -"
        lines.append(row)

    # ── Section 6: Sweet spot analysis ────────────────────────────────────
    lines.append("")
    lines.append("─" * w)
    lines.append("  SWEET SPOT ANALYSIS")
    lines.append("─" * w)

    # Compute average pruning (selective queries only) per shard size
    for sp in sweep_results:
        selective = [qr for qr in sp.queries if qr.prune_pct > 0]
        if selective:
            avg_prune = sum(qr.prune_pct for qr in selective) / len(selective)
            avg_latency = sum(qr.total_ms for qr in selective) / len(selective)
            avg_io = sum(qr.io_saved_pct for qr in selective) / len(selective)
            lines.append(
                f"  {sp.shard_size_kb:>4} KB: avg_prune={avg_prune:.1f}%, "
                f"avg_latency={avg_latency:.0f}ms, avg_io_saved={avg_io:.1f}%, "
                f"shards={sp.upload.total_shards}, levels={sp.upload.dag_levels}"
            )

    lines.append("")
    lines.append("  Key insight: The sweet spot is where pruning improvement plateaus")
    lines.append("  (smaller shards → more pruning but more per-shard overhead).")
    lines.append("  Look for the 'knee' in the pruning vs. latency curves.")
    lines.append("")
    lines.append("=" * w)

    report_text = "\n".join(lines)

    with open(output_path, "w") as f:
        f.write(report_text)

    print(f"\n  Report saved to: {output_path}")
    print(report_text)


# ─── Plotting ────────────────────────────────────────────────────────────────

def plot_sensitivity(sweep_results: List[SweepPoint], output_dir: str):
    """Generate publication-ready sensitivity plots."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    # ── IEEE style ────────────────────────────────────────────────────────
    plt.rcParams.update({
        "font.size": 11,
        "font.family": "serif",
        "axes.labelsize": 13,
        "xtick.labelsize": 11,
        "ytick.labelsize": 11,
        "legend.fontsize": 9,
        "axes.linewidth": 1.2,
        "lines.linewidth": 2.0,
        "lines.markersize": 8,
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.05,
    })

    shard_sizes = [sp.shard_size_kb for sp in sweep_results]

    # Categorize queries
    selective_queries = [q for q in QUERIES if q["selectivity"] in ("point", "narrow")]
    medium_queries = [q for q in QUERIES if q["selectivity"] == "medium"]
    wide_queries = [q for q in QUERIES if q["selectivity"] == "wide"]
    all_selective = [q for q in QUERIES if q["selectivity"] != "full"]

    # Color palette
    colors = plt.cm.Set2(np.linspace(0, 1, len(QUERIES)))
    query_colors = {q["id"]: colors[i] for i, q in enumerate(QUERIES)}

    markers = ["o", "s", "^", "D", "v", "P", "X", "*"]
    query_markers = {q["id"]: markers[i % len(markers)] for i, q in enumerate(QUERIES)}

    # ══════════════════════════════════════════════════════════════════════
    # Figure 1: 2x2 panel — the main publication figure
    # ══════════════════════════════════════════════════════════════════════
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    fig.suptitle("Shard Size Sensitivity Analysis — SeCAPa", fontsize=14, fontweight="bold", y=0.98)

    # ── Panel (a): Pruning % vs Shard Size ────────────────────────────────
    ax = axes[0, 0]
    for q in QUERIES:
        if q["selectivity"] == "full":
            continue
        prune_vals = []
        for sp in sweep_results:
            qr = next((r for r in sp.queries if r.query_id == q["id"]), None)
            prune_vals.append(qr.prune_pct if qr else 0)
        ax.plot(shard_sizes, prune_vals, marker=query_markers[q["id"]],
                color=query_colors[q["id"]], label=q["id"])

    ax.set_xlabel("Target Shard Size (KB)")
    ax.set_ylabel("Shards Pruned (%)")
    ax.set_title("(a) Pruning Effectiveness", fontsize=12)
    ax.set_xticks(shard_sizes)
    ax.set_xticklabels([str(s) for s in shard_sizes])
    ax.set_ylim(-5, 105)
    ax.legend(loc="lower left", ncol=2, framealpha=0.9)
    ax.grid(True, alpha=0.3)

    # ── Panel (b): Query Latency vs Shard Size ───────────────────────────
    ax = axes[0, 1]
    for q in QUERIES:
        latency_vals = []
        for sp in sweep_results:
            qr = next((r for r in sp.queries if r.query_id == q["id"]), None)
            latency_vals.append(qr.total_ms if qr else 0)
        ax.plot(shard_sizes, latency_vals, marker=query_markers[q["id"]],
                color=query_colors[q["id"]], label=q["id"])

    ax.set_xlabel("Target Shard Size (KB)")
    ax.set_ylabel("Total Query Latency (ms)")
    ax.set_title("(b) Query Latency", fontsize=12)
    ax.set_xticks(shard_sizes)
    ax.set_xticklabels([str(s) for s in shard_sizes])
    ax.legend(loc="upper left", ncol=2, framealpha=0.9)
    ax.grid(True, alpha=0.3)

    # ── Panel (c): I/O Savings vs Shard Size ─────────────────────────────
    ax = axes[1, 0]
    for q in QUERIES:
        if q["selectivity"] == "full":
            continue
        io_vals = []
        for sp in sweep_results:
            qr = next((r for r in sp.queries if r.query_id == q["id"]), None)
            io_vals.append(qr.io_saved_pct if qr else 0)
        ax.plot(shard_sizes, io_vals, marker=query_markers[q["id"]],
                color=query_colors[q["id"]], label=q["id"])

    ax.set_xlabel("Target Shard Size (KB)")
    ax.set_ylabel("Network I/O Saved (%)")
    ax.set_title("(c) Network I/O Savings", fontsize=12)
    ax.set_xticks(shard_sizes)
    ax.set_xticklabels([str(s) for s in shard_sizes])
    ax.set_ylim(-5, 105)
    ax.legend(loc="lower left", ncol=2, framealpha=0.9)
    ax.grid(True, alpha=0.3)

    # ── Panel (d): Shard Count & DAG Depth vs Shard Size ─────────────────
    ax = axes[1, 1]
    shard_counts = [sp.upload.total_shards for sp in sweep_results]
    dag_levels = [sp.upload.dag_levels for sp in sweep_results]

    color_shards = "#1f77b4"
    color_levels = "#d62728"

    ax.bar([s - 12 for s in shard_sizes], shard_counts, width=24, alpha=0.7,
           color=color_shards, label="Shard Count", zorder=3)
    ax.set_xlabel("Target Shard Size (KB)")
    ax.set_ylabel("Number of Shards", color=color_shards)
    ax.tick_params(axis="y", labelcolor=color_shards)
    ax.set_xticks(shard_sizes)
    ax.set_xticklabels([str(s) for s in shard_sizes])

    ax2 = ax.twinx()
    ax2.plot(shard_sizes, dag_levels, marker="D", color=color_levels,
             linewidth=2.5, markersize=9, label="DAG Levels", zorder=5)
    ax2.set_ylabel("DAG Depth (Levels)", color=color_levels)
    ax2.tick_params(axis="y", labelcolor=color_levels)

    # Combined legend
    bars_legend = ax.legend(loc="upper left", framealpha=0.9)
    ax2.legend(loc="upper right", framealpha=0.9)
    ax.add_artist(bars_legend)

    ax.set_title("(d) Shard Count & DAG Depth", fontsize=12)
    ax.grid(True, alpha=0.3, zorder=0)

    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Save
    for ext in ["pdf", "png"]:
        path = os.path.join(output_dir, f"shard_size_sensitivity.{ext}")
        fig.savefig(path)
        print(f"  Saved: {path}")
    plt.close(fig)

    # ══════════════════════════════════════════════════════════════════════
    # Figure 2: Sweet-spot highlight — avg pruning vs avg latency
    # ══════════════════════════════════════════════════════════════════════
    fig, ax1 = plt.subplots(figsize=(7, 4.5))

    avg_prune = []
    avg_latency = []
    for sp in sweep_results:
        sel = [qr for qr in sp.queries if qr.prune_pct > 0]
        if sel:
            avg_prune.append(sum(qr.prune_pct for qr in sel) / len(sel))
            avg_latency.append(sum(qr.total_ms for qr in sel) / len(sel))
        else:
            avg_prune.append(0)
            avg_latency.append(0)

    color1 = "#2ca02c"
    color2 = "#ff7f0e"

    ax1.plot(shard_sizes, avg_prune, marker="o", color=color1,
             linewidth=2.5, markersize=10, label="Avg Pruning %", zorder=5)
    ax1.set_xlabel("Target Shard Size (KB)", fontsize=13)
    ax1.set_ylabel("Avg Shard Pruning (%)", color=color1, fontsize=13)
    ax1.tick_params(axis="y", labelcolor=color1)
    ax1.set_xticks(shard_sizes)
    ax1.set_xticklabels([str(s) for s in shard_sizes])
    ax1.set_ylim(0, 100)

    ax2 = ax1.twinx()
    ax2.plot(shard_sizes, avg_latency, marker="s", color=color2,
             linewidth=2.5, markersize=10, label="Avg Latency (ms)", zorder=5)
    ax2.set_ylabel("Avg Query Latency (ms)", color=color2, fontsize=13)
    ax2.tick_params(axis="y", labelcolor=color2)

    # Find and annotate sweet spot (best pruning/latency ratio)
    ratios = [p / max(l, 1) for p, l in zip(avg_prune, avg_latency)]
    best_idx = ratios.index(max(ratios))
    best_size = shard_sizes[best_idx]

    ax1.axvline(x=best_size, color="gray", linestyle="--", alpha=0.6, linewidth=1.5)
    ax1.annotate(
        f"Sweet spot\n{best_size} KB",
        xy=(best_size, avg_prune[best_idx]),
        xytext=(best_size + 40, avg_prune[best_idx] - 15),
        fontsize=11, fontweight="bold", color="gray",
        arrowprops=dict(arrowstyle="->", color="gray", lw=1.5),
    )

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="center right", framealpha=0.9, fontsize=11)

    ax1.set_title("Shard Size Sweet Spot: Pruning vs. Latency", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()

    for ext in ["pdf", "png"]:
        path = os.path.join(output_dir, f"shard_size_sweet_spot.{ext}")
        fig.savefig(path)
        print(f"  Saved: {path}")
    plt.close(fig)

    # ══════════════════════════════════════════════════════════════════════
    # Figure 3: Stacked latency breakdown
    # ══════════════════════════════════════════════════════════════════════
    fig, ax = plt.subplots(figsize=(8, 5))

    # Average across selective queries for each shard size
    avg_trav = []
    avg_fetch = []
    avg_duck = []
    for sp in sweep_results:
        sel = [qr for qr in sp.queries if qr.prune_pct > 0]
        if sel:
            avg_trav.append(sum(qr.traversal_ms for qr in sel) / len(sel))
            avg_fetch.append(sum(qr.fetch_ms for qr in sel) / len(sel))
            avg_duck.append(sum(qr.duckdb_ms for qr in sel) / len(sel))
        else:
            avg_trav.append(0)
            avg_fetch.append(0)
            avg_duck.append(0)

    x = np.arange(len(shard_sizes))
    width = 0.5

    ax.bar(x, avg_trav, width, label="DAG Traversal", color="#1f77b4", zorder=3)
    ax.bar(x, avg_fetch, width, bottom=avg_trav, label="IPFS Fetch + Decrypt", color="#ff7f0e", zorder=3)
    ax.bar(x, avg_duck, width,
           bottom=[t + f for t, f in zip(avg_trav, avg_fetch)],
           label="DuckDB Execution", color="#2ca02c", zorder=3)

    ax.set_xlabel("Target Shard Size (KB)", fontsize=13)
    ax.set_ylabel("Avg Query Latency (ms)", fontsize=13)
    ax.set_title("Latency Breakdown by Shard Size (Selective Queries)", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in shard_sizes])
    ax.legend(loc="upper left", framealpha=0.9)
    ax.grid(True, alpha=0.3, axis="y", zorder=0)

    plt.tight_layout()
    for ext in ["pdf", "png"]:
        path = os.path.join(output_dir, f"shard_size_latency_breakdown.{ext}")
        fig.savefig(path)
        print(f"  Saved: {path}")
    plt.close(fig)

    print(f"\n  All plots saved to: {output_dir}/")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global BASE, DATASET_FILE

    parser = argparse.ArgumentParser(description="Shard Size Sensitivity Sweep Benchmark")
    parser.add_argument("--host", default=None, help=f"Server URL (default: {BASE})")
    parser.add_argument("--dataset", default=None, help=f"Path to CSV file (default: {DATASET_FILE})")
    parser.add_argument("--runs", type=int, default=3, help="Number of query runs to average (default: 3)")
    parser.add_argument("--output-dir", default="../plot", help="Directory for plots and report")
    parser.add_argument("--shard-sizes", default=None, help="Comma-separated shard sizes in KB (default: 32,64,128,256,512)")
    args = parser.parse_args()

    if args.host:
        BASE = args.host
    if args.dataset:
        DATASET_FILE = args.dataset
    if args.shard_sizes:
        SHARD_SIZES_KB.clear()
        SHARD_SIZES_KB.extend([int(x.strip()) for x in args.shard_sizes.split(",")])

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("╔" + "═" * 80 + "╗")
    print("║" + "  Shard Size Sensitivity Sweep — SeCAPa Benchmark".center(80) + "║")
    print("║" + f"  Server: {BASE}".ljust(80) + "║")
    print("║" + f"  Dataset: {DATASET_FILE}".ljust(80) + "║")
    print("║" + f"  Shard sizes: {SHARD_SIZES_KB} KB".ljust(80) + "║")
    print("║" + f"  Query runs: {args.runs}".ljust(80) + "║")
    print("║" + f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".ljust(80) + "║")
    print("╚" + "═" * 80 + "╝")

    sweep_results: List[SweepPoint] = []

    for shard_kb in SHARD_SIZES_KB:
        print(f"\n{'━' * 80}")
        print(f"  SHARD SIZE = {shard_kb} KB")
        print(f"{'━' * 80}")

        # Upload
        upload = upload_dataset(DATASET_FILE, shard_kb)
        if not upload:
            print(f"    ⚠ Upload failed for {shard_kb} KB — skipping")
            continue

        # Run queries
        print(f"    Running queries ({args.runs} runs, averaged) ...")
        query_results = run_queries_with_warmup(upload, QUERIES, args.runs)

        for qr in query_results:
            status = "✓" if qr else "✗"
            print(f"      {qr.query_id}: prune={qr.prune_pct:5.1f}%  "
                  f"io_saved={qr.io_saved_pct:5.1f}%  "
                  f"latency={qr.total_ms:7.0f}ms  {status}")

        sweep_results.append(SweepPoint(
            shard_size_kb=shard_kb,
            upload=upload,
            queries=query_results,
        ))

    if not sweep_results:
        print("\n  ✗ No successful sweep points. Exiting.")
        sys.exit(1)

    # ── Write text report ────────────────────────────────────────────────
    report_path = os.path.join(output_dir, f"shard_size_sweep_{timestamp}.txt")
    write_text_report(sweep_results, report_path, args.runs)

    # ── Also save raw JSON for reproducibility ───────────────────────────
    json_path = os.path.join(output_dir, f"shard_size_sweep_{timestamp}.json")
    json_data = []
    for sp in sweep_results:
        json_data.append({
            "shard_size_kb": sp.shard_size_kb,
            "upload": asdict(sp.upload),
            "queries": [asdict(qr) for qr in sp.queries],
        })
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"  JSON data saved to: {json_path}")

    # ── Generate plots ───────────────────────────────────────────────────
    print("\n  Generating publication-ready plots ...")
    plot_sensitivity(sweep_results, output_dir)

    print("\n" + "═" * 80)
    print("  Sweep complete. Files generated:")
    print(f"    Report : {report_path}")
    print(f"    JSON   : {json_path}")
    print(f"    Plots  : {output_dir}/shard_size_sensitivity.{{pdf,png}}")
    print(f"             {output_dir}/shard_size_sweet_spot.{{pdf,png}}")
    print(f"             {output_dir}/shard_size_latency_breakdown.{{pdf,png}}")
    print("═" * 80)


if __name__ == "__main__":
    main()
