"""
Semantic Chunking Controller
=============================
  - POST /upload-semantic/{table_name}  — Semantic chunked upload with range-bounded shards
  - POST /query-semantic                — Query with predicate pushdown over Merkle DAG
  - POST /dag-inspect                   — Inspect/visualize the full DAG structure
  - POST /dag-summary                   — Human-readable text summary of the DAG
"""

import io
import gc
import re
import time
import logging
from typing import List

import pandas as pd
import duckdb
import requests
from fastapi import APIRouter, UploadFile, File, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from crypto_utils import create_encrypted_package, extract_and_decrypt_package
from semantic_chunker import SemanticChunker
from semantic_dag import DAGBuilder, DAGTraverser, parse_predicates

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Semantic Chunking"])


# ── Request models ────────────────────────────────────────────────────────────

class SemanticQueryRequest(BaseModel):
    root_cid: str                        # Root CID of the Merkle DAG
    query: str                           # SQL query with WHERE predicates
    partition_attributes: List[str] = ["age", "salary_usd"]  # Attributes used for partitioning


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/upload-semantic/{table_name}")
async def upload_semantic(
    table_name: str,
    request: Request,
    file: UploadFile = File(...),
    partition_attributes: str = Query("age,salary_usd", description="Comma-separated top-K numeric attributes for partitioning"),
    target_shard_size_kb: int = Query(256, description="Target shard size in KB"),
    branching_factor: int = Query(4, description="Branching factor for the DAG tree"),
):
    """
    Semantic Upload: Partition CSV data into range-bounded shards,
    upload each shard to IPFS, and build an IPLD Merkle DAG with
    range metadata for predicate pushdown.

    The returned root_cid is the entry point for all future queries.

    Steps:
      1. Read CSV → DataFrame
      2. SemanticChunker partitions by top-K attributes (KD-tree bisection)
      3. Each shard → Parquet → AES-256-CBC encrypt → IPFS add → data CID
      4. DAGBuilder creates IPLD leaf nodes (metadata + data CID link)
      5. Bottom-up aggregation into internal nodes → root CID
      6. Return root_cid + partition statistics
    """
    logger.info(f"POST /upload-semantic/{table_name} - Semantic chunked upload")
    upload_start = time.time()

    try:
        content = await file.read()
        attrs = [a.strip() for a in partition_attributes.split(",")]

        # Read CSV
        df = pd.read_csv(io.BytesIO(content))
        total_rows = len(df)
        logger.info(f"Read {total_rows} rows, columns: {list(df.columns)}")

        # Validate partition attributes exist and are numeric
        for attr in attrs:
            if attr not in df.columns:
                return {"error": f"Partition attribute '{attr}' not found. Available columns: {list(df.columns)}"}
            if not pd.api.types.is_numeric_dtype(df[attr]):
                return {"error": f"Partition attribute '{attr}' is not numeric (dtype={df[attr].dtype})"}

        # Step 1: Semantic partitioning
        chunker_start = time.time()
        chunker = SemanticChunker(
            partition_attributes=attrs,
            target_shard_size=target_shard_size_kb * 1024,
        )
        shards, partition_stats = chunker.partition(df)
        chunker_time = time.time() - chunker_start
        del df
        gc.collect()

        # Step 2: Build IPLD Merkle DAG
        dag_start = time.time()
        encryption_key = request.app.state.encryption_key
        dag_builder = DAGBuilder(
            encryption_fn=lambda data: create_encrypted_package(data, encryption_key),
            branching_factor=branching_factor,
        )
        root_cid, dag_stats = dag_builder.build_dag(shards, attrs)
        dag_time = time.time() - dag_start

        # Clean up shard data
        del shards
        gc.collect()

        total_time = time.time() - upload_start

        return {
            "status": "success",
            "table_name": table_name,
            "root_cid": root_cid,
            "partition": {
                "attributes": attrs,
                "total_rows": partition_stats.total_rows,
                "total_shards": partition_stats.total_shards,
                "global_ranges": partition_stats.global_ranges,
                "avg_shard_size_kb": round(partition_stats.avg_shard_size / 1024, 2),
                "min_shard_size_kb": round(partition_stats.min_shard_size / 1024, 2),
                "max_shard_size_kb": round(partition_stats.max_shard_size / 1024, 2),
            },
            "dag": {
                "root_cid": root_cid,
                "levels": dag_stats["dag_levels"],
                "branching_factor": dag_stats["branching_factor"],
                "total_data_bytes": dag_stats["total_data_bytes"],
            },
            "timing": {
                "partition_ms": round(chunker_time * 1000, 2),
                "dag_build_ms": round(dag_time * 1000, 2),
                "total_ms": round(total_time * 1000, 2),
            },
            "message": (
                f"Semantic upload complete. {partition_stats.total_shards} shards "
                f"across {dag_stats['dag_levels']} DAG levels. "
                f"Use root_cid '{root_cid}' for queries."
            ),
        }

    except Exception as e:
        logger.error(f"Semantic upload error: {e}", exc_info=True)
        gc.collect()
        return {"status": "error", "error": str(e)}


@router.post("/query-semantic")
async def query_semantic(request: Request, body: SemanticQueryRequest):
    """
    Semantic Query: Traverse the IPLD Merkle DAG with predicate pushdown,
    fetch only matching shards, and execute the SQL query.

    Steps:
      1. Parse WHERE clause → extract predicates on partition attributes
      2. DAGTraverser walks the DAG top-down from root_cid
      3. At each node, check range metadata vs predicates → prune if no overlap
      4. Fetch only matching shard CIDs from IPFS (parallel)
      5. Decrypt → DuckDB executes full SQL on assembled data
      6. Return results + pushdown statistics
    """
    logger.info(f"POST /query-semantic - root_cid={body.root_cid}, query={body.query}")
    query_start = time.time()

    try:
        # Step 1: Parse predicates for pushdown
        predicates = parse_predicates(body.query, body.partition_attributes)
        logger.info(f"Parsed {len(predicates)} pushdown predicates: "
                    f"{[(p.attribute, p.operator, p.value) for p in predicates]}")

        # Step 2: Traverse DAG with predicate pushdown
        encryption_key = request.app.state.encryption_key
        traverser = DAGTraverser(
            decryption_fn=lambda data: extract_and_decrypt_package(data, encryption_key),
        )
        traversal_result = traverser.traverse(body.root_cid, predicates)

        if not traversal_result.matching_data_cids:
            total_time = (time.time() - query_start) * 1000
            return {
                "status": "success",
                "records": 0,
                "results": [],
                "pushdown": {
                    "predicates": [(p.attribute, p.operator, p.value) for p in predicates],
                    "shards_matched": 0,
                    "nodes_pruned": traversal_result.nodes_pruned,
                    "nodes_visited": traversal_result.total_nodes_visited,
                    "traversal_ms": traversal_result.traversal_time_ms,
                },
                "timing": {"total_ms": round(total_time, 2)},
                "message": "No shards matched the predicates — all pruned.",
            }

        # Step 3: Fetch and assemble matching shards
        fetch_start = time.time()
        assembled_df = traverser.fetch_and_assemble(traversal_result.matching_data_cids)
        fetch_time = (time.time() - fetch_start) * 1000

        if assembled_df.empty:
            return {"status": "error", "error": "Failed to fetch/decrypt matching shards"}

        # Step 4: Execute the full SQL query on the assembled data using DuckDB
        duckdb_start = time.time()
        try:
            # Register the assembled DataFrame as a temporary table
            temp_conn = duckdb.connect(":memory:")
            temp_conn.register("__semantic_data__", assembled_df)

            # Replace the table name in the query with our temp table
            # Extract table name from query (FROM clause)
            table_match = re.search(r'\bFROM\s+(\w+)', body.query, re.IGNORECASE)
            if table_match:
                original_table = table_match.group(1)
                exec_query = body.query.replace(original_table, "__semantic_data__")
            else:
                exec_query = body.query

            result = temp_conn.execute(exec_query)
            results_df = result.fetchdf()
            results = results_df.to_dict("records")
            temp_conn.close()
        except Exception as e:
            logger.error(f"DuckDB query error: {e}")
            return {"status": "error", "error": f"Query execution failed: {str(e)}"}
        duckdb_time = (time.time() - duckdb_start) * 1000

        total_time = (time.time() - query_start) * 1000

        return {
            "status": "success",
            "records": len(results),
            "results": results,
            "pushdown": {
                "predicates": [(p.attribute, p.operator, p.value) for p in predicates],
                "shards_matched": traversal_result.leaf_nodes_matched,
                "nodes_pruned": traversal_result.nodes_pruned,
                "nodes_visited": traversal_result.total_nodes_visited,
                "traversal_ms": traversal_result.traversal_time_ms,
            },
            "data_assembly": {
                "rows_from_shards": len(assembled_df),
                "rows_after_query": len(results),
                "fetch_time_ms": round(fetch_time, 2),
            },
            "timing": {
                "traversal_ms": traversal_result.traversal_time_ms,
                "fetch_decrypt_ms": round(fetch_time, 2),
                "duckdb_ms": round(duckdb_time, 2),
                "total_ms": round(total_time, 2),
            },
            "message": (
                f"Predicate pushdown: {traversal_result.leaf_nodes_matched} of "
                f"{traversal_result.total_nodes_visited} nodes matched, "
                f"{traversal_result.nodes_pruned} pruned. "
                f"Fetched {len(assembled_df)} rows → query returned {len(results)} rows."
            ),
        }

    except Exception as e:
        logger.error(f"Semantic query error: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


@router.post("/dag-inspect")
async def dag_inspect(root_cid: str = Query(..., description="Root CID of the Merkle DAG")):
    """
    Inspect/visualize the full DAG structure from a root CID.
    Returns the complete tree with ranges at each level — useful for debugging and paper figures.
    """
    try:
        def _walk(cid: str, depth: int = 0) -> dict:
            resp = requests.post(
                "http://localhost:5001/api/v0/dag/get",
                params={"arg": cid}, timeout=300,
            )
            resp.raise_for_status()
            node = resp.json()
            info = {
                "cid": cid,
                "node_type": node.get("node_type", "unknown"),
                "ranges": node.get("ranges", {}),
                "row_count": node.get("row_count", 0),
                "depth": depth,
            }
            if node.get("node_type") == "leaf":
                data_link = node.get("data", {})
                info["data_cid"] = data_link.get("/") if isinstance(data_link, dict) else data_link
                info["size_bytes"] = node.get("size_bytes", 0)
                info["shard_id"] = node.get("shard_id", -1)
            else:
                info["children"] = []
                for child_link in node.get("children", []):
                    child_cid = child_link.get("/") if isinstance(child_link, dict) else child_link
                    info["children"].append(_walk(child_cid, depth + 1))
            return info

        tree = _walk(root_cid)
        return {"status": "success", "dag": tree}
    except Exception as e:
        logger.error(f"DAG inspect error: {e}")
        return {"status": "error", "error": str(e)}


@router.post("/dag-summary")
async def dag_summary(root_cid: str = Query(..., description="Root CID of the Merkle DAG")):
    """
    Pretty-print a human-readable summary of the entire Merkle DAG.
    Returns a text tree diagram showing the hierarchy, ranges, and shard details
    at every level — designed for quick visual understanding.
    """
    try:
        lines = []

        def _fmt_ranges(ranges: dict) -> str:
            parts = []
            for attr, r in sorted(ranges.items()):
                lo, hi = r if isinstance(r, (list, tuple)) else (r, r)
                # Use int formatting if values are whole numbers
                if float(lo) == int(lo) and float(hi) == int(hi):
                    parts.append(f"{attr}:[{int(lo)}, {int(hi)}]")
                else:
                    parts.append(f"{attr}:[{lo:.1f}, {hi:.1f}]")
            return "  ".join(parts)

        def _short_cid(cid: str) -> str:
            return cid[:12] + "…" + cid[-6:] if len(cid) > 20 else cid

        stats = {"internal": 0, "leaf": 0, "total_rows": 0, "total_bytes": 0, "max_depth": 0}

        def _walk(cid: str, depth: int = 0, prefix: str = "", is_last: bool = True):
            resp = requests.post(
                "http://localhost:5001/api/v0/dag/get",
                params={"arg": cid}, timeout=300,
            )
            resp.raise_for_status()
            node = resp.json()
            node_type = node.get("node_type", "unknown")
            ranges = node.get("ranges", {})
            row_count = node.get("row_count", 0)
            stats["max_depth"] = max(stats["max_depth"], depth)

            connector = "└── " if is_last else "├── "
            child_prefix = prefix + ("    " if is_last else "│   ")

            if node_type == "leaf":
                stats["leaf"] += 1
                stats["total_rows"] += row_count
                size_bytes = node.get("size_bytes", 0)
                stats["total_bytes"] += size_bytes
                shard_id = node.get("shard_id", "?")
                data_link = node.get("data", {})
                data_cid = data_link.get("/") if isinstance(data_link, dict) else data_link
                lines.append(
                    f"{prefix}{connector}🟢 Shard #{shard_id}  "
                    f"{row_count:,} rows  {size_bytes/1024:.1f} KB  "
                    f"{_fmt_ranges(ranges)}"
                )
                lines.append(
                    f"{child_prefix}   data → {_short_cid(data_cid or '')}"
                )
            else:
                stats["internal"] += 1
                children = node.get("children", [])
                child_count = node.get("child_count", len(children))
                level_label = f"Level {depth}" if depth > 0 else "ROOT"
                lines.append(
                    f"{prefix}{connector if depth > 0 else ''}🔷 [{level_label}]  "
                    f"{child_count} children  {row_count:,} rows  "
                    f"{_fmt_ranges(ranges)}"
                )
                if depth == 0:
                    lines.append(f"{prefix}   cid: {_short_cid(cid)}")

                child_cids = [
                    link.get("/") if isinstance(link, dict) else link
                    for link in children
                ]
                for i, child_cid in enumerate(child_cids):
                    _walk(child_cid, depth + 1, child_prefix if depth > 0 else prefix, i == len(child_cids) - 1)

        # Header
        lines.append("=" * 80)
        lines.append(f"  IPLD Merkle DAG Summary")
        lines.append(f"  Root CID: {root_cid}")
        lines.append("=" * 80)
        lines.append("")

        _walk(root_cid, depth=0)

        # Footer stats
        lines.append("")
        lines.append("─" * 80)
        lines.append(f"  Summary")
        lines.append(f"    Internal nodes : {stats['internal']}")
        lines.append(f"    Leaf shards    : {stats['leaf']}")
        lines.append(f"    Total nodes    : {stats['internal'] + stats['leaf']}")
        lines.append(f"    Tree depth     : {stats['max_depth'] + 1} levels (0 = root)")
        lines.append(f"    Total rows     : {stats['total_rows']:,}")
        lines.append(f"    Total data     : {stats['total_bytes']/1024:.1f} KB ({stats['total_bytes']/1024/1024:.2f} MB)")
        if stats['leaf'] > 0:
            lines.append(f"    Avg shard      : {stats['total_bytes']/stats['leaf']/1024:.1f} KB")
        lines.append("─" * 80)

        return PlainTextResponse("\n".join(lines))

    except Exception as e:
        logger.error(f"DAG summary error: {e}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)
