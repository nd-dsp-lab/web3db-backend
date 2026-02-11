"""
Semantic DAG: IPLD Merkle DAG construction and traversal for semantic shards.

Builds a hierarchical DAG where:
- Leaf nodes: reference encrypted data shard CIDs + range metadata
- Internal nodes: aggregate ranges of children + IPLD links to child CIDs
- Root node: the single entry point stored on-chain

The DAG IS the index — no separate B+ tree needed. Predicate pushdown
is performed by traversing the DAG and pruning entire subtrees when
their range metadata proves no overlap with the query predicates.

Uses IPFS dag/put (dag-cbor codec) for IPLD-native linked data.
"""

import json
import logging
import requests
import io
import time
import re
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
import concurrent.futures
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

IPFS_API_BASE = "http://localhost:5001/api/v0"


@dataclass
class DAGLeafNode:
    """Leaf node in the Merkle DAG — references an actual data shard."""
    node_type: str = "leaf"
    data_cid: str = ""           # CID of the encrypted Parquet shard
    ranges: Dict[str, List[float]] = field(default_factory=dict)  # {attr: [min, max]}
    row_count: int = 0
    size_bytes: int = 0


@dataclass
class DAGInternalNode:
    """Internal node — aggregates ranges, links to children."""
    node_type: str = "internal"
    ranges: Dict[str, List[float]] = field(default_factory=dict)
    children: List[str] = field(default_factory=list)  # CIDs of child DAG nodes
    row_count: int = 0
    child_count: int = 0


# ============================================================
# DAG Builder: constructs the IPLD Merkle DAG bottom-up
# ============================================================

class DAGBuilder:
    """
    Builds an IPLD Merkle DAG from a list of shards.
    
    Construction is bottom-up:
    1. Upload each shard's encrypted Parquet data → get data CIDs
    2. Create leaf DAG nodes (metadata + data_cid link) → dag/put → get leaf CIDs
    3. Group leaves by branching factor and create internal nodes → dag/put
    4. Repeat until a single root node remains
    
    The resulting structure is a balanced tree where every node
    carries aggregate range metadata for its entire subtree.
    """

    def __init__(self, encryption_fn=None, branching_factor: int = 4):
        """
        Args:
            encryption_fn: Function(bytes) -> bytes that encrypts data. 
                          If None, data is stored unencrypted.
            branching_factor: Max children per internal node (default 4 for a quad-tree feel).
        """
        self.encryption_fn = encryption_fn
        self.branching_factor = branching_factor

    def build_dag(self, shards, partition_attributes: List[str]) -> Tuple[str, dict]:
        """
        Build the complete IPLD Merkle DAG from shards.
        
        Args:
            shards: List of Shard objects from SemanticChunker
            partition_attributes: The attribute names used for partitioning
            
        Returns:
            Tuple of (root_cid, dag_stats)
        """
        total_start = time.time()
        logger.info(f"Building IPLD Merkle DAG for {len(shards)} shards...")

        # Step 1: Upload shard data to IPFS in parallel and create leaf nodes
        # Prepare all shard payloads first (serialize + encrypt), then batch upload
        shard_payloads = []  # (shard, upload_bytes, parquet_size, leaf_metadata)
        total_data_bytes = 0
        for shard in shards:
            buf = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(shard.data), buf, compression='snappy')
            buf.seek(0)
            parquet_bytes = buf.read()
            shard_size = len(parquet_bytes)
            total_data_bytes += shard_size

            upload_bytes = self.encryption_fn(parquet_bytes) if self.encryption_fn else parquet_bytes
            leaf_meta = {
                "ranges": {attr: [shard.ranges[attr][0], shard.ranges[attr][1]]
                          for attr in partition_attributes if attr in shard.ranges},
                "row_count": shard.row_count,
                "size_bytes": shard_size,
                "shard_id": shard.shard_id,
            }
            shard_payloads.append((shard.shard_id, upload_bytes, leaf_meta))

        # Parallel upload all shard data to IPFS
        def _upload_shard(payload):
            shard_id, upload_bytes, meta = payload
            data_cid = self._ipfs_add(upload_bytes, f"shard_{shard_id}.parquet.enc")
            leaf_node = {
                "node_type": "leaf",
                "data": {"/": data_cid},
                **meta,
            }
            leaf_cid = self._dag_put(leaf_node)
            return leaf_cid, leaf_node  # Return metadata to avoid re-fetching

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as upload_executor:
            upload_results = list(upload_executor.map(_upload_shard, shard_payloads))

        leaf_cids = [cid for cid, _ in upload_results]
        # Metadata cache: map CID → node dict (avoids re-fetching during internal node construction)
        metadata_cache = {cid: meta for cid, meta in upload_results}

        logger.info(f"Uploaded {len(leaf_cids)} leaf nodes to IPLD (parallel)")

        # Step 2: Build internal nodes bottom-up using cached metadata
        current_level = leaf_cids
        level_num = 0

        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), self.branching_factor):
                group = current_level[i:i + self.branching_factor]

                # Use cached metadata instead of re-fetching from IPFS
                child_ranges = {}
                total_rows = 0
                for child_cid in group:
                    if child_cid in metadata_cache:
                        child_meta = metadata_cache[child_cid]
                    else:
                        child_meta = self._dag_get(child_cid)  # fallback
                    for attr, r in child_meta.get("ranges", {}).items():
                        rmin, rmax = r[0], r[1]
                        if attr not in child_ranges:
                            child_ranges[attr] = [rmin, rmax]
                        else:
                            child_ranges[attr][0] = min(child_ranges[attr][0], rmin)
                            child_ranges[attr][1] = max(child_ranges[attr][1], rmax)
                    total_rows += child_meta.get("row_count", 0)

                internal_node = {
                    "node_type": "internal",
                    "ranges": child_ranges,
                    "children": [{"/": cid} for cid in group],
                    "row_count": total_rows,
                    "child_count": len(group),
                    "level": level_num,
                }
                internal_cid = self._dag_put(internal_node)
                next_level.append(internal_cid)
                # Cache the internal node too for higher levels
                metadata_cache[internal_cid] = internal_node

            level_num += 1
            logger.info(f"  Level {level_num}: {len(current_level)} nodes → {len(next_level)} internal nodes")
            current_level = next_level

        root_cid = current_level[0]
        build_time = time.time() - total_start

        stats = {
            "root_cid": root_cid,
            "total_shards": len(shards),
            "total_data_bytes": total_data_bytes,
            "dag_levels": level_num + 1,  # +1 for leaf level
            "branching_factor": self.branching_factor,
            "build_time_ms": round(build_time * 1000, 2),
        }
        logger.info(f"DAG built: root_cid={root_cid}, levels={stats['dag_levels']}, time={build_time:.2f}s")
        return root_cid, stats

    def _ipfs_add(self, data: bytes, filename: str) -> str:
        """Upload raw bytes to IPFS via /api/v0/add."""
        resp = requests.post(
            f"{IPFS_API_BASE}/add",
            files={"file": (filename, data)},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["Hash"]

    def _dag_put(self, obj: dict) -> str:
        """Store a DAG-CBOR node in IPFS via /api/v0/dag/put."""
        json_bytes = json.dumps(obj).encode("utf-8")
        resp = requests.post(
            f"{IPFS_API_BASE}/dag/put",
            files={"file": ("node.json", json_bytes)},
            params={"store-codec": "dag-cbor", "input-codec": "dag-json"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["Cid"]["/"]

    def _dag_get(self, cid: str) -> dict:
        """Retrieve a DAG node from IPFS via /api/v0/dag/get."""
        resp = requests.post(
            f"{IPFS_API_BASE}/dag/get",
            params={"arg": cid},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


# ============================================================
# DAG Traverser: predicate pushdown via range pruning
# ============================================================

@dataclass
class Predicate:
    """A single parsed predicate: attribute op value."""
    attribute: str
    operator: str  # '>', '<', '>=', '<=', '=', '!='
    value: float


@dataclass
class TraversalResult:
    """Result of DAG traversal with pushdown."""
    matching_data_cids: List[str]
    total_nodes_visited: int
    nodes_pruned: int
    leaf_nodes_matched: int
    traversal_time_ms: float


class DAGTraverser:
    """
    Traverse the IPLD Merkle DAG with predicate pushdown.
    
    At each node:
    1. Check if the query predicates overlap with the node's range metadata
    2. If no overlap → prune the entire subtree (skip all children)
    3. If overlap → recurse into children
    4. At leaf nodes → collect the data CID if ranges match
    
    This gives us O(log N) traversal for selective queries instead of O(N).
    """

    def __init__(self, decryption_fn=None):
        """
        Args:
            decryption_fn: Function(bytes) -> bytes to decrypt shard data.
                          If None, data is assumed unencrypted.
        """
        self.decryption_fn = decryption_fn

    def traverse(self, root_cid: str, predicates: List[Predicate]) -> TraversalResult:
        """
        Traverse the DAG from root, applying predicate pushdown.
        
        Uses a single shared ThreadPoolExecutor for all parallel sibling
        fetches across the entire traversal (avoids creating ~21 executors).
        
        Args:
            root_cid: The root CID of the Merkle DAG
            predicates: List of parsed predicates from WHERE clause
            
        Returns:
            TraversalResult with matching data CIDs and stats
        """
        start = time.time()
        matching_cids = []
        visited = [0]
        pruned = [0]
        leaf_matched = [0]

        # Single executor shared across the entire traversal
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            self._traverse_node(root_cid, predicates, matching_cids, visited, pruned, leaf_matched, executor)

        elapsed = (time.time() - start) * 1000
        result = TraversalResult(
            matching_data_cids=matching_cids,
            total_nodes_visited=visited[0],
            nodes_pruned=pruned[0],
            leaf_nodes_matched=leaf_matched[0],
            traversal_time_ms=round(elapsed, 2),
        )
        logger.info(
            f"DAG traversal: {result.leaf_nodes_matched} shards matched, "
            f"{result.nodes_pruned} nodes pruned, "
            f"{result.total_nodes_visited} visited, "
            f"{result.traversal_time_ms}ms"
        )
        return result

    def _traverse_node(
        self,
        cid: str,
        predicates: List[Predicate],
        matching_cids: List[str],
        visited: List[int],
        pruned: List[int],
        leaf_matched: List[int],
        executor: concurrent.futures.ThreadPoolExecutor,
    ):
        """Recursively traverse a DAG node with parallel sibling fetching.
        
        Reuses a single shared ThreadPoolExecutor passed from traverse().
        At each internal node, all sibling metadata is fetched in one
        parallel batch, then pruned locally without further IPFS calls.
        """
        visited[0] += 1
        node = self._dag_get(cid)

        # Check if this node's ranges satisfy the predicates
        node_ranges = node.get("ranges", {})
        if not self._ranges_overlap(node_ranges, predicates):
            pruned[0] += 1
            return

        if node.get("node_type") == "leaf":
            # Leaf node: collect the data CID
            data_link = node.get("data", {})
            data_cid = data_link.get("/") if isinstance(data_link, dict) else data_link
            if data_cid:
                matching_cids.append(data_cid)
                leaf_matched[0] += 1
        else:
            # Internal node: fetch ALL sibling metadata in parallel
            self._process_children_parallel(
                node, predicates, matching_cids, visited, pruned, leaf_matched, executor
            )

    def _process_children_parallel(
        self,
        node: dict,
        predicates: List[Predicate],
        matching_cids: List[str],
        visited: List[int],
        pruned: List[int],
        leaf_matched: List[int],
        executor: concurrent.futures.ThreadPoolExecutor,
    ):
        """Fetch all children of an internal node in parallel, then prune/recurse."""
        children = node.get("children", [])
        child_cids = [
            link.get("/") if isinstance(link, dict) else link
            for link in children
        ]

        # Parallel fetch: submit all to the shared executor
        child_nodes = list(executor.map(self._dag_get, child_cids))

        # Prune locally using already-fetched metadata (no more IPFS calls)
        for child_cid, child_node in zip(child_cids, child_nodes):
            visited[0] += 1
            child_ranges = child_node.get("ranges", {})

            if not self._ranges_overlap(child_ranges, predicates):
                pruned[0] += 1
                continue

            if child_node.get("node_type") == "leaf":
                data_link = child_node.get("data", {})
                data_cid = data_link.get("/") if isinstance(data_link, dict) else data_link
                if data_cid:
                    matching_cids.append(data_cid)
                    leaf_matched[0] += 1
            else:
                # Recurse — reuse the same shared executor
                self._process_children_parallel(
                    child_node, predicates, matching_cids, visited, pruned, leaf_matched, executor
                )

    def _ranges_overlap(self, node_ranges: Dict, predicates: List[Predicate]) -> bool:
        """
        Check if a node's ranges could contain rows matching ALL predicates.
        
        For each predicate, check if the node's range for that attribute
        has any overlap with the predicate's constraint.
        All predicates must have potential overlap (AND semantics).
        
        Returns True if the node MAY contain matching rows (can't prune).
        Returns False if the node DEFINITELY has no matching rows (safe to prune).
        """
        for pred in predicates:
            attr = pred.attribute
            if attr not in node_ranges:
                # Attribute not in this node's metadata — can't prune, be conservative
                continue

            r = node_ranges[attr]
            r_min = r[0] if isinstance(r, list) else r
            r_max = r[1] if isinstance(r, list) else r

            # Check if the predicate eliminates this entire range
            if pred.operator == '>' and r_max <= pred.value:
                return False  # All values ≤ pred.value, but need > pred.value
            elif pred.operator == '>=' and r_max < pred.value:
                return False
            elif pred.operator == '<' and r_min >= pred.value:
                return False
            elif pred.operator == '<=' and r_min > pred.value:
                return False
            elif pred.operator == '=' and (r_min > pred.value or r_max < pred.value):
                return False
            elif pred.operator == '!=' and (r_min == r_max == pred.value):
                return False  # Only prune if entire range is exactly the excluded value

        return True  # Can't prune — ranges overlap with predicates

    def fetch_and_assemble(
        self,
        data_cids: List[str],
        max_workers: int = 16,
    ) -> pd.DataFrame:
        """
        Fetch matching shard CIDs from IPFS, decrypt, and assemble into a DataFrame.
        
        Args:
            data_cids: List of data CIDs from traversal
            max_workers: Parallelism for IPFS fetches
            
        Returns:
            Combined DataFrame from all matching shards
        """
        if not data_cids:
            return pd.DataFrame()

        logger.info(f"Fetching {len(data_cids)} shards from IPFS...")
        fetch_start = time.time()

        # Parallel fetch from IPFS
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            encrypted_chunks = list(executor.map(self._fetch_from_ipfs, data_cids))

        fetch_time = time.time() - fetch_start

        # Decrypt and parse each shard
        dfs = []
        for cid, enc_data in zip(data_cids, encrypted_chunks):
            if enc_data is None:
                logger.warning(f"Failed to fetch shard CID: {cid}")
                continue
            try:
                if self.decryption_fn:
                    parquet_bytes = self.decryption_fn(enc_data)
                else:
                    parquet_bytes = enc_data
                df = pq.read_table(io.BytesIO(parquet_bytes)).to_pandas()
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to decrypt/parse shard {cid}: {e}")

        if not dfs:
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Assembled {len(combined)} rows from {len(dfs)} shards in {fetch_time:.2f}s fetch + parse")
        return combined

    def _fetch_from_ipfs(self, cid: str) -> Optional[bytes]:
        """Fetch raw data from IPFS."""
        try:
            resp = requests.post(
                f"{IPFS_API_BASE}/cat",
                params={"arg": cid},
                timeout=30,
            )
            if resp.status_code != 200:
                return None
            return resp.content
        except Exception as e:
            logger.error(f"IPFS fetch error for {cid}: {e}")
            return None

    def _dag_get(self, cid: str) -> dict:
        """Retrieve a DAG node."""
        resp = requests.post(
            f"{IPFS_API_BASE}/dag/get",
            params={"arg": cid},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


# ============================================================
# Predicate Parser: extract predicates from SQL WHERE clause
# ============================================================

def parse_predicates(sql: str, valid_attributes: List[str] = None) -> List[Predicate]:
    """
    Parse WHERE clause predicates from a SQL query.
    Only extracts predicates on the specified attributes (for pushdown).
    
    Supports: >, <, >=, <=, =, !=, BETWEEN x AND y
    Handles AND-connected predicates.
    OR predicates on partition attributes are conservatively skipped
    (cannot prune when either branch might match).
    
    Args:
        sql: The SQL query string
        valid_attributes: List of attribute names eligible for pushdown.
                         If None, extracts all numeric predicates.
    
    Returns:
        List of Predicate objects
    """
    predicates = []
    
    # Extract WHERE clause
    where_match = re.search(r'\bWHERE\b\s+(.*?)(?:\bORDER\b|\bGROUP\b|\bLIMIT\b|\bHAVING\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return predicates

    where_clause = where_match.group(1).strip()
    
    # If clause contains OR at the top level, we can only pushdown
    # predicates that appear on ALL branches. For simplicity, skip
    # pushdown entirely if OR is present (conservative but correct).
    # Nested OR inside AND is handled per-condition below.
    top_level_or = re.split(r'\s+OR\s+', where_clause, flags=re.IGNORECASE)
    if len(top_level_or) > 1:
        logger.debug("OR detected in WHERE clause — skipping predicate pushdown (conservative)")
        return predicates
    
    # Split on AND (handles BETWEEN's internal AND separately below)
    conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
    
    # Pattern: attribute BETWEEN x AND y (must check before general split consumes it)
    between_pattern = re.compile(r'(\w+)\s+BETWEEN\s+([\d.]+)', re.IGNORECASE)
    # Pattern: attribute op value
    comparison_pattern = re.compile(r'(\w+)\s*(>=|<=|!=|>|<|=)\s*([\'"]?[\d.]+[\'"]?)', re.IGNORECASE)
    
    # Re-parse BETWEEN from the original clause (before AND-split breaks it)
    between_full = re.compile(r'(\w+)\s+BETWEEN\s+([\d.]+)\s+AND\s+([\d.]+)', re.IGNORECASE)
    for bm in between_full.finditer(where_clause):
        attr = bm.group(1)
        lo = bm.group(2)
        hi = bm.group(3)
        if valid_attributes and attr not in valid_attributes:
            continue
        try:
            predicates.append(Predicate(attribute=attr, operator='>=', value=float(lo)))
            predicates.append(Predicate(attribute=attr, operator='<=', value=float(hi)))
        except ValueError:
            continue
    
    # Track which attrs already covered by BETWEEN to avoid duplicates
    between_attrs = {p.attribute for p in predicates}
    
    for cond in conditions:
        cond_stripped = cond.strip()
        # Skip if this is the second half of a BETWEEN (just a bare number)
        if re.match(r'^[\d.]+$', cond_stripped):
            continue
        # Skip if this condition is part of a BETWEEN we already parsed
        if between_pattern.search(cond_stripped):
            continue

        match = comparison_pattern.search(cond_stripped)
        if match:
            attr = match.group(1)
            op = match.group(2)
            val_str = match.group(3).strip("'\"")
            
            if valid_attributes and attr not in valid_attributes:
                continue
            # Don't duplicate if BETWEEN already covered this attr
            if attr in between_attrs:
                continue
            
            try:
                value = float(val_str)
                predicates.append(Predicate(attribute=attr, operator=op, value=value))
            except ValueError:
                continue

    return predicates
