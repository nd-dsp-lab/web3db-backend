"""
Semantic Chunker: KD-tree-style multi-dimensional range partitioner for CSV data.

Partitions a DataFrame into variable-size shards based on top-K numeric attributes
using recursive binary splitting. Each shard maintains row integrity and carries
range metadata (min/max per attribute) for predicate pushdown.

This is the core of the "Semantic Content-Addressed Partitioning" (SeCAPa) approach:
the partition boundaries are semantically meaningful (data ranges) rather than
arbitrary byte offsets, enabling content-aware pruning during query time.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Target shard size in bytes (256KB default, matching IPFS block size)
DEFAULT_TARGET_SHARD_SIZE = 256 * 1024
# Minimum shard size to avoid over-partitioning
DEFAULT_MIN_SHARD_SIZE = 64 * 1024
# Minimum number of rows per shard
MIN_ROWS_PER_SHARD = 10


@dataclass
class Shard:
    """A data shard with range metadata for each partitioning attribute."""
    shard_id: int
    data: pd.DataFrame
    ranges: Dict[str, Tuple[float, float]]  # {attr: (min, max)}
    size_bytes: int = 0
    row_count: int = 0

    def __post_init__(self):
        self.row_count = len(self.data)


@dataclass
class PartitionStats:
    """Statistics from the partitioning process."""
    total_rows: int
    total_shards: int
    partition_attributes: List[str]
    global_ranges: Dict[str, Tuple[float, float]]
    shard_sizes: List[int]
    avg_shard_size: float
    min_shard_size: int
    max_shard_size: int


class SemanticChunker:
    """
    Multi-dimensional range partitioner using KD-tree-style recursive bisection.
    
    Partitions a DataFrame by recursively splitting on the attribute with the
    widest normalized range, alternating between attributes like a KD-tree.
    Splitting stops when shards are at or below the target size.
    
    Key properties:
    - Row integrity: no row is split across shards
    - Variable-size shards: ensures meaningful semantic boundaries
    - Multi-attribute awareness: splits on the most discriminating attribute at each level
    - Range metadata: each shard carries [min, max] per attribute for pushdown
    """

    def __init__(
        self,
        partition_attributes: List[str],
        target_shard_size: int = DEFAULT_TARGET_SHARD_SIZE,
        min_shard_size: int = DEFAULT_MIN_SHARD_SIZE,
    ):
        """
        Args:
            partition_attributes: Top-K numeric attributes to partition on (e.g., ['age', 'salary_usd'])
            target_shard_size: Target size per shard in bytes (~256KB)
            min_shard_size: Minimum shard size to prevent over-splitting (~64KB)
        """
        self.partition_attributes = partition_attributes
        self.target_shard_size = target_shard_size
        self.min_shard_size = min_shard_size
        self._shard_counter = 0

    def partition(self, df: pd.DataFrame) -> Tuple[List[Shard], PartitionStats]:
        """
        Partition a DataFrame into range-bounded shards.
        
        Returns:
            Tuple of (list of Shard objects, PartitionStats)
        """
        logger.info(f"Starting semantic partitioning: {len(df)} rows, attributes={self.partition_attributes}")
        
        # Validate attributes exist and are numeric
        for attr in self.partition_attributes:
            if attr not in df.columns:
                raise ValueError(f"Partition attribute '{attr}' not found in DataFrame columns: {list(df.columns)}")
            if not pd.api.types.is_numeric_dtype(df[attr]):
                raise ValueError(f"Partition attribute '{attr}' must be numeric, got {df[attr].dtype}")

        # Compute global ranges for normalization
        global_ranges = {}
        for attr in self.partition_attributes:
            global_ranges[attr] = (float(df[attr].min()), float(df[attr].max()))

        # Estimate row size for shard size calculations
        row_size_estimate = self._estimate_row_size(df)
        # target_rows is the hard upper bound: no shard should exceed target_shard_size.
        # Shards will range from [target/2, target], averaging ~0.75*target.
        target_rows = max(MIN_ROWS_PER_SHARD, self.target_shard_size // row_size_estimate)
        min_rows = max(MIN_ROWS_PER_SHARD, self.min_shard_size // row_size_estimate)

        logger.info(f"Row size estimate: {row_size_estimate} bytes, target rows/shard: {target_rows}, min rows/shard: {min_rows}")

        # Recursive partitioning
        self._shard_counter = 0
        shards = self._recursive_partition(df, global_ranges, target_rows, min_rows, depth=0)

        # Compute shard sizes (parquet-estimated) and enforce hard max
        for shard in shards:
            shard.size_bytes = self._estimate_shard_size(shard.data)

        # Post-partition enforcement: re-split any shard that exceeds the target size.
        # This handles estimation errors where actual Parquet size > row-estimate * rows.
        max_split_passes = 5  # safety limit to avoid infinite loops
        for _ in range(max_split_passes):
            oversized = [s for s in shards if s.size_bytes > self.target_shard_size]
            if not oversized:
                break
            logger.info(f"Post-partition: {len(oversized)} oversized shards, re-splitting...")
            new_shards = []
            for shard in shards:
                if shard.size_bytes > self.target_shard_size and len(shard.data) > MIN_ROWS_PER_SHARD * 2:
                    # Binary split this shard on the best attribute
                    halves = self._split_shard_in_half(shard.data, global_ranges)
                    for half_df in halves:
                        new_shard = self._make_shard(half_df)
                        new_shard.size_bytes = self._estimate_shard_size(new_shard.data)
                        new_shards.append(new_shard)
                else:
                    new_shards.append(shard)
            shards = new_shards

        # Build stats
        shard_sizes = [s.size_bytes for s in shards]
        stats = PartitionStats(
            total_rows=len(df),
            total_shards=len(shards),
            partition_attributes=self.partition_attributes,
            global_ranges=global_ranges,
            shard_sizes=shard_sizes,
            avg_shard_size=sum(shard_sizes) / len(shard_sizes) if shard_sizes else 0,
            min_shard_size=min(shard_sizes) if shard_sizes else 0,
            max_shard_size=max(shard_sizes) if shard_sizes else 0,
        )

        logger.info(
            f"Partitioning complete: {stats.total_shards} shards, "
            f"avg size: {stats.avg_shard_size/1024:.1f}KB, "
            f"range: [{stats.min_shard_size/1024:.1f}KB, {stats.max_shard_size/1024:.1f}KB]"
        )
        return shards, stats

    def _recursive_partition(
        self,
        df: pd.DataFrame,
        global_ranges: Dict[str, Tuple[float, float]],
        target_rows: int,
        min_rows: int,
        depth: int,
    ) -> List[Shard]:
        """
        Recursively split the DataFrame using KD-tree-style bisection.
        
        At each level, picks the attribute with the widest normalized range
        in the current subset and splits at the median.
        """
        # Base case: small enough to be a leaf shard
        if len(df) <= target_rows or len(df) <= min_rows:
            return [self._make_shard(df)]

        # Pick the best attribute to split on (widest normalized range)
        best_attr = self._pick_split_attribute(df, global_ranges)
        if best_attr is None:
            # All attributes are constant in this subset — can't split further
            return [self._make_shard(df)]

        # Split at median for balanced partitions
        median_val = df[best_attr].median()
        
        # Handle case where median equals min (all values clustered)
        left_mask = df[best_attr] <= median_val
        right_mask = df[best_attr] > median_val

        left_df = df[left_mask]
        right_df = df[right_mask]

        # If split is degenerate (one side empty), try unique-value split
        if len(left_df) == 0 or len(right_df) == 0:
            unique_vals = sorted(df[best_attr].unique())
            if len(unique_vals) <= 1:
                return [self._make_shard(df)]
            mid_idx = len(unique_vals) // 2
            split_val = unique_vals[mid_idx]
            left_mask = df[best_attr] < split_val
            right_mask = df[best_attr] >= split_val
            left_df = df[left_mask]
            right_df = df[right_mask]
            if len(left_df) == 0 or len(right_df) == 0:
                return [self._make_shard(df)]

        # Recurse on both halves
        shards = []
        shards.extend(self._recursive_partition(left_df, global_ranges, target_rows, min_rows, depth + 1))
        shards.extend(self._recursive_partition(right_df, global_ranges, target_rows, min_rows, depth + 1))
        return shards

    def _pick_split_attribute(
        self, df: pd.DataFrame, global_ranges: Dict[str, Tuple[float, float]]
    ) -> Optional[str]:
        """
        Pick the attribute with the widest normalized range in the current subset.
        Normalization by global range ensures fair comparison between attributes
        with different scales (e.g., Age [18-65] vs Salary [30000-200000]).
        """
        best_attr = None
        best_spread = -1.0

        for attr in self.partition_attributes:
            g_min, g_max = global_ranges[attr]
            g_range = g_max - g_min
            if g_range == 0:
                continue  # Constant attribute globally

            local_min = float(df[attr].min())
            local_max = float(df[attr].max())
            local_range = local_max - local_min

            if local_range == 0:
                continue  # Constant in this subset

            normalized_spread = local_range / g_range
            if normalized_spread > best_spread:
                best_spread = normalized_spread
                best_attr = attr

        return best_attr

    def _make_shard(self, df: pd.DataFrame) -> Shard:
        """Create a Shard with computed range metadata."""
        ranges = {}
        for attr in self.partition_attributes:
            ranges[attr] = (float(df[attr].min()), float(df[attr].max()))

        shard = Shard(
            shard_id=self._shard_counter,
            data=df.copy(),
            ranges=ranges,
        )
        self._shard_counter += 1
        return shard

    def _split_shard_in_half(
        self, df: pd.DataFrame, global_ranges: Dict[str, Tuple[float, float]]
    ) -> List[pd.DataFrame]:
        """Split a DataFrame into two halves on the best attribute (for post-partition enforcement)."""
        best_attr = self._pick_split_attribute(df, global_ranges)
        if best_attr is None:
            # Can't split further — return as-is
            return [df]

        median_val = df[best_attr].median()
        left_df = df[df[best_attr] <= median_val]
        right_df = df[df[best_attr] > median_val]

        if len(left_df) == 0 or len(right_df) == 0:
            unique_vals = sorted(df[best_attr].unique())
            if len(unique_vals) <= 1:
                return [df]
            split_val = unique_vals[len(unique_vals) // 2]
            left_df = df[df[best_attr] < split_val]
            right_df = df[df[best_attr] >= split_val]
            if len(left_df) == 0 or len(right_df) == 0:
                return [df]

        return [left_df, right_df]

    def _estimate_row_size(self, df: pd.DataFrame) -> int:
        """Estimate average row size in bytes (Parquet-approximate)."""
        # Use a larger sample to amortize the fixed Parquet metadata overhead,
        # which otherwise inflates the per-row estimate with small samples.
        sample = df.head(min(10000, len(df)))
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(sample), buf, compression='snappy')
        total_bytes = buf.tell()
        return max(1, total_bytes // len(sample))

    def _estimate_shard_size(self, df: pd.DataFrame) -> int:
        """Estimate the Parquet size of a shard."""
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf, compression='snappy')
        return buf.tell()
