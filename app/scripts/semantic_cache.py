"""
Semantic Cache for Web3DB
In-memory semantic-aware cache using DuckDB for storage and query execution.

Features:
- DuckDB in-memory tables for fast SQL queries on cached data
- Semantic-aware cache lookup with subset detection
- LRU eviction with configurable memory limits
- Metrics collection for cache performance analysis
- Cache invalidation on data changes (UPDATE/DELETE)

Designed for SGX enclave deployment with large EPC (128GB+).
"""

import time
import hashlib
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import duckdb
import pandas as pd

from query_parser import (
    QueryParser, 
    ParsedQuery, 
    PredicateGroup,
)

# Import Z3 containment checker
try:
    from z3_containment import Z3ContainmentChecker, is_z3_available
    Z3_ENABLED = is_z3_available()
except ImportError:
    Z3_ENABLED = False
    Z3ContainmentChecker = None

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Metadata for a cached query result"""
    cache_id: str
    table_name: str
    duckdb_table: str  # Name of the DuckDB in-memory table
    parsed_query: ParsedQuery
    signature: str
    row_count: int
    created_at: float
    last_accessed: float
    access_count: int
    size_bytes: int  # Estimated memory size
    
    def to_dict(self) -> dict:
        return {
            "cache_id": self.cache_id,
            "table_name": self.table_name,
            "duckdb_table": self.duckdb_table,
            "signature": self.signature,
            "original_query": self.parsed_query.original_query,  # Show what query created this cache
            "outer_filter": self.parsed_query.outer_predicates.to_sql() if not self.parsed_query.outer_predicates.is_empty() else None,
            "row_count": int(self.row_count),  # Convert to native int for JSON serialization
            "created_at": datetime.fromtimestamp(self.created_at).isoformat(),
            "last_accessed": datetime.fromtimestamp(self.last_accessed).isoformat(),
            "access_count": int(self.access_count),  # Convert to native int for JSON serialization
            "size_bytes": int(self.size_bytes)  # Convert to native int for JSON serialization
        }


@dataclass
class CacheMetrics:
    """Cache performance metrics"""
    total_queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    subset_hits: int = 0  # Hits where result was derived from superset
    exact_hits: int = 0   # Hits where result was exact match
    total_hit_latency_ms: float = 0.0
    total_miss_latency_ms: float = 0.0
    evictions: int = 0
    invalidations: int = 0
    current_entries: int = 0
    current_size_bytes: int = 0
    # Z3 metrics
    z3_checks: int = 0
    z3_total_latency_ms: float = 0.0
    
    @property
    def hit_rate(self) -> float:
        if self.total_queries == 0:
            return 0.0
        return self.cache_hits / self.total_queries
    
    @property
    def avg_hit_latency_ms(self) -> float:
        if self.cache_hits == 0:
            return 0.0
        return self.total_hit_latency_ms / self.cache_hits
    
    @property
    def avg_miss_latency_ms(self) -> float:
        if self.cache_misses == 0:
            return 0.0
        return self.total_miss_latency_ms / self.cache_misses
    
    def to_dict(self) -> dict:
        return {
            "total_queries": int(self.total_queries),
            "cache_hits": int(self.cache_hits),
            "cache_misses": int(self.cache_misses),
            "exact_hits": int(self.exact_hits),
            "subset_hits": int(self.subset_hits),
            "hit_rate": round(float(self.hit_rate) * 100, 2),
            "avg_hit_latency_ms": round(float(self.avg_hit_latency_ms), 3),
            "avg_miss_latency_ms": round(float(self.avg_miss_latency_ms), 3),
            "evictions": int(self.evictions),
            "invalidations": int(self.invalidations),
            "current_entries": int(self.current_entries),
            "current_size_bytes": int(self.current_size_bytes),
            "current_size_mb": round(float(self.current_size_bytes) / (1024 * 1024), 2),
            "z3_checks": int(self.z3_checks),
            "z3_avg_latency_ms": round(float(self.z3_total_latency_ms / max(1, self.z3_checks)), 3)
        }


@dataclass
class CacheLookupResult:
    """Result of a cache lookup"""
    hit: bool
    hit_type: str  # "exact", "subset", "miss"
    cache_entry: Optional[CacheEntry] = None
    additional_filter: Optional[str] = None  # SQL filter to apply on cached data
    lookup_time_ms: float = 0.0
    parsed_query: Optional[ParsedQuery] = None  # For ORDER BY, LIMIT, OFFSET


class SemanticCache:
    """
    Semantic-aware in-memory cache using DuckDB.
    
    Cache keys are based on semantic query signatures, enabling:
    - Exact match: Same query returns cached result
    - Subset match: New query can be derived from cached superset
    
    Storage: DuckDB in-memory tables (fast SQL queries on cached data)
    Eviction: LRU with memory pressure awareness
    """
    
    def __init__(
        self, 
        duckdb_conn: duckdb.DuckDBPyConnection,
        max_size_bytes: int = 64 * 1024 * 1024 * 1024,  # 64GB default
        max_entries: int = 10000,
        enable_subset_detection: bool = True
    ):
        """
        Initialize the semantic cache.
        
        Args:
            duckdb_conn: DuckDB connection to use for in-memory tables
            max_size_bytes: Maximum cache size in bytes (default 64GB)
            max_entries: Maximum number of cache entries
            enable_subset_detection: Whether to check for subset matches
        """
        self.conn = duckdb_conn
        self.max_size_bytes = max_size_bytes
        self.max_entries = max_entries
        self.enable_subset_detection = enable_subset_detection
        
        # Cache storage: OrderedDict for LRU ordering
        # Key: cache_id, Value: CacheEntry
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        
        # Index for fast lookup by table name
        # Key: table_name, Value: List of cache_ids
        self._table_index: Dict[str, List[str]] = {}
        
        # Query parser
        self._parser = QueryParser()
        
        # Metrics
        self._metrics = CacheMetrics()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Z3 containment checker
        self._z3_checker = None
        if Z3_ENABLED and Z3ContainmentChecker:
            self._z3_checker = Z3ContainmentChecker(timeout_ms=1000)
            logger.info("Z3 containment checker enabled")
        else:
            logger.warning("Z3 not available, containment checking disabled")
        
        logger.info(f"SemanticCache initialized: max_size={max_size_bytes/(1024**3):.1f}GB, max_entries={max_entries}")
    
    def _generate_cache_id(self, table_name: str, signature: str) -> str:
        """Generate unique cache ID"""
        return f"cache_{table_name}_{signature[:16]}"
    
    def _generate_duckdb_table_name(self, cache_id: str) -> str:
        """Generate DuckDB table name for cache entry"""
        # DuckDB table names must be valid identifiers
        return cache_id.replace("-", "_")
    
    def _estimate_dataframe_size(self, df: pd.DataFrame) -> int:
        """Estimate memory size of a DataFrame in bytes"""
        return df.memory_usage(deep=True).sum()
    
    def lookup(self, rewritten_query: str, table_name: str) -> CacheLookupResult:
        """
        Look up a query in the cache.
        
        Args:
            rewritten_query: The query after access control rewriting
            table_name: The table being queried
            
        Returns:
            CacheLookupResult with hit status and cache entry if found
            
        Cache Lookup Strategy:
        1. Check for exact match (same full signature, or base signature for queries without outer predicates)
        2. Check for base match (same CTE/access control predicates, different outer predicates)
           - If new query has outer predicates but cached query doesn't, use outer as filter
        3. Check for subset match using predicate analysis
        """
        start_time = time.time()
        
        with self._lock:
            self._metrics.total_queries += 1
            
            # Parse the query
            parsed = self._parser.parse(rewritten_query)
            full_signature = parsed.generate_signature()
            base_signature = parsed.generate_base_signature()
            cache_id = self._generate_cache_id(table_name, full_signature)
            base_cache_id = self._generate_cache_id(table_name, base_signature)
            
            # Check for exact match
            # For queries without outer predicates, check base_cache_id (that's how we store them)
            # For queries with outer predicates, check cache_id first (full signature)
            exact_lookup_id = base_cache_id if parsed.outer_predicates.is_empty() else cache_id
            
            # Debug logging
            logger.info(f"Cache LOOKUP: table={table_name}, exact_lookup_id={exact_lookup_id}")
            logger.info(f"Cache LOOKUP: cache_keys={list(self._cache.keys())}")
            
            if exact_lookup_id in self._cache:
                entry = self._cache[exact_lookup_id]
                # Move to end for LRU
                self._cache.move_to_end(exact_lookup_id)
                entry.last_accessed = time.time()
                entry.access_count += 1
                
                lookup_time = (time.time() - start_time) * 1000
                self._metrics.cache_hits += 1
                self._metrics.exact_hits += 1
                self._metrics.total_hit_latency_ms += lookup_time
                
                logger.info(f"Cache HIT (exact): {exact_lookup_id}, {entry.row_count} rows")
                
                return CacheLookupResult(
                    hit=True,
                    hit_type="exact",
                    cache_entry=entry,
                    additional_filter=None,
                    lookup_time_ms=lookup_time,
                    parsed_query=parsed
                )
            
            # Check for base match (same CTE predicates, different outer predicates)
            # This handles the case where we cached "SELECT * FROM table" and now query 
            # "SELECT * FROM table WHERE Age > 90"
            if base_cache_id in self._cache and not parsed.outer_predicates.is_empty():
                entry = self._cache[base_cache_id]
                # Verify the cached entry has no outer predicates (it's the base/superset)
                if entry.parsed_query.outer_predicates.is_empty():
                    # Move to end for LRU
                    self._cache.move_to_end(base_cache_id)
                    entry.last_accessed = time.time()
                    entry.access_count += 1
                    
                    # Generate SQL filter from outer predicates
                    additional_filter = parsed.outer_predicates.to_sql()
                    
                    lookup_time = (time.time() - start_time) * 1000
                    self._metrics.cache_hits += 1
                    self._metrics.subset_hits += 1
                    self._metrics.total_hit_latency_ms += lookup_time
                    
                    logger.info(f"Cache HIT (base+filter): {base_cache_id}, filter: {additional_filter}")
                    
                    return CacheLookupResult(
                        hit=True,
                        hit_type="subset",
                        cache_entry=entry,
                        additional_filter=additional_filter,
                        lookup_time_ms=lookup_time,
                        parsed_query=parsed
                    )
            
            # Check for subset match if enabled (using predicate analysis)
            if self.enable_subset_detection:
                subset_result = self._find_superset_entry(parsed, table_name)
                if subset_result:
                    entry, additional_filter = subset_result
                    # Move to end for LRU
                    self._cache.move_to_end(entry.cache_id)
                    entry.last_accessed = time.time()
                    entry.access_count += 1
                    
                    lookup_time = (time.time() - start_time) * 1000
                    self._metrics.cache_hits += 1
                    self._metrics.subset_hits += 1
                    self._metrics.total_hit_latency_ms += lookup_time
                    
                    logger.info(f"Cache HIT (subset): {entry.cache_id}, filter: {additional_filter}")
                    
                    return CacheLookupResult(
                        hit=True,
                        hit_type="subset",
                        cache_entry=entry,
                        additional_filter=additional_filter,
                        lookup_time_ms=lookup_time,
                        parsed_query=parsed
                    )
            
            # Cache miss
            lookup_time = (time.time() - start_time) * 1000
            self._metrics.cache_misses += 1
            self._metrics.total_miss_latency_ms += lookup_time
            
            logger.info(f"Cache MISS: {table_name}, signature={full_signature[:16]}, base_sig={base_signature[:16]}")
            
            return CacheLookupResult(
                hit=False,
                hit_type="miss",
                cache_entry=None,
                additional_filter=None,
                lookup_time_ms=lookup_time
            )
    
    def _find_superset_entry(
        self, 
        parsed: ParsedQuery, 
        table_name: str
    ) -> Optional[Tuple[CacheEntry, Optional[str]]]:
        """
        Find a cached entry that is a superset of the given query using Z3 SMT solver.
        
        Returns (CacheEntry, additional_filter) if found, None otherwise.
        
        Uses Z3 to verify: new_predicates ⊆ cached_predicates
        """
        # Get all cache entries for this table
        if table_name not in self._table_index:
            return None
        
        if not self._z3_checker:
            return None  # Z3 not available
        
        cache_ids = self._table_index[table_name]
        
        for cache_id in cache_ids:
            if cache_id not in self._cache:
                continue
            
            entry = self._cache[cache_id]
            
            # Check compatibility: Don't match aggregated cache with non-aggregated query
            # If cached query has GROUP BY/aggregations but new query doesn't, skip
            cached_has_aggregation = bool(entry.parsed_query.group_by) or bool(entry.parsed_query.aggregations)
            new_has_aggregation = bool(parsed.group_by) or bool(parsed.aggregations)
            
            if cached_has_aggregation and not new_has_aggregation:
                # Can't serve SELECT * from aggregated cache (e.g., COUNT(*) GROUP BY)
                continue
            
            if not cached_has_aggregation and new_has_aggregation:
                # Can serve aggregation from raw data, but need to re-aggregate
                # This is handled by query_cached with needs_reaggregation
                pass
            
            # Use Z3 to check containment of outer predicates
            z3_start = time.time()
            is_contained, additional_filter = self._z3_checker.is_contained(
                entry.parsed_query.outer_predicates,
                parsed.outer_predicates
            )
            z3_time = (time.time() - z3_start) * 1000
            
            # Update Z3 metrics
            self._metrics.z3_checks += 1
            self._metrics.z3_total_latency_ms += z3_time
            
            if is_contained:
                return entry, additional_filter
        
        return None
    
    def store(
        self, 
        rewritten_query: str, 
        table_name: str, 
        df: pd.DataFrame
    ) -> CacheEntry:
        """
        Store query results in the cache.
        
        Args:
            rewritten_query: The query after access control rewriting
            table_name: The table being queried
            df: The query results as a DataFrame
            
        Returns:
            The created CacheEntry
            
        Storage Strategy:
        - If query has no outer predicates (e.g., SELECT * FROM table), store with base_signature
          so that queries with outer predicates can find and filter from it
        - If query has outer predicates, store with full signature for exact match
        """
        with self._lock:
            # Parse query and generate IDs
            parsed = self._parser.parse(rewritten_query)
            
            # Use base signature if no outer predicates (makes this a reusable base cache)
            # Use full signature if there are outer predicates (specific filtered result)
            if parsed.outer_predicates.is_empty():
                signature = parsed.generate_base_signature()
            else:
                signature = parsed.generate_signature()
            
            cache_id = self._generate_cache_id(table_name, signature)
            duckdb_table = self._generate_duckdb_table_name(cache_id)
            
            # Check if already cached (race condition)
            if cache_id in self._cache:
                logger.debug(f"Cache entry already exists: {cache_id}")
                return self._cache[cache_id]
            
            # Estimate size
            size_bytes = self._estimate_dataframe_size(df)
            
            # Evict if necessary
            self._evict_if_needed(size_bytes)
            
            # Create DuckDB table
            try:
                # Drop if exists (shouldn't happen, but safety)
                self.conn.execute(f"DROP TABLE IF EXISTS {duckdb_table}")
                # Register DataFrame as table
                self.conn.register(f"{duckdb_table}_df", df)
                self.conn.execute(f"CREATE TABLE {duckdb_table} AS SELECT * FROM {duckdb_table}_df")
                self.conn.unregister(f"{duckdb_table}_df")
            except Exception as e:
                logger.error(f"Failed to create cache table {duckdb_table}: {e}")
                raise
            
            # Create cache entry
            now = time.time()
            entry = CacheEntry(
                cache_id=cache_id,
                table_name=table_name,
                duckdb_table=duckdb_table,
                parsed_query=parsed,
                signature=signature,
                row_count=len(df),
                created_at=now,
                last_accessed=now,
                access_count=0,
                size_bytes=size_bytes
            )
            
            # Store in cache
            self._cache[cache_id] = entry
            logger.info(f"Cache STORE: stored with cache_id={cache_id}, now cache_keys={list(self._cache.keys())}")
            
            # Update table index
            if table_name not in self._table_index:
                self._table_index[table_name] = []
            self._table_index[table_name].append(cache_id)
            
            # Update metrics
            self._metrics.current_entries = len(self._cache)
            self._metrics.current_size_bytes += size_bytes
            
            logger.info(f"Cache STORE: {cache_id}, {len(df)} rows, {size_bytes/(1024*1024):.2f}MB")
            
            return entry
    
    def query_cached(
        self, 
        entry: CacheEntry, 
        additional_filter: Optional[str] = None,
        columns: Optional[List[str]] = None,
        parsed_query: Optional[ParsedQuery] = None
    ) -> pd.DataFrame:
        """
        Query cached data, optionally with additional filters and ordering.
        
        Args:
            entry: The cache entry to query
            additional_filter: Optional SQL WHERE clause to apply
            columns: Optional list of columns to select (default: all)
            parsed_query: Optional ParsedQuery to apply SELECT columns, GROUP BY, ORDER BY, LIMIT, OFFSET
            
        Returns:
            Query results as DataFrame
        """
        with self._lock:
            # For exact cache hits (no additional_filter), the data is already in the correct format
            # including any aggregations. For subset hits, we need to re-apply aggregations.
            needs_reaggregation = additional_filter is not None
            
            # Build query - use parsed_query columns if available (for aggregations)
            if needs_reaggregation and parsed_query and parsed_query.columns and parsed_query.columns != ["*"]:
                # Use the original SELECT columns (includes aggregations like COUNT(*))
                cols = ", ".join(parsed_query.columns)
            elif columns:
                cols = ", ".join(columns)
            else:
                cols = "*"
            
            sql = f"SELECT {cols} FROM {entry.duckdb_table}"
            
            if additional_filter:
                sql += f" WHERE {additional_filter}"
            
            # Apply GROUP BY only if we're re-aggregating subset data
            if needs_reaggregation and parsed_query and parsed_query.group_by:
                sql += f" GROUP BY {', '.join(parsed_query.group_by)}"
            
            # Apply ORDER BY from parsed query
            if parsed_query and parsed_query.order_by:
                order_clauses = [f"{col} {direction}" for col, direction in parsed_query.order_by]
                sql += f" ORDER BY {', '.join(order_clauses)}"
            
            # Apply LIMIT from parsed query
            if parsed_query and parsed_query.limit:
                sql += f" LIMIT {parsed_query.limit}"
            
            # Apply OFFSET from parsed query
            if parsed_query and parsed_query.offset:
                sql += f" OFFSET {parsed_query.offset}"
            
            logger.debug(f"Cache query SQL: {sql}")
            
            try:
                result = self.conn.execute(sql).fetchdf()
                return result
            except Exception as e:
                logger.error(f"Failed to query cache table {entry.duckdb_table}: {e}")
                raise
    
    def _evict_if_needed(self, needed_bytes: int):
        """Evict entries if cache is full"""
        # Check entry count limit
        while len(self._cache) >= self.max_entries:
            self._evict_lru()
        
        # Check size limit
        while self._metrics.current_size_bytes + needed_bytes > self.max_size_bytes:
            if not self._cache:
                break
            self._evict_lru()
    
    def _evict_lru(self):
        """Evict the least recently used entry"""
        if not self._cache:
            return
        
        # Get oldest entry (first in OrderedDict)
        cache_id = next(iter(self._cache))
        self._remove_entry(cache_id)
        self._metrics.evictions += 1
        logger.info(f"Cache EVICT (LRU): {cache_id}")
    
    def _remove_entry(self, cache_id: str):
        """Remove a cache entry"""
        if cache_id not in self._cache:
            return
        
        entry = self._cache.pop(cache_id)
        
        # Remove from table index
        if entry.table_name in self._table_index:
            try:
                self._table_index[entry.table_name].remove(cache_id)
            except ValueError:
                pass
        
        # Drop DuckDB table
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {entry.duckdb_table}")
        except Exception as e:
            logger.warning(f"Failed to drop cache table {entry.duckdb_table}: {e}")
        
        # Update metrics
        self._metrics.current_entries = len(self._cache)
        self._metrics.current_size_bytes -= entry.size_bytes
    
    def invalidate_table(self, table_name: str):
        """
        Invalidate all cache entries for a table.
        Called when data changes (UPDATE/DELETE).
        """
        with self._lock:
            if table_name not in self._table_index:
                return
            
            cache_ids = list(self._table_index[table_name])
            for cache_id in cache_ids:
                self._remove_entry(cache_id)
                self._metrics.invalidations += 1
            
            logger.info(f"Cache INVALIDATE: table={table_name}, entries={len(cache_ids)}")
    
    def invalidate_all(self):
        """Invalidate entire cache"""
        with self._lock:
            cache_ids = list(self._cache.keys())
            for cache_id in cache_ids:
                self._remove_entry(cache_id)
                self._metrics.invalidations += 1
            
            logger.info(f"Cache INVALIDATE ALL: entries={len(cache_ids)}")
    
    def get_metrics(self) -> CacheMetrics:
        """Get cache performance metrics"""
        with self._lock:
            return self._metrics
    
    def get_entries(self, table_name: Optional[str] = None) -> List[CacheEntry]:
        """Get all cache entries, optionally filtered by table"""
        with self._lock:
            if table_name:
                cache_ids = self._table_index.get(table_name, [])
                return [self._cache[cid] for cid in cache_ids if cid in self._cache]
            return list(self._cache.values())
    
    def get_stats(self) -> dict:
        """Get comprehensive cache statistics"""
        with self._lock:
            entries_by_table = {}
            for table_name, cache_ids in self._table_index.items():
                entries_by_table[table_name] = len([cid for cid in cache_ids if cid in self._cache])
            
            return {
                "metrics": self._metrics.to_dict(),
                "entries_by_table": entries_by_table,
                "total_entries": len(self._cache),
                "max_entries": self.max_entries,
                "max_size_gb": self.max_size_bytes / (1024 ** 3),
                "current_size_gb": self._metrics.current_size_bytes / (1024 ** 3),
                "subset_detection_enabled": self.enable_subset_detection
            }
    
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self.invalidate_all()
            self._metrics = CacheMetrics()
            logger.info("Cache CLEARED")


# Global cache instance (initialized in app.py)
_semantic_cache: Optional[SemanticCache] = None


def get_semantic_cache() -> Optional[SemanticCache]:
    """Get the global semantic cache instance"""
    return _semantic_cache


def init_semantic_cache(
    duckdb_conn: duckdb.DuckDBPyConnection,
    max_size_bytes: int = 64 * 1024 * 1024 * 1024,
    max_entries: int = 10000,
    enable_subset_detection: bool = True
) -> SemanticCache:
    """Initialize the global semantic cache"""
    global _semantic_cache
    _semantic_cache = SemanticCache(
        duckdb_conn=duckdb_conn,
        max_size_bytes=max_size_bytes,
        max_entries=max_entries,
        enable_subset_detection=enable_subset_detection
    )
    return _semantic_cache


# Test the cache
if __name__ == "__main__":
    import pandas as pd
    
    # Create test connection
    conn = duckdb.connect(':memory:')
    
    # Initialize cache
    cache = SemanticCache(conn, max_size_bytes=1024*1024*100)  # 100MB for testing
    
    # Create test data
    test_data = pd.DataFrame({
        'PatientID': range(1, 101),
        'Name': [f'Patient {i}' for i in range(1, 101)],
        'Age': [20 + (i % 80) for i in range(1, 101)],
        'Condition': ['Diabetes' if i % 3 == 0 else 'Healthy' for i in range(1, 101)]
    })
    
    # Test store
    query1 = "SELECT * FROM patient_data WHERE Age > 40"
    entry1 = cache.store(query1, "patient_data", test_data[test_data['Age'] > 40])
    print(f"Stored: {entry1.cache_id}, {entry1.row_count} rows")
    
    # Test exact hit
    result1 = cache.lookup(query1, "patient_data")
    print(f"Lookup 1 (exact): hit={result1.hit}, type={result1.hit_type}")
    
    # Test subset hit
    query2 = "SELECT * FROM patient_data WHERE Age > 50"
    result2 = cache.lookup(query2, "patient_data")
    print(f"Lookup 2 (subset): hit={result2.hit}, type={result2.hit_type}, filter={result2.additional_filter}")
    
    if result2.hit:
        df = cache.query_cached(result2.cache_entry, result2.additional_filter)
        print(f"Query result: {len(df)} rows")
    
    # Test miss
    query3 = "SELECT * FROM patient_data WHERE Age < 30"
    result3 = cache.lookup(query3, "patient_data")
    print(f"Lookup 3 (miss): hit={result3.hit}, type={result3.hit_type}")
    
    # Print stats
    print(f"\nCache stats: {cache.get_stats()}")
