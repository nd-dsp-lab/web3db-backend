# Semantic Caching with Z3-Based Query Containment

## Overview

This document explains the semantic caching implementation in Web3DB, a decentralized database system that stores encrypted data on IPFS. The cache uses SMT (Satisfiability Modulo Theories) solving via Z3 to perform provably correct query containment checking.

## 1. Problem Statement

### Traditional Caching Limitation
Traditional query caches use exact string matching:
```
Cache Key: SHA256("SELECT * FROM patients WHERE Age > 50")
```
This fails when semantically equivalent or subset queries are issued:
```sql
-- These are cache MISSES even though data exists:
SELECT * FROM patients WHERE Age > 60   -- Subset of cached data
SELECT * FROM patients WHERE Age > 50   -- Same query, different whitespace
```

### Semantic Caching Solution
Semantic caching understands query semantics:
- **Exact Match**: Identical query structure → Return cached data
- **Subset Match**: New query's results are contained within cached data → Filter and return

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Query Request                                   │
│                    SELECT * FROM patients WHERE Age > 60                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           1. Query Parser                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ParsedQuery {                                                        │    │
│  │   tables: ["patients"]                                               │    │
│  │   columns: ["*"]                                                     │    │
│  │   outer_predicates: PredicateGroup([Age > 60])                      │    │
│  │   cte_predicates: PredicateGroup([OwnerID = '0x...'])  // Access    │    │
│  │   group_by: [], order_by: [], limit: None                           │    │
│  │ }                                                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        2. Cache Lookup Strategy                              │
│                                                                              │
│  Step 1: Generate Signatures                                                 │
│    full_signature = SHA256(all predicates + columns + order_by + ...)       │
│    base_signature = SHA256(CTE predicates only)  // For subset matching     │
│                                                                              │
│  Step 2: Check Exact Match                                                   │
│    if cache[full_signature] exists → Return cached data                      │
│                                                                              │
│  Step 3: Check Base Match (for filtering)                                    │
│    if cache[base_signature] exists AND cached has no outer predicates       │
│    → Filter cached data with new outer predicates                            │
│                                                                              │
│  Step 4: Z3 Subset Match                                                     │
│    For each cache entry with same table:                                     │
│      if Z3.is_contained(cached_predicates, new_predicates)                  │
│      → Filter cached data                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
            ┌──────────────┐                 ┌──────────────┐
            │  CACHE HIT   │                 │  CACHE MISS  │
            │  (2-10 ms)   │                 │  (100+ ms)   │
            └──────────────┘                 └──────────────┘
                    │                                 │
                    ▼                                 ▼
            ┌──────────────┐                 ┌──────────────┐
            │ Query DuckDB │                 │ Fetch IPFS   │
            │ In-Memory    │                 │ Decrypt      │
            └──────────────┘                 │ Execute SQL  │
                    │                        │ Store Cache  │
                    ▼                        └──────────────┘
            ┌──────────────┐                         │
            │   Response   │◄────────────────────────┘
            └──────────────┘
```

## 3. Z3-Based Containment Checking

### The Containment Problem

Given:
- **Cached query predicates**: P_cached (e.g., `Age > 50`)
- **New query predicates**: P_new (e.g., `Age > 60`)

**Question**: Can we derive P_new's results from P_cached's data?

**Formal Definition**:
```
P_new ⊆ P_cached  ⟺  ∀ tuple t: P_new(t) → P_cached(t)
                  ⟺  ¬∃ tuple t: P_new(t) ∧ ¬P_cached(t)
```

### SMT Encoding

We translate SQL predicates to Z3 constraints:

| SQL Predicate | Z3 Encoding |
|---------------|-------------|
| `Age > 50` | `z3.Int("Age") > 50` |
| `Age BETWEEN 20 AND 60` | `z3.And(Age >= 20, Age <= 60)` |
| `Status IN ('A','B')` | `z3.Or(Status == "A", Status == "B")` |
| `P1 AND P2` | `z3.And(P1, P2)` |
| `P1 OR P2` | `z3.Or(P1, P2)` |

### The Z3 Check

```python
def is_contained(cached_predicates, new_predicates):
    # Encode predicates to Z3 formulas
    φ_cached = encode(cached_predicates)
    φ_new = encode(new_predicates)
    
    # Check: ∃ tuple that satisfies new BUT NOT cached?
    solver = z3.Solver()
    solver.add(φ_new)           # Tuple satisfies new query
    solver.add(z3.Not(φ_cached)) # Tuple does NOT satisfy cached
    
    result = solver.check()
    
    if result == z3.unsat:
        # No such tuple exists → new ⊆ cached → CACHE HIT
        return True
    else:
        # Counterexample exists → new ⊄ cached → CACHE MISS
        return False
```

### Example

```
Cached: Age > 50
New:    Age > 60

Z3 checks: ∃ Age: (Age > 60) ∧ ¬(Age > 50)
         = ∃ Age: (Age > 60) ∧ (Age ≤ 50)

This is UNSAT (no Age can be both >60 and ≤50)
→ Age > 60 ⊆ Age > 50 → CACHE HIT
```

```
Cached: Age > 50
New:    Age > 40

Z3 checks: ∃ Age: (Age > 40) ∧ ¬(Age > 50)
         = ∃ Age: (Age > 40) ∧ (Age ≤ 50)

SAT with Age = 45 (counterexample)
→ Age > 40 ⊄ Age > 50 → CACHE MISS
```

## 4. Cache Storage (DuckDB In-Memory)

### Why DuckDB?
- Columnar storage optimized for analytical queries
- Fast SQL execution on cached data
- In-memory mode for maximum performance
- Native DataFrame support

### Storage Structure
```python
@dataclass
class CacheEntry:
    cache_id: str           # "cache_patients_a1b2c3d4..."
    table_name: str         # "patients"
    duckdb_table: str       # DuckDB table name for this entry
    parsed_query: ParsedQuery
    signature: str          # For cache key lookup
    row_count: int
    size_bytes: int
    created_at: float
    last_accessed: float    # For LRU eviction
    access_count: int
```

### Eviction Policies

1. **LRU Eviction**: When cache is full, evict least recently used entries
2. **Subsumption-Based Eviction**: When storing a broader query, evict narrower subsumed entries

```
Before: [Age > 50 (36 rows), Age > 60 (20 rows)]
Store:  Age > 40 (50 rows)
After:  [Age > 40 (50 rows)]  // Age > 50 and Age > 60 evicted (subsumed)
```

## 5. Access Control Integration

Web3DB uses smart contracts for access control. Queries are rewritten with CTE (Common Table Expression):

```sql
-- Original query
SELECT * FROM patients WHERE Age > 50

-- Rewritten with access control
WITH accessible_part AS (
    SELECT * FROM patients 
    WHERE OwnerID = '0x1A28...'    -- Access control predicate
    AND Condition = 'Diabetes'     -- Policy-defined filter
)
SELECT * FROM accessible_part WHERE Age > 50
```

### Cache Key Strategy

- **CTE predicates** (access control) → Part of base signature
- **Outer predicates** (user filter) → Part of full signature

This ensures:
1. Different users don't share cache (different CTE predicates)
2. Same user's subset queries can share cache (same CTE, different outer)

## 6. Performance Characteristics

| Operation | Latency |
|-----------|---------|
| Z3 simple predicate check | 2-3 ms |
| Z3 complex (OR) check | 8-14 ms |
| Cache hit (DuckDB query) | 5-15 ms |
| Cache miss (IPFS + decrypt) | 100-300 ms |

### Cache Hit Speedup
```
Cache hit:  10 ms
Cache miss: 200 ms
Speedup:    20x
```

## 7. Current Limitations

| Feature | Supported |
|---------|-----------|
| Simple WHERE predicates | ✅ |
| AND/OR combinations | ✅ |
| CTE (WITH clause) | ✅ |
| GROUP BY, ORDER BY | ✅ |
| JOIN | ⚠️ Parsed but not used in containment |
| Subqueries in WHERE | ❌ |
| EXISTS, HAVING | ❌ |

## 8. Key Files

| File | Purpose |
|------|---------|
| `semantic_cache.py` | Main cache class, lookup/store logic |
| `z3_containment.py` | Z3 encoder and containment checker |
| `query_parser.py` | SQL parsing to PredicateGroup |
| `app.py` | FastAPI integration |

## 9. References

1. **VeriEQL** - He et al. (2024) - Bounded Equivalence Verification for SQL
2. **Z3 Theorem Prover** - De Moura & Bjørner (2008)
3. **Semantic Caching** - Dar et al. (1996) - Semantic Data Caching and Replacement
