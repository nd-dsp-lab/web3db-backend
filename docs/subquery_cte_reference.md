# Subquery and CTE Support
---

## Current Support Status

| Feature | Supported | Notes |
|---------|-----------|-------|
| **Single CTE** | Yes | `WITH t AS (...) SELECT * FROM t` |
| **Multiple CTEs** | Yes | `WITH t1 AS (...), t2 AS (...) SELECT ...` |
| **Scalar subquery** | No | `SELECT * FROM A WHERE x = (SELECT MAX(y) FROM B)` |
| **IN subquery** | No | `SELECT * FROM A WHERE id IN (SELECT id FROM B)` |
| **EXISTS subquery** | No | `SELECT * FROM A WHERE EXISTS (SELECT 1 FROM B)` |
| **FROM subquery** | No | `SELECT * FROM (SELECT ... FROM A) AS sub` |

---

## Multi-CTE Implementation

### Algorithm

```
Input: WITH cte1 AS (body1), cte2 AS (body2) SELECT ...

1. Detect WITH keyword
2. Parse CTE definitions using balanced parenthesis matching
3. Extract outer query (after final CTE)
4. Combine predicates from ALL CTE bodies + outer query
```

### Key Methods

| Method | Location | Purpose |
|--------|----------|---------|
| `_parse_ctes()` | `query_parser.py` | Multi-CTE parsing with balanced parens |
| `_parse_where()` | `query_parser.py` | Combines predicates from all CTEs |

### Data Structures

```python
@dataclass
class ParsedQuery:
    cte_names: List[str]           # ['cte1', 'cte2']
    cte_bodies: Dict[str, str]     # {'cte1': 'SELECT ...', 'cte2': '...'}
```

---

## Design Decisions

### 1. CTEs as Subquery Alternative

Many subqueries can be rewritten as CTEs:

```sql
-- Subquery (NOT supported)
SELECT * FROM A WHERE id IN (SELECT id FROM B WHERE x > 10)

-- CTE equivalent (SUPPORTED)
WITH sub AS (SELECT id FROM B WHERE x > 10)
SELECT * FROM A WHERE id IN sub
```

### 2. Transient Table Filtering

CTE names are filtered from `ParsedQuery.tables` to ensure only physical tables are tracked for cache invalidation:

```
WITH regional_sales AS (SELECT * FROM orders ...)
→ tables = ['orders']  (not 'regional_sales')
```

### 3. Signature Collision Prevention

`cte_bodies` included in cache signature hash to prevent collisions:

```python
sig_dict = {
    ...,
    "cte_bodies": self.cte_bodies  # Raw CTE text
}
```

---

## Justification for No Subquery Support

> *"Subquery containment is fundamentally different from predicate/JOIN containment. The general case is undecidable, and supporting even restricted fragments would require substantial formal analysis. Our semantic cache supports Common Table Expressions (CTEs), which provide equivalent expressiveness for many subquery patterns while maintaining tractable containment checking."*

---

## Benchmark Compatibility

| Benchmark | Subquery-Free Queries | Compatible |
|-----------|----------------------|------------|
| TPC-H | 16/22 | Yes |
| TPC-DS | ~30/99 | Yes |
| Custom workload | Designed without | Yes |

---

## Files

| File | Description |
|------|-------------|
| `query_parser.py` | CTE parsing (`_parse_ctes`, `_parse_where`) |
| `semantic_cache.py` | Cache integration |
