# Z3-Based Semantic Query Containment for Database Caching


This document describes the implementation of an SMT-based query containment checker for semantic caching in Web3DB. The system uses Z3 theorem prover to verify if a new query's result set is contained within a cached query's result set, enabling provably correct cache hit detection.

## 1. Introduction

### Problem Statement

Traditional semantic caches use heuristic-based predicate comparison to detect cache hits. These heuristics handle simple cases (range comparisons, equality) but fail on complex predicates involving:
- OR combinations
- Nested boolean expressions
- Mixed operators across attributes

### Solution

We replace heuristic checks with **SMT-based containment verification** using the Z3 theorem prover. This provides:
1. **Provable correctness** - No false positives in cache hits
2. **Complete coverage** - Handles arbitrary boolean predicate combinations
3. **Formal guarantees** - Based on satisfiability modulo theories (SMT)

## 2. Related Work

This approach is inspired by **VeriEQL** [1], which uses SMT solving for SQL query equivalence verification. We adapt their encoding strategy for the containment problem specific to caching.

```
[1] He, Y., Zhao, P., Wang, X., & Wang, Y. (2024). VeriEQL: Bounded Equivalence 
    Verification for Complex SQL Queries with Integrity Constraints. arXiv:2403.03193
```

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Semantic Cache                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │ Query Parser │───▶│ Z3 Query Encoder │───▶│ Z3 Solver    │  │
│  └──────────────┘    └──────────────────┘    └──────────────┘  │
│         │                    │                      │           │
│         ▼                    ▼                      ▼           │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │ PredicateGroup│   │ Z3 BoolRef       │    │ SAT/UNSAT    │  │
│  └──────────────┘    └──────────────────┘    └──────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    DuckDB In-Memory Storage               │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## 4. Containment Checking Algorithm

### 4.1 Problem Formulation

Given:
- **Cached query predicates**: $P_{cached}$
- **New query predicates**: $P_{new}$

**Goal**: Determine if $Results(P_{new}) \subseteq Results(P_{cached})$

### 4.2 SMT Encoding

We formulate containment as a satisfiability problem:

$$\text{Containment holds} \iff \neg \exists \text{ tuple } t : P_{new}(t) \land \neg P_{cached}(t)$$

**Algorithm**:
1. Encode $P_{new}$ as Z3 constraint $\phi_{new}$
2. Encode $P_{cached}$ as Z3 constraint $\phi_{cached}$
3. Check satisfiability of: $\phi_{new} \land \neg\phi_{cached}$
4. If **UNSAT**: Containment holds → Cache HIT
5. If **SAT**: Counterexample exists → Cache MISS

### 4.3 Predicate Encoding

| SQL Predicate | Z3 Encoding |
|---------------|-------------|
| `attr > val` | `z3.Int(attr) > val` |
| `attr >= val` | `z3.Int(attr) >= val` |
| `attr < val` | `z3.Int(attr) < val` |
| `attr <= val` | `z3.Int(attr) <= val` |
| `attr = val` | `z3.Int(attr) == val` (numeric) or `z3.String(attr) == StringVal(val)` (string) |
| `attr IN (v1,v2,...)` | `z3.Or(attr == v1, attr == v2, ...)` |
| `attr BETWEEN a AND b` | `z3.And(attr >= a, attr <= b)` |
| `P1 AND P2` | `z3.And(P1, P2)` |
| `P1 OR P2` | `z3.Or(P1, P2)` |

### 4.4 Type Inference

Variables are assigned Z3 sorts based on value types:
```python
if isinstance(value, int):   return z3.Int(name)
if isinstance(value, float): return z3.Real(name)  
if isinstance(value, str):   return z3.String(name)
```

## 5. Implementation Details

### 5.1 Core Classes

**Z3QueryEncoder**: Converts SQL predicates to Z3 constraints
```python
class Z3QueryEncoder:
    def encode_predicate(pred: Predicate) -> z3.BoolRef
    def encode_predicate_group(group: PredicateGroup) -> z3.BoolRef
```

**Z3ContainmentChecker**: Performs containment verification
```python
class Z3ContainmentChecker:
    def is_contained(cached: PredicateGroup, new: PredicateGroup) 
        -> Tuple[bool, Optional[str]]
```

### 5.2 Integration with Cache

```python
def _find_superset_entry(self, parsed, table_name):
    for entry in self._cache[table_name]:
        is_contained, filter = self._z3_checker.is_contained(
            entry.parsed_query.outer_predicates,
            parsed.outer_predicates
        )
        if is_contained:
            return entry, filter  # Cache HIT
    return None  # Cache MISS
```

## 7. Limitations

1. **Latency**: Z3 checks are ~70x slower than heuristic checks
2. **String patterns**: LIKE patterns have limited support
3. **Aggregations**: Not supported in containment checks
4. **Bounded verification**: Checks validity for symbolic tuples, not all possible data

## 8. Usage

### Installation
```bash
pip install z3-solver
```

### Running Tests
```bash
cd app/scripts
python test_z3_containment.py
```

### Metrics
The cache exposes Z3 performance metrics:
```python
stats = cache.get_stats()
print(stats['metrics']['z3_checks'])        # Number of Z3 checks
print(stats['metrics']['z3_avg_latency_ms']) # Average latency
```

## 9. Source Files

| File | Description | Lines |
|------|-------------|-------|
| `z3_containment.py` | Core Z3 encoder and checker | ~350 |
| `semantic_cache.py` | Cache with Z3 integration | ~670 |
| `query_parser.py` | SQL predicate parser | ~900 |
| `test_z3_containment.py` | Unit tests | ~280 |

## 10. Conclusion

This implementation demonstrates that SMT-based query containment is practical for semantic caching. The additional overhead is acceptable when cache hits avoid expensive operations (e.g., IPFS fetches). The provable correctness eliminates false-positive cache hits that could return incorrect results.

## References

```
[1] He, Y., Zhao, P., Wang, X., & Wang, Y. (2024). VeriEQL: Bounded Equivalence 
    Verification for Complex SQL Queries with Integrity Constraints. 
    arXiv:2403.03193

[2] De Moura, L., & Bjørner, N. (2008). Z3: An efficient SMT solver. 
    TACAS 2008.
```
