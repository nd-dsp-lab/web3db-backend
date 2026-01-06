# Z3-Based JOIN Query Containment for MtDB Semantic Caching

SMT-based semantic containment checking for JOIN queries in MtDB semantic cache, enabling cache hits when a new JOIN query's results are a subset of cached JOIN query results.

---

## Algorithm: JOIN Containment Checking

```
Algorithm: CheckJoinContainment(cached_query, new_query)
Input: Cached query Q_c, New query Q_n
Output: (is_contained, additional_filter)

1. TABLE MATCHING
   if tables(Q_c) ≠ tables(Q_n) then
       return (False, ∅)

2. JOIN STRUCTURE EQUIVALENCE
   for each join_condition in Q_n:
       if ¬∃ equivalent in Q_c then
           return (False, ∅)
   // Note: Uses normalized keys for A.x=B.y ≡ B.y=A.x

3. JOIN TYPE COMPATIBILITY
   for each (cached_type, new_type) pair:
       if (cached_type, new_type) ∉ COMPATIBLE_PAIRS then
           return (False, ∅)
   // COMPATIBLE_PAIRS = {(INNER,INNER), (LEFT,LEFT), (RIGHT,RIGHT)}

4. PER-TABLE PREDICATE CONTAINMENT (Z3)
   filters ← ∅
   for each table T in tables(Q_n):
       P_c ← predicates of Q_c on T
       P_n ← predicates of Q_n on T
       
       // SMT check: ∃ tuple: P_n(tuple) ∧ ¬P_c(tuple)
       if SAT then return (False, ∅)
       else filters ← filters ∪ {P_n}
   
   return (True, AND(filters))
```

---

## Formal Guarantees

### Theorem 1 (Soundness)
If `CheckJoinContainment` returns `(True, F)`, then:
```
∀ tuple t: t ∈ result(Q_n) → t ∈ result(Q_c) ∧ F(t)
```
*Proof sketch*: Join structure equivalence ensures same join semantics. Per-table Z3 containment ensures predicates are satisfied.

### Theorem 2 (JOIN Type Safety)
LEFT JOIN results cannot serve INNER JOIN queries (and vice versa) due to NULL row differences.

---

## Complexity Analysis

| Operation | Complexity |
|-----------|------------|
| Table matching | O(n) where n = number of tables |
| Join structure equivalence | O(j) where j = number of joins |
| Z3 predicate containment | O(2^p) worst case for p predicates (SMT solving) |

---

## Implementation Files

| File | Component |
|------|-----------|
| `z3_containment.py` | `Z3JoinContainmentChecker` class |
| `query_parser.py` | `JoinCondition.get_normalized_key()` |
| `semantic_cache.py` | Integration in `_find_superset_entry()` |
| `test_z3_join_containment.py` | 24 unit tests |

---

## Supported SQL Fragments

| Feature | Supported | Notes |
|---------|-----------|-------|
| INNER JOIN | ✓ | Full subset matching |
| LEFT JOIN | ✓ | Same-type only |
| RIGHT JOIN | ✓ | Same-type only |
| Multi-way JOINs | ✓ | All conditions checked |
| Equi-joins | ✓ | A.x = B.y |
| Non-equi joins | ✗ | Future work |
| Cross-type conversion | ✗ | Blocked for correctness |

---

## Key Design Decisions

1. **Separate ON from WHERE**: Join conditions require equivalence, WHERE uses subset semantics
2. **Per-table containment**: Predicates checked independently for each table
3. **Conservative LEFT JOIN**: Cross-type blocked to preserve NULL semantics
4. **Normalized join keys**: Handles `A.x=B.y ≡ B.y=A.x`
5. **Table-qualified Z3 variables**: `users.id` → `z3.Int('users_id')` to avoid collision

---

## Semantic Weakness: Separable Containment

Our implementation enforces **Separable Containment**, checking predicates per-table independently:

```
P_A ⊆ Q_A  AND  P_B ⊆ Q_B
```

This is a **sound approximation** of full joint containment, but **incomplete**:

### Missed Opportunity Example

```sql
-- Cached: SELECT * FROM A JOIN B ON A.id = B.id WHERE A.val > 10
-- New:    SELECT * FROM A JOIN B ON A.id = B.id WHERE B.val > 10
```

| Checker | Result | Reason |
|---------|--------|--------|
| **Separable (current)** | MISS | `A.val > 10` vs `∅` on A fails |
| **Full joint** | HIT | `A.id = B.id` correlates A.val and B.val |


> *"Our Z3JoinContainmentChecker enforces **Separable Containment** (checking P_A ⊆ Q_A and P_B ⊆ Q_B independently), which is a sound approximation of full joint containment. This design trades completeness for predictable O(n) Z3 checks per table, avoiding the complexity of cross-table predicate propagation through join conditions."*

### Future Work (Full Joint Containment)

To achieve full joint containment would require:
1. Join condition propagation: If `A.id = B.id`, propagate predicates across the join
2. Cross-table Z3 encoding: Model correlated tuples in the SMT formula
3. Complexity tradeoff: May increase Z3 solving time significantly

---

## Key Points

- **Point 1**: Cache Lookup Algorithm (use algorithm box above)
- **Point 2**: Formal Correctness (theorems + proof sketches)
- **Point 3**: Experimental Evaluation (test coverage, latency)
- **Point 4**: Limitations (non-equi joins, cross-type conversion, separable containment)
