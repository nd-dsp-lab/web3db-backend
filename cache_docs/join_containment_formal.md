# JOIN Query Containment - Formal Reference

Technical reference for Z3-based JOIN containment checking.

---

## Formal Definition

**Definition (JOIN Query)**: A JOIN query $Q$ is a tuple $(T, J, P)$ where:
- $T = \{t_1, ..., t_n\}$ is a set of tables
- $J = \{j_1, ..., j_m\}$ is a set of equi-join conditions $t_i.a = t_j.b$
- $P = \{p_{t_1}, ..., p_{t_n}\}$ is a set of per-table predicates

**Definition (JOIN Containment)**: $Q_{new} \sqsubseteq Q_{cached}$ iff:
$$\forall \text{tuple } r: r \in \text{result}(Q_{new}) \implies r \in \text{result}(Q_{cached})$$

---

## Algorithm

```
Algorithm: CheckJoinContainment
Input: Q_cached = (T_c, J_c, P_c), Q_new = (T_n, J_n, P_n)
Output: (is_contained, filter)

1. TABLE MATCH
   if T_c ≠ T_n: return (False, ∅)

2. JOIN STRUCTURE EQUIVALENCE  
   J_c' = normalize(J_c)  // Sort pairs: (min(t.a, t'.b), max(t.a, t'.b))
   J_n' = normalize(J_n)
   if J_c' ≠ J_n': return (False, ∅)

3. JOIN TYPE COMPATIBILITY
   for each (j_c, j_n) in zip(J_c, J_n):
     if type(j_c) ≠ type(j_n): return (False, ∅)

4. PER-TABLE CONTAINMENT (Z3)
   F = ∅
   for each table t ∈ T:
     φ = ∃x: P_n[t](x) ∧ ¬P_c[t](x)
     if SAT(φ): return (False, ∅)
     F = F ∪ {P_n[t]}
   
   return (True, ⋀F)
```

---

## Separable Containment

We check containment **per-table independently**:

$$Q_{new} \sqsubseteq_{sep} Q_{cached} \iff \bigwedge_{t \in T} P_{new}[t] \subseteq P_{cached}[t]$$

**Theorem (Soundness)**: $Q_{new} \sqsubseteq_{sep} Q_{cached} \implies Q_{new} \sqsubseteq Q_{cached}$

**Non-Completeness**: The converse does not hold. Cross-table predicate propagation through join conditions may enable containment that separable checking misses.

---

## Visual Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     Input Queries                           │
├────────────────────────┬────────────────────────────────────┤
│     Q_cached           │           Q_new                    │
│  SELECT * FROM A ⋈ B   │    SELECT * FROM A ⋈ B             │
│  ON A.id = B.id        │    ON A.id = B.id                  │
│  WHERE A.x > 40        │    WHERE A.x > 50                  │
└────────────┬───────────┴───────────────┬────────────────────┘
             │                           │
             ▼                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 1: Table Match                                        │
│  {A, B} = {A, B}  →  PASS                                   │
└─────────────────────────────┬───────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 2: Join Structure Equivalence                         │
│  normalize(A.id = B.id) = normalize(A.id = B.id)  →  PASS   │
└─────────────────────────────┬───────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 3: Join Type Compatibility                            │
│  INNER = INNER  →  PASS                                     │
└─────────────────────────────┬───────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 4: Per-Table Z3 Containment                           │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │ Table A         │    │ Table B         │                 │
│  │ cached: x > 40  │    │ cached: (none)  │                 │
│  │ new:    x > 50  │    │ new:    (none)  │                 │
│  │                 │    │                 │                 │
│  │ Z3: x>50 ⊆ x>40 │    │ Z3: true ⊆ true │                 │
│  │ Result: UNSAT   │    │ Result: UNSAT   │                 │
│  │ → CONTAINED     │    │ → CONTAINED     │                 │
│  └─────────────────┘    └─────────────────┘                 │
└─────────────────────────────┬───────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Output: CACHE HIT                                          │
│  Filter: WHERE A.x > 50                                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Implementation Details

| Component | Method | Location |
|-----------|--------|----------|
| Normalize join keys | `JoinCondition.get_normalized_key()` | query_parser.py |
| Check equivalence | `Z3JoinContainmentChecker.check_join_structure_equivalence()` | z3_containment.py |
| Per-table grouping | `SemanticCache._group_predicates_by_table()` | semantic_cache.py |
| Z3 containment | `Z3ContainmentChecker.is_contained()` | z3_containment.py |
