# Containment-Aware Admission (CAA) Policy

A novel cache admission policy for semantic caching that prioritizes queries with high **containment potential**.

---

## Core Insight

Traditional admission policies (LRU, LFU) treat all queries equally. In semantic caching with containment checking, **some queries are more valuable** because their cached results can serve future queries via subset matching.

| Query Type | Containment Potential | Value |
|------------|----------------------|-------|
| `SELECT * FROM T` | Very High | Can serve any future query on T |
| `SELECT * WHERE x > 10` | High | Can serve `x > 20`, `x > 50`, etc. |
| `SELECT * WHERE x = 5` | Low | Only exact match |
| `SELECT COUNT(*)` | Near Zero | Cannot answer `SELECT *` |

---

## The Algorithm

$$\text{Score}(Q) = \frac{\text{Cost}(Q)}{\text{Size}(Q)} \times \text{Retain}(Q) \times \text{Breadth}(Q)$$

### Components

| Component | Description | Computation |
|-----------|-------------|-------------|
| **Cost** | Query execution time (ms) | Measured at runtime |
| **Size** | Memory footprint (bytes) | `sizeof(DataFrame)` |
| **Retain** | Information preservation | Based on SELECT columns |
| **Breadth** | Containment probability | Based on predicate types |

### Retain Score (Information Preservation)

| Pattern | Score | Rationale |
|---------|-------|-----------|
| `SELECT *` | 1.0 | Full information |
| `SELECT col1, col2` | 0.6-0.9 | Partial projection |
| `COUNT/SUM/AVG` | 0.05 | Lossy aggregation |

### Breadth Score (Predicate Analysis)

| Predicate | Score | Rationale |
|-----------|-------|-----------|
| No WHERE | 1.0 | Universal superset |
| `x > 10` (open range) | 0.9 | High containment |
| `x BETWEEN a AND b` | 0.5 | Limited range |
| `x IN (...)` | 0.3 | Specific values |
| `x = 5` (equality) | 0.1 | Point query |

---

## Implementation

```python
# admission_policy.py
class CAAScorer:
    def compute_score(self, parsed: ParsedQuery, cost_ms: float, size_bytes: int) -> float
    def compute_retain_score(self, parsed: ParsedQuery) -> float
    def compute_breadth_score(self, parsed: ParsedQuery) -> float
    def should_admit(self, score: float) -> bool
```

Integration in `semantic_cache.py`:
```python
def store(self, query, table, df, cost_ms=0.0):
    score = self._caa_scorer.compute_score(parsed, cost_ms, size_bytes)
    if not self._caa_scorer.should_admit(score):
        return None  # Reject
    # ... store in cache
```

---

## Formal Properties

### Theorem (Monotonicity)
For queries $Q_1$, $Q_2$ where $Q_1 \supseteq Q_2$ (Q1 contains Q2):
$$\text{Breadth}(Q_1) \geq \text{Breadth}(Q_2)$$

*Proof*: Broader predicates have higher Breadth scores by construction.

### Corollary (Admission Preference)
CAA preferentially admits broader queries, maximizing the probability of future subset hits.

---

> *"We introduce **Containment-Aware Admission (CAA)**, a novel cache admission policy that quantifies a query's containment potential—the probability that its cached result can serve future queries via subset matching. Unlike frequency-based policies, CAA prioritizes caching queries with broader predicates (e.g., `x > 10` over `x = 5`), directly optimizing for semantic cache hit rates."*

---

## Metrics

| Metric | Description |
|--------|-------------|
| `caa_admissions` | Queries admitted by CAA |
| `caa_rejections` | Queries rejected by CAA |
| `caa_scores_sum` | Sum of all CAA scores (for avg calculation) |

---

## Files

| File | Description |
|------|-------------|
| admission_policy.py| CAA implementation |
| semantic_cache.py | Integration point |
