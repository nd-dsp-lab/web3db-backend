# Semantic Sieve: Utility-Aware Cache Eviction for Semantic Caches

**Target Venue**: NSDI 2027  
**Building on**: SIEVE (NSDI '24 Community Award)

---

## Abstract

We present **Semantic Sieve**, a cache eviction system designed for **Intel SGX** enclaves that extends the **SIEVE** algorithm. In decentralized databases like **MtDB**, high network latency for data fetching from IPFS makes cache efficiency paramount. Semantic Sieve leverages a **two-bit metadata model**—Visited (V) and Utility (U)—and **Z3-based containment checking** to preserve broad, high-value queries. This design maintains SIEVE's **lockless hit path** and O(1) complexity while significantly improving hit rates for analytical workloads.

---

## 1. Introduction

Traditional cache eviction policies (LRU, LFU, SIEVE) treat all entries as independent and equal. In **semantic caching** with containment checking, some queries have higher **utility** — their cached results can serve many future queries via subset matching.

| Cached Query | Utility | Why |
|--------------|---------|-----|
| `SELECT * WHERE x > 10` | High | Broad, full info, reusable |
| `SELECT * WHERE x = 5` | Low | Narrow, limited reuse |
| `SELECT COUNT(*) WHERE x > 10` | Low | Broad but lossy, can't serve `SELECT *` |

By extending SIEVE's single-bit model to a **two-bit utility-aware model**, we address the core performance challenges of **MtDB**: high latency of distributed storage (IPFS) and memory constraints of **Intel SGX**.

---

## 2. System Design: The Utility-Aware Primitive

### 2.1 The Utility Score Function

Unlike traditional eviction which treats all objects as independent, Semantic Sieve assigns a **Utility Score** at insertion to quantify an entry's potential to serve future queries via containment.

$$\text{Utility}(Q) = \frac{\text{Cost}(Q)}{\text{Size}(Q)} \times \text{Retain}(Q) \times \text{Breadth}(Q)$$

| Component | Description | Computation |
|-----------|-------------|-------------|
| **Cost** | Query execution time (ms) | Measured at runtime |
| **Size** | Memory footprint (bytes) | `sizeof(DataFrame)` |
| **Retain** | Information preservation | Based on SELECT columns |
| **Breadth** | Containment probability | Based on predicate types |

- **Cost-to-Size Ratio**: Prioritizes expensive IPFS-fetched queries that occupy minimal enclave memory.
- **Retain Score**: Measures information preservation; `SELECT *` receives a high score (1.0), while lossy aggregations like `COUNT` receive low scores (0.05).
- **Breadth Score**: Quantifies containment potential based on predicates; open ranges (e.g., `x > 10`) are favored over point queries (e.g., `x = 5`).

### 2.2 Retain Score

| Pattern | Score | Rationale |
|---------|-------|-----------|
| `SELECT *` | 1.0 | Full information |
| `SELECT col1, col2, ...` | 0.6-0.9 | Partial projection |
| `COUNT/SUM/AVG` | 0.05 | Lossy aggregation |

### 2.3 Breadth Score

| Predicate | Score | Rationale |
|-----------|-------|-----------|
| No WHERE | 1.0 | Universal superset |
| `x > n` (open range) | 0.9 | High containment |
| `x BETWEEN a AND b` | 0.5 | Limited range |
| `x IN (...)` | 0.3 | Specific values |
| `x = n` (equality) | 0.1 | Point query |

### 2.4 Two-Bit Metadata Logic

We extend SIEVE's 1-bit metadata to 2 bits to separate temporal recency from semantic value.

| Bit | Name | Logic |
|-----|------|-------|
| **V** | Visited | Set to **1** on an exact match or a successful containment hit |
| **U** | Utility | Set to **1** at insertion if `Utility(Q) ≥ threshold` |

---

## 3. Subsumption-Aware Eviction

Semantic Sieve maintains SIEVE's **FIFO queue** and **hand pointer** moving from tail to head.

### Eviction Rules

- **CASE 1 (V=0, U=0)**: No recent hits AND low utility → **evict immediately**.

- **CASE 2 (V=0, U=1)**: High utility but no recent hits → perform **lazy redundancy check**. If subsumed by another entry, evict. Otherwise, **decay U to 0** (downgrade protection for next pass).

- **CASE 3 (V=1)**: Recent hit → reset V to 0 and skip. The entry survives this pass. U remains unchanged (utility is structural, not based on access).

### Liveness Guarantee

A high-utility entry (U=1) survives **at most 2 cold passes**:

```
Pass 1: V=0, U=1 → CASE 2 → demote to U=0 (if not subsumed)
Pass 2: V=0, U=0 → CASE 1 → evicted
```

If accessed between passes (V=1), CASE 3 triggers and entry survives.

### Algorithm

```
EVICT():
  while need_to_evict:
    entry = queue[hand]
    
    # CASE 1: No recent hits AND low utility
    if entry.V == 0 and entry.U == 0:
      evict(entry)
      return
      
    # CASE 2: High utility but no recent hits
    elif entry.V == 0 and entry.U == 1:
      if is_subsumed_by_another(entry):
        evict(entry)  # Remove redundant data
        return
      else:
        # DECAY: Downgrade protection for the next pass
        entry.U = 0
      
    # CASE 3: Recent hit (V=1)
    else:
      entry.V = 0  # Reset visited bit (emulate lazy promotion)
      # Note: U bit remains unchanged (utility is structural)
    
    hand = (hand + 1) % len(queue)  # Advance hand toward head
```

---

## 4. Implementation & Evaluation Plan

### 4.1 SGX and TEE Constraints

- **Enclave Memory**: The algorithm must operate within the limited memory of **Intel SGX**, making SIEVE's low metadata overhead (17 bytes per object) ideal.
- **Seal/Unseal Performance**: The two-layer cache uses SSD as a secondary tier, requiring efficient data movement for analytical workloads.

### 4.2 Workload and Metrics

| Aspect | Details |
|--------|---------|
| **Datasets** | TPC-H, TPC-DS |
| **Baselines** | LRU, LFU, ARC, SIEVE |
| **Throughput** | Target: 2× optimized LRU |
| **Efficiency** | Reduce object and byte miss ratios |

### 4.3 Ablations

| Variant | Description |
|---------|-------------|
| S-Sieve-NoUtil | SIEVE + subsumption, no utility bit |
| S-Sieve-NoSub | SIEVE + utility bit, no subsumption |
| S-Sieve-Full | Both mechanisms |

---

## 5. Novelty

1. **Semantic SIEVE**: The first application of the SIEVE eviction primitive to **semantic caching**.

2. **Post-Hoc Pruning**: Moving containment-based redundancy checks to the **eviction path** rather than the hit path to preserve O(1) hit latency.

3. **Utility Decay**: High-utility entries are protected but not forever — decay mechanism ensures liveness while prioritizing valuable entries.

---

## 6. Comparison with SIEVE

| Aspect | SIEVE | Semantic Sieve |
|--------|-------|----------------|
| Metadata | 1 bit (V) | 2 bits (V + U) |
| Hit cost | O(1), no lock | O(1), no lock ✓ |
| Eviction | First V=0 | First V=0 ∧ U=0, with subsumption |
| Awareness | Recency only | Utility + Containment |
| Target | Web caches | Semantic caches in TEEs |

---

## 7. Paper Outline

1. **Introduction**: MtDB, IPFS latency, SGX constraints, SIEVE limitation
2. **Background**: SIEVE, semantic caching, containment checking
3. **Semantic Sieve**: Utility score, two-bit eviction, subsumption
4. **Implementation**: DuckDB + Z3, SGX integration
5. **Evaluation**: TPC-H/TPC-DS results
6. **Discussion**: Overhead, SGX memory, when does it help
7. **Related Work**: Cache eviction, semantic caching, TEE systems
8. **Conclusion**
