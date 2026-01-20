# CTE Parsing - Formal Reference

Technical reference for multi-CTE parsing in semantic cache.

---

## Formal Definition

**Definition (CTE Query)**: A CTE query $Q$ is a tuple $(C, Q_{outer})$ where:
- $C = \{(c_1, B_1), ..., (c_n, B_n)\}$ is a set of CTE definitions (name, body)
- $Q_{outer}$ is the outer SELECT query referencing CTEs

**Definition (CTE Body)**: Each body $B_i$ is itself a query with predicates $P_{B_i}$

---

## Algorithm

```
Algorithm: ParseMultipleCTEs
Input: Query string Q
Output: (has_cte, cte_names[], cte_bodies{}, outer_query)

1. DETECT CTE
   if not Q.upper().startswith("WITH"):
     return (False, [], {}, Q)

2. INITIALIZE
   remaining = Q[4:]  // Remove "WITH"
   cte_names = []
   cte_bodies = {}

3. PARSE CTE DEFINITIONS
   while True:
     // Match: name AS (
     match = regex("(\w+)\s+AS\s*\(", remaining)
     if not match: break
     
     name = match.group(1)
     cte_names.append(name)
     
     // Balanced parenthesis matching
     depth = 1
     pos = match.end()
     while depth > 0 and pos < len(remaining):
       if remaining[pos] == '(': depth++
       if remaining[pos] == ')': depth--
       pos++
     
     body = remaining[match.end() : pos-1]
     cte_bodies[name] = body
     
     remaining = remaining[pos:].strip()
     
     // Check for more CTEs or end
     if remaining.startswith(','):
       remaining = remaining[1:].strip()
     elif remaining.upper().startswith("SELECT"):
       break

4. OUTPUT
   outer_query = remaining
   return (len(cte_names) > 0, cte_names, cte_bodies, outer_query)
```

---

## Predicate Combination

```
Algorithm: CombineCTEPredicates
Input: cte_bodies{}, outer_query
Output: combined_predicates

1. all_predicates = []

2. For each (name, body) in cte_bodies:
     P = ExtractWHERE(body)
     if P is not empty:
       all_predicates.append(P)

3. P_outer = ExtractWHERE(outer_query)
   if P_outer is not empty:
     all_predicates.append(P_outer)

4. return AND(all_predicates)
```

---

## Physical Table Extraction

```
Algorithm: ExtractPhysicalTables
Input: cte_names[], cte_bodies{}, outer_query
Output: physical_tables[]

1. // Get tables from outer query
   tables = ParseFROM(outer_query)

2. // Get tables from all CTE bodies
   for body in cte_bodies.values():
     tables.extend(ParseFROM(body))

3. // Filter out CTE names (they are transient, not physical)
   physical_tables = [t for t in tables if t not in cte_names]

4. return unique(physical_tables)
```

---

## Visual Flow

```
INPUT:
┌─────────────────────────────────────────────────────────────┐
│ WITH cte1 AS (SELECT * FROM A WHERE x > 10),                │
│      cte2 AS (SELECT * FROM B WHERE y > 20)                 │
│ SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id           │
│ WHERE z > 30                                                │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
STEP 1: Detect "WITH" keyword
                              │
                              ▼
STEP 2: Parse CTE Definitions (Balanced Parenthesis)
┌────────────────────┐       ┌────────────────────┐
│ cte1               │       │ cte2               │
│ ────────────────── │       │ ────────────────── │
│ SELECT * FROM A    │       │ SELECT * FROM B    │
│ WHERE x > 10       │       │ WHERE y > 20       │
└────────────────────┘       └────────────────────┘
                              │
                              ▼
STEP 3: Extract Physical Tables
┌─────────────────────────────────────────────────────────────┐
│ Outer query tables: [cte1, cte2]                            │
│ CTE body tables:    [A, B]                                  │
│ Filter CTE names:   remove [cte1, cte2]                     │
│ Result:             physical_tables = [A, B]                │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
STEP 4: Combine Predicates
┌─────────────────────────────────────────────────────────────┐
│ From cte1: x > 10                                           │
│ From cte2: y > 20                                           │
│ From outer: z > 30                                          │
│ Combined: (x > 10) AND (y > 20) AND (z > 30)                │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
OUTPUT:
┌─────────────────────────────────────────────────────────────┐
│ ParsedQuery:                                                │
│   cte_names = ['cte1', 'cte2']                              │
│   cte_bodies = {'cte1': 'SELECT...', 'cte2': 'SELECT...'}   │
│   tables = ['A', 'B']                                       │
│   predicates = (x > 10) AND (y > 20) AND (z > 30)           │
└─────────────────────────────────────────────────────────────┘
```

---

## Signature Generation

To prevent cache collisions, CTE bodies are included in the signature:

$$\text{signature} = \text{hash}(\text{tables}, \text{predicates}, \text{joins}, \text{cte\_bodies})$$

This ensures:
```sql
WITH t AS (SELECT sum(x) FROM A) ...  -- Signature S1
WITH t AS (SELECT avg(x) FROM A) ...  -- Signature S2 ≠ S1
```

---

## Files

| File | Method |
|------|--------|
| `query_parser.py` | `_parse_ctes()`, `_parse_where()` |
| `query_parser.py` | `generate_base_signature()` (includes cte_bodies) |
