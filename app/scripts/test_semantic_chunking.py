"""
End-to-end test for Semantic Chunking + IPLD DAG + Predicate Pushdown.

This test:
1. Uploads employee.csv via /upload-semantic (partitioned by age, salary_usd)
2. Inspects the DAG structure via /dag-inspect
3. Runs queries with different predicates via /query-semantic
4. Verifies predicate pushdown prunes subtrees correctly
"""

import requests
import json
import sys
import time

BASE = "http://localhost:8002"
CSV_PATH = "employee.csv"  # relative to scripts dir


def test_upload():
    """Upload employee.csv with semantic chunking."""
    print("=" * 70)
    print("STEP 1: Semantic Upload (employee.csv → sharded IPLD DAG)")
    print("=" * 70)

    with open(CSV_PATH, "rb") as f:
        resp = requests.post(
            f"{BASE}/upload-semantic/employee",
            files={"file": ("employee.csv", f, "text/csv")},
            params={
                "partition_attributes": "age,salary_usd",
                "target_shard_size_kb": 256,
                "branching_factor": 4,
            },
        )

    result = resp.json()
    print(json.dumps(result, indent=2))

    if result.get("status") != "success":
        print(f"\n✗ Upload FAILED: {result.get('error')}")
        sys.exit(1)

    root_cid = result["root_cid"]
    print(f"\n✓ Upload SUCCESS")
    print(f"  Root CID: {root_cid}")
    print(f"  Shards: {result['partition']['total_shards']}")
    print(f"  Rows: {result['partition']['total_rows']}")
    print(f"  DAG levels: {result['dag']['levels']}")
    print(f"  Total time: {result['timing']['total_ms']:.1f}ms")
    return root_cid


def test_dag_inspect(root_cid):
    """Inspect the DAG structure."""
    print("\n" + "=" * 70)
    print("STEP 2: DAG Inspection")
    print("=" * 70)

    resp = requests.post(f"{BASE}/dag-inspect", params={"root_cid": root_cid})
    result = resp.json()

    def print_tree(node, indent=0):
        prefix = "  " * indent
        if node["node_type"] == "leaf":
            ranges_str = ", ".join(f"{k}: [{v[0]:.0f}-{v[1]:.0f}]" for k, v in node["ranges"].items())
            print(f"{prefix}🍃 Leaf (shard {node.get('shard_id', '?')}): {node['row_count']} rows, "
                  f"{node.get('size_bytes', 0)/1024:.1f}KB, ranges=[{ranges_str}]")
        else:
            ranges_str = ", ".join(f"{k}: [{v[0]:.0f}-{v[1]:.0f}]" for k, v in node["ranges"].items())
            print(f"{prefix}🔷 Internal: {node['row_count']} rows, ranges=[{ranges_str}]")
            for child in node.get("children", []):
                print_tree(child, indent + 1)

    print_tree(result["dag"])


def test_query(root_cid, query, desc):
    """Run a query with predicate pushdown."""
    print(f"\n{'─' * 70}")
    print(f"QUERY: {desc}")
    print(f"  SQL: {query}")
    print(f"{'─' * 70}")

    resp = requests.post(
        f"{BASE}/query-semantic",
        json={
            "root_cid": root_cid,
            "query": query,
            "partition_attributes": ["age", "salary_usd"],
        },
    )
    result = resp.json()

    if result.get("status") != "success":
        print(f"  ✗ Query FAILED: {result.get('error')}")
        return result

    pd = result["pushdown"]
    timing = result["timing"]
    assembly = result.get("data_assembly", {})

    print(f"  ✓ Results: {result['records']} rows")
    print(f"  Pushdown:")
    print(f"    Predicates: {pd['predicates']}")
    print(f"    Shards matched: {pd['shards_matched']}")
    print(f"    Nodes pruned: {pd['nodes_pruned']}")
    print(f"    Nodes visited: {pd['nodes_visited']}")
    print(f"    Traversal: {pd['traversal_ms']}ms")
    if assembly:
        print(f"  Assembly:")
        print(f"    Rows from shards: {assembly['rows_from_shards']}")
        print(f"    Rows after query: {assembly['rows_after_query']}")
        print(f"    Fetch+decrypt: {assembly['fetch_time_ms']:.1f}ms")
    print(f"  Timing:")
    print(f"    Total: {timing['total_ms']:.1f}ms")

    # Show first 3 results as sample
    if result["results"]:
        print(f"  Sample results (first 3):")
        for r in result["results"][:3]:
            print(f"    {r}")

    return result


def main():
    print("\n🚀 Semantic Sieve: End-to-End Test")
    print("=" * 70)
    print(f"Testing against: {BASE}")
    print()

    # 1. Upload
    root_cid = test_upload()

    # 2. Inspect DAG
    test_dag_inspect(root_cid)

    # 3. Selective queries with predicate pushdown
    print("\n" + "=" * 70)
    print("STEP 3: Queries with Predicate Pushdown")
    print("=" * 70)

    # Query A: age > 50 — should prune young-age subtrees
    r1 = test_query(root_cid,
                    "SELECT * FROM employee WHERE age > 50",
                    "Age > 50 (should prune low-age shards)")

    # Query B: salary > 100000 — should prune low-salary subtrees
    r2 = test_query(root_cid,
                    "SELECT * FROM employee WHERE salary_usd > 100000",
                    "Salary > 100K (should prune low-salary shards)")

    # Query C: combined — age > 50 AND salary > 80000
    r3 = test_query(root_cid,
                    "SELECT * FROM employee WHERE age > 50 AND salary_usd > 80000",
                    "Age > 50 AND Salary > 80K (combined pushdown)")

    # Query D: very selective — age = 30
    r4 = test_query(root_cid,
                    "SELECT * FROM employee WHERE age = 25",
                    "Age = 25 (point query)")

    # Query E: full scan — no predicates on partition attributes
    r5 = test_query(root_cid,
                    "SELECT COUNT(*) as total FROM employee",
                    "COUNT(*) — no pushdown (full scan)")

    # Query F: aggregation with pushdown
    r6 = test_query(root_cid,
                    "SELECT department, AVG(salary_usd) as avg_salary FROM employee WHERE age >= 40 GROUP BY department ORDER BY avg_salary DESC",
                    "Avg salary by dept WHERE age >= 40 (pushdown + aggregation)")

    # Query G: BETWEEN predicate (new)
    r7 = test_query(root_cid,
                    "SELECT COUNT(*) as total FROM employee WHERE age BETWEEN 30 AND 40",
                    "BETWEEN 30 AND 40 (range pushdown)")

    # Query H: BETWEEN on both attributes
    r8 = test_query(root_cid,
                    "SELECT * FROM employee WHERE age BETWEEN 25 AND 35 AND salary_usd BETWEEN 50000 AND 80000",
                    "Age BETWEEN 25-35 AND Salary BETWEEN 50K-80K (dual range)")

    print("\n" + "=" * 70)
    print("✓ ALL TESTS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
