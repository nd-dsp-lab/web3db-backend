import pandas as pd
import os
import re
import time
import requests
import io
import pyarrow.parquet as pq
import duckdb
from cidindex import CIDIndex
from typing import List
from math import inf


# --- Helper functions ---

def retrieve_index(cid):
    if not cid:
        return None
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        if resp.status_code != 200:
            return None
        index = CIDIndex()
        index.load(io.BytesIO(resp.content))
        return index
    except Exception as e:
        print(f"Error retrieving index: {e}")
        return None

def query_index(index, query, attr) -> List[str]:
    where = re.search(r"where\s+(.*)", query, re.IGNORECASE)
    if not where:
        return index.query_range()
    conds = [c.strip() for c in re.split(r"\s+and\s+", where.group(1)) if attr in c]
    if not conds:
        return index.query_range()
    out = set()
    for c in conds:
        op = ">=" if ">=" in c else "<=" if "<=" in c else ">" if ">" in c else "<" if "<" in c else "!=" if "!=" in c else "="
        key = c.split(op)[1].strip().strip("'\"")
        key = int(key) if index.index_type == "bplustree" else key
        if op == "=": out.update(index.query(key))
        elif op == ">": out.update(index.query_range(key + 1, inf))
        elif op == "<": out.update(index.query_range(-inf, key - 1))
        elif op == ">=": out.update(index.query_range(key, inf))
        elif op == "<=": out.update(index.query_range(-inf, key))
        elif op == "!=": out.update(set(index.query_range()) - set(index.query(key)))
    return list(out)

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
query = "SELECT count(*) FROM read_parquet('/tmp/temp_data.parquet') WHERE PatientID = '10501'"
index_cid = "QmXpHwdeq73WD7SPZjhBVPwmGvbXDhEEEhCvGD5imXdZXK"
def printtime(message):
    print(message)

start_time = time.time()

idx_retrieve_start = time.time()
index = retrieve_index(index_cid)
idx_retrieve_end = time.time()
if not index:
    printtime("Index retrieval failed, exiting.")
    exit(1)
printtime(f"Index retrieved in {idx_retrieve_end - idx_retrieve_start:.6f} seconds")
idx_query_time_start = time.time()
cids = query_index(index, query, "PatientID")
idx_query_time_end = time.time()
printtime(f"Index query took {idx_query_time_end - idx_query_time_start:.6f} seconds")
print(len(cids), "CIDs found for query")
# Fetch data from IPFS
cid = cids[0] if cids else None
ipfs_api_url = "http://localhost:5001/api/v0/cat"
printtime(f"Fetching data from IPFS CID: {cid}")

ipfs_time_start = time.time()
response_parquet = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)
printtime(f"Data fetched from IPFS in {time.time() - ipfs_time_start:.6f} seconds")

if response_parquet.status_code != 200:
    raise ValueError(f"Error: Failed to fetch data. Status code: {response_parquet.status_code}")

# Write to temp file for better performance
temp_parquet = "/tmp/temp_data.parquet"
write_time_start = time.time()
with open(temp_parquet, 'wb') as f:
    f.write(response_parquet.content)
printtime(f"Data written to temp file in {time.time() - write_time_start:.6f} seconds")

# Create DuckDB connection with single-threaded mode
duckdb_time_start = time.time()
conn = duckdb.connect(':memory:', config={'threads': 1})
printtime(f"DuckDB connection established in {time.time() - duckdb_time_start:.6f} seconds")

# Execute query directly on parquet file
query_start = time.time()
result = conn.execute(query).fetchdf()
printtime(f"Query executed in {time.time() - query_start:.6f} seconds")
printtime(f"Total execution time: {time.time() - start_time:.6f} seconds")

# Write output
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
print(f"Result written to {output_file}")
print(f"Result shape: {result.shape}")

# Cleanup
os.remove(temp_parquet)
conn.close()
