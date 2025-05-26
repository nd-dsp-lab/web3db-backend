import pandas as pd
import os
import time
import requests
import io
import pyarrow.parquet as pq
import duckdb  # Import DuckDB

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

def printtime(message):
    print(message)

start_time = time.time()

# Fetch data from IPFS (same as before)
cid = "Qmeomrwk8HdpRbxUVpECsiHbLHnk367yu9JERCL4nMg2S5"
ipfs_api_url = "http://129.74.152.201:5001/api/v0/cat"
response_parquet = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)

if response_parquet.status_code != 200:
    raise ValueError(f"Error: Failed to fetch data. Status code: {response_parquet.status_code}")

# Read Parquet into PyArrow Table (no Pandas conversion)
parquet_buffer = io.BytesIO(response_parquet.content)
table = pq.read_table(parquet_buffer)
printtime(f"Data loaded to Arrow Table in {time.time() - start_time:.2f} seconds")

# Read SQL query (same as before)
query_file = os.path.join(script_dir, "../query/query.sql")
with open(query_file, "r") as f:
    query = f.read()

# Use DuckDB to query Arrow Table directly
conn = duckdb.connect()
conn.register('patient_data', table)  # Register Arrow Table as a virtual table

# Execute query and get result as DataFrame (only the result is converted)
query_start = time.time()
result = conn.execute(query).fetchdf()
printtime(f"Query executed in {time.time() - query_start:.2f} seconds")

# Write output (same as before)
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
print(f"Result written to {output_file}")