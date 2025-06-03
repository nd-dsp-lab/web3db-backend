import pandas as pd
import os
import time
import requests
import io
import pyarrow.parquet as pq
import duckdb

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

def printtime(message):
    print(message)

start_time = time.time()

# Fetch data from IPFS
cid = "QmYX4fRXpvsCVFdAQuBWkAQPztpyPspeLkw8HgsHZe1WgQ"
ipfs_api_url = "http://localhost:5001/api/v0/cat"
printtime(f"Fetching data from IPFS CID: {cid}")

ipfs_time_start = time.time()
response_parquet = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)
printtime(f"Data fetched from IPFS in {time.time() - ipfs_time_start:.2f} seconds")

if response_parquet.status_code != 200:
    raise ValueError(f"Error: Failed to fetch data. Status code: {response_parquet.status_code}")

# Write to temp file for better performance
temp_parquet = "/tmp/temp_data.parquet"
write_time_start = time.time()
with open(temp_parquet, 'wb') as f:
    f.write(response_parquet.content)
printtime(f"Data written to temp file in {time.time() - write_time_start:.2f} seconds")

# Create DuckDB connection with single-threaded mode
duckdb_time_start = time.time()
conn = duckdb.connect(':memory:', config={'threads': 1})
printtime(f"DuckDB connection established in {time.time() - duckdb_time_start:.2f} seconds")

# Execute query directly on parquet file
query = "SELECT * FROM read_parquet('/tmp/temp_data.parquet') WHERE PatientID = '10100'"
query_start = time.time()
result = conn.execute(query).fetchdf()
printtime(f"Query executed in {time.time() - query_start:.2f} seconds")
printtime(f"Total execution time: {time.time() - start_time:.2f} seconds")

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
