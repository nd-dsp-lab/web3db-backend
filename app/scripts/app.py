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

# Fetch data from IPFS (same as before)
cid = "QmYX4fRXpvsCVFdAQuBWkAQPztpyPspeLkw8HgsHZe1WgQ"
ipfs_api_url = "http://localhost:5001/api/v0/cat"
printtime(f"Fetching data from IPFS CID: {cid}")

start_time = time.time()
ipfs_time_start = start_time
response_parquet = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)
printtime(f"Data fetched from IPFS in {time.time() - ipfs_time_start:.2f} seconds")

if response_parquet.status_code != 200:
    raise ValueError(f"Error: Failed to fetch data. Status code: {response_parquet.status_code}")

# Read Parquet into PyArrow Table
parquet_time_start = time.time()
parquet_buffer = io.BytesIO(response_parquet.content)
table = pq.read_table(parquet_buffer)
printtime(f"Data loaded to Arrow Table in {time.time() - parquet_time_start:.2f} seconds")
print(f"Arrow Table shape: {table.shape}")
print(f"Arrow Table schema: {table.schema}")
print(f"Memory usage: {table.nbytes / (1024**2):.2f} MB")

# Read SQL query (same as before)
# query_file_read_start = time.time()
# query_file = os.path.join(script_dir, "../query/query.sql")
# with open(query_file, "r") as f:
#     query = f.read()
# printtime(f"SQL query read in {time.time() - query_file_read_start:.2f} seconds")

query = "SELECT * FROM patient_data WHERE PatientID = 10100"
# Use DuckDB to query Arrow Table directly
duckdb_time_start = time.time()
conn = duckdb.connect()
printtime(f"DuckDB connection established in {time.time() - duckdb_time_start:.2f} seconds")

register_time_start = time.time()
conn.register('patient_data', table)  # Register Arrow Table as a virtual table
printtime(f"Arrow Table registered in DuckDB in {time.time() - register_time_start:.2f} seconds")

# Execute query and get result as DataFrame (only the result is converted)
query_start = time.time()
result = conn.execute(query).fetchdf()
printtime(f"Query executed in {time.time() - query_start:.2f} seconds")
printtime(f"Total execution time: {time.time() - start_time:.2f} seconds")
# Write output (same as before)
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
print(f"Result written to {output_file}")