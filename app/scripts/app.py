import os
import time
import requests
import io
import pyarrow.parquet as pq
import polars as pl

# Set environment variables BEFORE importing polars
os.environ["POLARS_MAX_THREADS"] = "1"
os.environ["RAYON_NUM_THREADS"] = "1"

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

def printtime(message):
    print(message)

# Fetch data from IPFS
cid = "QmYX4fRXpvsCVFdAQuBWkAQPztpyPspeLkw8HgsHZe1WgQ"
ipfs_api_url = "http://localhost:5001/api/v0/cat"
printtime(f"Fetching data from IPFS CID: {cid}")

start_time = time.time()
ipfs_time_start = start_time
response_parquet = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)
printtime(f"Data fetched from IPFS in {time.time() - ipfs_time_start:.2f} seconds")

if response_parquet.status_code != 200:
    raise ValueError(f"Error: Failed to fetch data. Status code: {response_parquet.status_code}")

# Write to temp file first (more stable for Polars in constrained environments)
temp_parquet = "/tmp/temp_data.parquet"
with open(temp_parquet, 'wb') as f:
    f.write(response_parquet.content)

# Read with Polars using single-threaded mode
polars_time_start = time.time()
df = pl.read_parquet(temp_parquet, parallel="none")
printtime(f"Data loaded to Polars DataFrame in {time.time() - polars_time_start:.2f} seconds")
# print(f"DataFrame shape: {df.shape}")
# print(f"DataFrame schema: {df.schema}")
# print(f"Memory usage: {df.estimated_size() / (1024**2):.2f} MB")

# Simple filter instead of SQL (more efficient)
query_start = time.time()
result = df.filter(pl.col("PatientID") == "10100")
printtime(f"Query executed in {time.time() - query_start:.2f} seconds")
printtime(f"Total execution time: {time.time() - start_time:.2f} seconds")

# Write output
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.write_csv(output_file)
print(f"Result written to {output_file}")

# Cleanup
os.remove(temp_parquet)
