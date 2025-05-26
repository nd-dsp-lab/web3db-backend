import pandas as pd
from pandasql import sqldf
import os
import time
import requests
import io
import pyarrow.parquet as pq

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define a function to print time
def printtime(message):
    print(message)

# Record start time
start_time = time.time()

# Define the CID (this was missing in the original code)
cid = "Qmeomrwk8HdpRbxUVpECsiHbLHnk367yu9JERCL4nMg2S5"
if not cid:
    raise ValueError("Error: IPFS_CID environment variable not set")

# Fetch data from IPFS
ipfs_api_url = "http://localhost:5001/api/v0/cat"
response_parquet = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)
printtime(f"Data fetched in {time.time() - start_time:.2f} seconds")

# Check if the request was successful
if response_parquet.status_code != 200:
    raise ValueError(f"Error: Failed to fetch data from IPFS. Status code: {response_parquet.status_code}")

# Read Parquet data into DataFrame (fixed incorrect function name)
parquet_buffer = io.BytesIO(response_parquet.content)
table = pq.read_table(parquet_buffer)
patient_data = table.to_pandas()

# Check if data is empty
if patient_data.empty:
    raise ValueError("Error: Parquet file contains no data")

# Define the query file path
query_file = os.path.join(script_dir, "/query/query.sql")

# Check if query file exists
if not os.path.exists(query_file):
    raise ValueError(f"Error: SQL query file not found at {query_file}")

# Read the SQL query from file
with open(query_file, "r") as f:
    query = f.read()

# Check if query is empty
if not query.strip():
    raise ValueError("Error: SQL query file is empty")

# Execute SQL query
result = sqldf(query, locals())
query_end_time = time.time()
printtime(f"Query executed in {query_end_time - start_time:.2f} seconds")

# Create output directory if it doesn't exist
output_dir = "/output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Write result to output directory mounted by Gramine
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
end_time = time.time()
printtime(f"Result written in {end_time - start_time:.2f} seconds")
print(f"Result written to {output_file}")

# Print the result
print(result)
