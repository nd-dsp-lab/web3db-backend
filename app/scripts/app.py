import os
import time
import requests
import io
import pyarrow.parquet as pq
import sqlite3
import pandas as pd

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

# Write parquet to temp file (SQLite can query parquet files directly)
temp_parquet = "/tmp/temp_data.parquet"
write_time_start = time.time()
with open(temp_parquet, 'wb') as f:
    f.write(response_parquet.content)
printtime(f"Data written to temp file in {time.time() - write_time_start:.2f} seconds")

# Create SQLite connection
sqlite_time_start = time.time()
conn = sqlite3.connect(':memory:')
conn.execute("PRAGMA journal_mode=OFF")  # Faster for read-only operations
conn.execute("PRAGMA synchronous=OFF")   # Faster writes
conn.execute("PRAGMA cache_size=10000")  # Larger cache
printtime(f"SQLite connection established in {time.time() - sqlite_time_start:.2f} seconds")

# Install and load parquet extension for SQLite (if using SQLite with parquet support)
# Otherwise, we'll use the pandas approach
try:
    # Try to query parquet file directly (requires sqlite-parquet extension)
    query = f"SELECT * FROM parquet_scan('{temp_parquet}') WHERE PatientID = '10100'"
    query_start = time.time()
    cursor = conn.execute(query)
    result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    printtime(f"Query executed directly on parquet in {time.time() - query_start:.2f} seconds")
except:
    # Fallback to loading data into SQLite
    printtime("Direct parquet query not supported, loading data into SQLite...")

    # Read parquet file
    read_time_start = time.time()
    df = pd.read_parquet(temp_parquet)
    printtime(f"Parquet read in {time.time() - read_time_start:.2f} seconds")

    # Load into SQLite with optimizations
    load_time_start = time.time()
    df.to_sql('patient_data', conn, index=False, if_exists='replace', method='multi', chunksize=10000)

    # Create index for better query performance
    conn.execute("CREATE INDEX idx_patient_id ON patient_data(PatientID)")
    printtime(f"Data loaded to SQLite with index in {time.time() - load_time_start:.2f} seconds")

    # Execute query
    query = "SELECT * FROM patient_data WHERE PatientID = '10100'"
    query_start = time.time()
    result = pd.read_sql_query(query, conn)
    printtime(f"Query executed in {time.time() - query_start:.2f} seconds")

printtime(f"Total execution time: {time.time() - start_time:.2f} seconds")

# Write output
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
print(f"Result written to {output_file}")

# Cleanup
os.remove(temp_parquet)
conn.close()
