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
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile

# Encryption key - should match the one used for encryption
# In production, this should be securely provisioned to the SGX enclave
ENCRYPTION_KEY = base64.b64decode(os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs="))
# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
query = "SELECT count(*) FROM read_parquet('/tmp/temp_data.parquet') WHERE PatientID = '10100'"
index_cid = "Qmawt7HaXFAtsmfuENJKQHhYVCe7JK8xmkZ6i59cu3VyrQ"  # This should be the encrypted index CID
# Create DuckDB connection with single-threaded mode
duckdb_time_start = time.time()
conn = duckdb.connect(':memory:', config={'threads': 1})
print(f"DuckDB connection established in {time.time() - duckdb_time_start:.6f} seconds")

# --- Decryption Helper Functions ---

def decrypt_data(encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
    """
    Decrypt data using AES-256-CBC.
    """
    cipher = Cipher(
        algorithms.AES(key),
        modes.CBC(iv),
        backend=default_backend()
    )
    decryptor = cipher.decryptor()

    # Decrypt the data
    decrypted_padded = decryptor.update(encrypted_data) + decryptor.finalize()

    # Remove padding
    unpadder = padding.PKCS7(128).unpadder()
    decrypted_data = unpadder.update(decrypted_padded) + unpadder.finalize()

    return decrypted_data

def extract_and_decrypt_package(package: bytes, key: bytes) -> bytes:
    """
    Extract IV and decrypt the package.
    Format: [IV (16 bytes)][Encrypted Data]
    """
    # First 16 bytes are the IV
    iv = package[:16]
    encrypted_data = package[16:]
    return decrypt_data(encrypted_data, key, iv)

# --- Modified Helper functions ---

def retrieve_and_decrypt_index(cid):
    """
    Retrieve and decrypt an index from IPFS.
    Returns: (index, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    if not cid:
        return None, 0, 0
    try:
        # Fetch encrypted index from IPFS
        fetch_start = time.time()
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        fetch_end = time.time()
        fetch_time = fetch_end - fetch_start

        if resp.status_code != 200:
            return None, fetch_time, 0

        # Decrypt the index data
        decrypt_start = time.time()
        try:
            decrypted_data = extract_and_decrypt_package(resp.content, ENCRYPTION_KEY)
        except Exception as e:
            print(f"Failed to decrypt index: {e}")
            return None, fetch_time, 0
        decrypt_end = time.time()
        decrypt_time = decrypt_end - decrypt_start

        # Load the decrypted index
        index = CIDIndex()
        index.load(io.BytesIO(decrypted_data))
        return index, fetch_time, decrypt_time
    except Exception as e:
        print(f"Error retrieving index: {e}")
        return None, 0, 0

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

def fetch_and_decrypt_data(cid):
    """
    Fetch encrypted data from IPFS and decrypt it.
    Returns: (decrypted_data, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    try:
        # Fetch encrypted data from IPFS
        ipfs_api_url = "http://localhost:5001/api/v0/cat"
        fetch_start = time.time()
        resp = requests.post(ipfs_api_url, params={"arg": cid}, timeout=30)
        fetch_end = time.time()
        fetch_time = fetch_end - fetch_start

        if resp.status_code != 200:
            print(f"Failed to fetch {cid} from IPFS: Status {resp.status_code}")
            return None, fetch_time, 0

        # Decrypt the data
        decrypt_start = time.time()
        try:
            decrypted_data = extract_and_decrypt_package(resp.content, ENCRYPTION_KEY)
        except Exception as e:
            print(f"Failed to decrypt data from CID {cid}: {e}")
            return None, fetch_time, 0
        decrypt_end = time.time()
        decrypt_time = decrypt_end - decrypt_start

        return decrypted_data, fetch_time, decrypt_time

    except Exception as e:
        print(f"Error fetching/decrypting CID {cid}: {e}")
        return None, 0, 0

def fetch_and_decrypt_data_with_path(cid, temp_dir):
    """
    Fetch, decrypt and save data to a temporary file.
    Returns: (temp_file_path, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    decrypted_data, fetch_time, decrypt_time = fetch_and_decrypt_data(cid)
    if not decrypted_data:
        return None, fetch_time, decrypt_time
    
    # Write to a temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='wb', dir=temp_dir, delete=False, suffix='.parquet')
    temp_file.write(decrypted_data)
    temp_file.close()
    
    return temp_file.name, fetch_time, decrypt_time

def printtime(message):
    print(message)

start_time = time.time()
# Retrieve and decrypt index
idx_retrieve_start = time.time()
index, idx_fetch_time, idx_decrypt_time = retrieve_and_decrypt_index(index_cid)
idx_retrieve_end = time.time()
if not index:
    printtime("Index retrieval/decryption failed, exiting.")
    exit(1)
printtime(f"Index retrieved and decrypted in {idx_retrieve_end - idx_retrieve_start:.6f} seconds")
printtime(f"  - Index fetch time: {idx_fetch_time:.6f} seconds")
printtime(f"  - Index decrypt time: {idx_decrypt_time:.6f} seconds")

# Query the index
idx_query_time_start = time.time()
cids = query_index(index, query, "PatientID")
idx_query_time_end = time.time()
printtime(f"Index query took {idx_query_time_end - idx_query_time_start:.6f} seconds")
print(f"{len(cids)} CIDs found for query")

if not cids:
    printtime("No CIDs found, exiting.")
    exit(1)

# Create a temporary directory for parquet files
temp_dir = tempfile.mkdtemp()
printtime(f"Using temporary directory: {temp_dir}")

# Fetch and decrypt all data from IPFS
data_retrieve_start = time.time()
total_fetch_time = 0x
total_decrypt_time = 0
parquet_files = []

# Configure parallelism (adjust based on your system)
max_workers = min(2, len(cids))  # Limit concurrent fetches

printtime(f"Fetching and decrypting {len(cids)} files from IPFS using {max_workers} workers...")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit all tasks
    future_to_cid = {executor.submit(fetch_and_decrypt_data_with_path, cid, temp_dir): cid 
                     for cid in cids}
    
    # Process completed tasks
    completed = 0
    for future in as_completed(future_to_cid):
        cid = future_to_cid[future]
        try:
            file_path, fetch_time, decrypt_time = future.result()
            if file_path:
                parquet_files.append(file_path)
                total_fetch_time += fetch_time
                total_decrypt_time += decrypt_time
                completed += 1
                # if completed % 10 == 0:  # Progress update every 10 files
                #     printtime(f"  Progress: {completed}/{len(cids)} files processed")
            else:
                print(f"  Failed to process CID: {cid}")
        except Exception as e:
            print(f"  Exception processing CID {cid}: {e}")

data_retrieve_end = time.time()

printtime(f"\nSuccessfully fetched and decrypted {len(parquet_files)}/{len(cids)} files")
printtime(f"Total data fetch and decrypt time: {data_retrieve_end - data_retrieve_start:.6f} seconds")
printtime(f"  - Cumulative fetch time: {total_fetch_time:.6f} seconds")
printtime(f"  - Cumulative decrypt time: {total_decrypt_time:.6f} seconds")
printtime(f"  - Average time per file: {(data_retrieve_end - data_retrieve_start) / len(cids):.6f} seconds")

if not parquet_files:
    printtime("No files were successfully fetched/decrypted, exiting.")
    exit(1)

# Modify the query to use glob pattern for all parquet files
glob_pattern = os.path.join(temp_dir, "*.parquet")
# Replace the entire read_parquet('/tmp/temp_data.parquet') with read_parquet('glob_pattern')
modified_query = query.replace("read_parquet('/tmp/temp_data.parquet')", f"read_parquet('{glob_pattern}')")

printtime(f"\nExecuting query on {len(parquet_files)} parquet files using glob pattern...")
printtime(f"Query: {modified_query}")

# Execute query on all parquet files
query_start = time.time()
result = conn.execute(modified_query).fetchdf()
duckdb_query_time = time.time() - query_start
printtime(f"Query executed in {duckdb_query_time:.6f} seconds")

total_execution_time = time.time() - start_time

idx_lookup_time_seconds = idx_query_time_end - idx_query_time_start
# For multiple files, we need to consider the parallel processing time
avg_decrypt_time_per_file = total_decrypt_time / len(parquet_files) if parquet_files else 0
avg_fetch_time_per_file = total_fetch_time / len(parquet_files) if parquet_files else 0
# Actual wall time for fetching/decrypting (considers parallelism)
actual_fetch_decrypt_time = data_retrieve_end - data_retrieve_start

query_execution_time_seconds_excluding_index_overhead = idx_lookup_time_seconds + actual_fetch_decrypt_time + duckdb_query_time

# Summary of timing breakdown
print("\n=== Timing Summary ===")
print(f"idx_fetch_time_seconds: {idx_fetch_time:.6f} seconds")
print(f"idx_decrypt_time_seconds: {idx_decrypt_time:.6f} seconds")
print(f"idx_lookup_time_seconds: {idx_lookup_time_seconds:.6f} seconds")
print(f"Number of CIDs processed: {len(parquet_files)}")
print(f"Parallel fetch/decrypt time (wall time): {actual_fetch_decrypt_time:.6f} seconds")
print(f"  - Cumulative fetch time: {total_fetch_time:.6f} seconds")
print(f"  - Cumulative decrypt time: {total_decrypt_time:.6f} seconds")
print(f"  - Average per file: {actual_fetch_decrypt_time / len(parquet_files):.6f} seconds")
print(f"duckdb_query_time_seconds: {duckdb_query_time:.6f} seconds")
print(f"query_execution_time_seconds_excluding_index_overhead: {query_execution_time_seconds_excluding_index_overhead:.6f} seconds")
print(f"total_query_execution_time_seconds: {total_execution_time:.6f} seconds")

# Write output
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
print(f"\nResult written to {output_file}")
print(f"Result shape: {result.shape}")

# Cleanup
import shutil
shutil.rmtree(temp_dir)
conn.close()
printtime(f"\nTemporary files cleaned up")