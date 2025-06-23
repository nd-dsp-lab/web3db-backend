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

# Encryption key - should match the one used for encryption
# In production, this should be securely provisioned to the SGX enclave
ENCRYPTION_KEY = base64.b64decode(os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs="))
# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
query = "SELECT count(*) FROM read_parquet('/tmp/temp_data.parquet') WHERE PatientID = '10100'"
index_cid = "QmaTVubhNgWGZRUwpqXHWNStiGupwGLyXS4tc2xV5q7Xbj"  # This should be the encrypted index CID
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
print(len(cids), "CIDs found for query")

# Fetch and decrypt data from IPFS
cid = cids[0] if cids else None
if not cid:
    printtime("No CIDs found, exiting.")
    exit(1)

printtime(f"Fetching and decrypting data from IPFS CID: {cid}")

data_retrieve_start = time.time()
decrypted_parquet_data, data_fetch_time, data_decrypt_time = fetch_and_decrypt_data(cid)
data_retrieve_end = time.time()

if not decrypted_parquet_data:
    printtime("Failed to fetch/decrypt data, exiting.")
    exit(1)

printtime(f"Data fetched and decrypted from IPFS in {data_retrieve_end - data_retrieve_start:.6f} seconds")
printtime(f"  - Data fetch time: {data_fetch_time:.6f} seconds")
printtime(f"  - Data decrypt time: {data_decrypt_time:.6f} seconds")

# Write decrypted data to temp file for DuckDB
temp_parquet = "/tmp/temp_data.parquet"
write_time_start = time.time()
with open(temp_parquet, 'wb') as f:
    f.write(decrypted_parquet_data)
write_time = time.time() - write_time_start
printtime(f"Decrypted data written to temp file in {write_time:.6f} seconds")

# Execute query directly on parquet file
query_start = time.time()
result = conn.execute(query).fetchdf()
duckdb_query_time = time.time() - query_start
printtime(f"Query executed in {duckdb_query_time:.6f} seconds")

total_ececution_time = time.time() - start_time

idx_lookup_time_seconds = idx_query_time_end - idx_query_time_start
cid_decrypt_time_seconds = data_decrypt_time + write_time
query_execution_time_seconds_excluding_index_overhead = idx_lookup_time_seconds + data_fetch_time + cid_decrypt_time_seconds + duckdb_query_time
# Summary of timing breakdown
print("\n=== Timing Summary ===")
print(f"idx_fetch_time_seconds: {idx_fetch_time:.6f} seconds")
print(f"idx_decrypt_time_seconds: {idx_decrypt_time:.6f} seconds")
print(f"idx_lookup_time_seconds: {idx_lookup_time_seconds:.6f} seconds")
print(f"cid_fetch_time_seconds: {data_fetch_time:.6f} seconds")
print(f"cid_decrypt_time_seconds: {cid_decrypt_time_seconds:.6f} seconds")
print(f"duckdb_query_time_seconds: {duckdb_query_time:.6f} seconds")
print(f"query_execution_time_seconds_excluding_index_overhead: {query_execution_time_seconds_excluding_index_overhead:.6f} seconds")
print(f"total_query_execution_time_seconds: {total_ececution_time:.6f} seconds")

# Write output
output_dir = "/output"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "result.csv")
result.to_csv(output_file, index=False)
print(f"\nResult written to {output_file}")
print(f"Result shape: {result.shape}")

# Cleanup
os.remove(temp_parquet)
conn.close()
