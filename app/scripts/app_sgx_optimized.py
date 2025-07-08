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
import gc
import sys

# Encryption key - should match the one used for encryption
# In production, this should be securely provisioned to the SGX enclave
ENCRYPTION_KEY = base64.b64decode(os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs="))

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
query = "SELECT count(*) FROM read_parquet('/tmp/temp_data.parquet') WHERE PatientID = '10100'"
index_cid = "Qmawt7HaXFAtsmfuENJKQHhYVCe7JK8xmkZ6i59cu3VyrQ"  # This should be the encrypted index CID

# Performance optimizations for SGX
CHUNK_SIZE = 64 * 1024  # 64KB chunks for streaming
MAX_WORKERS = 1  # Single-threaded for SGX to avoid context switching overhead
SESSION = None  # Reusable session

def init_session():
    """Initialize a reusable HTTP session for IPFS requests"""
    global SESSION
    if SESSION is None:
        SESSION = requests.Session()
        # Optimize session for local IPFS
        SESSION.headers.update({
            'Connection': 'keep-alive',
            'Keep-Alive': 'timeout=5, max=1000'
        })
        # Set connection pool size
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1,
            pool_maxsize=1,
            pool_block=False
        )
        SESSION.mount('http://', adapter)
    return SESSION

# Create DuckDB connection with optimized settings for SGX
duckdb_time_start = time.time()
conn = duckdb.connect(':memory:', config={
    'threads': 1,
    'memory_limit': '512MB',  # Explicit memory limit
    'temp_directory': '/tmp',
    'default_order': 'ASC',
    'preserve_insertion_order': False
})
print(f"DuckDB connection established in {time.time() - duckdb_time_start:.6f} seconds")

# --- Optimized Decryption Helper Functions ---

class OptimizedDecryptor:
    """Reusable decryptor to avoid object creation overhead"""
    def __init__(self, key: bytes):
        self.key = key
        self._unpadder_cache = {}
    
    def decrypt_data_optimized(self, encrypted_data: bytes, iv: bytes) -> bytes:
        """
        Optimized decrypt using pre-allocated objects and streaming.
        """
        # Create cipher and decryptor
        cipher = Cipher(
            algorithms.AES(self.key),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()

        # For small data, use direct decryption
        if len(encrypted_data) < CHUNK_SIZE:
            decrypted_padded = decryptor.update(encrypted_data) + decryptor.finalize()
        else:
            # Stream large data in chunks to reduce memory pressure
            decrypted_chunks = []
            for i in range(0, len(encrypted_data), CHUNK_SIZE):
                chunk = encrypted_data[i:i + CHUNK_SIZE]
                if i + CHUNK_SIZE >= len(encrypted_data):
                    # Last chunk
                    decrypted_chunks.append(decryptor.update(chunk) + decryptor.finalize())
                else:
                    decrypted_chunks.append(decryptor.update(chunk))
            decrypted_padded = b''.join(decrypted_chunks)

        # Remove padding
        unpadder = padding.PKCS7(128).unpadder()
        decrypted_data = unpadder.update(decrypted_padded) + unpadder.finalize()

        return decrypted_data

    def extract_and_decrypt_package(self, package: bytes) -> bytes:
        """
        Extract IV and decrypt the package with optimizations.
        Format: [IV (16 bytes)][Encrypted Data]
        """
        # First 16 bytes are the IV
        iv = package[:16]
        encrypted_data = package[16:]
        return self.decrypt_data_optimized(encrypted_data, iv)

# Global decryptor instance
DECRYPTOR = OptimizedDecryptor(ENCRYPTION_KEY)

# --- Optimized Helper functions ---

def retrieve_and_decrypt_index_optimized(cid):
    """
    Optimized index retrieval and decryption.
    Returns: (index, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    if not cid:
        return None, 0, 0
    try:
        session = init_session()
        
        # Fetch encrypted index from IPFS with optimized session
        fetch_start = time.time()
        resp = session.post("http://localhost:5001/api/v0/cat", 
                           params={"arg": cid}, 
                           timeout=30,
                           stream=False)  # Don't stream for small index
        fetch_end = time.time()
        fetch_time = fetch_end - fetch_start

        if resp.status_code != 200:
            return None, fetch_time, 0

        # Decrypt the index data with optimized decryptor
        decrypt_start = time.time()
        try:
            decrypted_data = DECRYPTOR.extract_and_decrypt_package(resp.content)
        except Exception as e:
            print(f"Failed to decrypt index: {e}")
            return None, fetch_time, 0
        finally:
            # Explicitly clear response content to free memory
            resp.close()
            del resp
        
        decrypt_end = time.time()
        decrypt_time = decrypt_end - decrypt_start

        # Load the decrypted index
        index = CIDIndex()
        index.load(io.BytesIO(decrypted_data))
        
        # Clear decrypted data from memory
        del decrypted_data
        gc.collect()
        
        return index, fetch_time, decrypt_time
    except Exception as e:
        print(f"Error retrieving index: {e}")
        return None, 0, 0

def query_index(index, query, attr) -> List[str]:
    """Optimized index querying with early returns"""
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
        
        if op == "=": 
            result = index.query(key)
            out.update(result)
            # For equality, we can often stop early
            if result:
                break
        elif op == ">": out.update(index.query_range(key + 1, inf))
        elif op == "<": out.update(index.query_range(-inf, key - 1))
        elif op == ">=": out.update(index.query_range(key, inf))
        elif op == "<=": out.update(index.query_range(-inf, key))
        elif op == "!=": out.update(set(index.query_range()) - set(index.query(key)))
    
    return list(out)

def fetch_and_decrypt_data_optimized(cid):
    """
    Optimized data fetching and decryption with memory management.
    Returns: (decrypted_data, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    try:
        session = init_session()
        
        # Fetch encrypted data from IPFS with optimized session
        fetch_start = time.time()
        resp = session.post("http://localhost:5001/api/v0/cat", 
                           params={"arg": cid}, 
                           timeout=30,
                           stream=True)  # Stream for potentially large data
        fetch_end = time.time()
        fetch_time = fetch_end - fetch_start

        if resp.status_code != 200:
            print(f"Failed to fetch {cid} from IPFS: Status {resp.status_code}")
            resp.close()
            return None, fetch_time, 0

        # Read content in chunks to manage memory
        content_chunks = []
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                content_chunks.append(chunk)
        
        content = b''.join(content_chunks)
        resp.close()
        
        # Clear chunks to free memory
        del content_chunks
        
        # Decrypt the data with optimized decryptor
        decrypt_start = time.time()
        try:
            decrypted_data = DECRYPTOR.extract_and_decrypt_package(content)
        except Exception as e:
            print(f"Failed to decrypt data from CID {cid}: {e}")
            return None, fetch_time, 0
        finally:
            # Clear encrypted content
            del content
        
        decrypt_end = time.time()
        decrypt_time = decrypt_end - decrypt_start

        return decrypted_data, fetch_time, decrypt_time

    except Exception as e:
        print(f"Error fetching/decrypting CID {cid}: {e}")
        return None, 0, 0

def fetch_and_decrypt_data_with_path_optimized(cid, temp_dir):
    """
    Optimized fetch, decrypt and save to temporary file.
    Returns: (temp_file_path, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    decrypted_data, fetch_time, decrypt_time = fetch_and_decrypt_data_optimized(cid)
    if not decrypted_data:
        return None, fetch_time, decrypt_time
    
    # Write to a temporary file with buffering
    temp_file = tempfile.NamedTemporaryFile(mode='wb', dir=temp_dir, delete=False, suffix='.parquet', buffering=8192)
    try:
        temp_file.write(decrypted_data)
        temp_file.flush()
        os.fsync(temp_file.fileno())  # Ensure data is written
    finally:
        temp_file.close()
        # Clear decrypted data from memory immediately
        del decrypted_data
        gc.collect()
    
    return temp_file.name, fetch_time, decrypt_time

def printtime(message):
    print(message)

# Main execution with optimizations
start_time = time.time()

# Retrieve and decrypt index with optimizations
idx_retrieve_start = time.time()
index, idx_fetch_time, idx_decrypt_time = retrieve_and_decrypt_index_optimized(index_cid)
idx_retrieve_end = time.time()

if not index:
    printtime("Index retrieval/decryption failed, exiting.")
    sys.exit(1)

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
    sys.exit(1)

# Create a temporary directory for parquet files
temp_dir = tempfile.mkdtemp()
printtime(f"Using temporary directory: {temp_dir}")

# Fetch and decrypt all data from IPFS with optimizations
data_retrieve_start = time.time()
total_fetch_time = 0
total_decrypt_time = 0
parquet_files = []

# For SGX, use single-threaded processing to avoid overhead
printtime(f"Fetching and decrypting {len(cids)} files from IPFS sequentially (SGX optimized)...")

for i, cid in enumerate(cids):
    try:
        file_path, fetch_time, decrypt_time = fetch_and_decrypt_data_with_path_optimized(cid, temp_dir)
        if file_path:
            parquet_files.append(file_path)
            total_fetch_time += fetch_time
            total_decrypt_time += decrypt_time
            # Periodic garbage collection to manage memory
            if (i + 1) % 10 == 0:
                gc.collect()
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
    sys.exit(1)

# Modify the query to use glob pattern for all parquet files
glob_pattern = os.path.join(temp_dir, "*.parquet")
modified_query = query.replace("read_parquet('/tmp/temp_data.parquet')", f"read_parquet('{glob_pattern}')")

printtime(f"\nExecuting query on {len(parquet_files)} parquet files using glob pattern...")
printtime(f"Query: {modified_query}")

# Execute query with explicit memory management
query_start = time.time()
result = conn.execute(modified_query).fetchdf()
duckdb_query_time = time.time() - query_start
printtime(f"Query executed in {duckdb_query_time:.6f} seconds")

total_execution_time = time.time() - start_time

idx_lookup_time_seconds = idx_query_time_end - idx_query_time_start
actual_fetch_decrypt_time = data_retrieve_end - data_retrieve_start
query_execution_time_seconds_excluding_index_overhead = idx_lookup_time_seconds + actual_fetch_decrypt_time + duckdb_query_time

# Summary of timing breakdown
print("\n=== Timing Summary (Optimized) ===")
print(f"idx_fetch_time_seconds: {idx_fetch_time:.6f} seconds")
print(f"idx_decrypt_time_seconds: {idx_decrypt_time:.6f} seconds")
print(f"idx_lookup_time_seconds: {idx_lookup_time_seconds:.6f} seconds")
print(f"Number of CIDs processed: {len(parquet_files)}")
print(f"Sequential fetch/decrypt time: {actual_fetch_decrypt_time:.6f} seconds")
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

# Cleanup with explicit memory management
import shutil
shutil.rmtree(temp_dir)
conn.close()
if SESSION:
    SESSION.close()
gc.collect()
printtime(f"\nTemporary files cleaned up and memory freed")
