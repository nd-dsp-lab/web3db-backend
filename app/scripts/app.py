from math import inf
import re
import os
import io
import gc
import time
import logging
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from typing import List, Tuple, Optional
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import concurrent.futures
from cidindex import CIDIndex
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import secrets
import base64
from dotenv import load_dotenv
from index_state import IndexState

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Add CORS middleware to allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Directory to store Parquet files from IPFS
SHARED_TMP_DIR = "/tmp/ipfs_parquet"
os.makedirs(SHARED_TMP_DIR, exist_ok=True)

# Smart contract integration configuration
USE_SMART_CONTRACT = os.getenv("USE_SMART_CONTRACT", "false").lower() == "true"
logger.info(f"Smart contract integration: {'ENABLED' if USE_SMART_CONTRACT else 'DISABLED'}")

# Initialize smart contract connection if enabled
app.state.index_storage = None
if USE_SMART_CONTRACT:
    try:
        app.state.index_storage = IndexState(
            contract_address=os.getenv("CONTRACT_ADDRESS"),
            infura_api_key=os.getenv("INFURA_API_KEY"),
            private_key=os.getenv("PRIVATE_KEY")
        )
        logger.info("Smart contract connection initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize smart contract connection: {e}")
        logger.info("Falling back to in-memory storage")
        USE_SMART_CONTRACT = False
        app.state.index_storage = None

# Global index tracking
app.state.index_cids = {
    'PatientID': None,
    'HospitalID': None,
    'Age': None,
}
app.state.index_sizes = {}

# Encryption key management
# In production, use a proper key management service
# For now, we'll generate a key on startup and store it in app state
# app.state.encryption_key = secrets.token_bytes(32)  # 256-bit key for AES-256
app.state.encryption_key = base64.b64decode(os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs="))  # Default key for testing
logger.info("Generated AES-256 encryption key")

# Initialize DuckDB connection
logger.info("Initializing DuckDB Connection")
# Use in-memory database for better performance
duckdb_conn = duckdb.connect(':memory:')
logger.info("DuckDB Connection created")

# --- Encryption/Decryption Helper Functions ---

def encrypt_data(data: bytes, key: bytes) -> "Tuple[bytes, bytes]":
    """
    Encrypt data using AES-256-CBC.
    Returns: (encrypted_data, iv)
    """
    # Generate a random IV (Initialization Vector)
    iv = secrets.token_bytes(16)  # 128-bit IV for AES

    # Create cipher
    cipher = Cipher(
        algorithms.AES(key),
        modes.CBC(iv),
        backend=default_backend()
    )
    encryptor = cipher.encryptor()

    # Pad the data to be a multiple of 16 bytes (AES block size)
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(data) + padder.finalize()

    # Encrypt the data
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

    return encrypted_data, iv

def decrypt_data(encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
    """
    Decrypt data using AES-256-CBC.
    """
    # Create cipher
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

def create_encrypted_package(data: bytes, key: bytes) -> bytes:
    """
    Create an encrypted package with IV prepended to encrypted data.
    Format: [IV (16 bytes)][Encrypted Data]
    """
    encrypted_data, iv = encrypt_data(data, key)
    # Prepend IV to encrypted data for storage
    return iv + encrypted_data

def extract_and_decrypt_package(package: bytes, key: bytes) -> bytes:
    """
    Extract IV and decrypt the package.
    """
    # First 16 bytes are the IV
    iv = package[:16]
    encrypted_data = package[16:]
    return decrypt_data(encrypted_data, key, iv)

# --- Separate fetch and decrypt functions ---

def fetch_from_ipfs(cid: str) -> Optional[bytes]:
    """
    Fetch encrypted data from IPFS.
    Returns encrypted data bytes or None on failure.
    """
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"Failed to fetch {cid} from IPFS: Status {resp.status_code}")
            return None
        return resp.content
    except Exception as e:
        logger.error(f"Error fetching CID {cid}: {e}")
        return None

def decrypt_to_file(encrypted_data: bytes, cid: str, key: bytes) -> Optional[str]:
    """
    Decrypt data and save to a file.
    Returns file path or None on failure.
    """
    try:
        decrypted_data = extract_and_decrypt_package(encrypted_data, key)
        path = os.path.join(SHARED_TMP_DIR, f"{cid}.parquet")
        with open(path, "wb") as f:
            f.write(decrypted_data)
        return path
    except Exception as e:
        logger.error(f"Failed to decrypt CID {cid}: {e}")
        return None

@app.post("/upload/patient-data")
async def upload_patient_data(file: UploadFile = File(...)):
    logger.info("POST /upload/patient-data - Processing patient data upload")
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content), dtype={"PatientID": str, "HospitalID": str, "Age": int})
        indexed_values = {k: set(df[k].values) for k in app.state.index_cids if k in df.columns}

        time_start = time.time()
        # Convert to Parquet
        parquet_time_start = time.time()
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)
        parquet_data = buffer.read()
        parquet_time_end = time.time()

        # Encrypt the Parquet data
        encryption_start = time.time()
        encrypted_package = create_encrypted_package(parquet_data, app.state.encryption_key)
        encryption_end = time.time()

        # Upload encrypted data to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        ipfs_upload_start = time.time()
        resp = requests.post(ipfs_api, files={"file": ("patient_data.enc", encrypted_package)})
        ipfs_upload_end = time.time()
        resp.raise_for_status()
        data_cid = resp.json()["Hash"]
        buffer.close()
        del df 

        # Build/update encrypted indexes with timing
        idx_start = time.time()
        total_index_encrypt_time = 0
        total_index_upload_time = 0
        
        # Collect all index CIDs for batch update
        index_cids_to_update = {}

        for attr, values in indexed_values.items():
            data_to_add = [(v, data_cid) for v in values]
            existing_index = retrieve_index(attr)  # This now handles decryption
            if existing_index:
                existing_index.update(data_to_add)
                index = existing_index
            else:
                index = CIDIndex(data=data_to_add)

            # Upload encrypted index with timing
            index_cid, encrypt_time, upload_time = upload_encrypted_index(index, attr)
            # Collect index CID for batch update
            index_cids_to_update[attr] = index_cid
            total_index_encrypt_time += encrypt_time
            total_index_upload_time += upload_time
            logger.info(f"Uploaded encrypted index for {attr}: {index_cid}")

        # Batch update all index CIDs in smart contract (single call instead of multiple)
        if index_cids_to_update:
            batch_update_success = set_all_index_cids(index_cids_to_update)
            if batch_update_success:
                logger.info(f"Batch updated {len(index_cids_to_update)} index CIDs in smart contract")
            else:
                logger.warning("Batch update to smart contract failed, using fallback storage")

        idx_end = time.time()
        time_end = time.time()
        gc.collect()
        return {
            "data_cid": data_cid,
            "index_cids": get_all_index_cids(),  # Get from smart contract or in-memory
            "index_sizes": app.state.index_sizes,
            "parquet_time_seconds": parquet_time_end - parquet_time_start,
            "data_encryption_time_seconds": encryption_end - encryption_start,
            "ipfs_upload_time_seconds": ipfs_upload_end - ipfs_upload_start,
            "index_encryption_time_seconds": total_index_encrypt_time,
            "index_upload_time_seconds": total_index_upload_time,
            "index_build_time_seconds": idx_end - idx_start,
            "total_time_seconds": time_end - time_start
        }

    except Exception as e:
        logger.error(f"Upload error: {e}")
        gc.collect()
        return {"error": str(e)}


class QueryRequest(BaseModel):
    index_attribute: str = 'PatientID'
    query: str = "select * from patient_data where PatientID = 'X'"

@app.post("/query")
async def query(request: QueryRequest):
    logger.info("POST /query - Processing query")
    query_start_time = time.time()

    # Retrieve and decrypt index with timing
    index, idx_fetch_time, idx_decrypt_time = retrieve_index_with_timing(request.index_attribute)

    if not index:
        return {"error": f"Index for {request.index_attribute} not found"}

    idx_query_time_start = time.time()
    cids = query_index(index, request.query, request.index_attribute)
    idx_query_time_end = time.time()

    if not cids:
        return {"message": "No matching CIDs found"}

    # Fetch all CIDs in parallel
    fetch_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        encrypted_data_list = list(executor.map(fetch_from_ipfs, cids))
    fetch_end = time.time()
    total_fetch_time = fetch_end - fetch_start

    # Decrypt all data sequentially (or in parallel if needed)
    decrypt_start = time.time()
    paths = []
    for cid, encrypted_data in zip(cids, encrypted_data_list):
        if encrypted_data:
            path = decrypt_to_file(encrypted_data, cid, app.state.encryption_key)
            if path:
                paths.append(path)
    decrypt_end = time.time()
    total_decrypt_time = decrypt_end - decrypt_start

    if not paths:
        return {"error": "No valid Parquet files retrieved"}

    duckdb_query_start = time.time()
    # Apply DuckDB SQL directly on those Parquet files
    try:
        # For large number of files, use glob pattern or process in batches
        if len(paths) == 1:
            query_with_table = request.query.replace("patient_data", f"'{paths[0]}'")
            result = duckdb_conn.execute(query_with_table)
        else:
            # Method 1: Use glob pattern if files are in same directory
            # This is more efficient for many files
            glob_pattern = os.path.join(SHARED_TMP_DIR, "*.parquet")
            query_with_table = request.query.replace("patient_data", f"read_parquet('{glob_pattern}')")
            result = duckdb_conn.execute(query_with_table)

        # Fetch all results and convert to list of dictionaries
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Query error: {e}")
        return {"error": str(e)}
    finally:
        # Delete temporary files
        for p in paths:
            try:
                os.remove(p)
            except Exception as e:
                logger.warning(f"Failed to delete {p}: {e}")
    duckdb_query_end = time.time()
    query_end_time = time.time()
    return {
        "cids": len(cids),
        "records": len(results),
        "results": results,
        "idx_fetch_time_seconds": idx_fetch_time,
        "idx_decrypt_time_seconds": idx_decrypt_time,
        "idx_lookup_time_seconds": idx_query_time_end - idx_query_time_start,
        "cid_fetch_time_seconds": total_fetch_time,
        "cid_decrypt_time_seconds": total_decrypt_time,
        "duckdb_query_time_seconds": duckdb_query_end - duckdb_query_start,
        "total_query_execution_time_seconds": query_end_time - query_start_time
    }


@app.get("/ipfs/fetch/{cid}")
async def fetch_from_ipfs_endpoint(cid: str):
    logger.info(f"GET /ipfs/fetch/{cid}")
    try:
        start = time.time()
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        elapsed = time.time() - start
        if resp.status_code != 200:
            return {"status": "error", "message": resp.text, "time": elapsed}
        return {"status": "success", "size_bytes": len(resp.content), "time": elapsed}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- Helper functions ---

def retrieve_index_with_timing(name):
    """
    Retrieve and decrypt an index from IPFS with timing information.
    Returns: (index, fetch_time, decrypt_time) or (None, 0, 0) on failure
    """
    cid = get_index_cid(name)
    if not cid:
        return None, 0, 0

    # Fetch encrypted index from IPFS
    fetch_start = time.time()
    encrypted_data = fetch_from_ipfs(cid)
    fetch_end = time.time()
    fetch_time = fetch_end - fetch_start

    if not encrypted_data:
        return None, fetch_time, 0

    # Decrypt the index data
    decrypt_start = time.time()
    try:
        decrypted_data = extract_and_decrypt_package(encrypted_data, app.state.encryption_key)
        # Load the decrypted index
        index = CIDIndex()
        index.load(io.BytesIO(decrypted_data))
        decrypt_end = time.time()
        decrypt_time = decrypt_end - decrypt_start
        return index, fetch_time, decrypt_time
    except Exception as e:
        logger.error(f"Failed to decrypt index {name}: {e}")
        return None, fetch_time, 0

def retrieve_index(name):
    """
    Retrieve and decrypt an index from IPFS (wrapper for backward compatibility).
    """
    index, _, _ = retrieve_index_with_timing(name)
    return index

def upload_encrypted_index(index, attr):
    """
    Serialize, encrypt, and upload an index to IPFS with timing.
    Returns: (cid, encryption_time, upload_time)
    """
    try:
        # Serialize the index
        serialized = index.dump()
        serialized.seek(0)
        index_data = serialized.read()

        # Get size before encryption
        index_size_bytes = len(index_data)
        app.state.index_sizes[attr] = index_size_bytes

        # Encrypt the index data
        encrypt_start = time.time()
        encrypted_index = create_encrypted_package(index_data, app.state.encryption_key)
        encrypt_end = time.time()
        encryption_time = encrypt_end - encrypt_start

        # Upload encrypted index to IPFS
        upload_start = time.time()
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": (f"{attr}_index.enc", encrypted_index)})
        resp.raise_for_status()
        upload_end = time.time()
        upload_time = upload_end - upload_start

        serialized.close()
        return resp.json()["Hash"], encryption_time, upload_time

    except Exception as e:
        logger.error(f"Failed to upload encrypted index for {attr}: {e}")
        raise

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

# Legacy function for backward compatibility
def fetch_and_decrypt_cid(cid):
    """
    Legacy function - fetch and decrypt in one go
    """
    encrypted_data = fetch_from_ipfs(cid)
    if not encrypted_data:
        return None, 0, 0

    path = decrypt_to_file(encrypted_data, cid, app.state.encryption_key)
    return path, 0, 0

def fetch_cid(cid):
    """
    Legacy function - now redirects to fetch_and_decrypt_cid
    """
    path, _, _ = fetch_and_decrypt_cid(cid)
    return path


class UpdateIndexCIDsRequest(BaseModel):
    index_cids: dict


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "FastAPI server running inside SGX enclave",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "sgx_enabled": True
    }

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Web3DB SGX API",
        "description": "Decentralized Database with Privacy-Preserving Query Processing using Intel SGX",
        "version": "1.0.0",
        "endpoints": {
            "health": "GET /health",
            "query": "POST /query", 
            "upload": "POST /upload/patient-data",
            "index-cids": "GET /index-cids",
            "docs": "GET /docs"
        }
    }

@app.put("/index-cids")
async def update_index_cids(request: UpdateIndexCIDsRequest):
    """
    Update the index CIDs mapping.

    Example request body:
    {
        "index_cids": {
            "PatientID": "QmXxxxx...",
            "HospitalID": "QmYyyyy...",
            "Age": "QmZzzzz..."
        }
    }
    """
    global USE_SMART_CONTRACT
    logger.info("PUT /index-cids - Updating index CIDs")
    try:
        # Validate that the keys match expected index attributes
        valid_keys = set(app.state.index_cids.keys())
        provided_keys = set(request.index_cids.keys())

        # Check for invalid keys
        invalid_keys = provided_keys - valid_keys
        if invalid_keys:
            return {
                "status": "error",
                "message": f"Invalid index attributes: {invalid_keys}. Valid attributes are: {valid_keys}"
            }

        # Update the index CIDs using helper function (smart contract or in-memory)
        success = set_all_index_cids(request.index_cids)
        
        storage_type = "smart contract" if USE_SMART_CONTRACT else "in-memory"
        
        if success:
            logger.info(f"Updated index CIDs in {storage_type}")
            return {
                "status": "success",
                "message": f"Index CIDs updated successfully in {storage_type}",
                "updated_cids": request.index_cids,
                "current_cids": get_all_index_cids(),
                "smart_contract_enabled": USE_SMART_CONTRACT
            }
        else:
            return {
                "status": "warning",
                "message": f"Index CIDs updated in fallback storage (smart contract update failed)",
                "updated_cids": request.index_cids,
                "current_cids": get_all_index_cids(),
                "smart_contract_enabled": USE_SMART_CONTRACT
            }

    except Exception as e:
        logger.error(f"Error updating index CIDs: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/index-cids")
async def get_index_cids():
    """
    Get the current index CIDs mapping along with index sizes if available.

    Returns:
    {
        "index_cids": {
            "PatientID": "QmXxxxx..." or null,
            "HospitalID": "QmYyyyy..." or null,
            "Age": "QmZzzzz..." or null
        },
        "index_sizes": {
            "PatientID": 12345,
            "HospitalID": 23456,
            "Age": 34567
        }
    }
    """
    global USE_SMART_CONTRACT
    logger.info("GET /index-cids - Retrieving current index CIDs")
    try:
        return {
            "status": "success",
            "index_cids": get_all_index_cids(),  # Get from smart contract or in-memory
            "index_sizes": app.state.index_sizes,
            "smart_contract_enabled": USE_SMART_CONTRACT,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
    except Exception as e:
        logger.error(f"Error retrieving index CIDs: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/smart-contract-status")
async def get_smart_contract_status():
    """
    Get the current smart contract integration status.
    
    Returns:
    {
        "smart_contract_enabled": true/false,
        "connection_status": "connected"/"disconnected"/"error",
        "contract_address": "0x...",
        "message": "Status message"
    }
    """
    global USE_SMART_CONTRACT
    logger.info("GET /smart-contract-status - Checking smart contract status")
    
    try:
        if not USE_SMART_CONTRACT:
            return {
                "smart_contract_enabled": False,
                "connection_status": "disabled",
                "contract_address": None,
                "message": "Smart contract integration is disabled in configuration"
            }
        
        if not app.state.index_storage:
            return {
                "smart_contract_enabled": True,
                "connection_status": "error",
                "contract_address": os.getenv("CONTRACT_ADDRESS"),
                "message": "Smart contract connection failed during initialization"
            }
        
        # Test connection by checking if we're connected to the network
        try:
            is_connected = app.state.index_storage.w3.is_connected()
            if is_connected:
                return {
                    "smart_contract_enabled": True,
                    "connection_status": "connected",
                    "contract_address": os.getenv("CONTRACT_ADDRESS"),
                    "account_address": app.state.index_storage.address,
                    "network": "Sepolia",
                    "message": "Smart contract connection is active"
                }
            else:
                return {
                    "smart_contract_enabled": True,
                    "connection_status": "disconnected",
                    "contract_address": os.getenv("CONTRACT_ADDRESS"),
                    "message": "Smart contract connection is not active"
                }
        except Exception as connection_error:
            return {
                "smart_contract_enabled": True,
                "connection_status": "error",
                "contract_address": os.getenv("CONTRACT_ADDRESS"),
                "message": f"Error checking connection: {str(connection_error)}"
            }
    
    except Exception as e:
        logger.error(f"Error checking smart contract status: {e}")
        return {
            "smart_contract_enabled": USE_SMART_CONTRACT,
            "connection_status": "error",
            "contract_address": os.getenv("CONTRACT_ADDRESS"),
            "message": f"Error: {str(e)}"
        }

# --- Index CID Management Helper Functions ---

def get_index_cid(attribute: str) -> Optional[str]:
    """
    Get index CID for an attribute from smart contract or in-memory storage.
    
    Args:
        attribute (str): The attribute name
        
    Returns:
        Optional[str]: The CID or None if not found
    """
    global USE_SMART_CONTRACT
    if USE_SMART_CONTRACT and app.state.index_storage:
        try:
            success, cid = app.state.index_storage.get_index(attribute)
            if success and cid:  # Make sure it's not an empty string
                return cid
            return None
        except Exception as e:
            logger.error(f"Failed to get index CID for {attribute} from smart contract: {e}")
            # Fall back to in-memory storage
            return app.state.index_cids.get(attribute)
    else:
        return app.state.index_cids.get(attribute)

def get_all_index_cids() -> dict:
    """
    Get all index CIDs from smart contract or in-memory storage.
    
    Returns:
        dict: Dictionary mapping attribute names to CIDs
    """
    global USE_SMART_CONTRACT
    if USE_SMART_CONTRACT and app.state.index_storage:
        try:
            attributes = list(app.state.index_cids.keys())  # Get known attributes
            success, cid_dict = app.state.index_storage.batch_get_indices(attributes)
            if success:
                # Filter out empty CIDs
                return {attr: cid for attr, cid in cid_dict.items() if cid}
            else:
                logger.error("Failed to get batch index CIDs from smart contract")
                return app.state.index_cids
        except Exception as e:
            logger.error(f"Failed to get all index CIDs from smart contract: {e}")
            # Fall back to in-memory storage
            return app.state.index_cids
    else:
        return app.state.index_cids

def set_index_cid(attribute: str, cid: str) -> bool:
    """
    Set index CID for an attribute in smart contract or in-memory storage.
    
    Args:
        attribute (str): The attribute name
        cid (str): The CID value
        
    Returns:
        bool: True if successful, False otherwise
    """
    global USE_SMART_CONTRACT
    if USE_SMART_CONTRACT and app.state.index_storage:
        try:
            success = app.state.index_storage.update_index(attribute, cid)
            if success:
                # Also update in-memory storage as backup
                app.state.index_cids[attribute] = cid
                logger.info(f"Updated index CID for {attribute} in smart contract: {cid}")
                return True
            else:
                logger.error(f"Failed to update index CID for {attribute} in smart contract")
                # Fall back to in-memory storage
                app.state.index_cids[attribute] = cid
                return False
        except Exception as e:
            logger.error(f"Failed to set index CID for {attribute} in smart contract: {e}")
            # Fall back to in-memory storage
            app.state.index_cids[attribute] = cid
            return False
    else:
        app.state.index_cids[attribute] = cid
        return True

def set_all_index_cids(cid_dict: dict) -> bool:
    """
    Set multiple index CIDs in smart contract or in-memory storage.
    
    Args:
        cid_dict (dict): Dictionary mapping attribute names to CIDs
        
    Returns:
        bool: True if successful, False otherwise
    """
    global USE_SMART_CONTRACT
    if USE_SMART_CONTRACT and app.state.index_storage:
        try:
            attributes = list(cid_dict.keys())
            cids = list(cid_dict.values())
            success = app.state.index_storage.batch_update_indices(attributes, cids)
            if success:
                # Also update in-memory storage as backup
                app.state.index_cids.update(cid_dict)
                logger.info(f"Updated {len(cid_dict)} index CIDs in smart contract")
                return True
            else:
                logger.error("Failed to batch update index CIDs in smart contract")
                # Fall back to in-memory storage
                app.state.index_cids.update(cid_dict)
                return False
        except Exception as e:
            logger.error(f"Failed to set batch index CIDs in smart contract: {e}")
            # Fall back to in-memory storage
            app.state.index_cids.update(cid_dict)
            return False
    else:
        app.state.index_cids.update(cid_dict)
        return True

# Load initial index CIDs from smart contract if enabled
if USE_SMART_CONTRACT and app.state.index_storage:
    try:
        attributes = list(app.state.index_cids.keys())
        success, cid_dict = app.state.index_storage.batch_get_indices(attributes)
        if success:
            # Update in-memory storage with smart contract data
            for attr, cid in cid_dict.items():
                if cid:  # Only update if CID is not empty
                    app.state.index_cids[attr] = cid
            logger.info(f"Loaded initial index CIDs from smart contract: {cid_dict}")
        else:
            logger.warning("Failed to load initial index CIDs from smart contract")
    except Exception as e:
        logger.error(f"Failed to load initial index CIDs from smart contract: {e}")

# Cleanup on shutdown
@app.on_event("shutdown")
def shutdown_event():
    logger.info("Closing DuckDB connection")
    duckdb_conn.close()

# Main execution block to start the FastAPI server
if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting FastAPI server inside SGX enclave...")
    logger.info("Server will be available at http://0.0.0.0:8000")
    logger.info("API documentation available at http://0.0.0.0:8000/docs")
    
    # Start the uvicorn server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,  # Disable reload in SGX environment
        access_log=True,
        log_level="info",
        loop="asyncio"  # Explicitly specify event loop
    )