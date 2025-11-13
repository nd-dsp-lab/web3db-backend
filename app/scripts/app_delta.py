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
from web3db_contract_index import Web3dbContract
import json

# Load environment variables
# Use absolute path to ensure .env is loaded regardless of current working directory
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')
load_dotenv(env_path)
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



# Initialize smart contract connection
app.state.index_storage = None
try:
    app.state.index_storage = Web3dbContract(
        # contract_address=os.getenv("CONTRACT_ADDRESS"),
        contract_address="0x5FbDB2315678afecb367f032d93F642f64180aa3",
        infura_api_key=os.getenv("INFURA_API_KEY", "eb1d43f1429e49fba50e18fbf5ebd4ab"),
        # private_key=os.getenv("PRIVATE_KEY")
        private_key="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        abi_path="/Users/aaron/Documents/repos/web3db-backend/contracts/artifacts/contracts/Web3dbContract.sol/Web3dbContract.json"
    )
    logger.info("Smart contract connection initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize smart contract connection: {e}")
    raise Exception("Smart contract connection is required but failed to initialize")

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

# --- Helper Functions ---

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

def fetch_and_decrypt(cid: str):
    logger.info(f"GET /ipfs/fetch/{cid}")
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        if resp.status_code != 200:
            return {"status": "error", "message": resp.text}
        decrypted_data = extract_and_decrypt_package(resp.content, app.state.encryption_key)
        return {"status": "success", "content": decrypted_data, "size_bytes": len(resp.content)}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
def upload_encrypted_delta(delta, attr):
    """
    Serialize, encrypt, and upload an index to IPFS.
    Returns: (cid, 0, 0) - last two values for backward compatibility
    """
    try:
        # Serialize the index

        index_data = delta.encode()


        # Encrypt the index data
        encrypted_index = create_encrypted_package(index_data, app.state.encryption_key)

        # Upload encrypted index to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": (f"{attr}_index.enc", encrypted_index)})
        resp.raise_for_status()
        return resp.json()["Hash"]
    except Exception as e:
        logger.error(f"Failed to upload encrypted index for {attr}: {e}")
        raise
  
# --- app routes ---
@app.post("/upload/patient-data")
async def upload_patient_data(file: UploadFile = File(...)):
    logger.info("POST /upload/patient-data - Processing patient data upload")
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content), dtype={"PatientID": str, "HospitalID": str, "Age": int})
        indexed_values = {k: set(df[k].values) for k in app.state.index_cids if k in df.columns}

        # Schema auto-detection disabled - use POST /schemas endpoint to manage schemas separately
        # schema = auto_detect_and_store_schema(df, "patient_data")
        schema = None
        
        # Convert to Parquet
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)
        parquet_data = buffer.read()

        # Encrypt the Parquet data
        encrypted_package = create_encrypted_package(parquet_data, app.state.encryption_key)

        # Upload encrypted data to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": ("patient_data.enc", encrypted_package)})
        resp.raise_for_status()
        data_cid = resp.json()["Hash"]
        buffer.close()
        del df 

        # Build encrypted delta files
        delta_cids = {} 
        for attr, values in indexed_values.items():
          data_to_add = data_cid+','+','.join([str(v) for v in values])
          logger.info(f"Uploading index for {attr}")

          # Upload encrypted delta file
          delta_cids[attr] = upload_encrypted_delta(data_to_add, attr)
          app.state.index_storage.update_index(attr, delta_cids[attr])


        gc.collect()
        return {
            "data_cid": data_cid,
            "delta_cids": delta_cids,
            "message": "Data uploaded successfully. Use POST /schemas to manage table schemas separately."
        }

    except Exception as e:
        logger.error(f"Upload error: {e}")
        gc.collect()
        return {"error": str(e)}

@app.get("/ipfs/fetch/{cid}")
async def fetch_from_ipfs_endpoint(cid: str):
    logger.info(f"GET /ipfs/fetch/{cid}")
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        if resp.status_code != 200:
            return {"status": "error", "message": resp.text}
        decrypted_data = extract_and_decrypt_package(resp.content, app.state.encryption_key)
        return {"status": "success", "content": decrypted_data, "size_bytes": len(resp.content)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/index")
async def get_indexes():
    indexes = {}
    logger.info("Getting all indexes")
    for attr in ["PatientID", "HospitalID", "Age"]:
        logger.info(f"Retrieving {attr} from Smart Contract")
        success, delta_cids = app.state.index_storage.get_index(attr)
        if not success:
            logger.error(f"Failed to retrieve {attr} from Smart Contract")
            continue
        logger.info(f"Succesfully retrieved {attr} from Smart Contract")
        for delta_cid in delta_cids:
            delta = fetch_and_decrypt(delta_cid)
            if delta["status"] != "success":
                continue
            delta = delta["content"]
            cid, *values = delta.decode().split(',')
            data = [(v, cid) for v in values]
            if attr not in indexes:
                indexes[attr] = CIDIndex(data)
            else:
                indexes[attr].update(data)
        logger.info(f"Succesfully retrieved {attr} deltas from IPFS")
        print(indexes[attr].index)
    logger.info(indexes)
    return indexes




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
        "description": "Decentralized Database with Privacy-Preserving Query Processing using Intel SGX and Access Control",
        "version": "1.0.0",
        "endpoints": {
            "health": "GET /health",
            "query": "POST /query (requires wallet_address for access control)", 
            "query-count": "GET /query/count",
            "upload": "POST /upload/patient-data",
            "index-cids": "GET /index-cids",
            "schemas": "GET /schemas, POST /schemas",
            "schema-by-table": "GET /schemas/{table_name}, DELETE /schemas/{table_name}",
            "access-policies": "POST /access-policies, GET /access-policies/{wallet_address}, DELETE /access-policies",
            "policy-count": "GET /access-policies/{wallet_address}/count",
            "remove-all-policies": "DELETE /access-policies/{wallet_address}/all",
            "docs": "GET /docs"
        },
        "access_control": {
            "enabled": True,
            "description": "All queries require a wallet_address parameter and are filtered based on access policies stored in the smart contract"
        }
    }

# Cleanup on shutdown
@app.on_event("shutdown")
def shutdown_event():
    logger.info("Application shutting down...")
    if hasattr(app.state, 'index_storage'):
        logger.info("Cleaning up smart contract connections...")

# Main execution block to start the FastAPI server
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")