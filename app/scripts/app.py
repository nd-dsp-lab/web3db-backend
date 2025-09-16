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
from web3db_contract import Web3dbContract

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
        contract_address=os.getenv("CONTRACT_ADDRESS"),
        infura_api_key=os.getenv("INFURA_API_KEY"),
        private_key=os.getenv("PRIVATE_KEY")
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
        
        # Determine file type and process accordingly
        file_extension = file.filename.lower().split('.')[-1] if file.filename else 'csv'
        
        if file_extension == 'sql':
            # Process SQL file
            df = process_sql_file(content)
        elif file_extension == 'csv':
            # Process CSV file
            df = pd.read_csv(io.BytesIO(content), dtype={"PatientID": str, "HospitalID": str, "Age": int})
        else:
            return {"error": f"Unsupported file type: {file_extension}. Only CSV and SQL files are supported."}
        
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
        
        # Store row count before deleting DataFrame
        rows_processed = len(df)
        del df 

        # Build/update encrypted indexes
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

            # Upload encrypted index
            index_cid, _, _ = upload_encrypted_index(index, attr)
            # Collect index CID for batch update
            index_cids_to_update[attr] = index_cid
            logger.info(f"Uploaded encrypted index for {attr}: {index_cid}")

        # Batch update all index CIDs in smart contract (single call instead of multiple)
        if index_cids_to_update:
            batch_update_success = set_all_index_cids(index_cids_to_update)
            if batch_update_success:
                logger.info(f"Batch updated {len(index_cids_to_update)} index CIDs in smart contract")
            else:
                logger.error("Batch update to smart contract failed")
                return {"error": "Failed to update index CIDs in smart contract"}

        gc.collect()
        return {
            "data_cid": data_cid,
            "index_cids": get_all_index_cids(),  # Get from smart contract or in-memory
            "index_sizes": app.state.index_sizes,
            "file_type": file_extension,
            "rows_processed": rows_processed,
            "message": "Data uploaded successfully. Supports CSV and SQL files. Use POST /schemas to manage table schemas separately."
        }

    except Exception as e:
        logger.error(f"Upload error: {e}")
        gc.collect()
        return {"error": str(e)}


class QueryRequest(BaseModel):
    index_attribute: str = 'PatientID'
    query: str = "select * from patient_data where PatientID = 'X'"
    wallet_address: str  # Required wallet address for access control

def rewrite_query_with_access_policies(original_query: str, policies: List[dict], table_name: str = "patient_data") -> str:
    """
    Rewrite the original query to incorporate access control policies.
    
    For multiple policies, this creates a more complex CTE that combines the conditions 
    using OR logic rather than UNION to avoid column compatibility issues.
    
    Args:
        original_query (str): The original SQL query
        policies (List[dict]): List of access policies with 'policySql' field
        table_name (str): The table name to apply policies to
        
    Returns:
        str: The rewritten query with access control
    """
    if not policies:
        return ""  # Return empty query if no policies
    
    # Extract valid policy SQLs and analyze them
    policy_conditions = []
    
    for policy in policies:
        policy_sql = policy.get('policySql', '').strip()
        if policy_sql:
            # Extract the WHERE clause from each policy SQL
            # This is a simple approach - we'll extract conditions from WHERE clauses
            policy_sql_lower = policy_sql.lower()
            
            if 'where' in policy_sql_lower:
                # Find the WHERE clause
                where_index = policy_sql_lower.find('where')
                condition = policy_sql[where_index + 5:].strip()  # +5 for "where"
                policy_conditions.append(f"({condition})")
            else:
                # If no WHERE clause, this policy allows all data
                # We'll treat this as a catch-all condition
                policy_conditions.append("(1=1)")  # Always true condition
    
    if not policy_conditions:
        return ""  # Return empty query if no valid policies
    
    # Combine all conditions with OR
    combined_condition = " OR ".join(policy_conditions)
    
    # Create the accessible_part CTE with all columns from original table
    # and the combined WHERE condition
    accessible_part_definition = f"SELECT * FROM {table_name} WHERE {combined_condition}"
    
    # Rewrite the original query to use the accessible_part CTE
    modified_query = original_query.replace(table_name, "accessible_part")
    
    # Construct the final query with CTE
    final_query = f"WITH accessible_part AS ({accessible_part_definition}) {modified_query}"
    
    return final_query

@app.post("/query")
async def query(request: QueryRequest):
    logger.info("POST /query - Processing query with access control")

    # Step 1: Fetch access policies for the wallet address
    try:
        success, policies = app.state.index_storage.get_access_policies(request.wallet_address)
        if not success:
            logger.error(f"Failed to fetch access policies from smart contract for {request.wallet_address}")
            return {"error": "Failed to fetch access policies from smart contract"}
    except Exception as e:
        logger.error(f"Error fetching access policies: {e}")
        return {"error": f"Error fetching access policies: {str(e)}"}
    
    # Step 2: If no policies found, return no data
    if not policies:
        logger.info(f"No access policies found for wallet {request.wallet_address}, returning no data")
        return {
            "message": "No access policies found for this wallet address",
            "wallet_address": request.wallet_address,
            "policy_count": 0,
            "records": 0,
            "results": []
        }
    
    # Step 3: Rewrite query with access policies
    rewritten_query = rewrite_query_with_access_policies(request.query, policies, "patient_data")
    
    if not rewritten_query:
        logger.warning(f"Failed to rewrite query with access policies for wallet {request.wallet_address}")
        return {
            "error": "Failed to create access-controlled query",
            "wallet_address": request.wallet_address,
            "policy_count": len(policies)
        }
    
    logger.info(f"Rewritten query for wallet {request.wallet_address}: {rewritten_query}")

    # Step 4: Continue with normal query processing using the rewritten query
    # Retrieve and decrypt index
    index = retrieve_index(request.index_attribute)

    if not index:
        return {"error": f"Index for {request.index_attribute} not found"}

    cids = query_index(index, request.query, request.index_attribute)  # Use original query for index lookup

    if not cids:
        return {"message": "No matching CIDs found"}

    # Fetch all CIDs in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        encrypted_data_list = list(executor.map(fetch_from_ipfs, cids))

    # Decrypt all data sequentially (or in parallel if needed)
    paths = []
    for cid, encrypted_data in zip(cids, encrypted_data_list):
        if encrypted_data:
            path = decrypt_to_file(encrypted_data, cid, app.state.encryption_key)
            if path:
                paths.append(path)

    if not paths:
        return {"error": "No valid Parquet files retrieved"}

    # Apply DuckDB SQL using the rewritten query with access control
    try:
        # For large number of files, use glob pattern or process in batches
        if len(paths) == 1:
            query_with_table = rewritten_query.replace("patient_data", f"'{paths[0]}'")
            result = duckdb_conn.execute(query_with_table)
        else:
            # Method 1: Use glob pattern if files are in same directory
            # This is more efficient for many files
            glob_pattern = os.path.join(SHARED_TMP_DIR, "*.parquet")
            query_with_table = rewritten_query.replace("patient_data", f"read_parquet('{glob_pattern}')")
            result = duckdb_conn.execute(query_with_table)

        # Fetch all results and convert to list of dictionaries
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Query error with rewritten query: {e}")
        logger.error(f"Rewritten query was: {rewritten_query}")
        return {"error": f"Query execution failed: {str(e)}"}
    finally:
        # Delete temporary files
        for p in paths:
            try:
                os.remove(p)
            except Exception as e:
                logger.warning(f"Failed to delete {p}: {e}")
    
    return {
        "wallet_address": request.wallet_address,
        "policy_count": len(policies),
        "policies_applied": [{"table": p.get('tableName'), "sql": p.get('policySql')} for p in policies],
        "rewritten_query": rewritten_query,
        "cids": len(cids),
        "records": len(results),
        "results": results
    }


@app.get("/ipfs/fetch/{cid}")
async def fetch_from_ipfs_endpoint(cid: str):
    logger.info(f"GET /ipfs/fetch/{cid}")
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
        if resp.status_code != 200:
            return {"status": "error", "message": resp.text}
        return {"status": "success", "size_bytes": len(resp.content)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- Helper functions ---

def process_sql_file(content: bytes) -> pd.DataFrame:
    """
    Process SQL file by executing it in DuckDB and extracting the resulting data.
    
    Args:
        content (bytes): Raw SQL file content
        
    Returns:
        pd.DataFrame: Data extracted from executed SQL statements
    """
    try:
        # Decode the SQL content
        sql_content = content.decode('utf-8')
        
        # Connect to in-memory DuckDB
        temp_conn = duckdb.connect()
        
        try:
            # Create the patient_data table first (since SQL file only has INSERTs)
            create_table_sql = """
            CREATE TABLE patient_data (
                PatientID VARCHAR,
                Name VARCHAR,
                Age INTEGER,
                Gender VARCHAR,
                BloodType VARCHAR,
                Condition VARCHAR,
                VisitDate VARCHAR,
                Doctor VARCHAR,
                HospitalID VARCHAR,
                Prescription VARCHAR,
                DiagnosisReport VARCHAR
            )
            """
            temp_conn.execute(create_table_sql)
            
            # Execute the SQL content (INSERT statements)
            temp_conn.execute(sql_content)
            
            # Query the patient_data table into DataFrame
            df = temp_conn.execute("SELECT * FROM patient_data").fetchdf()
            
            # Convert data types to match expected schema
            if 'PatientID' in df.columns:
                df['PatientID'] = df['PatientID'].astype(str)
            if 'HospitalID' in df.columns:
                df['HospitalID'] = df['HospitalID'].astype(str)
            if 'Age' in df.columns:
                df['Age'] = pd.to_numeric(df['Age'], errors='coerce').astype('Int64')
            
            logger.info(f"Processed SQL file: {len(df)} rows extracted from patient_data table")
            return df
            
        finally:
            # Close the temporary connection
            temp_conn.close()
        
    except Exception as e:
        logger.error(f"Error processing SQL file: {e}")
        raise ValueError(f"Failed to process SQL file: {str(e)}")

def auto_detect_and_store_schema(df, table_name):
    """
    Auto-detect schema from a pandas DataFrame and store it in the smart contract.
    
    Args:
        df (pd.DataFrame): The DataFrame to analyze
        table_name (str): The name of the table
        
    Returns:
        dict: The detected schema
    """
    try:
        import json
        
        # Detect schema from DataFrame
        schema = {
            "table_name": table_name,
            "columns": [],
            "indexes": list(app.state.index_cids.keys()),  # Use the configured index attributes
            "row_count": len(df),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
        
        # Analyze each column
        for col_name in df.columns:
            col_info = {
                "name": col_name,
                "type": str(df[col_name].dtype),
                "nullable": bool(df[col_name].isnull().any()),  # Convert numpy.bool_ to Python bool
                "unique_values": int(df[col_name].nunique()),   # Convert numpy.int64 to Python int
                "sample_values": df[col_name].dropna().head(3).tolist() if not df[col_name].empty else []
            }
            
            # Convert numpy types to JSON-serializable types
            if col_info["type"] == "object":
                col_info["type"] = "string"
            elif "int" in col_info["type"]:
                col_info["type"] = "integer"
            elif "float" in col_info["type"]:
                col_info["type"] = "float"
            elif "bool" in col_info["type"]:
                col_info["type"] = "boolean"
            elif "datetime" in col_info["type"]:
                col_info["type"] = "datetime"
                
            schema["columns"].append(col_info)
        
        # Determine primary key (assuming PatientID if present)
        if "PatientID" in df.columns:
            schema["primary_key"] = ["PatientID"]
        else:
            schema["primary_key"] = [df.columns[0]]  # Use first column as default
        
        # Store schema in smart contract
        schema_json = json.dumps(schema)
        
        success = app.state.index_storage.update_table_schema(table_name, schema_json)
        if success:
            logger.info(f"Schema for table '{table_name}' stored in smart contract")
        else:
            logger.error(f"Failed to store schema in smart contract")
            return None
        
        return schema
        
    except Exception as e:
        logger.error(f"Failed to auto-detect and store schema: {e}")
        return None

def retrieve_index(name):
    """
    Retrieve and decrypt an index from IPFS.
    """
    cid = get_index_cid(name)
    if not cid:
        return None

    # Fetch encrypted index from IPFS
    encrypted_data = fetch_from_ipfs(cid)

    if not encrypted_data:
        return None

    # Decrypt the index data
    try:
        decrypted_data = extract_and_decrypt_package(encrypted_data, app.state.encryption_key)
        # Load the decrypted index
        index = CIDIndex()
        index.load(io.BytesIO(decrypted_data))
        return index
    except Exception as e:
        logger.error(f"Failed to decrypt index {name}: {e}")
        return None

def upload_encrypted_index(index, attr):
    """
    Serialize, encrypt, and upload an index to IPFS.
    Returns: (cid, 0, 0) - last two values for backward compatibility
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
        encrypted_index = create_encrypted_package(index_data, app.state.encryption_key)

        # Upload encrypted index to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": (f"{attr}_index.enc", encrypted_index)})
        resp.raise_for_status()

        serialized.close()
        return resp.json()["Hash"], 0, 0

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

# --- Smart Contract Integration Helper Functions ---

def get_index_cid(attribute_name):
    """
    Get the CID for a specific index attribute from smart contract.
    
    Args:
        attribute_name (str): Name of the attribute (e.g., 'PatientID', 'HospitalID', 'Age')
        
    Returns:
        str or None: The CID if found, None otherwise
    """
    try:
        success, cid = app.state.index_storage.get_index(attribute_name)
        if success:
            return cid if cid else None  # Return None for empty strings
        else:
            logger.error(f"Failed to get index CID for {attribute_name} from smart contract")
            return None
    except Exception as e:
        logger.error(f"Error getting index CID for {attribute_name}: {e}")
        return None

def set_index_cid(attribute_name, cid):
    """
    Set the CID for a specific index attribute in smart contract.
    
    Args:
        attribute_name (str): Name of the attribute
        cid (str): The CID to store
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        success = app.state.index_storage.update_index(attribute_name, cid)
        if success:
            # Also update in-memory cache
            app.state.index_cids[attribute_name] = cid
            return True
        else:
            logger.error(f"Smart contract update failed for {attribute_name}")
            return False
    except Exception as e:
        logger.error(f"Error setting index CID for {attribute_name}: {e}")
        return False

def get_all_index_cids():
    """
    Get all index CIDs as a dictionary from smart contract.
    
    Returns:
        dict: Dictionary mapping attribute names to CIDs
    """
    try:
        attribute_names = list(app.state.index_cids.keys())
        success, cid_dict = app.state.index_storage.batch_get_indices(attribute_names)
        if success:
            # Update in-memory cache and return
            for attr, cid in cid_dict.items():
                app.state.index_cids[attr] = cid if cid else None
            return app.state.index_cids
        else:
            logger.error("Failed to get index CIDs from smart contract")
            return {}
    except Exception as e:
        logger.error(f"Error getting all index CIDs: {e}")
        return {}

def set_all_index_cids(cid_dict):
    """
    Set multiple index CIDs at once in smart contract.
    
    Args:
        cid_dict (dict): Dictionary mapping attribute names to CIDs
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert dictionary to separate lists for the smart contract method
        attributes = list(cid_dict.keys())
        new_cids = list(cid_dict.values())
        success = app.state.index_storage.batch_update_indices(attributes, new_cids)
        if success:
            # Update in-memory cache
            app.state.index_cids.update(cid_dict)
            return True
        else:
            logger.error("Smart contract batch update failed")
            return False
    except Exception as e:
        logger.error(f"Error setting all index CIDs: {e}")
        return False

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


class UpdateTableSchemaRequest(BaseModel):
    table_name: str
    table_schema: dict  # The schema as a dictionary (renamed to avoid shadowing BaseModel.schema)


class BatchUpdateTableSchemasRequest(BaseModel):
    schemas: dict  # Dictionary mapping table names to schemas


class AddAccessPolicyRequest(BaseModel):
    wallet_address: str
    table_name: str
    policy_sql: str


class RemoveAccessPolicyRequest(BaseModel):
    wallet_address: str
    policy_index: int


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
            "upload": "POST /upload/patient-data (supports CSV and SQL files)",
            "index-cids": "GET /index-cids",
            "schemas": "GET /schemas, POST /schemas",
            "schema-by-table": "GET /schemas/{table_name}, DELETE /schemas/{table_name}",
            "access-policies": "POST /access-policies, GET /access-policies/{wallet_address}, DELETE /access-policies",
            "policy-count": "GET /access-policies/{wallet_address}/count",
            "remove-all-policies": "DELETE /access-policies/{wallet_address}/all",
            "docs": "GET /docs"
        },
        "file_support": {
            "csv": "Comma-separated values with headers",
            "sql": "INSERT statements for patient_data table"
        },
        "access_control": {
            "enabled": True,
            "description": "All queries require a wallet_address parameter and are filtered based on access policies stored in the smart contract"
        }
    }

@app.put("/index-cids")
async def update_index_cids(request: UpdateIndexCIDsRequest):
    """
    Update the index CIDs mapping in smart contract.

    Example request body:
    {
        "index_cids": {
            "PatientID": "QmXxxxx...",
            "HospitalID": "QmYyyyy...",
            "Age": "QmZzzzz..."
        }
    }
    """
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

        # Update the index CIDs using helper function (smart contract)
        success = set_all_index_cids(request.index_cids)
        
        if success:
            logger.info(f"Updated index CIDs in smart contract")
            return {
                "status": "success",
                "message": f"Index CIDs updated successfully in smart contract",
                "updated_cids": request.index_cids,
                "current_cids": get_all_index_cids()
            }
        else:
            return {
                "status": "error",
                "message": f"Index CIDs update failed in smart contract"
            }

    except Exception as e:
        logger.error(f"Error updating index CIDs: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/index-cids")
async def get_index_cids():
    """
    Get the current index CIDs mapping along with index sizes from smart contract.

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
    logger.info("GET /index-cids - Retrieving current index CIDs")
    try:
        return {
            "status": "success",
            "index_cids": get_all_index_cids(),  # Get from smart contract
            "index_sizes": app.state.index_sizes,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
    except Exception as e:
        logger.error(f"Error retrieving index CIDs: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/schemas")
async def create_or_update_table_schema(request: UpdateTableSchemaRequest):
    """
    Create or update a table schema in the smart contract.
    
    Example request body:
    {
        "table_name": "patient_data",
        "table_schema": {
            "columns": [
                {"name": "PatientID", "type": "string", "nullable": false},
                {"name": "Name", "type": "string", "nullable": false},
                {"name": "Age", "type": "integer", "nullable": true},
                {"name": "Gender", "type": "string", "nullable": true},
                {"name": "BloodType", "type": "string", "nullable": true},
                {"name": "Condition", "type": "string", "nullable": true},
                {"name": "VisitDate", "type": "string", "nullable": true},
                {"name": "Doctor", "type": "string", "nullable": true},
                {"name": "HospitalID", "type": "string", "nullable": false},
                {"name": "Prescription", "type": "string", "nullable": true},
                {"name": "DiagnosisReport", "type": "string", "nullable": true}
            ],
            "primary_key": ["PatientID"],
            "indexes": ["PatientID", "HospitalID", "Age"]
        }
    }
    """
    logger.info(f"POST /schemas - Creating/updating schema for table: {request.table_name}")
    
    try:
        import json
        schema_json = json.dumps(request.table_schema)
        logger.info(f"Schema JSON length: {len(schema_json)} characters")
        
        # Store in smart contract
        logger.info(f"Attempting to store schema in smart contract for table: {request.table_name}")
        success = app.state.index_storage.update_table_schema(request.table_name, schema_json)
        logger.info(f"Smart contract update result: {success}")
        
        if success:
            return {
                "status": "success",
                "message": f"Schema for table '{request.table_name}' updated successfully in smart contract",
                "table_name": request.table_name,
                "table_schema": request.table_schema
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to update schema for table '{request.table_name}' in smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error updating table schema: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/schemas")
async def get_all_table_schemas():
    """
    Get all table schemas stored in the smart contract.
    
    Returns:
    {
        "status": "success",
        "schemas": {
            "patient_data": {
                "columns": [...],
                "primary_key": [...],
                "indexes": [...]
            },
            ...
        },
        "smart_contract_enabled": true
    }
    """
    logger.info("GET /schemas - Retrieving all table schemas")
    
    try:
        schemas = {}
        
        # Get known table names (you may want to maintain a list or discover them)
        # For now, we'll assume patient_data is the main table
        known_tables = ["patient_data"]  # You can extend this or make it dynamic
        
        success, schema_dict = app.state.index_storage.batch_get_table_schemas(known_tables)
        if success:
            import json
            for table_name, schema_json in schema_dict.items():
                if schema_json:  # Only include non-empty schemas
                    try:
                        schemas[table_name] = json.loads(schema_json)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON schema for table {table_name}")
        else:
            return {
                "status": "error",
                "message": "Failed to retrieve schemas from smart contract"
            }
        
        return {
            "status": "success",
            "schemas": schemas,
            "storage_type": "smart contract",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
    
    except Exception as e:
        logger.error(f"Error retrieving table schemas: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/schemas/{table_name}")
async def get_table_schema(table_name: str):
    """
    Get the schema for a specific table from smart contract.
    
    Returns:
    {
        "status": "success",
        "table_name": "patient_data",
        "schema": {
            "columns": [...],
            "primary_key": [...],
            "indexes": [...]
        },
        "smart_contract_enabled": true
    }
    """
    logger.info(f"GET /schemas/{table_name} - Retrieving schema for table: {table_name}")
    
    try:
        schema = None
        
        # Get from smart contract
        success, schema_json = app.state.index_storage.get_table_schema(table_name)
        if success and schema_json:
            import json
            try:
                schema = json.loads(schema_json)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON schema for table {table_name}")
        elif not success:
            return {
                "status": "error",
                "message": f"Failed to retrieve schema for table '{table_name}' from smart contract"
            }
        
        if schema:
            return {
                "status": "success",
                "table_name": table_name,
                "schema": schema,
                "storage_type": "smart contract"
            }
        else:
            return {
                "status": "not_found",
                "message": f"Schema for table '{table_name}' not found",
                "table_name": table_name
            }
    
    except Exception as e:
        logger.error(f"Error retrieving schema for table {table_name}: {e}")
        return {"status": "error", "message": str(e)}

@app.delete("/schemas/{table_name}")
async def delete_table_schema(table_name: str):
    """
    Delete the schema for a specific table from smart contract.
    """
    logger.info(f"DELETE /schemas/{table_name} - Deleting schema for table: {table_name}")
    
    try:
        # Remove from smart contract
        success = app.state.index_storage.remove_table_schema(table_name)
        
        if success:
            return {
                "status": "success",
                "message": f"Schema for table '{table_name}' deleted successfully from smart contract",
                "table_name": table_name
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to delete schema for table '{table_name}' or schema not found"
            }
    
    except Exception as e:
        logger.error(f"Error deleting schema for table {table_name}: {e}")
        return {"status": "error", "message": str(e)}

# Access Policy Management Endpoints

@app.post("/access-policies")
async def add_access_policy(request: AddAccessPolicyRequest):
    """
    Add an access policy for a wallet address.
    
    Example request body:
    {
        "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "table_name": "patient_data",
        "policy_sql": "SELECT * FROM patient_data WHERE PatientID = '38'"
    }
    """
    logger.info(f"POST /access-policies - Adding access policy for wallet: {request.wallet_address}")
    
    try:
        # Store in smart contract
        success = app.state.index_storage.add_access_policy(
            request.wallet_address, 
            request.table_name, 
            request.policy_sql
        )
        
        if success:
            return {
                "status": "success",
                "message": f"Access policy added successfully in smart contract",
                "wallet_address": request.wallet_address,
                "table_name": request.table_name,
                "policy_sql": request.policy_sql
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to add access policy in smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error adding access policy: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/access-policies/{wallet_address}")
async def get_access_policies(wallet_address: str):
    """
    Get all access policies for a wallet address from smart contract.
    
    Returns:
    {
        "status": "success",
        "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "policies": [
            {
                "ownerAddress": "0x123...",
                "tableName": "patient_data",
                "policySql": "SELECT * FROM patient_data WHERE PatientID = '38'"
            }
        ],
        "smart_contract_enabled": true
    }
    """
    logger.info(f"GET /access-policies/{wallet_address} - Retrieving access policies")
    
    try:
        # Get from smart contract
        success, policies = app.state.index_storage.get_access_policies(wallet_address)
        
        if success:
            return {
                "status": "success",
                "wallet_address": wallet_address,
                "policies": policies,
                "policy_count": len(policies),
                "storage_type": "smart contract",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to retrieve access policies from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error retrieving access policies for {wallet_address}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/access-policies/{wallet_address}/count")
async def get_policy_count(wallet_address: str):
    """
    Get the count of policies for a wallet address from smart contract.
    
    Returns:
    {
        "status": "success",
        "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "count": 3
    }
    """
    logger.info(f"GET /access-policies/{wallet_address}/count - Getting policy count")
    
    try:
        # Get from smart contract
        success, count = app.state.index_storage.get_policy_count(wallet_address)
        
        if success:
            return {
                "status": "success",
                "wallet_address": wallet_address,
                "count": count,
                "storage_type": "smart contract"
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to retrieve policy count from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error getting policy count for {wallet_address}: {e}")
        return {"status": "error", "message": str(e)}

@app.delete("/access-policies")
async def remove_access_policy(request: RemoveAccessPolicyRequest):
    """
    Remove a specific access policy by index from smart contract.
    
    Example request body:
    {
        "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "policy_index": 0
    }
    """
    logger.info(f"DELETE /access-policies - Removing policy index {request.policy_index} for wallet: {request.wallet_address}")
    
    try:
        # Remove from smart contract
        success = app.state.index_storage.remove_access_policy(
            request.wallet_address, 
            request.policy_index
        )
        
        if success:
            return {
                "status": "success",
                "message": f"Access policy removed successfully from smart contract",
                "wallet_address": request.wallet_address,
                "policy_index": request.policy_index
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to remove access policy or invalid policy index"
            }
    
    except Exception as e:
        logger.error(f"Error removing access policy: {e}")
        return {"status": "error", "message": str(e)}

@app.delete("/access-policies/{wallet_address}/all")
async def remove_all_access_policies(wallet_address: str):
    """
    Remove all access policies for a wallet address from smart contract.
    """
    logger.info(f"DELETE /access-policies/{wallet_address}/all - Removing all policies for wallet")
    
    try:
        # Remove from smart contract
        success = app.state.index_storage.remove_all_access_policies(wallet_address)
        
        if success:
            return {
                "status": "success",
                "message": f"All access policies removed successfully from smart contract",
                "wallet_address": wallet_address
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to remove all access policies from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error removing all access policies for {wallet_address}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/query/count")
async def get_row_count():
    """
    Get the total number of rows in the patient_data table by fetching all CIDs 
    and performing a COUNT(*) query.
    
    Returns:
    {
        "status": "success",
        "total_rows": 123456,
        "cids_processed": 25
    }
    """
    logger.info("GET /query/count - Getting total row count from database")
    
    try:
        # Get all available index CIDs to find all data
        all_index_cids = get_all_index_cids()
        
        # Use PatientID index to get all CIDs (since PatientID should cover all data)
        # If PatientID index is not available, try other indexes
        index_attr = None
        for attr in ['PatientID', 'HospitalID', 'Age']:
            if all_index_cids.get(attr):
                index_attr = attr
                break
        
        if not index_attr:
            return {
                "status": "error", 
                "message": "No indexes available to retrieve data CIDs"
            }
        
        # Retrieve and decrypt index
        index = retrieve_index(index_attr)
        
        if not index:
            return {
                "status": "error", 
                "message": f"Index for {index_attr} not found"
            }
        
        # Get all CIDs from the index (no WHERE clause filtering)
        all_cids = index.query_range()  # Get all CIDs without any filtering
        
        if not all_cids:
            return {
                "status": "success",
                "total_rows": 0,
                "cids_processed": 0,
                "message": "No data found in database"
            }
        
        # Fetch all CIDs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
            encrypted_data_list = list(executor.map(fetch_from_ipfs, all_cids))
        
        # Decrypt all data sequentially
        paths = []
        for cid, encrypted_data in zip(all_cids, encrypted_data_list):
            if encrypted_data:
                path = decrypt_to_file(encrypted_data, cid, app.state.encryption_key)
                if path:
                    paths.append(path)
        
        if not paths:
            return {
                "status": "error", 
                "message": "No valid Parquet files retrieved"
            }
        
        # Execute COUNT(*) query using DuckDB
        try:
            if len(paths) == 1:
                count_query = f"SELECT COUNT(*) as total_rows FROM '{paths[0]}'"
                result = duckdb_conn.execute(count_query)
            else:
                # Use glob pattern for multiple files
                glob_pattern = os.path.join(SHARED_TMP_DIR, "*.parquet")
                count_query = f"SELECT COUNT(*) as total_rows FROM read_parquet('{glob_pattern}')"
                result = duckdb_conn.execute(count_query)
            
            # Get the count result
            row = result.fetchone()
            total_rows = row[0] if row else 0
            
        except Exception as e:
            logger.error(f"Count query error: {e}")
            return {"status": "error", "message": f"Count query execution failed: {str(e)}"}
        finally:
            # Delete temporary files
            for p in paths:
                try:
                    os.remove(p)
                except Exception as e:
                    logger.warning(f"Failed to delete {p}: {e}")
        
        return {
            "status": "success",
            "total_rows": total_rows,
            "cids_processed": len(all_cids),
            "index_used": index_attr
        }
        
    except Exception as e:
        logger.error(f"Error getting row count: {e}")
        return {"status": "error", "message": str(e)}

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