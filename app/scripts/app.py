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
import hashlib
import uuid
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
    'deletion_log': None,  # Track deletion operations
}
app.state.index_sizes = {}
app.state.deletion_stats = {
    'total_deletions': 0,
    'last_deletion': None
}

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

class DeleteRequest(BaseModel):
    delete_query: str  # "DELETE FROM patient_data WHERE PatientID = '323'"
    wallet_address: str  # Required wallet address for access control

def rewrite_query_with_access_policies(original_query: str, policies: List[dict], table_name: str = "patient_data") -> str:
    """
    Rewrite the original query to incorporate access control policies with subject validation.
    
    For multi-tenant security, each policy condition is combined with OwnerID = subject
    to ensure the querier can only access data owned by the policy subject.
    
    Args:
        original_query (str): The original SQL query
        policies (List[dict]): List of access policies with 'subject', 'object', 'policySql' fields
        table_name (str): The table name to apply policies to
        
    Returns:
        str: The rewritten query with access control and subject validation
    """
    if not policies:
        return ""  # Return empty query if no policies
    
    # Extract valid policy SQLs and analyze them
    policy_conditions = []
    
    for policy in policies:
        policy_sql = policy.get('policySql', '').strip()
        subject = policy.get('subject', '').strip()
        
        if policy_sql and subject:
            # Extract the WHERE clause from each policy SQL
            policy_sql_lower = policy_sql.lower()
            
            if 'where' in policy_sql_lower:
                # Find the WHERE clause
                where_index = policy_sql_lower.find('where')
                condition = policy_sql[where_index + 5:].strip()  # +5 for "where"
                # Combine subject validation with policy condition
                policy_conditions.append(f"(OwnerID = '{subject}' AND {condition})")
            else:
                # If no WHERE clause, this policy allows all data for this subject
                policy_conditions.append(f"(OwnerID = '{subject}')")
    
    if not policy_conditions:
        return ""  # Return empty query if no valid policies
    
    # Combine all conditions with OR
    combined_condition = " OR ".join(policy_conditions)
    
    # Create the accessible_part CTE with all columns from original table
    # and the combined WHERE condition with subject validation
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
        "policies_applied": [
            {
                "subject": p.get('subject'), 
                "object": p.get('object'), 
                "table": p.get('tableName'), 
                "original_sql": p.get('policySql'),
                "enforced_condition": f"OwnerID = '{p.get('subject')}' AND ({p.get('policySql', '').split('WHERE')[-1].strip() if 'WHERE' in p.get('policySql', '').upper() else '1=1'})"
            } for p in policies
        ],
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

def execute_schema_sql(table_name: str, connection=None) -> bool:
    """
    Retrieve and execute schema SQL from smart contract for a given table.
    
    Args:
        table_name (str): Name of the table
        connection: DuckDB connection to use (defaults to global connection)
        
    Returns:
        bool: True if schema was successfully executed, False otherwise
    """
    try:
        # Use provided connection or default to global connection
        conn = connection if connection else duckdb_conn
        
        # Get schema SQL from smart contract
        success, schema_sql = app.state.index_storage.get_table_schema(table_name)
        
        if success and schema_sql:
            # Execute the CREATE TABLE statement
            conn.execute(schema_sql)
            logger.info(f"Successfully executed schema SQL for table '{table_name}'")
            return True
        else:
            logger.error(f"Failed to retrieve schema SQL for table '{table_name}' from smart contract")
            return False
            
    except Exception as e:
        logger.error(f"Error executing schema SQL for table '{table_name}': {e}")
        return False

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
            # Try to get schema from smart contract first
            create_table_sql = None
            try:
                success, schema_sql = app.state.index_storage.get_table_schema("patient_data")
                if success and schema_sql:
                    create_table_sql = schema_sql
                    logger.info("Using schema from smart contract")
                else:
                    logger.warning("Schema not found in smart contract, using fallback")
            except Exception as e:
                logger.warning(f"Failed to retrieve schema from smart contract: {e}")
            
            # Fallback to hardcoded schema if smart contract schema is not available
            if not create_table_sql:
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
                logger.info("Using fallback hardcoded schema")
            
            # Create the patient_data table using the retrieved or fallback schema
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
    Auto-detect schema from a pandas DataFrame and store it as SQL CREATE TABLE statement in the smart contract.
    
    Args:
        df (pd.DataFrame): The DataFrame to analyze
        table_name (str): The name of the table
        
    Returns:
        str: The generated SQL CREATE TABLE statement
    """
    try:
        # Build SQL CREATE TABLE statement
        columns_sql = []
        
        for col_name in df.columns:
            col_dtype = str(df[col_name].dtype)
            
            # Map pandas dtypes to SQL types
            if col_dtype == "object":
                sql_type = "VARCHAR"
            elif "int" in col_dtype:
                sql_type = "INTEGER"
            elif "float" in col_dtype:
                sql_type = "DOUBLE"
            elif "bool" in col_dtype:
                sql_type = "BOOLEAN"
            elif "datetime" in col_dtype:
                sql_type = "TIMESTAMP"
            else:
                sql_type = "VARCHAR"  # Default fallback
            
            columns_sql.append(f"    {col_name} {sql_type}")
        
        # Generate CREATE TABLE statement
        create_table_sql = f"CREATE TABLE {table_name} (\n" + ",\n".join(columns_sql) + "\n)"
        
        # Store schema in smart contract
        success = app.state.index_storage.update_table_schema(table_name, create_table_sql)
        if success:
            logger.info(f"SQL schema for table '{table_name}' stored in smart contract")
        else:
            logger.error(f"Failed to store SQL schema in smart contract")
            return None
        
        return create_table_sql
        
    except Exception as e:
        logger.error(f"Failed to auto-detect and store SQL schema: {e}")
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
    schema_sql: str  # The schema as SQL CREATE TABLE statement


class BatchUpdateTableSchemasRequest(BaseModel):
    schemas: dict  # Dictionary mapping table names to schemas


class AddAccessPolicyRequest(BaseModel):
    subject_address: str  # The policy creator/owner address
    object_address: str  # The querier address (who the policy applies to)
    table_name: str
    policy_sql: str


class RemoveAccessPolicyRequest(BaseModel):
    object_address: str  # The querier address (who the policy applies to)
    policy_index: int


# DELETE Query Processing Functions
def apply_where_clause_to_dataframe(df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
    """
    Apply WHERE clause directly to DataFrame for fast filtering
    Optimized for common DELETE patterns
    """
    try:
        # Handle simple equality conditions: PatientID = 'value'
        if "PatientID = " in where_clause:
            import re
            match = re.search(r"PatientID\s*=\s*['\"]([^'\"]+)['\"]?", where_clause)
            if match:
                patient_id = match.group(1)
                return df[df['PatientID'] == patient_id]
        
        # Handle age comparisons: Age > 95, Age < 30, etc.
        elif "Age " in where_clause:
            import re
            match = re.search(r"Age\s*(>=|<=|>|<|=)\s*(\d+)", where_clause)
            if match:
                operator = match.group(1)
                age_value = int(match.group(2))
                
                if operator == "=":
                    return df[df['Age'] == age_value]
                elif operator == ">":
                    return df[df['Age'] > age_value]
                elif operator == "<":
                    return df[df['Age'] < age_value]
                elif operator == ">=":
                    return df[df['Age'] >= age_value]
                elif operator == "<=":
                    return df[df['Age'] <= age_value]
        
        # Handle HospitalID conditions
        elif "HospitalID = " in where_clause:
            import re
            match = re.search(r"HospitalID\s*=\s*['\"]([^'\"]+)['\"]?", where_clause)
            if match:
                hospital_id = match.group(1)
                return df[df['HospitalID'] == hospital_id]
        
        # For complex conditions, fall back to empty DataFrame (safe approach)
        else:
            logger.warning(f"Complex WHERE clause not optimized: {where_clause}")
            return pd.DataFrame()  # Return empty for safety
            
    except Exception as e:
        logger.error(f"Error applying WHERE clause {where_clause}: {e}")
        return pd.DataFrame()
    
    return df

def parse_delete_query(delete_query: str):
    """
    Parse DELETE query to extract table name and WHERE conditions
    DELETE FROM patient_data WHERE PatientID = '323'
    """
    import re
    
    # Extract table name
    table_match = re.search(r'DELETE\s+FROM\s+(\w+)', delete_query, re.IGNORECASE)
    table_name = table_match.group(1) if table_match else None
    
    # Extract WHERE clause
    where_match = re.search(r'WHERE\s+(.*)', delete_query, re.IGNORECASE | re.DOTALL)
    where_clause = where_match.group(1).strip() if where_match else None
    
    # Extract primary key condition (assuming PatientID is primary key)
    primary_key_value = None
    if where_clause:
        # Look for PatientID = 'value' or PatientID = "value"
        pk_match = re.search(r'PatientID\s*=\s*[\'"]([^\'"]+)[\'"]', where_clause, re.IGNORECASE)
        if pk_match:
            primary_key_value = pk_match.group(1)
    
    return table_name, where_clause, primary_key_value

async def find_cids_containing_records(where_clause: str, index_attribute: str = 'PatientID'):
    """
    Find all CIDs that contain records matching the WHERE clause
    """
    # Retrieve and decrypt index
    index = retrieve_index(index_attribute)
    if not index:
        return []
    
    # Use existing query_index function to find relevant CIDs
    dummy_query = f"SELECT * FROM patient_data WHERE {where_clause}"
    cids = query_index(index, dummy_query, index_attribute)
    return cids

def process_cid_for_deletion(cid: str, where_clause: str, wallet_address: str):
    """
    Process a single CID: decrypt, apply deletion, re-encrypt, and return new CID
    Note: This function is synchronous to work with ThreadPoolExecutor
    Simplified and optimized version
    """
    try:
        # Fetch and decrypt data from IPFS (same as upload)
        encrypted_data = fetch_from_ipfs(cid)
        if not encrypted_data:
            logger.error(f"Failed to fetch CID {cid}")
            return None, [], []
        
        decrypted_data = extract_and_decrypt_package(encrypted_data, app.state.encryption_key)
        
        # Load as DataFrame
        df = pd.read_parquet(io.BytesIO(decrypted_data))
        original_count = len(df)
        logger.info(f"Processing CID {cid} with {original_count} records")
        
        # Get all records that will be affected (for index updates)
        all_records_in_cid = df.to_dict('records')
        
        # SIMPLIFIED: Apply access control directly on DataFrame
        # Filter by wallet's OwnerID (much faster than temp files)
        accessible_df = df[df['OwnerID'] == wallet_address]
        
        if len(accessible_df) == 0:
            logger.info(f"No accessible records in CID {cid} for wallet {wallet_address}")
            return None, all_records_in_cid, []
        
        # Apply WHERE clause directly on accessible DataFrame
        deletable_df = apply_where_clause_to_dataframe(accessible_df, where_clause)
        
        if len(deletable_df) == 0:
            logger.info(f"No records match deletion criteria in CID {cid}")
            return None, all_records_in_cid, []
        
        deletable_records = deletable_df.to_dict('records')
        deletable_patient_ids = set(deletable_df['PatientID'])
        
        # Filter out the deletable records from the original DataFrame
        filtered_df = df[~df['PatientID'].isin(deletable_patient_ids)]
        deleted_count = original_count - len(filtered_df)
        
        logger.info(f"Deleted {deleted_count} records from CID {cid}, {len(filtered_df)} records remaining")
        
        # If all records are deleted, return empty indicators
        if len(filtered_df) == 0:
            logger.info(f"All records deleted from CID {cid}")
            return "EMPTY", all_records_in_cid, deletable_records
        
        # Convert to Parquet directly (same as upload process)
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(filtered_df), buffer)
        buffer.seek(0)
        new_parquet_data = buffer.read()
        
        # Encrypt the new data (same as upload)
        encrypted_package = create_encrypted_package(new_parquet_data, app.state.encryption_key)
        
        # Upload to IPFS (same as upload)
        resp = requests.post("http://localhost:5001/api/v0/add", 
                           files={"file": (f"filtered_data_{int(time.time())}.enc", encrypted_package)})
        resp.raise_for_status()
        new_cid = resp.json()["Hash"]
        
        buffer.close()
        return new_cid, all_records_in_cid, deletable_records
        
    except Exception as e:
        logger.error(f"Error processing CID {cid} for deletion: {e}")
        return None, [], []

async def update_indexes_after_deletion(old_cid: str, new_cid: str, all_records: List[dict], deleted_records: List[dict]):
    """
    Update all indexes after deletion operation
    CRITICAL FIX: Rebuild indexes to remove old CID references completely
    """
    index_cids_to_update = {}
    
    for attr in ['PatientID', 'HospitalID', 'Age']:
        if attr not in app.state.index_cids:
            continue
            
        try:
            logger.info(f"Rebuilding index for {attr} - removing old CID {old_cid}")
            
            # Get current index
            existing_index = retrieve_index(attr)
            if not existing_index:
                logger.warning(f"Index for {attr} not found, skipping")
                continue
            
            # SOLUTION: Completely rebuild the index excluding the old CID
            new_index_data = []
            
            # Get all unique values that need to be re-indexed
            unique_values = set()
            for record in all_records:
                if attr in record and record[attr] is not None:
                    attr_value = record[attr]
                    if attr == 'Age' and isinstance(attr_value, (int, float)):
                        attr_value = int(attr_value)
                    elif isinstance(attr_value, (int, float)):
                        attr_value = str(attr_value)
                    unique_values.add(attr_value)
            
            # For each unique value, get current CIDs and filter out the old one
            for value in unique_values:
                try:
                    current_cids = existing_index.query(value)
                    # Remove the old CID from the list
                    filtered_cids = [cid for cid in current_cids if cid != old_cid]
                    
                    # Add the new CID if this value still has records after deletion
                    value_has_remaining_records = False
                    for record in all_records:
                        if (attr in record and 
                            record[attr] == value and 
                            not any(dr.get('PatientID') == record.get('PatientID') for dr in deleted_records)):
                            value_has_remaining_records = True
                            break
                    
                    if value_has_remaining_records and new_cid and new_cid != "EMPTY":
                        if new_cid not in filtered_cids:
                            filtered_cids.append(new_cid)
                    
                    # Add all valid CIDs for this value to the new index
                    for cid in filtered_cids:
                        new_index_data.append((value, cid))
                        
                except Exception as e:
                    logger.error(f"Error processing value {value} for {attr}: {e}")
                    continue
            
            # Create completely new index with filtered data
            if new_index_data:
                new_index = CIDIndex(new_index_data)
            else:
                new_index = CIDIndex()  # Empty index
            
            # Upload the rebuilt index
            index_cid, _, _ = upload_encrypted_index(new_index, attr)
            index_cids_to_update[attr] = index_cid
            logger.info(f"Rebuilt clean index for {attr}: {index_cid}")
            
        except Exception as e:
            logger.error(f"Error rebuilding index for {attr}: {e}")
            continue
    
    # Batch update smart contract (same as upload API)
    if index_cids_to_update:
        batch_update_success = set_all_index_cids(index_cids_to_update)
        if batch_update_success:
            logger.info(f"Batch updated {len(index_cids_to_update)} index CIDs in smart contract")
            return True
        else:
            logger.error("Batch update to smart contract failed")
            return False
    
    return True

@app.post("/delete")
async def delete_records(request: DeleteRequest):
    """
    DELETE FROM patient_data WHERE PatientID = '323'
    Process deletion by creating new versions of affected CIDs without deleted records
    """
    logger.info(f"POST /delete - Processing DELETE query for wallet: {request.wallet_address}")
    
    operation_start_time = time.time()
    
    try:
        # Parse the DELETE query
        table_name, where_clause, primary_key_value = parse_delete_query(request.delete_query)
        
        if not table_name or not where_clause:
            return {"error": "Invalid DELETE query. Expected format: DELETE FROM table_name WHERE condition"}
        
        if table_name.lower() != 'patient_data':
            return {"error": f"Unsupported table: {table_name}. Only 'patient_data' is supported."}
        
        logger.info(f"Parsed DELETE query - Table: {table_name}, WHERE: {where_clause}, Primary Key: {primary_key_value}")
        
        # Check access policies
        success, policies = app.state.index_storage.get_access_policies(request.wallet_address)
        if not success:
            return {"error": "Failed to fetch access policies from smart contract"}
        
        if not policies:
            return {
                "error": "No access policies found for this wallet address",
                "wallet_address": request.wallet_address
            }
        
        # Find all CIDs that might contain records matching the WHERE clause
        index_attribute = 'PatientID'  # Use PatientID as primary index
        try:
            relevant_cids = await find_cids_containing_records(where_clause, index_attribute)
        except Exception as e:
            logger.error(f"Error finding CIDs: {e}")
            return {"error": f"Failed to find relevant CIDs: {str(e)}"}
        
        if not relevant_cids:
            return {
                "message": "No records found matching the DELETE criteria",
                "deleted_count": 0,
                "affected_cids": 0
            }
        
        logger.info(f"Found {len(relevant_cids)} CIDs that might contain matching records")
        
        # Process each CID: decrypt, filter, re-encrypt
        processed_results = []
        total_deleted_count = 0
        
        logger.info(f"Starting processing of {len(relevant_cids)} CIDs...")
        start_time = time.time()
        
        # Use simpler parallel processing to avoid hanging
        try:
            # Use ThreadPoolExecutor with reduced worker count
            max_workers = min(4, len(relevant_cids))  # Conservative worker count
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all CID processing tasks
                future_to_cid = {
                    executor.submit(process_cid_for_deletion, cid, where_clause, request.wallet_address): cid 
                    for cid in relevant_cids
                }
                
                # Collect results with shorter timeout
                for future in concurrent.futures.as_completed(future_to_cid, timeout=30):
                    cid = future_to_cid[future]
                    try:
                        new_cid, all_records, deleted_records = future.result(timeout=15)
                        if new_cid is not None:  # Successfully processed
                            processed_results.append({
                                'old_cid': cid,
                                'new_cid': new_cid,
                                'all_records': all_records,
                                'deleted_records': deleted_records
                            })
                            total_deleted_count += len(deleted_records)
                            logger.info(f"CID {cid} -> {new_cid}, deleted {len(deleted_records)} records")
                    except Exception as e:
                        logger.error(f"Error processing CID {cid}: {e}")
        
        except concurrent.futures.TimeoutError:
            logger.error("CID processing timed out")
            return {"error": "DELETE operation timed out"}
        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            return {"error": f"CID processing failed: {str(e)}"}
        
        processing_time = time.time() - start_time
        logger.info(f"CID processing completed in {processing_time:.2f} seconds")
        
        if not processed_results:
            return {
                "message": "No records were deleted (possibly due to access control restrictions)",
                "deleted_count": 0,
                "affected_cids": 0
            }
        
        # Update indexes for all processed CIDs with timeout
        logger.info("Updating indexes after deletion...")
        index_update_success = True
        
        try:
            # Use asyncio timeout to prevent hanging
            import asyncio
            
            async def update_all_indexes():
                success = True
                for result in processed_results:
                    individual_success = await update_indexes_after_deletion(
                        result['old_cid'], 
                        result['new_cid'], 
                        result['all_records'], 
                        result['deleted_records']
                    )
                    if not individual_success:
                        success = False
                        logger.error(f"Failed to update indexes for CID {result['old_cid']}")
                return success
            
            # Apply timeout to index updates
            index_update_success = await asyncio.wait_for(update_all_indexes(), timeout=30.0)
            
        except asyncio.TimeoutError:
            logger.error("Index update timed out after 30 seconds")
            index_update_success = False
        except Exception as e:
            logger.error(f"Index update failed: {e}")
            index_update_success = False
        
        # Update deletion statistics
        app.state.deletion_stats['total_deletions'] += total_deleted_count
        app.state.deletion_stats['last_deletion'] = time.time()
        
        # Prepare response with timing and debug information
        cid_mapping = {
            result['old_cid']: result['new_cid'] for result in processed_results
        }
        
        # Collect debug information
        deleted_patient_ids = []
        for result in processed_results:
            for record in result['deleted_records']:
                if 'PatientID' in record:
                    deleted_patient_ids.append(record['PatientID'])
        
        return {
            "message": "DELETE operation completed successfully",
            "deleted_count": total_deleted_count,
            "affected_cids": len(processed_results),
            "cid_mapping": cid_mapping,
            "wallet_address": request.wallet_address,
            "query": request.delete_query,
            "index_update_success": index_update_success,
            "policy_count": len(policies),
            "deletion_stats": app.state.deletion_stats,
            "performance": {
                "cid_processing_time_seconds": processing_time,
                "records_processed": total_deleted_count
            },
            "debug_info": {
                "deleted_patient_ids": deleted_patient_ids,
                "old_cids_replaced": list(cid_mapping.keys()),
                "new_cids_created": list(cid_mapping.values())
            }
        }
        
    except Exception as e:
        logger.error(f"DELETE operation error: {e}")
        return {"error": f"DELETE operation failed: {str(e)}"}

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
            "delete": "POST /delete (requires wallet_address for access control)",
            "upload": "POST /upload/patient-data (supports CSV and SQL files)",
            "index-cids": "GET /index-cids",
            "schemas": "GET /schemas, POST /schemas",
            "schema-tables": "GET /schemas/tables",
            "schema-by-table": "GET /schemas/{table_name}, DELETE /schemas/{table_name}",
            "access-policies": "POST /access-policies, GET /access-policies/{object_address}, DELETE /access-policies",
            "policy-count": "GET /access-policies/{object_address}/count",
            "remove-all-policies": "DELETE /access-policies/{object_address}/all",
            "docs": "GET /docs"
        },
        "file_support": {
            "csv": "Comma-separated values with headers",
            "sql": "INSERT statements for patient_data table"
        },
        "schema_storage": {
            "format": "SQL CREATE TABLE statements",
            "description": "Schemas are stored as executable SQL DDL statements in the smart contract"
        },
        "access_control": {
            "enabled": True,
            "description": "All queries require a wallet_address parameter and are filtered based on access policies stored in the smart contract. Multi-tenant security ensures users can only access data where OwnerID matches the policy subject.",
            "enforcement": "Query rewriting with CTE combining OwnerID = subject AND policy conditions",
            "example": "WITH accessible_part AS (SELECT * FROM table WHERE (OwnerID = 'subject1' AND condition1) OR (OwnerID = 'subject2' AND condition2)) SELECT * FROM accessible_part"
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
        "schema_sql": "CREATE TABLE patient_data (PatientID VARCHAR PRIMARY KEY, Name VARCHAR, Age INTEGER, Gender VARCHAR, BloodType VARCHAR, Condition VARCHAR, VisitDate VARCHAR, Doctor VARCHAR, HospitalID VARCHAR, Prescription VARCHAR, DiagnosisReport VARCHAR)"
    }
    """
    logger.info(f"POST /schemas - Creating/updating schema for table: {request.table_name}")
    
    try:
        # Validate SQL syntax by attempting to parse it with DuckDB
        temp_conn = duckdb.connect()
        try:
            # Test the SQL by executing it in a temporary connection
            temp_conn.execute(request.schema_sql)
            logger.info("Schema SQL validated successfully")
        except Exception as sql_error:
            logger.error(f"Invalid SQL schema: {sql_error}")
            return {
                "status": "error",
                "message": f"Invalid SQL schema: {str(sql_error)}"
            }
        finally:
            temp_conn.close()
        
        # Store SQL schema directly in smart contract
        logger.info(f"Attempting to store SQL schema in smart contract for table: {request.table_name}")
        success = app.state.index_storage.update_table_schema(request.table_name, request.schema_sql)
        logger.info(f"Smart contract update result: {success}")
        
        if success:
            return {
                "status": "success",
                "message": f"SQL schema for table '{request.table_name}' updated successfully in smart contract",
                "table_name": request.table_name,
                "schema_sql": request.schema_sql
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to update SQL schema for table '{request.table_name}' in smart contract"
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
            "patient_data": "CREATE TABLE patient_data (PatientID VARCHAR, Name VARCHAR, ...)",
            ...
        },
        "storage_type": "smart contract"
    }
    """
    logger.info("GET /schemas - Retrieving all table schemas")
    
    try:
        # Use the new smart contract method to get all table schemas
        success, schemas = app.state.index_storage.get_all_table_schemas()
        
        if success:
            return {
                "status": "success",
                "schemas": schemas,
                "storage_type": "smart contract (SQL format)",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
        else:
            return {
                "status": "error",
                "message": "Failed to retrieve schemas from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error retrieving table schemas: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/schemas/tables")
async def get_all_table_names():
    """
    Get all table names that have schemas stored in the smart contract.
    
    Returns:
    {
        "status": "success",
        "table_names": ["patient_data", "user_profiles", ...],
        "total_tables": 2
    }
    """
    logger.info("GET /schemas/tables - Retrieving all table names")
    
    try:
        # Use the new smart contract method to get all table names
        success, table_names = app.state.index_storage.get_all_table_names()
        
        if success:
            return {
                "status": "success",
                "table_names": table_names,
                "total_tables": len(table_names),
                "storage_type": "smart contract",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
        else:
            return {
                "status": "error",
                "message": "Failed to retrieve table names from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error retrieving table names: {e}")
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

@app.get("/schemas/{table_name}")
async def get_table_schema(table_name: str):
    """
    Get the schema for a specific table from smart contract.
    
    Returns:
    {
        "status": "success",
        "table_name": "patient_data",
        "schema_sql": "CREATE TABLE patient_data (PatientID VARCHAR, Name VARCHAR, ...)",
        "storage_type": "smart contract"
    }
    """
    logger.info(f"GET /schemas/{table_name} - Retrieving schema for table: {table_name}")
    
    try:
        schema_sql = None
        
        # Get from smart contract
        success, schema_sql = app.state.index_storage.get_table_schema(table_name)
        if not success:
            return {
                "status": "error",
                "message": f"Failed to retrieve schema for table '{table_name}' from smart contract"
            }
        
        if schema_sql:
            return {
                "status": "success",
                "table_name": table_name,
                "schema_sql": schema_sql,
                "storage_type": "smart contract (SQL format)"
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

# Access Policy Management Endpoints

@app.post("/access-policies")
async def add_access_policy(request: AddAccessPolicyRequest):
    """
    Add an access policy for an object address (querier).
    
    Example request body:
    {
        "subject_address": "0x123...",
        "object_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "table_name": "patient_data",
        "policy_sql": "SELECT * FROM patient_data WHERE PatientID = '38'"
    }
    """
    logger.info(f"POST /access-policies - Adding access policy for subject: {request.subject_address}, object: {request.object_address}")
    
    try:
        # Store in smart contract
        success = app.state.index_storage.add_access_policy(
            request.subject_address,
            request.object_address, 
            request.table_name, 
            request.policy_sql
        )
        
        if success:
            return {
                "status": "success",
                "message": f"Access policy added successfully in smart contract",
                "subject_address": request.subject_address,
                "object_address": request.object_address,
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

@app.get("/access-policies/{object_address}")
async def get_access_policies(object_address: str):
    """
    Get all access policies for an object address (querier) from smart contract.
    
    Returns:
    {
        "status": "success",
        "object_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "policies": [
            {
                "subject": "0x123...",
                "tableName": "patient_data",
                "policySql": "SELECT * FROM patient_data WHERE PatientID = '38'",
                "object": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
            }
        ],
        "smart_contract_enabled": true
    }
    """
    logger.info(f"GET /access-policies/{object_address} - Retrieving access policies")
    
    try:
        # Get from smart contract
        success, policies = app.state.index_storage.get_access_policies(object_address)
        
        if success:
            return {
                "status": "success",
                "object_address": object_address,
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
        logger.error(f"Error retrieving access policies for {object_address}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/access-policies/{object_address}/count")
async def get_policy_count(object_address: str):
    """
    Get the count of policies for an object address (querier) from smart contract.
    
    Returns:
    {
        "status": "success",
        "object_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "count": 3
    }
    """
    logger.info(f"GET /access-policies/{object_address}/count - Getting policy count")
    
    try:
        # Get from smart contract
        success, count = app.state.index_storage.get_policy_count(object_address)
        
        if success:
            return {
                "status": "success",
                "object_address": object_address,
                "count": count,
                "storage_type": "smart contract"
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to retrieve policy count from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error getting policy count for {object_address}: {e}")
        return {"status": "error", "message": str(e)}

@app.delete("/access-policies")
async def remove_access_policy(request: RemoveAccessPolicyRequest):
    """
    Remove a specific access policy by index from smart contract.
    
    Example request body:
    {
        "object_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
        "policy_index": 0
    }
    """
    logger.info(f"DELETE /access-policies - Removing policy index {request.policy_index} for object: {request.object_address}")
    
    try:
        # Remove from smart contract
        success = app.state.index_storage.remove_access_policy(
            request.object_address, 
            request.policy_index
        )
        
        if success:
            return {
                "status": "success",
                "message": f"Access policy removed successfully from smart contract",
                "object_address": request.object_address,
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

@app.delete("/access-policies/{object_address}/all")
async def remove_all_access_policies(object_address: str):
    """
    Remove all access policies for an object address (querier) from smart contract.
    """
    logger.info(f"DELETE /access-policies/{object_address}/all - Removing all policies for object")
    
    try:
        # Remove from smart contract
        success = app.state.index_storage.remove_all_access_policies(object_address)
        
        if success:
            return {
                "status": "success",
                "message": f"All access policies removed successfully from smart contract",
                "object_address": object_address
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to remove all access policies from smart contract"
            }
    
    except Exception as e:
        logger.error(f"Error removing all access policies for {object_address}: {e}")
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