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

# Global index tracking - Multi-table support using composite keys: "table.attribute"
# Default table configuration for backward compatibility
app.state.default_table = 'patient_data'
app.state.default_indexed_attributes = ['PatientID', 'HospitalID', 'Age']

# Index CIDs cache - dynamically populated
app.state.index_cids = {}
app.state.index_sizes = {}

# Table-specific configurations
app.state.table_configs = {
    'patient_data': {
        'indexed_attributes': ['PatientID', 'HospitalID', 'Age']
    }
}

app.state.deletion_stats = {
    'total_deletions': 0,
    'last_deletion': None
}

# Load encryption key from environment
app.state.encryption_key = base64.b64decode(os.getenv("ENCRYPTION_KEY", "AlmbEPmAR2M4o+ohmFb2oyUV1/JqdNnlG1mG9/JbUBs="))
logger.info("Loaded AES-256 encryption key")

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

@app.post("/upload/{table_name}")
async def upload_data(table_name: str, file: UploadFile = File(...)):
    """
    Upload data to any table with auto-indexing and encryption.
    
    Supports CSV/SQL formats. Auto-detects indexes or uses first column as default.
    """
    logger.info(f"POST /upload/{table_name} - Processing data upload for table: {table_name}")
    try:
        content = await file.read()
        
        # Determine file type and process accordingly
        file_extension = file.filename.lower().split('.')[-1] if file.filename else 'csv'
        
        if file_extension == 'sql':
            # Process SQL file
            df = process_sql_file(content, table_name)
        elif file_extension == 'csv':
            # Process CSV file - infer types
            df = pd.read_csv(io.BytesIO(content))
        else:
            return {"error": f"Unsupported file type: {file_extension}. Only CSV and SQL files are supported."}
        
        # Get indexed attributes for this table
        indexed_attributes = get_table_indexed_attributes(table_name)
        indexed_values = {k: set(df[k].values) for k in indexed_attributes if k in df.columns}
        
        # If no indexed attributes found in config, auto-detect from data
        if not indexed_values and len(df.columns) > 0:
            # Use first column as default index
            first_col = df.columns[0]
            indexed_values = {first_col: set(df[first_col].values)}
            register_table_config(table_name, [first_col])
            logger.info(f"Auto-registered index for {table_name}: {first_col}")
        
        # Convert to Parquet
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)
        parquet_data = buffer.read()

        # Encrypt the Parquet data
        encrypted_package = create_encrypted_package(parquet_data, app.state.encryption_key)

        # Upload encrypted data to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": (f"{table_name}_data.enc", encrypted_package)})
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
            existing_index = retrieve_index(attr, table_name)  # This now handles decryption
            if existing_index:
                existing_index.update(data_to_add)
                index = existing_index
            else:
                index = CIDIndex(data=data_to_add)

            # Upload encrypted index
            index_cid, _, _ = upload_encrypted_index(index, attr, table_name)
            # Collect index CID for batch update (use composite key)
            index_key = make_index_key(table_name, attr)
            index_cids_to_update[index_key] = index_cid
            logger.info(f"Uploaded encrypted index for {index_key}: {index_cid}")

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
            "table_name": table_name,
            "data_cid": data_cid,
            "index_cids": index_cids_to_update,  # Return the CIDs that were just uploaded
            "index_sizes": {k: v for k, v in app.state.index_sizes.items() if k.startswith(f"{table_name}.")},
            "file_type": file_extension,
            "rows_processed": rows_processed,
            "indexed_attributes": list(indexed_values.keys()),
            "message": f"Data uploaded successfully to table '{table_name}'. Supports CSV and SQL files."
        }

    except Exception as e:
        logger.error(f"Upload error for table {table_name}: {e}")
        gc.collect()
        return {"error": str(e)}


# Backward compatibility: keep original endpoint for patient_data
@app.post("/upload/patient-data")
async def upload_patient_data(file: UploadFile = File(...)):
    """[DEPRECATED] Use POST /upload/patient_data instead."""
    logger.info("POST /upload/patient-data - [DEPRECATED] Redirecting to /upload/patient_data")
    return await upload_data("patient_data", file)


class QueryRequest(BaseModel):
    table_name: str = 'patient_data'  # Table to query
    index_attribute: str = 'PatientID'
    query: str = "select * from patient_data where PatientID = 'X'"
    wallet_address: str  # Required wallet address for access control

class DeleteRequest(BaseModel):
    delete_query: str  # "DELETE FROM patient_data WHERE PatientID = '323'"
    wallet_address: str  # Required wallet address for access control

class UpdateRequest(BaseModel):
    update_query: str  # "UPDATE patient_data SET Name = 'John Doe', Age = 30 WHERE PatientID = '323'"
    wallet_address: str  # Required wallet address for access control

def rewrite_query_with_access_policies(original_query: str, policies: List[dict], table_name: str) -> str:
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
    logger.info(f"POST /query - Processing query for table '{request.table_name}' with access control")

    # Step 1: Fetch access policies for the wallet address
    try:
        success, policies = app.state.index_storage.get_access_policies(request.wallet_address)
        if not success:
            logger.error(f"Failed to fetch access policies from smart contract for {request.wallet_address}")
            return {"error": "Failed to fetch access policies from smart contract"}
    except Exception as e:
        logger.error(f"Error fetching access policies: {e}")
        return {"error": f"Error fetching access policies: {str(e)}"}
    
    # Step 2: Filter policies for the requested table
    table_policies = [p for p in policies if p.get('tableName') == request.table_name]
    
    # Step 3: If no policies found for this table, return no data
    if not table_policies:
        logger.info(f"No access policies found for wallet {request.wallet_address} on table {request.table_name}, returning no data")
        return {
            "message": f"No access policies found for this wallet address on table '{request.table_name}'",
            "wallet_address": request.wallet_address,
            "table_name": request.table_name,
            "policy_count": 0,
            "records": 0,
            "results": []
        }
    
    # Step 4: Rewrite query with access policies
    rewritten_query = rewrite_query_with_access_policies(request.query, table_policies, request.table_name)
    
    if not rewritten_query:
        logger.warning(f"Failed to rewrite query with access policies for wallet {request.wallet_address}")
        return {
            "error": "Failed to create access-controlled query",
            "wallet_address": request.wallet_address,
            "table_name": request.table_name,
            "policy_count": len(table_policies)
        }
    
    logger.info(f"Rewritten query for wallet {request.wallet_address}: {rewritten_query}")

    # Step 5: Continue with normal query processing using the rewritten query
    # Retrieve and decrypt index
    index = retrieve_index(request.index_attribute, request.table_name)

    if not index:
        return {
            "error": f"Index for {request.table_name}.{request.index_attribute} not found",
            "table_name": request.table_name,
            "index_attribute": request.index_attribute,
            "hint": f"Upload data to create index or check available indexes at GET /index-cids?table_name={request.table_name}"
        }

    cids = query_index(index, request.query, request.index_attribute)  # Use original query for index lookup

    if not cids:
        return {
            "message": "No matching CIDs found",
            "table_name": request.table_name,
            "index_attribute": request.index_attribute
        }

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
            query_with_table = rewritten_query.replace(request.table_name, f"'{paths[0]}'")
            result = duckdb_conn.execute(query_with_table)
        else:
            # Method 1: Use glob pattern if files are in same directory
            # This is more efficient for many files
            glob_pattern = os.path.join(SHARED_TMP_DIR, "*.parquet")
            query_with_table = rewritten_query.replace(request.table_name, f"read_parquet('{glob_pattern}')")
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
        "table_name": request.table_name,
        "policy_count": len(table_policies),
        "policies_applied": [
            {
                "subject": p.get('subject'), 
                "object": p.get('object'), 
                "table": p.get('tableName'), 
                "original_sql": p.get('policySql'),
                "enforced_condition": f"OwnerID = '{p.get('subject')}' AND ({p.get('policySql', '').split('WHERE')[-1].strip() if 'WHERE' in p.get('policySql', '').upper() else '1=1'})"
            } for p in table_policies
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

def process_sql_file(content: bytes, table_name: str = "patient_data") -> pd.DataFrame:
    """
    Process SQL file by executing it in DuckDB and extracting the resulting data.
    
    Args:
        content (bytes): Raw SQL file content
        table_name (str): Name of the table to create/query
        
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
                success, schema_sql = app.state.index_storage.get_table_schema(table_name)
                if success and schema_sql:
                    create_table_sql = schema_sql
                    logger.info(f"Using schema from smart contract for table {table_name}")
                else:
                    logger.warning(f"Schema not found in smart contract for {table_name}, using fallback")
            except Exception as e:
                logger.warning(f"Failed to retrieve schema from smart contract for {table_name}: {e}")
            
            # Fallback to hardcoded schema if smart contract schema is not available
            if not create_table_sql:
                if table_name == "patient_data":
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
                else:
                    # Generic fallback - let DuckDB infer schema
                    create_table_sql = f"CREATE TABLE {table_name} AS SELECT * FROM (VALUES (NULL)) t(dummy) WHERE 1=0"
                logger.info(f"Using fallback schema for table {table_name}")
            
            # Create the table using the retrieved or fallback schema
            temp_conn.execute(create_table_sql)
            
            # Execute the SQL content (INSERT statements)
            temp_conn.execute(sql_content)
            
            # Query the table into DataFrame
            df = temp_conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Convert data types if specific columns exist
            if 'PatientID' in df.columns:
                df['PatientID'] = df['PatientID'].astype(str)
            if 'HospitalID' in df.columns:
                df['HospitalID'] = df['HospitalID'].astype(str)
            if 'Age' in df.columns:
                df['Age'] = pd.to_numeric(df['Age'], errors='coerce').astype('Int64')
            
            logger.info(f"Processed SQL file: {len(df)} rows extracted from {table_name} table")
            return df
            
        finally:
            # Close the temporary connection
            temp_conn.close()
        
    except Exception as e:
        logger.error(f"Error processing SQL file for table {table_name}: {e}")
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

def retrieve_index(name, table_name=None):
    """
    Retrieve and decrypt an index from IPFS.
    Supports multi-table using composite keys.
    
    Args:
        name (str): Attribute name
        table_name (str): Table name (optional, uses default if not provided)
    """
    cid = get_index_cid(name, table_name)
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
        logger.error(f"Failed to decrypt index {name} for table {table_name}: {e}")
        return None

def upload_encrypted_index(index, attr, table_name=None):
    """
    Serialize, encrypt, and upload an index to IPFS.
    Supports multi-table using composite keys.
    
    Args:
        index: The CIDIndex object
        attr (str): Attribute name
        table_name (str): Table name (optional, uses default if not provided)
        
    Returns: (cid, 0, 0) - last two values for backward compatibility
    """
    if table_name is None:
        table_name = app.state.default_table
    
    try:
        # Serialize the index
        serialized = index.dump()
        serialized.seek(0)
        index_data = serialized.read()

        # Get size before encryption
        index_size_bytes = len(index_data)
        index_key = make_index_key(table_name, attr)
        app.state.index_sizes[index_key] = index_size_bytes

        # Encrypt the index data
        encrypted_index = create_encrypted_package(index_data, app.state.encryption_key)

        # Upload encrypted index to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": (f"{table_name}_{attr}_index.enc", encrypted_index)})
        resp.raise_for_status()

        serialized.close()
        return resp.json()["Hash"], 0, 0

    except Exception as e:
        logger.error(f"Failed to upload encrypted index for {table_name}.{attr}: {e}")
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

# --- Multi-Table Helper Functions ---

def get_table_indexed_attributes(table_name: str) -> List[str]:
    """
    Get the list of indexed attributes for a table.
    Falls back to default if table not configured.
    """
    if table_name in app.state.table_configs:
        return app.state.table_configs[table_name]['indexed_attributes']
    
    # Try to infer from schema or use common attributes
    return ['ID']  # Default minimal index

def register_table_config(table_name: str, indexed_attributes: List[str]):
    """
    Register index configuration for a table.
    """
    if table_name not in app.state.table_configs:
        app.state.table_configs[table_name] = {}
    app.state.table_configs[table_name]['indexed_attributes'] = indexed_attributes
    logger.info(f"Registered table '{table_name}' with indexed attributes: {indexed_attributes}")

def make_index_key(table_name: str, attribute_name: str) -> str:
    """
    Create composite key for index storage: "table_name.attribute_name"
    """
    return f"{table_name}.{attribute_name}"

def parse_index_key(index_key: str) -> Tuple[str, str]:
    """
    Parse composite index key into table_name and attribute_name.
    Returns: (table_name, attribute_name)
    """
    if '.' in index_key:
        parts = index_key.split('.', 1)
        return parts[0], parts[1]
    # Backward compatibility: assume default table
    return app.state.default_table, index_key

# --- Smart Contract Integration Helper Functions ---

def get_index_cid(attribute_name, table_name=None):
    """
    Get the CID for a specific index attribute from smart contract.
    Supports multi-table using composite keys.
    
    Args:
        attribute_name (str): Name of the attribute (e.g., 'PatientID', 'Age')
        table_name (str): Name of the table (optional, uses default if not provided)
        
    Returns:
        str or None: The CID if found, None otherwise
    """
    if table_name is None:
        table_name = app.state.default_table
    
    # Create composite key for smart contract storage
    index_key = make_index_key(table_name, attribute_name)
    
    try:
        success, cid = app.state.index_storage.get_index(index_key)
        if success:
            return cid if cid else None  # Return None for empty strings
        else:
            logger.error(f"Failed to get index CID for {index_key} from smart contract")
            return None
    except Exception as e:
        logger.error(f"Error getting index CID for {index_key}: {e}")
        return None

def set_index_cid(attribute_name, cid, table_name=None):
    """
    Set the CID for a specific index attribute in smart contract.
    Supports multi-table using composite keys.
    
    Args:
        attribute_name (str): Name of the attribute
        cid (str): The CID to store
        table_name (str): Name of the table (optional, uses default if not provided)
        
    Returns:
        bool: True if successful, False otherwise
    """
    if table_name is None:
        table_name = app.state.default_table
    
    # Create composite key for smart contract storage
    index_key = make_index_key(table_name, attribute_name)
    
    try:
        success = app.state.index_storage.update_index(index_key, cid)
        if success:
            # Also update in-memory cache
            app.state.index_cids[index_key] = cid
            return True
        else:
            logger.error(f"Smart contract update failed for {index_key}")
            return False
    except Exception as e:
        logger.error(f"Error setting index CID for {index_key}: {e}")
        return False

def get_all_index_cids(table_name=None):
    """
    Get all index CIDs as a dictionary from smart contract.
    Supports filtering by table name.
    
    Args:
        table_name (str): Optional table name to filter indexes
        
    Returns:
        dict: Dictionary mapping index keys to CIDs
    """
    try:
        if table_name:
            # Get indexes for specific table
            indexed_attributes = get_table_indexed_attributes(table_name)
            index_keys = [make_index_key(table_name, attr) for attr in indexed_attributes]
        else:
            # Get all known index keys from cache
            index_keys = list(app.state.index_cids.keys())
            
            # Also try to get indexes for all configured tables
            for tbl in app.state.table_configs.keys():
                attrs = get_table_indexed_attributes(tbl)
                for attr in attrs:
                    key = make_index_key(tbl, attr)
                    if key not in index_keys:
                        index_keys.append(key)
        
        if not index_keys:
            return {}
        
        success, cid_dict = app.state.index_storage.batch_get_indices(index_keys)
        if success:
            # Update in-memory cache and return
            for index_key, cid in cid_dict.items():
                app.state.index_cids[index_key] = cid if cid else None
            return cid_dict
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


def parse_update_query(update_query: str):
    """
    Parse UPDATE query to extract table name, SET clause, and WHERE conditions
    UPDATE patient_data SET Name = 'John Doe', Age = 30 WHERE PatientID = '323'
    """
    import re
    
    # Extract table name
    table_match = re.search(r'UPDATE\s+(\w+)', update_query, re.IGNORECASE)
    table_name = table_match.group(1) if table_match else None
    
    # Extract SET clause
    set_match = re.search(r'SET\s+(.*?)\s+WHERE', update_query, re.IGNORECASE | re.DOTALL)
    if not set_match:
        # If no WHERE clause, get everything after SET
        set_match = re.search(r'SET\s+(.*)', update_query, re.IGNORECASE | re.DOTALL)
    set_clause = set_match.group(1).strip() if set_match else None
    
    # Extract WHERE clause
    where_match = re.search(r'WHERE\s+(.*)', update_query, re.IGNORECASE | re.DOTALL)
    where_clause = where_match.group(1).strip() if where_match else None
    
    # Extract primary key condition (assuming PatientID is primary key)
    primary_key_value = None
    if where_clause:
        # Look for PatientID = 'value' or PatientID = "value"
        pk_match = re.search(r'PatientID\s*=\s*[\'"]([^\'"]+)[\'"]', where_clause, re.IGNORECASE)
        if pk_match:
            primary_key_value = pk_match.group(1)
    
    return table_name, set_clause, where_clause, primary_key_value


def parse_set_clause(set_clause: str):
    """
    Parse SET clause to extract column-value pairs
    Input: "Name = 'John Doe', Age = 30, Gender = 'Male'"
    Output: {'Name': 'John Doe', 'Age': 30, 'Gender': 'Male'}
    """
    import re
    
    updates = {}
    if not set_clause:
        return updates
    
    # Split by comma, but be careful about quoted strings
    parts = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)(?=(?:[^\']*\'[^\']*\')*[^\']*$)', set_clause)
    
    for part in parts:
        part = part.strip()
        # Match column = value pattern
        match = re.match(r'(\w+)\s*=\s*(.+)', part, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            value_str = match.group(2).strip()
            
            # Parse the value (remove quotes, convert types)
            if value_str.startswith("'") and value_str.endswith("'"):
                # String value
                value = value_str[1:-1]
            elif value_str.startswith('"') and value_str.endswith('"'):
                # String value with double quotes
                value = value_str[1:-1]
            elif value_str.lower() == 'null':
                # NULL value
                value = None
            else:
                # Try to convert to number
                try:
                    if '.' in value_str:
                        value = float(value_str)
                    else:
                        value = int(value_str)
                except ValueError:
                    # If conversion fails, treat as string
                    value = value_str
            
            updates[column] = value
    
    return updates


async def find_cids_containing_records(where_clause: str, table_name: str, index_attribute: str = None):
    """
    Find all CIDs that contain records matching the WHERE clause
    
    Args:
        where_clause (str): SQL WHERE clause
        table_name (str): Name of the table
        index_attribute (str): Attribute to use for index lookup (auto-detected if None)
    """
    # Auto-detect index attribute if not provided
    if index_attribute is None:
        indexed_attrs = get_table_indexed_attributes(table_name)
        if indexed_attrs:
            index_attribute = indexed_attrs[0]  # Use first indexed attribute
        else:
            logger.error(f"No indexed attributes found for table {table_name}")
            return []
    
    # Retrieve and decrypt index
    index = retrieve_index(index_attribute, table_name)
    if not index:
        return []
    
    # Use existing query_index function to find relevant CIDs
    dummy_query = f"SELECT * FROM {table_name} WHERE {where_clause}"
    cids = query_index(index, dummy_query, index_attribute)
    return cids

def process_cid_for_deletion(cid: str, where_clause: str, wallet_address: str, table_name: str):
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

async def update_indexes_after_deletion(old_cid: str, new_cid: str, all_records: List[dict], deleted_records: List[dict], table_name: str):
    """
    Update all indexes after deletion operation
    CRITICAL FIX: Rebuild indexes to remove old CID references completely
    
    Args:
        old_cid: Original CID being replaced
        new_cid: New CID (or "EMPTY" if all records deleted)
        all_records: All records that were in the original CID
        deleted_records: Records that were deleted
        table_name: Name of the table
    """
    index_cids_to_update = {}
    
    # Get indexed attributes for this table
    indexed_attributes = get_table_indexed_attributes(table_name)
    
    for attr in indexed_attributes:
        try:
            logger.info(f"Rebuilding index for {table_name}.{attr} - removing old CID {old_cid}")
            
            # Get current index
            existing_index = retrieve_index(attr, table_name)
            if not existing_index:
                logger.warning(f"Index for {table_name}.{attr} not found, skipping")
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
            index_cid, _, _ = upload_encrypted_index(new_index, attr, table_name)
            index_key = make_index_key(table_name, attr)
            index_cids_to_update[index_key] = index_cid
            logger.info(f"Rebuilt clean index for {index_key}: {index_cid}")
            
        except Exception as e:
            logger.error(f"Error rebuilding index for {table_name}.{attr}: {e}")
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
    DELETE FROM table_name WHERE condition
    Process deletion by creating new versions of affected CIDs without deleted records.
    Supports multi-table operations.
    """
    logger.info(f"POST /delete - Processing DELETE query for wallet: {request.wallet_address}")
    
    operation_start_time = time.time()
    
    try:
        # Parse the DELETE query
        table_name, where_clause, primary_key_value = parse_delete_query(request.delete_query)
        
        if not table_name or not where_clause:
            return {"error": "Invalid DELETE query. Expected format: DELETE FROM table_name WHERE condition"}
        
        logger.info(f"Parsed DELETE query - Table: {table_name}, WHERE: {where_clause}, Primary Key: {primary_key_value}")
        
        # Check access policies
        success, policies = app.state.index_storage.get_access_policies(request.wallet_address)
        if not success:
            return {"error": "Failed to fetch access policies from smart contract"}
        
        # Filter policies for this table
        table_policies = [p for p in policies if p.get('tableName') == table_name]
        
        if not table_policies:
            return {
                "error": f"No access policies found for this wallet address on table '{table_name}'",
                "wallet_address": request.wallet_address,
                "table_name": table_name
            }
        
        # Find all CIDs that might contain records matching the WHERE clause
        try:
            relevant_cids = await find_cids_containing_records(where_clause, table_name)
        except Exception as e:
            logger.error(f"Error finding CIDs: {e}")
            return {"error": f"Failed to find relevant CIDs: {str(e)}"}
        
        if not relevant_cids:
            return {
                "message": "No records found matching the DELETE criteria",
                "table_name": table_name,
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
                    executor.submit(process_cid_for_deletion, cid, where_clause, request.wallet_address, table_name): cid 
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
                "table_name": table_name,
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
                        result['deleted_records'],
                        table_name  # Pass table_name for multi-table support
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
        
        # Collect debug information (use generic ID field if PatientID not present)
        deleted_record_ids = []
        id_field = 'PatientID' if table_name == 'patient_data' else get_table_indexed_attributes(table_name)[0] if get_table_indexed_attributes(table_name) else 'ID'
        for result in processed_results:
            for record in result['deleted_records']:
                if id_field in record:
                    deleted_record_ids.append(record[id_field])
        
        return {
            "message": "DELETE operation completed successfully",
            "table_name": table_name,
            "deleted_count": total_deleted_count,
            "affected_cids": len(processed_results),
            "cid_mapping": cid_mapping,
            "wallet_address": request.wallet_address,
            "query": request.delete_query,
            "index_update_success": index_update_success,
            "policy_count": len(table_policies),
            "deletion_stats": app.state.deletion_stats,
            "performance": {
                "cid_processing_time_seconds": processing_time,
                "records_processed": total_deleted_count
            },
            "debug_info": {
                "deleted_record_ids": deleted_record_ids,
                "old_cids_replaced": list(cid_mapping.keys()),
                "new_cids_created": list(cid_mapping.values())
            }
        }
        
    except Exception as e:
        logger.error(f"DELETE operation error: {e}")
        return {"error": f"DELETE operation failed: {str(e)}"}


def process_cid_for_update(cid: str, where_clause: str, update_fields: dict, wallet_address: str, table_name: str):
    """
    Process a single CID for UPDATE operation:
    1. Fetch and decrypt the CID data
    2. Apply access control policies
    3. Find records matching WHERE clause
    4. Apply UPDATE changes to matching records
    5. Re-encrypt and upload the modified data
    
    Returns: (new_cid, all_records, updated_records)
    """
    try:
        logger.info(f"Processing CID {cid} for update...")
        
        # Fetch and decrypt data from IPFS (same as DELETE)
        encrypted_data = fetch_from_ipfs(cid)
        if not encrypted_data:
            logger.error(f"Failed to fetch CID {cid}")
            return None, [], []
        
        decrypted_data = extract_and_decrypt_package(encrypted_data, app.state.encryption_key)
        
        # Load into DataFrame
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
        updatable_df = apply_where_clause_to_dataframe(accessible_df, where_clause)
        
        if len(updatable_df) == 0:
            logger.info(f"No records match WHERE clause in CID {cid}")
            return None, all_records_in_cid, []
        
        logger.info(f"Found {len(updatable_df)} matching records for update in CID {cid}")
        
        # Apply updates to matching records
        updated_records = []
        for idx in updatable_df.index:
            # Store the original record for tracking
            original_record = df.loc[idx].to_dict()
            updated_record = original_record.copy()
            
            # Apply the updates
            for column, value in update_fields.items():
                if column in df.columns:
                    df.loc[idx, column] = value
                    updated_record[column] = value
                else:
                    logger.warning(f"Column {column} not found in data, skipping")
            
            updated_records.append(updated_record)
        
        # Convert updated DataFrame back to Parquet
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)
        updated_parquet_data = buffer.read()
        
        # Encrypt the new data (same as DELETE and upload)
        encrypted_package = create_encrypted_package(updated_parquet_data, app.state.encryption_key)
        
        # Upload to IPFS (same as DELETE and upload)
        resp = requests.post("http://localhost:5001/api/v0/add", 
                           files={"file": (f"updated_data_{int(time.time())}.enc", encrypted_package)})
        resp.raise_for_status()
        new_cid = resp.json()["Hash"]
        
        buffer.close()
        logger.info(f"Successfully updated CID {cid} -> {new_cid}")
        
        # Return all records and updated records (updated df)
        all_records = df.to_dict('records')
        
        return new_cid, all_records, updated_records
        
    except Exception as e:
        logger.error(f"Error processing CID {cid} for update: {e}")
        return None, [], []


async def update_indexes_after_update(old_cid: str, new_cid: str, all_records: List[dict], updated_records: List[dict], table_name: str):
    """
    Update indexes after UPDATE operation - use same exact logic as DELETE
    
    Args:
        old_cid: Original CID being replaced
        new_cid: New CID with updated records
        all_records: All records in the new CID
        updated_records: Records that were updated
        table_name: Name of the table
    """
    try:
        logger.info(f"Updating indexes after update: {old_cid} -> {new_cid}")
        
        # For UPDATE, we can use the same logic as DELETE since we're replacing one CID with another
        # The key difference is that for UPDATE, we pass all_records (not deleted records)
        # This ensures all records are preserved in the new CID
        return await update_indexes_after_deletion(old_cid, new_cid, all_records, [], table_name)
        
    except Exception as e:
        logger.error(f"Error in update_indexes_after_update: {e}")
        return False


@app.post("/update")
async def update_records(request: UpdateRequest):
    """
    UPDATE table_name SET column = value WHERE condition
    Process update by modifying records in place and creating new CID versions.
    Supports multi-table operations.
    """
    logger.info(f"POST /update - Processing UPDATE query for wallet: {request.wallet_address}")
    
    operation_start_time = time.time()
    
    try:
        # Parse the UPDATE query
        table_name, set_clause, where_clause, primary_key_value = parse_update_query(request.update_query)
        
        if not table_name or not set_clause or not where_clause:
            return {"error": "Invalid UPDATE query. Expected format: UPDATE table_name SET column = value WHERE condition"}
        
        logger.info(f"Parsed UPDATE query - Table: {table_name}, SET: {set_clause}, WHERE: {where_clause}, Primary Key: {primary_key_value}")
        
        # Parse SET clause to get update fields
        update_fields = parse_set_clause(set_clause)
        if not update_fields:
            return {"error": "Invalid SET clause. Unable to parse column-value pairs."}
        
        logger.info(f"Update fields: {update_fields}")
        
        # Check access policies
        success, policies = app.state.index_storage.get_access_policies(request.wallet_address)
        if not success:
            return {"error": "Failed to fetch access policies from smart contract"}
        
        # Filter policies for this table
        table_policies = [p for p in policies if p.get('tableName') == table_name]
        
        if not table_policies:
            return {
                "error": f"No access policies found for this wallet address on table '{table_name}'",
                "wallet_address": request.wallet_address,
                "table_name": table_name
            }
        
        # Find all CIDs that might contain records matching the WHERE clause
        try:
            relevant_cids = await find_cids_containing_records(where_clause, table_name)
        except Exception as e:
            logger.error(f"Error finding CIDs: {e}")
            return {"error": f"Failed to find relevant CIDs: {str(e)}"}
        
        if not relevant_cids:
            return {
                "message": "No records found matching the UPDATE criteria",
                "table_name": table_name,
                "updated_count": 0,
                "affected_cids": 0
            }
        
        logger.info(f"Found {len(relevant_cids)} CIDs that might contain matching records")
        
        # Process each CID: decrypt, update records, re-encrypt
        processed_results = []
        total_updated_count = 0
        
        logger.info(f"Starting processing of {len(relevant_cids)} CIDs...")
        start_time = time.time()
        
        # Use simpler parallel processing to avoid hanging
        try:
            # Use ThreadPoolExecutor with reduced worker count
            max_workers = min(4, len(relevant_cids))  # Conservative worker count
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all CID processing tasks
                future_to_cid = {
                    executor.submit(process_cid_for_update, cid, where_clause, update_fields, request.wallet_address, table_name): cid 
                    for cid in relevant_cids
                }
                
                # Collect results with shorter timeout
                for future in concurrent.futures.as_completed(future_to_cid, timeout=30):
                    cid = future_to_cid[future]
                    try:
                        new_cid, all_records, updated_records = future.result(timeout=15)
                        if new_cid is not None:  # Successfully processed
                            processed_results.append({
                                'old_cid': cid,
                                'new_cid': new_cid,
                                'all_records': all_records,
                                'updated_records': updated_records
                            })
                            total_updated_count += len(updated_records)
                            logger.info(f"CID {cid} -> {new_cid}, updated {len(updated_records)} records")
                    except Exception as e:
                        logger.error(f"Error processing CID {cid}: {e}")
        
        except concurrent.futures.TimeoutError:
            logger.error("CID processing timed out")
            return {"error": "UPDATE operation timed out"}
        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            return {"error": f"CID processing failed: {str(e)}"}
        
        processing_time = time.time() - start_time
        logger.info(f"CID processing completed in {processing_time:.2f} seconds")
        
        if not processed_results:
            return {
                "message": "No records were updated (possibly due to access control restrictions)",
                "table_name": table_name,
                "updated_count": 0,
                "affected_cids": 0
            }
        
        # Update indexes for all processed CIDs with timeout
        logger.info("Updating indexes after update...")
        index_update_success = True
        
        try:
            # Use asyncio timeout to prevent hanging
            import asyncio
            
            async def update_all_indexes():
                success = True
                for result in processed_results:
                    individual_success = await update_indexes_after_update(
                        result['old_cid'], 
                        result['new_cid'], 
                        result['all_records'], 
                        result['updated_records'],
                        table_name  # Pass table_name for multi-table support
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
        
        # Update operation statistics
        if not hasattr(app.state, 'update_stats'):
            app.state.update_stats = {
                'total_updates': 0,
                'last_update': None
            }
        
        app.state.update_stats['total_updates'] += total_updated_count
        app.state.update_stats['last_update'] = time.time()
        
        # Prepare response with timing and debug information
        cid_mapping = {
            result['old_cid']: result['new_cid'] for result in processed_results
        }
        
        # Collect debug information (use generic ID field)
        id_field = 'PatientID' if table_name == 'patient_data' else get_table_indexed_attributes(table_name)[0] if get_table_indexed_attributes(table_name) else 'ID'
        updated_record_ids = []
        updated_fields_summary = {}
        for result in processed_results:
            for record in result['updated_records']:
                if id_field in record:
                    updated_record_ids.append(record[id_field])
                # Track which fields were updated
                for field in update_fields.keys():
                    if field not in updated_fields_summary:
                        updated_fields_summary[field] = 0
                    updated_fields_summary[field] += 1
        
        return {
            "message": "UPDATE operation completed successfully",
            "table_name": table_name,
            "updated_count": total_updated_count,
            "affected_cids": len(processed_results),
            "cid_mapping": cid_mapping,
            "wallet_address": request.wallet_address,
            "query": request.update_query,
            "update_fields": update_fields,
            "index_update_success": index_update_success,
            "policy_count": len(table_policies),
            "update_stats": app.state.update_stats,
            "performance": {
                "cid_processing_time_seconds": processing_time,
                "records_processed": total_updated_count
            },
            "debug_info": {
                "updated_record_ids": updated_record_ids,
                "updated_fields_summary": updated_fields_summary,
                "old_cids_replaced": list(cid_mapping.keys()),
                "new_cids_created": list(cid_mapping.values())
            }
        }
        
    except Exception as e:
        logger.error(f"UPDATE operation error: {e}")
        return {"error": f"UPDATE operation failed: {str(e)}"}
        return {"error": f"UPDATE operation failed: {str(e)}"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "FastAPI server running inside SGX enclave",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "sgx_enabled": True,
        "deletion_stats": app.state.deletion_stats if hasattr(app.state, 'deletion_stats') else {},
        "update_stats": app.state.update_stats if hasattr(app.state, 'update_stats') else {}
    }

@app.get("/debug/indexes")
async def debug_indexes():
    """Debug endpoint to check index status"""
    try:
        # Simple test: just check if PatientID index can find specific values
        result = {
            "patientid_index_tests": {},
            "error": None
        }
        
        if 'PatientID' in app.state.index_cids:
            index = app.state.index_cids['PatientID']
            
            # Test specific PatientIDs that we know should exist
            test_patient_ids = ['10', '12', '13']
            for patient_id in test_patient_ids:
                try:
                    cids = index.query(patient_id)
                    result["patientid_index_tests"][patient_id] = {
                        "found_cids": cids if isinstance(cids, list) else [cids] if cids else [],
                        "cid_count": len(cids) if cids else 0
                    }
                except Exception as e:
                    result["patientid_index_tests"][patient_id] = {
                        "error": str(e)
                    }
        else:
            result["error"] = "PatientID index not loaded in memory"
        
        return result
        
    except Exception as e:
        return {"error": f"Debug failed: {str(e)}"}

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Web3DB SGX API",
        "description": "Decentralized Multi-Table Database with Privacy-Preserving Query Processing using Intel SGX and Access Control",
        "version": "2.0.0",
        "features": {
            "multi_table_support": True,
            "encrypted_storage": "AES-256-CBC encryption",
            "decentralized": "IPFS + Blockchain metadata",
            "access_control": "Fine-grained policy-based",
            "sgx_enclave": "Confidential query processing"
        },
        "endpoints": {
            "health": "GET /health",
            "upload": "POST /upload/{table_name} 🌟 GENERIC API - works for ANY table! (CSV and SQL supported)",
            "upload-legacy": "POST /upload/patient-data [DEPRECATED] (redirects to generic endpoint)",
            "query": "POST /query (requires table_name and wallet_address for access control)", 
            "query-count": "GET /query/count?table_name=X&index_attribute=Y 🌟 GENERIC - get row count for any table",
            "delete": "POST /delete (supports multi-table DELETE queries with access control)",
            "update": "POST /update (supports multi-table UPDATE queries with access control)",
            "index-cids": "GET /index-cids, PUT /index-cids, DELETE /index-cids?index_key=table.attribute",
            "schemas": "GET /schemas, POST /schemas",
            "schema-tables": "GET /schemas/tables",
            "schema-by-table": "GET /schemas/{table_name}, DELETE /schemas/{table_name}",
            "access-policies": "POST /access-policies, GET /access-policies/{object_address}, DELETE /access-policies",
            "policy-count": "GET /access-policies/{object_address}/count",
            "remove-all-policies": "DELETE /access-policies/{object_address}/all",
            "table-config": "POST /tables/config (register indexed attributes for a table)",
            "table-configs-list": "GET /tables/config (list all configured tables)",
            "table-config-detail": "GET /tables/config/{table_name} (get config for specific table)",
            "docs": "GET /docs"
        },
        "multi_table": {
            "enabled": True,
            "description": "System supports multiple independent tables with separate indexes and access policies",
            "index_storage": "Composite keys format: 'table_name.attribute_name' stored in smart contract",
            "examples": {
                "upload": "POST /upload/users, POST /upload/orders, POST /upload/patient_data",
                "query": "Include 'table_name' field in QueryRequest body",
                "indexes": "Automatically managed per table based on registered configuration"
            }
        },
        "file_support": {
            "csv": "Comma-separated values with headers (auto-detect schema)",
            "sql": "INSERT statements for any table (schema from smart contract or fallback)"
        },
        "schema_storage": {
            "format": "SQL CREATE TABLE statements",
            "description": "Schemas are stored as executable SQL DDL statements in the smart contract",
            "multi_table": "Each table has independent schema stored by table name"
        },
        "index_management": {
            "format": "Composite keys: table_name.attribute_name",
            "storage": "Smart contract stores index CIDs with table-qualified names",
            "auto_registration": "First upload auto-registers indexed attributes if not configured",
            "configuration": "Use POST /tables/config to pre-register indexed attributes"
        },
        "access_control": {
            "enabled": True,
            "description": "All queries require a wallet_address parameter and are filtered based on access policies stored in the smart contract. Multi-tenant security ensures users can only access data where OwnerID matches the policy subject.",
            "table_aware": "Policies are table-specific, ensuring isolation between tables",
            "enforcement": "Query rewriting with CTE combining OwnerID = subject AND policy conditions",
            "example": "WITH accessible_part AS (SELECT * FROM table WHERE (OwnerID = 'subject1' AND condition1) OR (OwnerID = 'subject2' AND condition2)) SELECT * FROM accessible_part"
        }
    }

@app.put("/index-cids")
async def update_index_cids(request: UpdateIndexCIDsRequest):
    """
    Update the index CIDs mapping in smart contract.
    Supports composite keys for multi-table: "table_name.attribute_name"

    Example request body:
    {
        "index_cids": {
            "patient_data.PatientID": "QmXxxxx...",
            "patient_data.HospitalID": "QmYyyyy...",
            "users.UserID": "QmZzzzz..."
        }
    }
    """
    logger.info("PUT /index-cids - Updating index CIDs")
    try:
        # Update the index CIDs using helper function (smart contract)
        success = set_all_index_cids(request.index_cids)
        
        if success:
            logger.info(f"Updated {len(request.index_cids)} index CIDs in smart contract")
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


@app.delete("/index-cids")
async def delete_index(index_key: str):
    """
    Delete a specific index from the smart contract.
    
    Query Parameters:
        index_key (str): Index key in format "table_name.attribute" (e.g., "orders.OrderID", "patient_data.PatientID")
    
    Examples:
        DELETE /index-cids?index_key=orders.OrderID
        DELETE /index-cids?index_key=patient_data.PatientID
        DELETE /index-cids?index_key=users.UserID
    """
    logger.info(f"DELETE /index-cids - Removing index '{index_key}'")
    try:
        # Validate index key format
        if '.' not in index_key:
            return {
                "status": "error",
                "message": "Invalid index_key format. Expected 'table_name.attribute' (e.g., 'orders.OrderID')",
                "index_key": index_key
            }
        
        # Parse table and attribute
        table_name, attribute = parse_index_key(index_key)
        
        # Remove from smart contract
        try:
            success = app.state.index_storage.remove_index(index_key)
            if not success:
                return {
                    "status": "error",
                    "message": f"Failed to remove index '{index_key}' from smart contract",
                    "index_key": index_key,
                    "table_name": table_name,
                    "attribute": attribute
                }
            
            logger.info(f"Successfully removed index: {index_key}")
            
        except Exception as e:
            logger.error(f"Error removing index {index_key}: {e}")
            return {
                "status": "error",
                "message": f"Exception removing index: {str(e)}",
                "index_key": index_key
            }
        
        # Clear from in-memory cache
        if index_key in app.state.index_cids:
            del app.state.index_cids[index_key]
        if index_key in app.state.index_sizes:
            del app.state.index_sizes[index_key]
        
        return {
            "status": "success",
            "index_key": index_key,
            "table_name": table_name,
            "attribute": attribute,
            "message": f"Successfully removed index '{index_key}'"
        }
        
    except Exception as e:
        logger.error(f"Error deleting index {index_key}: {e}")
        return {
            "status": "error",
            "index_key": index_key,
            "message": str(e)
        }


class TableConfigRequest(BaseModel):
    table_name: str
    indexed_attributes: List[str]


@app.post("/tables/config")
async def register_table_config_endpoint(request: TableConfigRequest):
    """
    Register index configuration for a table.
    This tells the system which attributes should be indexed for a given table.
    
    Example request body:
    {
        "table_name": "users",
        "indexed_attributes": ["UserID", "Email", "Age"]
    }
    """
    logger.info(f"POST /tables/config - Registering config for table {request.table_name}")
    try:
        register_table_config(request.table_name, request.indexed_attributes)
        return {
            "status": "success",
            "message": f"Table configuration registered successfully",
            "table_name": request.table_name,
            "indexed_attributes": request.indexed_attributes
        }
    except Exception as e:
        logger.error(f"Error registering table config: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/tables/config/{table_name}")
async def get_table_config(table_name: str):
    """
    Get the index configuration for a specific table.
    """
    try:
        indexed_attributes = get_table_indexed_attributes(table_name)
        return {
            "status": "success",
            "table_name": table_name,
            "indexed_attributes": indexed_attributes,
            "configured": table_name in app.state.table_configs
        }
    except Exception as e:
        logger.error(f"Error getting table config: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/tables/config")
async def get_all_table_configs():
    """
    Get all registered table configurations.
    """
    try:
        return {
            "status": "success",
            "tables": app.state.table_configs,
            "total_tables": len(app.state.table_configs)
        }
    except Exception as e:
        logger.error(f"Error getting all table configs: {e}")
        return {"status": "error", "message": str(e)}


@app.put("/index-cids-old")
async def update_index_cids_old(request: UpdateIndexCIDsRequest):
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
async def get_index_cids_endpoint(table_name: str = None):
    """
    Get the current index CIDs mapping along with index sizes from smart contract.
    Supports filtering by table_name.
    
    Query Parameters:
        table_name (optional): Filter indexes for specific table

    Returns:
    {
        "index_cids": {
            "patient_data.PatientID": "QmXxxxx..." or null,
            "patient_data.HospitalID": "QmYyyyy..." or null,
            "users.UserID": "QmZzzzz..." or null
        },
        "index_sizes": {
            "patient_data.PatientID": 12345,
            "patient_data.HospitalID": 23456,
            "users.UserID": 34567
        }
    }
    """
    logger.info(f"GET /index-cids - Retrieving current index CIDs for table: {table_name or 'all'}")
    try:
        all_cids = get_all_index_cids(table_name)
        
        # Filter index sizes if table_name provided
        if table_name:
            filtered_sizes = {k: v for k, v in app.state.index_sizes.items() 
                            if k.startswith(f"{table_name}.")}
        else:
            filtered_sizes = app.state.index_sizes
        
        return {
            "status": "success",
            "table_name": table_name,
            "index_cids": all_cids,
            "index_sizes": filtered_sizes,
            "total_indexes": len(all_cids),
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
async def get_row_count(
    table_name: str = 'patient_data',
    index_attribute: Optional[str] = None
):
    """
    Get total row count for any table using specified or auto-detected index.
    """
    logger.info(f"GET /query/count - Getting total row count for table '{table_name}'")
    
    try:
        # Get all available index CIDs for this table
        all_index_cids = get_all_index_cids(table_name)
        
        # Determine which index attribute to use
        if index_attribute:
            # User specified an index attribute
            index_key = make_index_key(table_name, index_attribute)
            if index_key not in all_index_cids and index_attribute not in all_index_cids:
                return {
                    "status": "error", 
                    "message": f"Index '{index_attribute}' not found for table '{table_name}'",
                    "available_indexes": list(all_index_cids.keys())
                }
            index_attr = index_attribute
        else:
            # Auto-detect: try to find any available index for this table
            # Get indexed attributes from table config
            indexed_attrs = get_table_indexed_attributes(table_name)
            
            index_attr = None
            for attr in indexed_attrs:
                index_key = make_index_key(table_name, attr)
                if index_key in all_index_cids or attr in all_index_cids:
                    index_attr = attr
                    break
            
            if not index_attr:
                return {
                    "status": "error", 
                    "message": f"No indexes available for table '{table_name}'",
                    "table_name": table_name,
                    "hint": "Upload data first or specify index_attribute parameter"
                }
        
        # Retrieve and decrypt index
        index = retrieve_index(index_attr, table_name)
        
        if not index:
            return {
                "status": "error", 
                "message": f"Index for '{index_attr}' not found in table '{table_name}'"
            }
        
        # Get all CIDs from the index (no WHERE clause filtering)
        all_cids = index.query_range()  # Get all CIDs without any filtering
        
        if not all_cids:
            return {
                "status": "success",
                "table_name": table_name,
                "total_rows": 0,
                "cids_processed": 0,
                "message": f"No data found in table '{table_name}'"
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
            "table_name": table_name,
            "total_rows": total_rows,
            "cids_processed": len(all_cids),
            "index_used": index_attr
        }
        
    except Exception as e:
        logger.error(f"Error getting row count for table '{table_name}': {e}")
        return {
            "status": "error", 
            "table_name": table_name,
            "message": str(e)
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