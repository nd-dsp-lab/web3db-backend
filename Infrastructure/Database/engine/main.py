import logging
from typing import Optional, Dict, Any, List, Tuple
import sqlparse
from sqlparse.sql import Token, Identifier
from sqlparse.tokens import Keyword
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from spark_shell import SparkSQLShell
from web3_hash_manager import Web3HashManager

# Constants and Logger Configuration
DEFAULT_PARTITION = "0"
DEFAULT_USER_ID = "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
# DEFAULT_USER_ID = "0x1f6Da6843Bad2a6F7a3fC7824289f197556f99b0"


# Web3 Configuration
PROVIDER_URL = "https://sepolia.infura.io/v3/eb1d43f1429e49fba50e18fbf5ebd4ab"
CONTRACT_ADDRESS = "0xb2bBb5917f702e1Cdb3Ee5DE7b5bc46e44AB972c"
PRIVATE_KEY = "34cf59aaa5ef0a24e65b4e4dbe6fb23c2bd23a4d9a6b584d7995a141de719d53"
# PRIVATE_KEY = "0a13259d86ea6ffefda50f074c0aa4c72db594433585e572b27ffd9b6f60fd12"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQL Parser Class
class SQLParser:
    @staticmethod
    def parse_query(query: str) -> Tuple[Optional[str], str]:
        """Parse SQL query to extract table name and operation type."""
        parsed = sqlparse.parse(query)[0]
        tokens = parsed.tokens
        
        table_name = None
        operation = tokens[0].value.lower()
        
        if operation == "create":
            for token in tokens:
                if isinstance(token, Identifier):
                    table_name = token.get_real_name()
                    break
                    
        elif operation in ("insert", "update", "delete"):
            for token in tokens:
                if isinstance(token, Identifier):
                    table_name = token.get_real_name()
                    break
                elif token.ttype is Keyword and token.value.lower() == "into":
                    next_token = tokens[tokens.index(token) + 2]
                    if isinstance(next_token, Identifier):
                        table_name = next_token.get_real_name()
                        break
                        
        elif operation == "select":
            from_seen = False
            for token in tokens:
                if from_seen and isinstance(token, Identifier):
                    table_name = token.get_real_name()
                    break
                elif token.ttype is Keyword and token.value.lower() == "from":
                    from_seen = True
                    
        return table_name, operation

# Pydantic Models
class HashMapping(BaseModel):
    user_id: str
    table_name: str
    hash_value: str
    prev_hash: Optional[str] = ""
    record_type: str = "create"
    flag: str = "initial"
    row_count: int = 0

class QueryRequest(BaseModel):
    query: str
    user_id: Optional[str] = DEFAULT_USER_ID
    state_hash: Optional[str] = None

class SQLResult(BaseModel):
    type: str
    hash: str
    data: Optional[List[Dict[str, Any]]] = None
    includes_shared: Optional[bool] = False
    shared_count: Optional[int] = 0
    own_count: Optional[int] = 0

class ShareCIDRequest(BaseModel):
    to_user: str
    table_name: str
    cid: str

class RevokeCIDRequest(BaseModel):
    from_user: str
    table_name: str
    cid: str

# Initialize services
sql_shell = SparkSQLShell()
hash_manager = Web3HashManager(
    provider_url=PROVIDER_URL,
    contract_address=CONTRACT_ADDRESS,
    private_key=PRIVATE_KEY
)

# FastAPI App Configuration
app = FastAPI(
    title="SQL State Management API",
    description="API for managing SQL states with IPFS versioning",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper Functions
def get_final_state_hash(
    user_id: str, 
    table_name: str, 
    partition: str, 
    provided_state_hash: Optional[str]
) -> Optional[str]:
    """Get final state hash for a user and table."""
    if provided_state_hash:
        return provided_state_hash

    try:
        # Check if user has their own data
        state_hash = hash_manager.get_latest_hash(user_id, table_name, partition)
        return state_hash  # Return whatever we get (including None)
    except Exception as e:
        logger.error(f"Error in get_final_state_hash: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve state information: {str(e)}"
        )

# Endpoints
@app.get("/", include_in_schema=False)
async def root():
    """Redirect to API documentation."""
    return RedirectResponse(url="/docs")

@app.post("/query")
async def execute_query(query_request: QueryRequest):
    """Execute SQL query with hash resolution."""
    try:
        table_name, operation = SQLParser.parse_query(query_request.query)
        if not table_name:
            raise HTTPException(status_code=400, detail="Could not determine table name")

        user_id = query_request.user_id or DEFAULT_USER_ID
        operation = operation.lower()
        
        # For non-CREATE operations, get the state hash
        state_hash = None if operation == "create" else get_final_state_hash(
            user_id, table_name, DEFAULT_PARTITION, query_request.state_hash
        )

        result = None
        if operation == "select":
            if state_hash:
                # Execute query only if user has own data
                result = sql_shell.execute_sql(query_request.query, state_hash)
            else:
                # Initialize empty result structure for select with a valid hash value
                result = {
                    "type": "select",
                    "hash": "",  # Empty string instead of None
                    "data": []
                }

            # Get shared CIDs for this user and table
            shared_cids, owners = hash_manager.get_shared_cids(user_id, table_name)
            shared_data = []
            
            for shared_cid, owner in zip(shared_cids, owners):
                shared_state = sql_shell.ipfs_handler.load_state(shared_cid)
                if shared_state:
                    shared_result = sql_shell.execute_sql(query_request.query, shared_cid)
                    if shared_result and shared_result.get("data"):
                        for row in shared_result["data"]:
                            row["_shared_by"] = owner
                        shared_data.extend(shared_result["data"])

                        # If this is our first data and we don't have own data, use this hash
                        if not state_hash and not result["hash"]:
                            result["hash"] = shared_cid

            # Add ownership information to original data if exists
            if result.get("data"):
                for row in result["data"]:
                    row["_owner"] = user_id

            # Combine own data with shared data
            all_data = (result.get("data") or []) + shared_data
            
            # Check if we have any data at all
            if not all_data:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data (owned or shared) found for user {user_id} and table {table_name}"
                )

            # Update the result with combined data
            result["data"] = all_data
            result["includes_shared"] = len(shared_data) > 0  # Only true if we have shared data
            result["shared_count"] = len(shared_data)
            result["own_count"] = len(result.get("data", []) or []) - len(shared_data)
        
        else:
            # For non-SELECT operations, execute normally
            result = sql_shell.execute_sql(query_request.query, state_hash)
            
            if operation != "select":
                row_count = len(result.get("data", [])) if result.get("data") is not None else 0
                hash_manager.add_hash_mapping(
                    table_name=table_name,
                    partition=DEFAULT_PARTITION,
                    hash_value=result["hash"],
                    prev_hash=state_hash if state_hash else "",
                    record_type=operation,
                    flag="latest",
                    row_count=row_count
                )

        return SQLResult(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hash/latest/{user_id}/{table_name}")
async def get_latest_hash(user_id: str, table_name: str):
    """Get latest hash for a table."""
    try:
        latest_hash = hash_manager.get_latest_hash(user_id, table_name, DEFAULT_PARTITION)
        if not latest_hash:
            raise HTTPException(status_code=404, detail="Hash not found")
        return {"hash": latest_hash}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hash/history/{user_id}/{table_name}")
async def get_hash_history(user_id: str, table_name: str):
    """Get complete hash history for a table."""
    try:
        history = hash_manager.get_hash_history(user_id, table_name, DEFAULT_PARTITION)
        return {"history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{user_id}")
async def get_user_tables(user_id: str):
    """Get all tables owned by a user."""
    try:
        tables = hash_manager.get_user_tables(user_id)
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/share/cid")
async def share_cid(request: ShareCIDRequest):
    """Share a CID with another user."""
    try:
        success = hash_manager.share_cid(request.to_user, request.table_name, request.cid)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to share CID")
        return {"status": "success", "message": "CID shared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/revoke/cid")
async def revoke_cid_access(request: RevokeCIDRequest):
    """Revoke CID access from a user."""
    try:
        success = hash_manager.revoke_cid_access(
            request.from_user, request.table_name, request.cid
        )
        if not success:
            raise HTTPException(status_code=400, detail="Failed to revoke CID access")
        return {"status": "success", "message": "CID access revoked successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/shared/cids/{user_id}/{table_name}")
async def get_shared_cids(user_id: str, table_name: str):
    """Get all shared CIDs for a table and user."""
    try:
        user_id = user_id or DEFAULT_USER_ID
        cids, owners = hash_manager.get_shared_cids(user_id, table_name)
        return {"cids": cids, "owners": owners}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/state/{hash}")
async def get_state(hash: str):
    """Get state information for a specific hash."""
    try:
        state = sql_shell.ipfs_handler.load_state(hash)
        if not state:
            raise HTTPException(status_code=404, detail="State not found")
        return state
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/cleanup")
async def cleanup():
    """Cleanup all temporary tables and state."""
    try:
        sql_shell.cleanup()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))