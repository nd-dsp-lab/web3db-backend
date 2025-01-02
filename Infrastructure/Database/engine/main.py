import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Tuple
from spark_shell import SparkSQLShell
from web3_hash_manager import Web3HashManager
import sqlparse
from sqlparse.sql import Token, Identifier
from sqlparse.tokens import Keyword

class SQLParser:
    @staticmethod
    def parse_query(query: str) -> Tuple[Optional[str], str]:
        """
        Parse SQL query to extract table name and operation type.
        Returns (table_name, operation_type)
        """
        parsed = sqlparse.parse(query)[0]
        tokens = parsed.tokens
        
        table_name = None
        operation = tokens[0].value.lower()
        
        if operation == 'create':
            for token in tokens:
                if isinstance(token, Identifier):
                    table_name = token.get_real_name()
                    break
                
        elif operation in ('insert', 'update', 'delete'):
            for token in tokens:
                if isinstance(token, Identifier):
                    table_name = token.get_real_name()
                    break
                elif token.ttype is Keyword and token.value.lower() == 'into':
                    next_token = tokens[tokens.index(token) + 2]
                    if isinstance(next_token, Identifier):
                        table_name = next_token.get_real_name()
                        break
                        
        elif operation == 'select':
            from_seen = False
            for token in tokens:
                if from_seen and isinstance(token, Identifier):
                    table_name = token.get_real_name()
                    break
                elif token.ttype is Keyword and token.value.lower() == 'from':
                    from_seen = True

        return table_name, operation

# Initialize SparkSQL shell and Redis Hash Manager
sql_shell = SparkSQLShell()
hash_manager = Web3HashManager()

# Pydantic models
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
    user_id: Optional[str] = "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
    state_hash: Optional[str] = None

class SQLQuery(BaseModel):
    query: str
    state_hash: Optional[str] = None

class SQLResult(BaseModel):
    type: str
    hash: str
    data: Optional[List[Dict[str, Any]]] = None

class ShareRequest(BaseModel):
    from_user: str
    to_user: str
    table_name: str

DEFAULT_PARTITION = "0"
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="SQL State Management API",
    description="API for managing SQL states with IPFS versioning",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize SparkSQL shell and Hash Manager
sql_shell = SparkSQLShell()
hash_manager = Web3HashManager()
DEFAULT_PARTITION = "0"

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.post("/query")
def execute_with_hash_resolution(query_request: QueryRequest):
    """Execute SQL query with automatic hash resolution and storage"""
    try:
        # Parse the query to get table name and operation
        table_name, operation = SQLParser.parse_query(query_request.query)
        
        if not table_name:
            raise HTTPException(
                status_code=400, 
                detail="Could not determine table name from query"
            )

        # Use the provided user_id or default
        user_id = query_request.user_id or "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
        logger.info(f"Executing {operation} operation on table {table_name} for user {user_id}")

        # If state_hash is not provided, get latest hash from blockchain
        if not query_request.state_hash:
            try:
                state_hash = hash_manager.get_latest_hash(
                    user_id,
                    table_name,
                    DEFAULT_PARTITION
                )
                logger.info(f"Retrieved latest state hash: {state_hash}")
            except Exception as e:
                logger.error(f"Failed to get latest hash: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to retrieve latest state: {str(e)}"
                )
        else:
            state_hash = query_request.state_hash
            logger.info(f"Using provided state hash: {state_hash}")

        # Check if data exists for non-create operations
        if operation != "create" and not state_hash:
            logger.warning(f"No existing data found for user {user_id} and table {table_name}")
            raise HTTPException(
                status_code=404,
                detail=f"No existing data found for user {user_id} and table {table_name}"
            )

        # Execute query with resolved hash
        try:
            result = sql_shell.execute_sql(query_request.query, state_hash)
            if result is None:
                raise HTTPException(status_code=400, detail="Query execution failed")
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Query execution failed: {str(e)}"
            )

        # Only store hash for non-SELECT operations
        if operation.lower() != "select":
            try:
                hash_manager.add_hash_mapping(
                    user_id=user_id,
                    table_name=table_name,
                    partition=DEFAULT_PARTITION,
                    hash_value=result["hash"],
                    prev_hash=state_hash if state_hash else "",
                    record_type=operation,
                    flag="latest",
                    row_count=len(result.get("data", [])) if result.get("data") else 0
                )
                logger.info(f"Successfully stored new state hash: {result['hash']}")
            except Exception as e:
                logger.error(f"Failed to store hash mapping: {str(e)}")
                # Continue execution - we don't want to fail the query just because hash storage failed
        
        return SQLResult(
            type=result["type"],
            hash=result["hash"],
            data=result.get("data")
        )

    except HTTPException:
        # Re-raise HTTP exceptions without modification
        raise
    except Exception as e:
        logger.exception("Unexpected error executing query")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute query: {str(e)}"
        )

@app.post("/share/table")
async def share_table_data(share_request: ShareRequest):
    """Share table data from one user to another"""
    try:
        # Get the latest hash from source user
        source_hash = hash_manager.get_latest_hash(
            share_request.from_user,
            share_request.table_name,
            DEFAULT_PARTITION
        )
        
        if not source_hash:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for table {share_request.table_name} from user {share_request.from_user}"
            )

        # Create new hash mapping for target user
        success = hash_manager.add_hash_mapping(
            user_id=share_request.to_user,
            table_name=share_request.table_name,
            partition=DEFAULT_PARTITION,
            hash_value=source_hash,
            prev_hash="",  # Empty as this is initial data for target user
            record_type="share",  # New record type to indicate shared data
            flag="latest",
            row_count=0  # Will be updated when queried
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to share table data"
            )

        return {
            "status": "success",
            "message": f"Table {share_request.table_name} shared successfully from {share_request.from_user} to {share_request.to_user}",
            "shared_hash": source_hash
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to share table: {str(e)}"
        )

@app.get("/state/{hash}")
async def get_state(hash: str):
    """Get state information for a specific hash"""
    try:
        state = sql_shell.ipfs_handler.load_state(hash)
        if not state:
            raise HTTPException(status_code=404, detail="State not found")
        return state
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/cleanup")
async def cleanup():
    """Cleanup all temporary tables and state"""
    try:
        sql_shell.cleanup()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))