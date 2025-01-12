import logging
from typing import Optional, Dict, Any, List, Tuple
import sqlparse
from sqlparse.sql import Identifier
from sqlparse.tokens import Keyword
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from spark_shell import SparkSQLShell
from web3_hash_manager import Web3HashManager

# Constants
DEFAULT_PARTITION = "0"
PROVIDER_URL = "https://sepolia.infura.io/v3/eb1d43f1429e49fba50e18fbf5ebd4ab"
CONTRACT_ADDRESS = "0xb2bBb5917f702e1Cdb3Ee5DE7b5bc46e44AB972c"

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class WalletConfig(BaseModel):
    user_id: str
    private_key: str

class HashMapping(BaseModel):
    table_name: str
    hash_value: str
    prev_hash: Optional[str] = ""
    record_type: str = "create"
    flag: str = "initial"
    row_count: int = 0

class QueryRequest(BaseModel):
    query: str
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

# Global Configuration Class
class GlobalConfig:
    def __init__(self):
        self.wallets: Dict[str, WalletConfig] = {
            "wallet1": WalletConfig(
                user_id="0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
                private_key="34cf59aaa5ef0a24e65b4e4dbe6fb23c2bd23a4d9a6b584d7995a141de719d53"
            ),
            "wallet2": WalletConfig(
                user_id="0x1f6Da6843Bad2a6F7a3fC7824289f197556f99b0",
                private_key="0a13259d86ea6ffefda50f074c0aa4c72db594433585e572b27ffd9b6f60fd12"
            )
        }
        self.current_wallet = "wallet1"
        self.hash_manager = self._create_hash_manager("wallet1")

    def _create_hash_manager(self, wallet_name: str) -> Web3HashManager:
        wallet = self.wallets[wallet_name]
        return Web3HashManager(
            provider_url=PROVIDER_URL,
            contract_address=CONTRACT_ADDRESS,
            private_key=wallet.private_key
        )

    def switch_wallet(self, wallet_name: str):
        if wallet_name not in self.wallets:
            raise ValueError(f"Wallet {wallet_name} not found")
        self.current_wallet = wallet_name
        self.hash_manager = self._create_hash_manager(wallet_name)

    @property
    def current_user_id(self) -> str:
        return self.wallets[self.current_wallet].user_id

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

# Initialize services
config = GlobalConfig()
sql_shell = SparkSQLShell()

# FastAPI App Configuration
app = FastAPI(
    title="Web3DB APIs",
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
        state_hash = config.hash_manager.get_latest_hash(user_id, table_name, partition)
        return state_hash
    except Exception as e:
        logger.error(f"Error in get_final_state_hash: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve state information: {str(e)}"
        )

# Routes
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response

@app.get("/", include_in_schema=False)
async def root():
    """Redirect to API documentation."""
    logger.info("Redirecting to /docs")
    return RedirectResponse(url="/docs")

@app.post("/switch-wallet/{wallet_name}")
async def switch_wallet(wallet_name: str):
    """Switch to a different wallet configuration."""
    try:
        config.switch_wallet(wallet_name)
        logger.info(f"Switched to wallet: {wallet_name}")
        return {
            "status": "success",
            "current_wallet": wallet_name,
            "current_user_id": config.current_user_id
        }
    except Exception as e:
        logger.error(f"Error switching wallet: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/current-wallet")
async def get_current_wallet():
    """Get current wallet information."""
    logger.info("Fetching current wallet information")
    return {
        "current_wallet": config.current_wallet,
        "current_user_id": config.current_user_id
    }

@app.post("/query")
async def execute_query(query_request: QueryRequest):
    """Execute SQL query with hash resolution."""
    logger.info(f"Executing query: {query_request.query}")
    try:
        table_name, operation = SQLParser.parse_query(query_request.query)
        if not table_name:
            logger.warning("Could not determine table name from query")
            raise HTTPException(status_code=400, detail="Could not determine table name")

        # Always use current wallet's user_id
        user_id = config.current_user_id
        operation = operation.lower()
        
        # For non-CREATE operations, get the state hash
        state_hash = None if operation == "create" else get_final_state_hash(
            user_id, table_name, DEFAULT_PARTITION, query_request.state_hash
        )

        result = None
        if operation == "select":
            if state_hash:
                result = sql_shell.execute_sql(query_request.query, state_hash)
            else:
                result = {
                    "type": "select",
                    "hash": "",
                    "data": []
                }

            # Get shared CIDs for current user and table
            shared_cids, owners = config.hash_manager.get_shared_cids(user_id, table_name)
            shared_data = []
            
            for shared_cid, owner in zip(shared_cids, owners):
                shared_state = sql_shell.ipfs_handler.load_state(shared_cid)
                if shared_state:
                    shared_result = sql_shell.execute_sql(query_request.query, shared_cid)
                    if shared_result and shared_result.get("data"):
                        for row in shared_result["data"]:
                            row["_shared_by"] = owner
                        shared_data.extend(shared_result["data"])

                        if not state_hash and not result["hash"]:
                            result["hash"] = shared_cid

            # Add ownership information to original data
            if result.get("data"):
                for row in result["data"]:
                    row["_owner"] = user_id

            all_data = (result.get("data") or []) + shared_data
            
            if not all_data:
                logger.info(f"No data found for table {table_name}")
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for table {table_name}"
                )

            result["data"] = all_data
            result["includes_shared"] = len(shared_data) > 0
            result["shared_count"] = len(shared_data)
            result["own_count"] = len(result.get("data", []) or []) - len(shared_data)
        
        else:
            result = sql_shell.execute_sql(query_request.query, state_hash)
            
            if operation != "select":
                row_count = len(result.get("data", [])) if result.get("data") is not None else 0
                config.hash_manager.add_hash_mapping(
                    table_name=table_name,
                    partition=DEFAULT_PARTITION,
                    hash_value=result["hash"],
                    prev_hash=state_hash if state_hash else "",
                    record_type=operation,
                    flag="latest",
                    row_count=row_count
                )

        logger.info(f"Query executed successfully for table: {table_name}")
        return SQLResult(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hash/latest/{table_name}")
async def get_latest_hash(table_name: str):
    """Get latest hash for a table."""
    logger.info(f"Fetching latest hash for table: {table_name}")
    try:
        latest_hash = config.hash_manager.get_latest_hash(
            config.current_user_id, 
            table_name, 
            DEFAULT_PARTITION
        )
        if not latest_hash:
            raise HTTPException(status_code=404, detail="Hash not found")
        logger.info(f"Latest hash for table {table_name}: {latest_hash}")
        return {"hash": latest_hash}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest hash: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hash/history/{table_name}")
async def get_hash_history(table_name: str):
    """Get complete hash history for a table."""
    logger.info(f"Fetching hash history for table: {table_name}")
    try:
        history = config.hash_manager.get_hash_history(
            config.current_user_id, 
            table_name, 
            DEFAULT_PARTITION
        )
        logger.info(f"Hash history for table {table_name} fetched successfully")
        return {"history": history}
    except Exception as e:
        logger.error(f"Error fetching hash history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables")
async def get_user_tables():
    """Get all tables owned by current user."""
    logger.info("Fetching user tables")
    try:
        tables = config.hash_manager.get_user_tables(config.current_user_id)
        logger.info("User tables fetched successfully")
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error fetching user tables: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/share/cid")
async def share_cid(request: ShareCIDRequest):
    """Share a CID with another user."""
    logger.info(f"Sharing CID: {request.cid} for table: {request.table_name} with user: {request.to_user}")
    try:
        success = config.hash_manager.share_cid(
            request.to_user, 
            request.table_name, 
            request.cid
        )
        if not success:
            raise HTTPException(status_code=400, detail="Failed to share CID")
        logger.info("CID shared successfully")
        return {"status": "success", "message": "CID shared successfully"}
    except Exception as e:
        logger.error(f"Error sharing CID: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/revoke/cid")
async def revoke_cid_access(request: RevokeCIDRequest):
    """Revoke CID access from a user."""
    logger.info(f"Revoking CID: {request.cid} for table: {request.table_name} from user: {request.from_user}")
    try:
        success = config.hash_manager.revoke_cid_access(
            request.from_user, 
            request.table_name, 
            request.cid
        )
        if not success:
            raise HTTPException(status_code=400, detail="Failed to revoke CID access")
        logger.info("CID access revoked successfully")
        return {"status": "success", "message": "CID access revoked successfully"}
    except Exception as e:
        logger.error(f"Error revoking CID access: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/shared/cids/{table_name}")
async def get_shared_cids(table_name: str):
    """Get all shared CIDs for a table and current user."""
    logger.info(f"Fetching shared CIDs for table: {table_name}")
    try:
        cids, owners = config.hash_manager.get_shared_cids(
            config.current_user_id, 
            table_name
        )
        logger.info(f"Shared CIDs for table {table_name} fetched successfully")
        return {"cids": cids, "owners": owners}
    except Exception as e:
        logger.error(f"Error fetching shared CIDs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/state/{hash}")
async def get_state(hash: str):
    """Get state information for a specific hash."""
    logger.info(f"Fetching state for hash: {hash}")
    try:
        state = sql_shell.ipfs_handler.load_state(hash)
        if not state:
            raise HTTPException(status_code=404, detail="State not found")
        logger.info("State fetched successfully")
        return state
    except Exception as e:
        logger.error(f"Error fetching state: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/cleanup")
async def cleanup():
    """Cleanup all temporary tables and state."""
    logger.info("Cleaning up temporary tables and state")
    try:
        sql_shell.cleanup()
        logger.info("Cleanup completed successfully")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
