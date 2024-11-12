from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from spark_shell import SparkSQLShell

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

# Initialize SparkSQL shell as a global instance
sql_shell = SparkSQLShell()

class SQLQuery(BaseModel):
    query: str
    state_hash: Optional[str] = None

class SQLResult(BaseModel):
    type: str
    hash: str
    data: Optional[List[Dict[str, Any]]] = None

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.post("/execute", response_model=SQLResult)
async def execute_sql(sql_query: SQLQuery):
    """
    Execute SQL query with optional state hash
    """
    try:
        result = sql_shell.execute_sql(sql_query.query, sql_query.state_hash)
        if result is None:
            raise HTTPException(status_code=400, detail="Query execution failed")
            
        return SQLResult(
            type=result["type"],
            hash=result["hash"],
            data=result.get("data")
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/state/{hash}")
async def get_state(hash: str):
    """
    Get state information for a specific hash
    """
    try:
        state = sql_shell.ipfs_handler.load_state(hash)
        if not state:
            raise HTTPException(status_code=404, detail="State not found")
        return state
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/cleanup")
async def cleanup():
    """
    Cleanup all temporary tables and state
    """
    try:
        sql_shell.cleanup()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))