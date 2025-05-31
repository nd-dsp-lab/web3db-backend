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
from typing import List
from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from pyspark.sql import SparkSession
import concurrent.futures
from cidindex import CIDIndex

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Directory to store Parquet files from IPFS
SHARED_TMP_DIR = "/tmp/ipfs_parquet"
os.makedirs(SHARED_TMP_DIR, exist_ok=True)

# Global index tracking
app.state.index_cids = {
    'PatientID': None,
    'HospitalID': None,
    'Age': None,
}
app.state.index_sizes = {}
# Initialize Spark session in local mode
logger.info("Initializing Spark Session")
spark = (
    SparkSession.builder.appName("FastAPISparkDriver")
    .master("local[*]")  # Must be local to access /tmp paths reliably
    .config("spark.python.worker.reuse", "true")
    .config("spark.pyspark.python", "/usr/bin/python3")
    .config("spark.pyspark.driver.python", "/usr/bin/python3")
    .getOrCreate()
)
logger.info(f"Spark Session created: {spark.sparkContext.appName}")

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
        parquet_time_end = time.time()

        # Upload to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        ipfs_upload_start = time.time()
        resp = requests.post(ipfs_api, files={"file": ("patient_data.parquet", buffer)})
        ipfs_upload_end = time.time()
        resp.raise_for_status()
        data_cid = resp.json()["Hash"]
        buffer.close()
        del df 
        
        # Build/update index
        idx_start = time.time()
        for attr, values in indexed_values.items():
            data_to_add = [(v, data_cid) for v in values]
            existing_index = retrieve_index(attr)
            if existing_index:
                existing_index.update(data_to_add)
                index = existing_index
            else:
                index = CIDIndex(data=data_to_add)
            serialized = index.dump()
            serialized.seek(0, io.SEEK_END)
            index_size_bytes = serialized.tell()
            serialized.seek(0)
            app.state.index_sizes[attr] = index_size_bytes
            resp = requests.post(ipfs_api, files={"file": (f"{attr}_index", serialized)})
            resp.raise_for_status()
            app.state.index_cids[attr] = resp.json()["Hash"]
            serialized.close()
        idx_end = time.time()
        time_end = time.time()
        gc.collect()
        return {
            "data_cid": data_cid,
            "index_cids": app.state.index_cids,
            "index_sizes": app.state.index_sizes,
            "parquet_time_seconds": parquet_time_end - parquet_time_start,
            "ipfs_upload_time_seconds": ipfs_upload_end - ipfs_upload_start,
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
async def query_distributed(request: QueryRequest):
    logger.info("POST /query - Processing distributed query")
    query_start_time = time.time()
    idx_retrieve_start = time.time()
    index = retrieve_index(request.index_attribute)
    idx_retrieve_end = time.time()
    if not index:
        return {"error": f"Index for {request.index_attribute} not found"}
    idx_query_time_start = time.time()
    cids = query_index(index, request.query, request.index_attribute)
    idx_query_time_end = time.time()
    if not cids:
        return {"message": "No matching CIDs found"}

    paths = []

    cid_retrieve_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        results = list(executor.map(fetch_cid, cids))

    # Filter successful paths
    paths = [p for p in results if p]
    cid_retrieve_end = time.time()

    if not paths:
        return {"error": "No valid Parquet files retrieved"}

    # Apply Spark SQL directly on those Parquet files
    try:
        df = spark.read.option("mergeSchema", "false").parquet(*paths)
        df.createOrReplaceTempView("patient_data")
        result_df = spark.sql(request.query)
        results = [row.asDict() for row in result_df.collect()]
    except Exception as e:
        logger.error(f"Query error: {e}")
        return {"error": str(e)}
    finally:
        for p in paths:
            try:
                os.remove(p)
            except Exception as e:
                logger.warning(f"Failed to delete {p}: {e}")

    query_end_time = time.time()
    return {
        "cids": len(cids),
        "records": len(results),
        "results": results,
        "idx_retrieve_time_seconds": idx_retrieve_end - idx_retrieve_start,
        "idx_query_time_seconds": idx_query_time_end - idx_query_time_start,
        "cid_retrieve_time_seconds": cid_retrieve_end - cid_retrieve_start,
        "query_execution_time_seconds": query_end_time - query_start_time
    }

@app.get("/ipfs/fetch/{cid}")
async def fetch_from_ipfs(cid: str):
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

def retrieve_index(name):
    cid = app.state.index_cids.get(name)
    if not cid:
        return None
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=10)
        if resp.status_code != 200:
            return None
        index = CIDIndex()
        index.load(io.BytesIO(resp.content))
        return index
    except Exception as e:
        logger.error(f"Index retrieval failed for {name}: {e}")
        return None

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

def fetch_cid(cid):
    try:
        resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=10)
        if resp.status_code == 200:
            path = os.path.join(SHARED_TMP_DIR, f"{cid}.parquet")
            with open(path, "wb") as f:
                f.write(resp.content)
            return path
        else:
            logger.warning(f"Failed to fetch {cid} from IPFS")
    except Exception as e:
        logger.error(f"CID {cid} fetch failed: {e}")
    return None


class UpdateIndexCIDsRequest(BaseModel):
    index_cids: dict

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

        # Update the index CIDs
        for key, value in request.index_cids.items():
            app.state.index_cids[key] = value
            logger.info(f"Updated index CID for {key}: {value}")

        return {
            "status": "success",
            "message": "Index CIDs updated successfully",
            "updated_cids": request.index_cids,
            "current_cids": app.state.index_cids
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
    logger.info("GET /index-cids - Retrieving current index CIDs")
    try:
        return {
            "status": "success",
            "index_cids": app.state.index_cids,
            "index_sizes": app.state.index_sizes,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
    except Exception as e:
        logger.error(f"Error retrieving index CIDs: {e}")
        return {"status": "error", "message": str(e)}
