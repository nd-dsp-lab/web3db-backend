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

        # Convert to Parquet
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)

        # Upload to IPFS
        ipfs_api = "http://localhost:5001/api/v0/add"
        resp = requests.post(ipfs_api, files={"file": ("patient_data.parquet", buffer)})
        resp.raise_for_status()
        data_cid = resp.json()["Hash"]
        buffer.close()
        del df

        # Build/update index
        for attr, values in indexed_values.items():
            data_to_add = [(v, data_cid) for v in values]
            existing_index = retrieve_index(attr)
            if existing_index:
                existing_index.update(data_to_add)
                index = existing_index
            else:
                index = CIDIndex(data=data_to_add)
            serialized = index.dump()
            resp = requests.post(ipfs_api, files={"file": (f"{attr}_index", serialized)})
            resp.raise_for_status()
            app.state.index_cids[attr] = resp.json()["Hash"]
            serialized.close()

        gc.collect()
        return {
            "data_cid": data_cid,
            "index_cids": app.state.index_cids,
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
    index = retrieve_index(request.index_attribute)
    if not index:
        return {"error": f"Index for {request.index_attribute} not found"}

    cids = query_index(index, request.query, request.index_attribute)
    if not cids:
        return {"message": "No matching CIDs found"}

    start_time = time.time()
    paths = []

    for cid in cids:
        try:
            resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=10)
            if resp.status_code == 200:
                path = os.path.join(SHARED_TMP_DIR, f"{cid}.parquet")
                with open(path, "wb") as f:
                    f.write(resp.content)
                paths.append(path)
            else:
                logger.warning(f"Failed to fetch {cid} from IPFS")
        except Exception as e:
            logger.error(f"CID {cid} fetch failed: {e}")

    if not paths:
        return {"error": "No valid Parquet files retrieved"}

    # Apply Spark SQL directly on those Parquet files
    try:
        logger.info(f"Reading Parquet files: {paths}")
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

    elapsed = time.time() - start_time
    return {
        "records": len(results),
        "results": results,
        "execution_time_seconds": elapsed,
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
