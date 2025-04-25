from math import inf
import re
from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from typing import List
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import logging
import gc
from cidindex import CIDIndex
from pyspark.sql import SparkSession


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize global variables for index
# Add HospitalID for testing (because there is no repeated PatientID in the test data)

app.state.index_cids = {
    'PatientID': None,
    'HospitalID': None,    
}

# change the default index attribute to test the index
default_index_attribute = 'HospitalID'


logger.info("Starting FastAPI application")

logger.info("Initializing Spark Session")
spark = SparkSession.builder \
    .appName("FastAPISparkDriver") \
    .master("spark://spark-master:7077") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.pyspark.python", "/usr/bin/python3") \
    .config("spark.pyspark.driver.python", "/usr/bin/python3") \
    .getOrCreate()
logger.info(f"Spark Session created: {spark.sparkContext.appName}")

@app.post("/upload/patient-data")
async def upload_patient_data(file: UploadFile = File(...)):
    """
    Upload patient_data.csv to IPFS in Parquet format
    Returns a Content Identifier (CID) for the uploaded file
    """
    logger.info("POST /upload/patient-data - Processing patient data upload")
    
    try:
        # Read CSV file content into memory
        logger.info("Reading uploaded CSV file into memory")
        content = await file.read()
        
        # Process CSV in memory
        logger.info("Converting CSV to DataFrame")
        csv_buffer = io.BytesIO(content)
        df = pd.read_csv(csv_buffer)
        
        # Get values for the default index attribute
        indexed_values = set(df[default_index_attribute].values)
        
        # Clear initial content and CSV buffer
        del content
        csv_buffer.close()
        del csv_buffer
        
        # Convert DataFrame to Parquet in memory
        logger.info("Converting DataFrame to Parquet format in memory")
        parquet_buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        
        # Clear DataFrame and table after conversion
        del df
        
        pq.write_table(table, parquet_buffer)
        del table
        
        # Reset buffer position to beginning
        parquet_buffer.seek(0)
        
        # Upload to IPFS
        ipfs_api_url = "http://ipfs:5001/api/v0/add"
        logger.info(f"Uploading Parquet data to IPFS node at {ipfs_api_url}")
        
        response = requests.post(
            ipfs_api_url, 
            files={"file": ("patient_data.parquet", parquet_buffer, "application/octet-stream")}
        )
        response.raise_for_status()
        cid = response.json()["Hash"]
        logger.info(f"Patient data uploaded to IPFS with CID: {cid}")
        
        # Clear the parquet buffer
        parquet_buffer.close()
        del parquet_buffer
        
        # Create/update index for the default index attribute
        logger.info(f"Creating/updating index for attribute: {default_index_attribute}")
        if default_index_attribute not in app.state.index_cids or app.state.index_cids[default_index_attribute] is None:
            # Create new index
            logger.info(f"Creating new index for {default_index_attribute}")
            index = CIDIndex([(val, cid) for val in indexed_values])
        else:
            index = retrieve_index(default_index_attribute)
            # Update existing index
            logger.info(f"Updating existing index for {default_index_attribute}")
            index.update([(val, cid) for val in indexed_values])
        # Serialize and upload index to IPFS
        logger.info(f"Serializing index for {default_index_attribute}")
        serialized_index = index.dump()
        # Put index on IPFS
        logger.info(f"Uploading index for {default_index_attribute} to IPFS")
        response = requests.post(
            ipfs_api_url, 
            files={"file": (f"{default_index_attribute}_index", serialized_index, "application/octet-stream")}
        )
        response.raise_for_status()
        index_cid = response.json()["Hash"]
        logger.info(f"Index for {default_index_attribute} uploaded to IPFS with CID: {index_cid}")
        # Update global state with new index CID
        app.state.index_cids[default_index_attribute] = index_cid
        logger.info(f"Updated index CID for {default_index_attribute} in global state")
        # Clear the serialized index
        serialized_index.close()
        del serialized_index
        # Clear the index object
        del index

        
        
        # Explicitly trigger garbage collection
        gc.collect()
        
        logger.info("Memory buffers cleared")
        
        # Return the CID
        return {"data_message": "Patient data uploaded successfully at " + cid, "index_message": "Index uploaded successfully at " + index_cid}
    except Exception as e:
        # Make sure to clean up memory even if an error occurs
        logger.error(f"Error processing patient data: {str(e)}")
        gc.collect()
        return {"error": f"Failed to process and upload data: {str(e)}"}
# Define request model
class QueryRequest(BaseModel):
    cids: List[str]
    index_attribute: str = default_index_attribute
    query: str = "select * from patient_data where HospitalID = 'HOSP-003'"

@app.post("/query")
async def query_distributed(request: QueryRequest):
    """
    Distributed query across multiple IPFS CIDs with parallel data retrieval
    and centralized query execution
    
    Example query: select * from patient_data where HospitalID = 'HOSP-003'
    """
    logger.info("POST /query - Processing distributed query across CIDs")
    
    index = retrieve_index(default_index_attribute)
    if not index:
        logger.error(f"Index for {default_index_attribute} not found")
        return {"error": f"Index for {default_index_attribute} not found"}
    logger.info(f"Index for {default_index_attribute} retrieved successfully")
    
    cids = query_index(index, request.query, default_index_attribute)
    # check the cids
    logger.info(f"Query returned {len(cids)} CIDs")
    logger.info(f"Query CIDs: {cids}")
    
    try:
        # cids = request.cids
        query = request.query
        
        if not cids or not query:
            return {"message": "No data to process or query is empty"}
            
        logger.info(f"Processing {len(cids)} CIDs with query: {query}")
        
        # Create an RDD from the CIDs list
        cids_rdd = spark.sparkContext.parallelize(cids)
        
        # Process each CID in parallel to fetch and transform data
        def fetch_and_process_data(cid):
            import requests
            import base64
            import socket
            import os
            import io
            import pandas as pd
            import pyarrow.parquet as pq
            
            # Get worker information
            hostname = socket.gethostname()
            worker_pid = os.getpid()
            worker_id = f"{hostname}-{worker_pid}"
            
            try:
                # Log at the beginning of processing
                print(f"Worker {worker_id} starting to fetch CID: {cid}")
                
                # Fetch data from IPFS
                ipfs_api_url = f"http://ipfs:5001/api/v0/cat"
                response = requests.post(ipfs_api_url, params={"arg": cid}, timeout=10)
                
                if response.status_code != 200:
                    print(f"Worker {worker_id} failed to fetch CID: {cid} with status code: {response.status_code}")
                    return []  # Return empty list for concat
                
                # Process the Parquet data
                binary_data = response.content
                parquet_buffer = io.BytesIO(binary_data)
                table = pq.read_table(parquet_buffer)
                df = table.to_pandas()
                
                # Add source CID and worker info
                df['source_cid'] = cid
                df['worker_id'] = worker_id
                
                print(f"Worker {worker_id} successfully processed CID: {cid}")
                
                # Return as list of dictionaries (records)
                return df.to_dict(orient='records')
                
            except Exception as e:
                print(f"Worker {worker_id} encountered error with CID {cid}: {str(e)}")
                return []  # Return empty list for failed CIDs
        
        # Map each CID to its processed records and flatten the results
        all_records = cids_rdd.flatMap(fetch_and_process_data).collect()
        
        # If no data was processed, return error
        if not all_records:
            return {"error": "Failed to process any data from IPFS"}
            
        # Create a Spark DataFrame directly from all records
        spark_df = spark.createDataFrame(all_records)
        
        # Extract worker assignments for reporting
        worker_data = spark_df.select("source_cid", "worker_id").distinct().collect()
        worker_assignments = {row["source_cid"]: row["worker_id"] for row in worker_data}
        processed_cids = list(worker_assignments.keys())
        
        # Register as temporary view
        spark_df.createOrReplaceTempView("patient_data")
        
        # Execute the SQL query
        logger.info(f"Executing query: {query}")
        result_df = spark.sql(query)
        
        # Convert results to JSON
        result_json = result_df.toJSON().collect()
        result_json = [eval(record.replace('null', 'None')) for record in result_json]
        
        logger.info(f"Query completed successfully, returning {len(result_json)} records")
        
        return {
            "message": "Distributed query executed successfully",
            "cids_processed": len(processed_cids),
            "worker_assignments": worker_assignments,
            "record_count": len(result_json),
            "results": result_json
        }
        
    except Exception as e:
        logger.error(f"Error processing distributed query: {str(e)}")
        return {"error": f"Failed to process distributed query: {str(e)}"}
    
    
# put here for testing
def retrieve_index(name):
    """
    Retrieve index from IPFS
    Args:
        name (str): Name of the index to retrieve
    Returns:
        CIDIndex: The retrieved index object
    """
    logger.info(f"Retrieving index for {name}")
    if name not in app.state.index_cids:
        logger.error(f"Index for {name} not found in global state")
        return None
    serialized_index = None
    index_cid = app.state.index_cids[name]

    # try fetch from IPFS
    try:
        ipfs_url = f"http://ipfs:8080/ipfs/{index_cid}"
        logger.info(f"Requesting index from IPFS at URL: {ipfs_url}")
        
        response = requests.get(ipfs_url, timeout=10)
        # Fetch chunk data from IPFS
        logger.info(response)
        if response.status_code == 200:
            serialized_index = response.content
        else:
            serialized_index = None
    except:
        logger.info(f"{name} index does not exist")
        serialized_index = None

    index = CIDIndex()
    logger.info(f"Deserializing {name} index")
    if not serialized_index:
        logger.info(f"Index for {name} not found in IPFS")
    else:
        index.load(io.BytesIO(serialized_index))
        logger.info(f"Index for {name} loaded successfully")
    return index


def query_index(index, query, index_attribute) -> list:
    """
    Query the index for CIDs matching the given query, this function assumes the index_attribute condition is presented as an "and" condition in the query.
    Args:
        index (CIDIndex): The index to query
        query (str): The sql query string
        index_attribute (str): The attribute to use for querying
    Returns:
        list: List of CIDs matching the query for the default index attribute
    """

    # Parse the query to extract the attribute and value
    # Simplified version, only handles integer and string comparisons
    
    # Parse the WHERE clause from the query
    where_pattern = re.compile(r"where\s+(.*)", re.IGNORECASE)
    where_match = where_pattern.search(query)
    if not where_match:
        raise ValueError("No WHERE clause found in the query.")
    
    where_clause = where_match.group(1).strip()

    # Extract conditions related to the index_attribute
    conditions = []
    for condition in re.split(r"\s+and\s+", where_clause, flags=re.IGNORECASE):
        if index_attribute in condition:
            conditions.append(condition.strip())

    logger.info(f"Extracted conditions for index attribute '{index_attribute}': {conditions}")
    
    if not conditions:
        raise ValueError(f"No conditions found for index attribute '{index_attribute}'.")

    # Process conditions and query the index
    cids = set()
    for condition in conditions:
        if ">=" in condition:
            key = condition.split(">=")[1].strip().strip("'\"")
            key = int(key) if index.index_type == "bplustree" else key
            cids.update(index.query_range(key))
        elif "<=" in condition:
            key = condition.split("<=")[1].strip().strip("'\"")
            key = int(key) if index.index_type == "bplustree" else key
            cids.update(index.query_range(-inf, key))
        elif ">" in condition:
            key = condition.split(">")[1].strip().strip("'\"")
            key = int(key) if index.index_type == "bplustree" else key
            cids.update(index.query_range(key+1, inf))
        elif "<" in condition:
            key = condition.split("<")[1].strip().strip("'\"")
            key = int(key) if index.index_type == "bplustree" else key
            cids.update(index.query_range(-inf, key-1))
        elif "=" in condition:
            key = condition.split("=")[1].strip().strip("'\"")
            key = int(key) if index.index_type == "bplustree" else key
            cids.update(index.query(key))
        elif "!=" in condition:
            key = condition.split("!=")[1].strip().strip("'\"")
            key = int(key) if index.index_type == "bplustree" else key
            all_cids = set(index.query_range(None, None))  # Query all CIDs
            excluded_cids = set(index.query(key))
            cids.update(all_cids - excluded_cids)
        else:
            raise ValueError(f"Unsupported condition format: {condition}")

    return list(cids)