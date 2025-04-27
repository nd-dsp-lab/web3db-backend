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
from index_state import IndexState
from pyspark.sql import SparkSession
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI()

logger.info("Starting FastAPI application")
logger.info("Loading environment variables")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
print(f"Spark master: {SPARK_MASTER}")
SPARK_DRIVER_HOST = os.environ.get("SPARK_DRIVER_HOST", "localhost")
print(f"Spark driver host: {SPARK_DRIVER_HOST}")
INFURA_API_KEY = os.environ.get("INFURA_API_KEY")
print(f"Infura API key: {INFURA_API_KEY}")
PRIVATE_KEY = os.environ.get("PRIVATE_KEY")
print(f"Private key: {PRIVATE_KEY}")
CONTRACT_ADDRESS = os.environ.get("CONTRACT_ADDRESS")
print(f"Contract address: {CONTRACT_ADDRESS}")

# Initialize the index state with smart contract connection
logger.info("Initializing IndexState with smart contract")
index_state = IndexState(
    contract_address=CONTRACT_ADDRESS,
    infura_api_key=INFURA_API_KEY,
    private_key=PRIVATE_KEY
)

# Define supported index attributes
SUPPORTED_INDEX_ATTRIBUTES = ['PatientID', 'HospitalID', 'Age']

logger.info("Initializing Spark Session")
spark = (
    SparkSession.builder.appName("FastAPISparkDriver")
    .master(SPARK_MASTER)
    .config("spark.blockManager.port", "10025")
    .config("spark.driver.blockManager.port", "10026")
    .config("spark.driver.host", SPARK_DRIVER_HOST)
    .config("spark.driver.port", "10027")
    .config("spark.python.worker.reuse", "true")
    .config("spark.pyspark.python", "/usr/bin/python3")
    .config("spark.pyspark.driver.python", "/usr/bin/python3")
    .getOrCreate()
)
logger.info(f"Spark Session created: {spark.sparkContext.appName}")


@app.post("/upload/patient-data")
async def upload_patient_data(file: UploadFile = File(...)):
    """
    Upload patient_data.csv to IPFS in Parquet format
    Returns a Content Identifier (CID) for the uploaded file
    Updates all indices in a single blockchain transaction
    """
    logger.info("POST /upload/patient-data - Processing patient data upload")

    try:
        # Read CSV file content into memory
        logger.info("Reading uploaded CSV file into memory")
        content = await file.read()

        # Process CSV in memory
        logger.info("Converting CSV to DataFrame")
        csv_buffer = io.BytesIO(content)
        df = pd.read_csv(csv_buffer, dtype={"PatientID": str, "HospitalID": str, "Age": int})
        
        # Get values for the index attributes
        indexed_values = {}
        for index_key in SUPPORTED_INDEX_ATTRIBUTES:
            if index_key in df.columns:
                indexed_values[index_key] = set(df[index_key].values)
                logger.info(f"Found {len(indexed_values[index_key])} unique values for {index_key}")
            else:
                logger.warning(f"Index attribute {index_key} not found in DataFrame columns")
        
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
        ipfs_api_url = "http://localhost:5001/api/v0/add"
        logger.info(f"Uploading Parquet data to IPFS node at {ipfs_api_url}")

        response = requests.post(
            ipfs_api_url,
            files={
                "file": (
                    "patient_data.parquet",
                    parquet_buffer,
                    "application/octet-stream",
                )
            },
        )
        response.raise_for_status()
        cid = response.json()["Hash"]
        logger.info(f"Patient data uploaded to IPFS with CID: {cid}")

        # Clear the parquet buffer
        parquet_buffer.close()
        del parquet_buffer
        
        # Prepare for batch processing
        attributes_to_update = []
        cids_to_update = []
        updated_indices = {}
        
        # First, retrieve current index CIDs
        for index_key in indexed_values.keys():
            # Get current index CID from smart contract
            logger.info(f"Retrieving current index for attribute: {index_key}")
            success, current_cid = index_state.get_index(index_key)
            
            if not success:
                logger.error(f"Failed to retrieve index for {index_key} from smart contract")
                continue
                
            # Process the index
            if current_cid == "":
                # Create new index
                logger.info(f"Creating new index for {index_key}")
                index = CIDIndex([(val, cid) for val in indexed_values[index_key]])
                logger.info(f"Index type: {index.index_type}")
            else:
                # Retrieve existing index
                index = retrieve_index(index_key)
                if not index:
                    logger.error(f"Failed to retrieve existing index for {index_key}")
                    continue
                    
                # Update existing index
                logger.info(f"Updating existing index for {index_key}")
                index.update([(val, cid) for val in indexed_values[index_key]])
                
            # Serialize and upload index to IPFS
            logger.info(f"Serializing index for {index_key}")
            serialized_index = index.dump()
            
            # Put index on IPFS
            logger.info(f"Uploading index for {index_key} to IPFS")
            response = requests.post(
                ipfs_api_url, 
                files={"file": (f"{index_key}_index", serialized_index, "application/octet-stream")}
            )
            response.raise_for_status()
            index_cid = response.json()["Hash"]
            logger.info(f"Index for {index_key} uploaded to IPFS with CID: {index_cid}")
            
            # Add to batch update lists
            attributes_to_update.append(index_key)
            cids_to_update.append(index_cid)
            updated_indices[index_key] = index_cid
            
            # Clear the serialized index
            serialized_index.close()
            del serialized_index
            # Clear the index object
            del index

        # Perform batch update to smart contract
        if attributes_to_update:
            logger.info(f"Performing batch update for {len(attributes_to_update)} indices")
            logger.info(f"Attributes: {attributes_to_update}")
            logger.info(f"CIDs: {cids_to_update}")
            
            success = index_state.batch_update_indices(attributes_to_update, cids_to_update)
            
            if success:
                logger.info(f"Successfully updated all {len(attributes_to_update)} indices in a single transaction")
            else:
                logger.error("Batch update failed, falling back to individual updates")
                
                # Fall back to individual updates if batch update fails
                updated_indices = {}
                for i, attr in enumerate(attributes_to_update):
                    success = index_state.update_index(attr, cids_to_update[i])
                    if success:
                        logger.info(f"Successfully updated {attr} index")
                        updated_indices[attr] = cids_to_update[i]
                    else:
                        logger.error(f"Failed to update {attr} index")

        # Explicitly trigger garbage collection
        gc.collect()

        logger.info("Memory buffers cleared")

        # Return the CID and updated indices
        return {
            "data_message": f"Patient data uploaded successfully at {cid}", 
            "index_message": f"Successfully updated indices: {updated_indices}"
        }
        
    except Exception as e:
        # Make sure to clean up memory even if an error occurs
        logger.error(f"Error processing patient data: {str(e)}")
        gc.collect()
        return {"error": f"Failed to process and upload data: {str(e)}"}

# Define request model
class QueryRequest(BaseModel):
    index_attribute: str = 'HospitalID'
    query: str = "select * from patient_data where HospitalID = 'HOSP-003'"


@app.post("/query")
async def query_distributed(request: QueryRequest):
    """
    Distributed query across multiple IPFS CIDs with parallel data retrieval
    and centralized query execution
    
    Example query: select * from patient_data where HospitalID = 'HOSP-003'
    """
    logger.info("POST /query - Processing distributed query across CIDs")
    
    # Validate index attribute
    if request.index_attribute not in SUPPORTED_INDEX_ATTRIBUTES:
        return {"error": f"Unsupported index attribute: {request.index_attribute}. Supported attributes are: {SUPPORTED_INDEX_ATTRIBUTES}"}
    
    # Get index from smart contract and IPFS
    index = retrieve_index(request.index_attribute)
    if not index:
        logger.error(f"Index for {request.index_attribute} not found or could not be retrieved")
        return {"error": f"Index for {request.index_attribute} not found or could not be retrieved"}
    
    logger.info(f"Index for {request.index_attribute} retrieved successfully")
    
    # Query the index to find relevant CIDs
    cids = query_index(index, request.query, request.index_attribute)
    logger.info(f"Query returned {len(cids)} CIDs")
    logger.info(f"Query CIDs: {cids}")

    try:
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
                ipfs_api_url = f"http://localhost:5001/api/v0/cat"
                response = requests.post(ipfs_api_url, params={"arg": cid}, timeout=10)

                if response.status_code != 200:
                    print(
                        f"Worker {worker_id} failed to fetch CID: {cid} with status code: {response.status_code}"
                    )
                    return []  # Return empty list for concat

                # Process the Parquet data
                binary_data = response.content
                parquet_buffer = io.BytesIO(binary_data)
                table = pq.read_table(parquet_buffer)
                df = table.to_pandas()

                # Add source CID and worker info
                df["source_cid"] = cid
                df["worker_id"] = worker_id

                print(f"Worker {worker_id} successfully processed CID: {cid}")

                # Return as list of dictionaries (records)
                return df.to_dict(orient="records")

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
        worker_assignments = {
            row["source_cid"]: row["worker_id"] for row in worker_data
        }
        processed_cids = list(worker_assignments.keys())

        # Register as temporary view
        spark_df.createOrReplaceTempView("patient_data")

        # Execute the SQL query
        logger.info(f"Executing query: {query}")
        result_df = spark.sql(query)

        # Convert results to JSON
        result_json = result_df.toJSON().collect()
        result_json = [eval(record.replace("null", "None")) for record in result_json]

        logger.info(
            f"Query completed successfully, returning {len(result_json)} records"
        )

        return {
            "message": "Distributed query executed successfully",
            "cids_processed": len(processed_cids),
            "worker_assignments": worker_assignments,
            "record_count": len(result_json),
            "results": result_json,
        }

    except Exception as e:
        logger.error(f"Error processing distributed query: {str(e)}")
        return {"error": f"Failed to process distributed query: {str(e)}"}
    
    
def retrieve_index(name):
    """
    Retrieve an index from IPFS using the CID stored in the smart contract.
    
    Args:
        name (str): The name of the index to retrieve
        
    Returns:
        CIDIndex: The retrieved index, or None if not found
    """
    logger.info(f"Retrieving index for {name}")
    
    # Get index CID from smart contract
    success, index_cid = index_state.get_index(name)
    if not success:
        logger.error(f"Failed to retrieve index CID for {name} from blockchain")
        return None
        
    if not index_cid or index_cid == "":
        logger.info(f"No index CID found for {name} in smart contract")
        return None

    # Fetch from IPFS using POST for the cat API
    try:
        ipfs_api_url = f"http://localhost:5001/api/v0/cat"
        logger.info(f"Requesting index from IPFS at URL: {ipfs_api_url} with CID: {index_cid}")
        
        response = requests.post(ipfs_api_url, params={"arg": index_cid}, timeout=10)
        
        if response.status_code == 200:
            serialized_index = response.content
        else:
            logger.error(f"Failed to retrieve index: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error retrieving {name} index from IPFS: {str(e)}")
        return None

    # Deserialize the index
    index = CIDIndex()
    logger.info(f"Deserializing {name} index")
    try:
        index.load(io.BytesIO(serialized_index))
        logger.info(f"Index for {name} loaded successfully")
        return index
    except Exception as e:
        logger.error(f"Failed to deserialize index: {str(e)}")
        return None


def query_index(index, query, index_attribute) -> list:
    """
    Query the index for CIDs matching the given query, this function assumes the index_attribute condition 
    is presented as an "and" condition in the query.
    
    Args:
        index (CIDIndex): The index to query
        query (str): The sql query string
        index_attribute (str): The attribute to use for querying
        
    Returns:
        list: List of CIDs matching the query for the default index attribute
    """

    # Parse the WHERE clause from the query
    where_pattern = re.compile(r"where\s+(.*)", re.IGNORECASE)
    where_match = where_pattern.search(query)
    if not where_match:
        logger.info("No WHERE clause found in the query, retrieving all CIDs")
        cids = index.query_range()  # Query all CIDs
        return cids
    
    where_clause = where_match.group(1).strip()

    # Extract conditions related to the index_attribute
    conditions = []
    for condition in re.split(r"\s+and\s+", where_clause, flags=re.IGNORECASE):
        if index_attribute in condition:
            conditions.append(condition.strip())

    logger.info(f"Extracted conditions for index attribute '{index_attribute}': {conditions}")
    logger.info(f"Index type: {index.index_type}")
    
    if not conditions:
        logger.error(f"No conditions found for index on '{index_attribute}', return all CIDs")
        cids = index.query_range()
        return cids

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
            all_cids = set(index.query_range())  # Query all CIDs
            excluded_cids = set(index.query(key))
            cids.update(all_cids - excluded_cids)
        else:
            raise ValueError(f"Unsupported condition format: {condition}")

    return list(cids)