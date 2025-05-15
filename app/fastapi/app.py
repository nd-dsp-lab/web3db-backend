# fastapi/app.py
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
from cidindex import CIDIndex # Assuming cidindex.py is in the same directory or PYTHONPATH
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

# Initialize global variables for index
app.state.index_cids = {
    'PatientID': None,
    'HospitalID': None,
    'Age': None,
}

logger.info("Starting FastAPI application")
logger.info("Initializing Spark Session in LOCAL mode")

# --- Spark Session for Local Mode ---
# Determine the appropriate host for Spark UI if needed; for enclave, often 127.0.0.1 is safest internally
# If network_mode: host is used for docker, Spark might try to use the host's actual IP.
# SPARK_DRIVER_HOST_ENV = os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1")

spark = (
    SparkSession.builder.appName("FastAPISparkLocalDriver")
    .master("local[*]")  # Run Spark locally using all available cores
    # .config("spark.driver.host", SPARK_DRIVER_HOST_ENV) # Sets the hostname Spark uses for its services (e.g., UI)
                                                       # Default usually works, but can be set for clarity.
    .config("spark.driver.bindAddress", "0.0.0.0") # Binds Spark services (like UI) to all interfaces within container
                                                  # So it can be accessed via mapped ports or host network.
    .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040")) # Standard Spark UI port
    .config("spark.python.worker.reuse", "true")
    .config("spark.pyspark.python", "/usr/bin/python3") # Ensure this matches python in container
    .config("spark.pyspark.driver.python", "/usr/bin/python3") # Ensure this matches python in container
    .getOrCreate()
)
logger.info(f"Spark Session created: {spark.sparkContext.appName}, Master: {spark.sparkContext.master}")
if spark.sparkContext.uiWebUrl:
    logger.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
else:
    logger.info("Spark UI URL not available (yet or Spark UI is disabled). Typically http://<driver-host>:4040")

@app.post("/upload/patient-data")
async def upload_patient_data(file: UploadFile = File(...)):
    """
    Upload patient_data.csv to IPFS in Parquet format
    Returns a Content Identifier (CID) for the uploaded file
    """
    logger.info("POST /upload/patient-data - Processing patient data upload")

    try:
        logger.info("Reading uploaded CSV file into memory")
        content = await file.read()

        logger.info("Converting CSV to DataFrame")
        csv_buffer = io.BytesIO(content)
        df = pd.read_csv(csv_buffer, dtype={"PatientID": str, "HospitalID": str, "Age": int})

        indexed_values = {}
        for index_key in app.state.index_cids.keys():
            if index_key in df.columns:
                indexed_values[index_key] = set(df[index_key].astype(str if index_key != 'Age' else int).values) # Ensure correct type
            else:
                logger.warning(f"Index attribute {index_key} not found in DataFrame columns")

        del content
        csv_buffer.close()
        del csv_buffer

        logger.info("Converting DataFrame to Parquet format in memory")
        parquet_buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        del df
        pq.write_table(table, parquet_buffer)
        del table
        parquet_buffer.seek(0)

        ipfs_api_url_add = os.environ.get("IPFS_API_URL_ADD", "http://127.0.0.1:5001/api/v0/add")
        logger.info(f"Uploading Parquet data to IPFS node at {ipfs_api_url_add}")

        response = requests.post(
            ipfs_api_url_add,
            files={"file": ("patient_data.parquet", parquet_buffer, "application/octet-stream")},
            timeout=30 # Add a timeout
        )
        response.raise_for_status()
        cid = response.json()["Hash"]
        logger.info(f"Patient data uploaded to IPFS with CID: {cid}")

        parquet_buffer.close()
        del parquet_buffer

        for index_key in indexed_values.keys():
            logger.info(f"Creating/updating index for attribute: {index_key}")
            if not app.state.index_cids.get(index_key): # Simplified check
                logger.info(f"Creating new index for {index_key}")
                # Ensure data type matches expected by CIDIndex (int for Age, str otherwise)
                data_for_index = [(val, cid) for val in indexed_values[index_key]]
                if not data_for_index:
                    logger.warning(f"No values to index for {index_key}")
                    continue

                index = CIDIndex(data_for_index)
                logger.info(f"Index type for {index_key}: {index.index_type}")
            else:
                index = retrieve_index(index_key)
                if not index: # If retrieve_index fails
                     logger.error(f"Failed to retrieve existing index for {index_key}, cannot update.")
                     continue
                logger.info(f"Updating existing index for {index_key}")
                data_for_update = [(val, cid) for val in indexed_values[index_key]]
                if not data_for_update:
                    logger.warning(f"No new values to update index for {index_key}")
                    continue
                index.update(data_for_update)

            logger.info(f"Serializing index for {index_key}")
            serialized_index_stream = index.dump() # Returns BytesIO stream

            logger.info(f"Uploading index for {index_key} to IPFS")
            response_idx = requests.post(
                ipfs_api_url_add,
                files={"file": (f"{index_key}_index", serialized_index_stream, "application/octet-stream")},
                timeout=30 # Add a timeout
            )
            response_idx.raise_for_status()
            index_cid = response_idx.json()["Hash"]
            logger.info(f"Index for {index_key} uploaded to IPFS with CID: {index_cid}")

            app.state.index_cids[index_key] = index_cid
            logger.info(f"Updated index CID for {index_key} in global state: {app.state.index_cids[index_key]}")

            serialized_index_stream.close()
            del serialized_index_stream
            del index

        gc.collect()
        logger.info("Memory buffers cleared and garbage collected.")
        return {"data_message": f"Patient data uploaded successfully at {cid}",
                "index_message": f"Successfully created/updated indices. Current index CIDs: {app.state.index_cids}"}

    except requests.exceptions.RequestException as e:
        logger.error(f"IPFS request failed: {str(e)}")
        gc.collect()
        return {"error": f"Failed to communicate with IPFS: {str(e)}"}
    except Exception as e:
        logger.error(f"Error processing patient data: {str(e)}", exc_info=True)
        gc.collect()
        return {"error": f"Failed to process and upload data: {str(e)}"}

class QueryRequest(BaseModel):
    index_attribute: str = 'HospitalID'
    query: str = "select * from patient_data where HospitalID = 'HOSP-003'"

@app.post("/query")
async def query_distributed(request: QueryRequest):
    logger.info(f"POST /query - Processing query with index attribute: {request.index_attribute}")

    index_attribute = request.index_attribute
    index_to_query = retrieve_index(index_attribute)

    if not index_to_query:
        logger.error(f"Index for {index_attribute} not found or failed to load.")
        return {"error": f"Index for {index_attribute} not found or failed to load."}
    logger.info(f"Index for {index_attribute} retrieved successfully. Type: {index_to_query.index_type}")

    cids_from_index = query_index(index_to_query, request.query, index_attribute)
    logger.info(f"Querying index for '{request.index_attribute}' returned {len(cids_from_index)} CIDs: {cids_from_index}")

    if not cids_from_index:
        return {"message": "No data CIDs found for the given query from the index. No data to process."}

    try:
        query_sql = request.query
        logger.info(f"Processing {len(cids_from_index)} CIDs with query: {query_sql}")

        cids_rdd = spark.sparkContext.parallelize(cids_from_index)

        def fetch_and_process_data(cid_to_fetch):
            # Ensure all imports are within the function for Spark execution on workers (even in local mode)
            import requests
            import socket
            import os
            import io
            import pandas as pd
            import pyarrow.parquet as pq_worker # Alias to avoid conflict

            worker_hostname = socket.gethostname()
            worker_pid = os.getpid()
            worker_id = f"{worker_hostname}-{worker_pid}"
            ipfs_api_url_cat_worker = os.environ.get("IPFS_API_URL_CAT", "http://127.0.0.1:5001/api/v0/cat")


            try:
                logger_worker = logging.getLogger(f"worker.{worker_id}") # Worker specific logger
                logger_worker.info(f"Worker {worker_id} starting to fetch CID: {cid_to_fetch}")

                response_worker = requests.post(ipfs_api_url_cat_worker, params={"arg": cid_to_fetch}, timeout=60) # Increased timeout

                if response_worker.status_code != 200:
                    logger_worker.error(f"Worker {worker_id} failed to fetch CID: {cid_to_fetch} with status: {response_worker.status_code} - {response_worker.text}")
                    return []

                binary_data_worker = response_worker.content
                parquet_buffer_worker = io.BytesIO(binary_data_worker)
                table_worker = pq_worker.read_table(parquet_buffer_worker)
                df_worker = table_worker.to_pandas()

                df_worker["source_cid"] = cid_to_fetch
                df_worker["worker_id"] = worker_id
                logger_worker.info(f"Worker {worker_id} successfully processed CID: {cid_to_fetch}, {len(df_worker)} rows")
                return df_worker.to_dict(orient="records")

            except Exception as e_worker:
                logger_worker.error(f"Worker {worker_id} encountered error with CID {cid_to_fetch}: {str(e_worker)}", exc_info=True)
                return []

        all_records = cids_rdd.flatMap(fetch_and_process_data).collect()

        if not all_records:
            logger.warning("No records were processed from IPFS CIDs after RDD execution.")
            # Check if CIDs were valid but empty, or if fetching failed.
            # This response might indicate data not found or processing errors.
            return {"message": "Query executed, but no data records were retrieved or processed from the identified CIDs.", "results": []}


        logger.info(f"Collected {len(all_records)} records from RDD. Creating Spark DataFrame.")
        spark_df = spark.createDataFrame(all_records) # Create DataFrame from collected records

        worker_assignments = {}
        if "source_cid" in spark_df.columns and "worker_id" in spark_df.columns:
            worker_data = spark_df.select("source_cid", "worker_id").distinct().collect()
            worker_assignments = {row["source_cid"]: row["worker_id"] for row in worker_data}

        processed_cids_count = len(worker_assignments.keys())

        spark_df.createOrReplaceTempView("patient_data")
        logger.info(f"Executing SQL query: {query_sql}")
        result_df = spark.sql(query_sql)
        result_json_str_list = result_df.toJSON().collect()

        # Safely evaluate JSON strings, converting "null" to None
        results_final = []
        for r_str in result_json_str_list:
            try:
                # More robust null handling if direct eval is problematic
                processed_r_str = r_str.replace(":null", ":None").replace(": null", ": None")
                results_final.append(eval(processed_r_str))
            except Exception as e_eval:
                logger.error(f"Error evaluating record string '{r_str}': {e_eval}")
                # Decide how to handle: skip, add as is, add with error marker
        
        logger.info(f"Query completed successfully, returning {len(results_final)} records")
        return {
            "message": "Distributed query executed successfully",
            "cids_queried_from_index": len(cids_from_index),
            "cids_processed_by_spark": processed_cids_count,
            "worker_assignments": worker_assignments,
            "record_count": len(results_final),
            "results": results_final,
        }
    except Exception as e:
        logger.error(f"Error processing distributed query: {str(e)}", exc_info=True)
        return {"error": f"Failed to process distributed query: {str(e)}"}

def retrieve_index(name):
    logger.info(f"Retrieving index for {name}")
    index_cid = app.state.index_cids.get(name) # Use .get for safer access

    if not index_cid:
        logger.error(f"Index CID for '{name}' not found in global state. Available: {app.state.index_cids}")
        return None

    serialized_index = None
    ipfs_api_url_cat = os.environ.get("IPFS_API_URL_CAT", "http://127.0.0.1:5001/api/v0/cat")

    try:
        logger.info(f"Requesting index from IPFS at URL: {ipfs_api_url_cat} with CID: {index_cid}")
        response = requests.post(ipfs_api_url_cat, params={"arg": index_cid}, timeout=30) # Add timeout

        if response.status_code == 200:
            serialized_index = response.content
            logger.info(f"Successfully retrieved {len(serialized_index)} bytes for index '{name}' from IPFS.")
        else:
            logger.error(f"Failed to retrieve index '{name}' (CID: {index_cid}): {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"IPFS request failed for index '{name}' (CID: {index_cid}): {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error retrieving {name} index (CID: {index_cid}): {str(e)}", exc_info=True)
        return None

    if not serialized_index: # Should be redundant due to checks above, but good practice
        logger.info(f"Index for {name} (CID: {index_cid}) not found or data is empty in IPFS.")
        return None

    index_instance = CIDIndex() # Create an empty instance first
    logger.info(f"Deserializing {name} index")
    try:
        index_instance.load(io.BytesIO(serialized_index)) # Load into the instance
        logger.info(f"Index for {name} (type: {index_instance.index_type}) loaded successfully")
        return index_instance
    except Exception as e:
        logger.error(f"Failed to deserialize index '{name}' (CID: {index_cid}): {str(e)}", exc_info=True)
        return None


def query_index(index, query, index_attribute) -> list:
    logger.info(f"Querying index of type '{index.index_type}' for attribute '{index_attribute}' with SQL: {query}")
    where_pattern = re.compile(r"where\s+(.*)", re.IGNORECASE)
    where_match = where_pattern.search(query)

    if not where_match:
        logger.info("No WHERE clause found in the query, retrieving all CIDs from index.")
        # Ensure query_range handles None for start/end if that's how it gets all CIDs
        return list(index.query_range()) # Make sure it's a list

    where_clause = where_match.group(1).strip()
    conditions_on_indexed_attr = []
    # Split by AND, considering potential spaces and case insensitivity
    for condition_part in re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE):
        # More robust check for the index_attribute at the beginning of a condition
        # e.g., "HospitalID = 'HOSP-001'" or "Age >= 30"
        # Regex to capture `attribute operator value`
        match = re.match(rf"^\s*{re.escape(index_attribute)}\s*([<>=!]+)\s*(.+)$", condition_part.strip(), re.IGNORECASE)
        if match:
            conditions_on_indexed_attr.append(condition_part.strip())

    logger.info(f"Extracted conditions for index attribute '{index_attribute}': {conditions_on_indexed_attr}")

    if not conditions_on_indexed_attr:
        logger.info(f"No specific conditions found for index attribute '{index_attribute}' in WHERE clause. Retrieving all CIDs.")
        return list(index.query_range())

    # Using a set to automatically handle duplicate CIDs from multiple conditions if any
    final_cids = set()
    first_condition = True

    for condition_str in conditions_on_indexed_attr:
        current_cids = set()
        operator_match = re.search(r"([<>=!]+)", condition_str)
        if not operator_match:
            logger.warning(f"Could not parse operator from condition: {condition_str}")
            continue
        
        operator = operator_match.group(1)
        value_str = condition_str.split(operator)[1].strip().strip("'\"")
        
        # Convert value based on index type (assuming 'bplustree' for numeric, 'trie' for string)
        try:
            if index.index_type == "bplustree": # Typically for numbers
                key_value = int(value_str)
            else: # Typically for strings (trie)
                key_value = str(value_str)
        except ValueError as ve:
            logger.error(f"Could not convert value '{value_str}' for index type '{index.index_type}': {ve}")
            continue

        logger.debug(f"Processing condition: {index_attribute} {operator} {key_value} (original value string: '{value_str}')")

        if operator == ">=":
            current_cids.update(index.query_range(start_key=key_value))
        elif operator == "<=":
            current_cids.update(index.query_range(end_key=key_value))
        elif operator == ">":
            if index.index_type == "bplustree":
                current_cids.update(index.query_range(start_key=key_value + 1)) # For integers, > X is >= X+1
            else: # For strings, this exact logic might need refinement or might not be supported by trie range query.
                  # Assuming query_range for trie with only start_key means "all keys greater than"
                logger.warning("Exact '>' behavior for string tries might depend on implementation of query_range. Assuming keys strictly greater.")
                # This might require a more complex trie walk if not directly supported.
                # For simplicity, we proceed, but this is a potential area for deeper Trie implementation.
                all_cids_temp = set(index.query_range(start_key=key_value))
                exact_match_cids = set(index.query(key_value))
                current_cids.update(all_cids_temp - exact_match_cids)
        elif operator == "<":
            if index.index_type == "bplustree":
                current_cids.update(index.query_range(end_key=key_value - 1)) # For integers, < X is <= X-1
            else: # Similar caveat for strings as '>'
                logger.warning("Exact '<' behavior for string tries might depend on implementation of query_range. Assuming keys strictly smaller.")
                current_cids.update(index.query_range(end_key=key_value)) # Trie might include exact match if not handled
                exact_match_cids = set(index.query(key_value))
                current_cids.difference_update(exact_match_cids)
        elif operator == "=":
            current_cids.update(index.query(key_value))
        elif operator == "!=":
            all_cids_in_index = set(index.query_range()) # Query all CIDs
            excluded_cids = set(index.query(key_value))
            current_cids.update(all_cids_in_index - excluded_cids)
        else:
            logger.error(f"Unsupported operator '{operator}' in condition: {condition_str}")
            # If an unsupported operator for the indexed field is used, it implies we can't use the index for this part.
            # For AND logic, if one part can't be resolved by index, we should ideally return all CIDs and let Spark filter.
            # However, to be conservative and avoid incorrect empty results if other conditions *could* have been used:
            logger.warning(f"Unsupported operator, returning all CIDs as index cannot filter this: {condition_str}")
            return list(index.query_range())


        if first_condition:
            final_cids = current_cids
            first_condition = False
        else:
            # For AND logic, we intersect the results from each condition
            final_cids.intersection_update(current_cids)
            if not final_cids: # If intersection is empty, no need to process further
                logger.info("Intersection of conditions resulted in zero CIDs.")
                break
    
    logger.info(f"Final CIDs from index query after processing all conditions: {list(final_cids)}")
    return list(final_cids)
