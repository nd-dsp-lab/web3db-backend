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

logger.info("Initializing Spark Session")
spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
spark = (
    SparkSession.builder.appName("FastAPISparkDriver")
    .master(spark_master)
    .config("spark.blockManager.port", "10025")
    .config("spark.driver.blockManager.port", "10026")
    .config("spark.driver.host", "129.74.152.201")
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

        # Explicitly trigger garbage collection
        gc.collect()

        logger.info("Memory buffers cleared")

        # Return the CID
        return {"message": "Patient data uploaded successfully", "cid": cid}
    except Exception as e:
        # Make sure to clean up memory even if an error occurs
        logger.error(f"Error processing patient data: {str(e)}")
        gc.collect()
        return {"error": f"Failed to process and upload data: {str(e)}"}


# Define request model
class QueryRequest(BaseModel):
    cids: List[str]
    query: str


@app.post("/query")
async def query_distributed(request: QueryRequest):
    """
    Distributed query across multiple IPFS CIDs with parallel data retrieval
    and centralized query execution
    """
    logger.info("POST /query - Processing distributed query across CIDs")

    try:
        cids = request.cids
        query = request.query

        if not cids or not query:
            return {"error": "Both 'cids' and 'query' parameters are required"}

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
