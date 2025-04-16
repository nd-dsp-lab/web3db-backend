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
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = FastAPI()

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
        
        # Process each CID in parallel to fetch data
        def fetch_data(cid):
            import requests
            import base64
            
            try:
                # Fetch data from IPFS
                ipfs_api_url = f"http://ipfs:5001/api/v0/cat"
                response = requests.post(ipfs_api_url, params={"arg": cid}, timeout=10)
                
                if response.status_code != 200:
                    return (cid, None, f"HTTP error: {response.status_code}")
                
                # Return the binary data encoded as base64
                encoded_data = base64.b64encode(response.content).decode('utf-8')
                return (cid, encoded_data, None)
                
            except Exception as e:
                return (cid, None, str(e))
        
        # Execute data fetching in parallel
        results = cids_rdd.map(fetch_data).collect()
        
        # Process the fetched data on the driver
        dfs = []
        processed_cids = []
        
        for cid, encoded_data, error in results:
            if encoded_data:
                try:
                    # Decode the data
                    import base64
                    import io
                    import pandas as pd
                    import pyarrow.parquet as pq
                    
                    binary_data = base64.b64decode(encoded_data)
                    parquet_buffer = io.BytesIO(binary_data)
                    
                    # Parse the Parquet data
                    table = pq.read_table(parquet_buffer)
                    df = table.to_pandas()
                    
                    # Add source CID
                    df['source_cid'] = cid
                    dfs.append(df)
                    processed_cids.append(cid)
                    
                    logger.info(f"Successfully processed data from CID: {cid}")
                except Exception as e:
                    logger.error(f"Error processing data from CID {cid}: {str(e)}")
            else:
                logger.error(f"Error fetching CID {cid}: {error}")
        
        if not dfs:
            return {"error": "Failed to process any data from IPFS"}
        
        # Combine all DataFrames
        import pandas as pd
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Convert to Spark DataFrame - use the to_dict method differently
        records = combined_df.to_dict(orient='records')
        spark_df = spark.createDataFrame(records)
        
        # Register as temporary view
        spark_df.createOrReplaceTempView("patient_data")
        
        # Execute the SQL query (this will run in parallel using Spark's distributed execution)
        logger.info(f"Executing query: {query}")
        result_df = spark.sql(query)
        
        # Convert results to JSON
        result_pandas = result_df.toPandas()
        result_json = result_pandas.to_dict(orient="records")
        
        # Clean up
        del result_df
        del result_pandas
        del dfs
        del combined_df
        gc.collect()
        
        logger.info(f"Query completed successfully, returning {len(result_json)} records")
        
        return {
            "message": "Distributed query executed successfully",
            "cids_processed": len(processed_cids),
            "record_count": len(result_json),
            "results": result_json
        }
        
    except Exception as e:
        logger.error(f"Error processing distributed query: {str(e)}")
        return {"error": f"Failed to process distributed query: {str(e)}"}