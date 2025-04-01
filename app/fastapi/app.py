from fastapi import FastAPI, UploadFile, File
from pyspark.sql import SparkSession
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import io
import logging
import gc  # For garbage collection
from fastapi.responses import Response

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

@app.get("/cid/{cid}")
def get_cid_content(cid: str, download: bool = False):
    """
    Fetch content directly from IPFS by CID
    - Set download=True to download the file instead of viewing it
    """
    logger.info(f"GET /cid/{cid} - Retrieving content from IPFS")
    
    try:
        # Fetch from IPFS gateway
        ipfs_url = f"http://ipfs:8080/ipfs/{cid}"
        logger.info(f"Requesting content from IPFS at URL: {ipfs_url}")
        
        response = requests.get(ipfs_url, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        content = response.content
        
        # Get content type if possible
        content_type = response.headers.get('Content-Type', 'application/octet-stream')
        
        # Set filename based on CID if download requested
        if download:
            headers = {
                'Content-Disposition': f'attachment; filename=patient-data-{cid}.parquet'
            }
        else:
            headers = {}
            
        logger.info(f"Successfully fetched content for CID: {cid}")
        return Response(
            content=content,
            media_type=content_type,
            headers=headers
        )
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching content for CID {cid}: {str(e)}")
        return {"error": f"Failed to retrieve content: {str(e)}"}
    
    from typing import Optional
from fastapi import Request

@app.get("/query/{cid}")
async def query_data(
    request: Request,
    cid: str
):
    """
    Query patient data from IPFS by CID with filtering
    - cid: Content Identifier for the Parquet file on IPFS
    - Any column name can be used as a query parameter for exact matching
    - For advanced filtering, append operators to column names:
      - _gt: greater than
      - _lt: less than
      - _gte: greater than or equal
      - _lte: less than or equal
      - _ne: not equal
      - _contains: string contains (case insensitive)
    
    Examples:
    - /query/QmHash123?Age=30&Gender=Male
    - /query/QmHash123?Age_gt=30&Age_lt=50
    - /query/QmHash123?Name_contains=Smith
    """
    logger.info(f"GET /query/{cid} - Querying data with filters")
    
    try:
        # Get all query parameters
        query_params = dict(request.query_params)
        logger.info(f"Query parameters: {query_params}")
        
        # Fetch from IPFS gateway
        ipfs_url = f"http://ipfs:8080/ipfs/{cid}"
        logger.info(f"Requesting content from IPFS at URL: {ipfs_url}")
        
        response = requests.get(ipfs_url, timeout=10)
        response.raise_for_status()
        
        # Read Parquet data into buffer
        parquet_buffer = io.BytesIO(response.content)
        
        # Load Parquet into DataFrame
        logger.info("Loading Parquet data into DataFrame")
        table = pq.read_table(parquet_buffer)
        df = table.to_pandas()
        
        # Clean up
        del table
        parquet_buffer.close()
        del parquet_buffer
        
        # Apply filters
        for param, value in query_params.items():
            # Skip empty values
            if not value:
                continue
                
            # Parse operators
            if "_" in param:
                column, operator = param.split("_", 1)
            else:
                column, operator = param, "eq"
            
            # Skip if column doesn't exist
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame, skipping filter")
                continue
            
            logger.info(f"Filtering {column} {operator} {value}")
            
            # Convert value type based on column type if possible
            try:
                if df[column].dtype.kind in 'ifu':  # integer, float, unsigned
                    value = float(value) if '.' in value else int(value)
            except (ValueError, TypeError):
                # Keep as string if conversion fails
                pass
            
            # Apply filter based on operator
            if operator == "eq":
                df = df[df[column] == value]
            elif operator == "gt":
                df = df[df[column] > value]
            elif operator == "lt":
                df = df[df[column] < value]
            elif operator == "gte":
                df = df[df[column] >= value]
            elif operator == "lte":
                df = df[df[column] <= value]
            elif operator == "ne":
                df = df[df[column] != value]
            elif operator == "contains":
                df = df[df[column].astype(str).str.contains(value, case=False, na=False)]
            else:
                logger.warning(f"Unknown operator {operator}, using equality")
                df = df[df[column] == value]
        
        # Convert DataFrame to JSON
        logger.info(f"Returning {len(df)} records after filtering")
        result = df.to_dict(orient="records")
        
        # Clean up
        del df
        gc.collect()
        
        return {"results": result, "count": len(result)}
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching content for CID {cid}: {str(e)}")
        return {"error": f"Failed to retrieve content: {str(e)}"}
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        gc.collect()
        return {"error": f"Failed to process query: {str(e)}"}