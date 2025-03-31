from fastapi import FastAPI, UploadFile, File
from pyspark.sql import SparkSession
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import os
import logging
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
    
    # Create temp directory if it doesn't exist
    temp_dir = "/data/patient_data"
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Created directory at {temp_dir}")
    
    # Save uploaded file temporarily
    csv_path = f"{temp_dir}/patient_data.csv"
    parquet_path = f"{temp_dir}/patient_data.parquet"
    
    # Save the uploaded file
    try:
        logger.info(f"Saving uploaded file to {csv_path}")
        with open(csv_path, "wb") as f:
            content = await file.read()
            f.write(content)
    except Exception as e:
        logger.error(f"Error saving uploaded file: {str(e)}")
        return {"error": f"Failed to save file: {str(e)}"}
    
    # Convert CSV to Parquet
    try:
        logger.info("Converting CSV to Parquet format")
        df = pd.read_csv(csv_path)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        logger.info(f"Successfully converted and saved Parquet file at {parquet_path}")
    except Exception as e:
        logger.error(f"Error converting CSV to Parquet: {str(e)}")
        return {"error": f"Failed to convert to Parquet: {str(e)}"}
    
    # Upload to IPFS
    try:
        ipfs_api_url = "http://ipfs:5001/api/v0/add"
        logger.info(f"Uploading Parquet file to IPFS node at {ipfs_api_url}")
        
        with open(parquet_path, "rb") as f:
            response = requests.post(ipfs_api_url, files={"file": f})
            response.raise_for_status()
            cid = response.json()["Hash"]
            logger.info(f"Patient data uploaded to IPFS with CID: {cid}")
            
            # Return the CID
            return {"message": "Patient data uploaded successfully", "cid": cid}
    except Exception as e:
        logger.error(f"Error uploading to IPFS: {str(e)}")
        return {"error": f"Failed to upload to IPFS: {str(e)}"}

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