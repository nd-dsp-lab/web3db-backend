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

# @app.get("/cid/{cid}")
# def get_cid_content(cid: str, download: bool = False):
#     """
#     Fetch content directly from IPFS by CID
#     - Set download=True to download the file instead of viewing it
#     """
#     logger.info(f"GET /cid/{cid} - Retrieving content from IPFS")
    
#     try:
#         # Fetch from IPFS gateway
#         ipfs_url = f"http://ipfs:8080/ipfs/{cid}"
#         logger.info(f"Requesting content from IPFS at URL: {ipfs_url}")
        
#         response = requests.get(ipfs_url, timeout=10)
#         response.raise_for_status()  # Raise exception for HTTP errors
        
#         content = response.content
        
#         # Get content type if possible
#         content_type = response.headers.get('Content-Type', 'application/octet-stream')
        
#         # Set filename based on CID if download requested
#         if download:
#             headers = {
#                 'Content-Disposition': f'attachment; filename=patient-data-{cid}.parquet'
#             }
#         else:
#             headers = {}
            
#         logger.info(f"Successfully fetched content for CID: {cid}")
#         return Response(
#             content=content,
#             media_type=content_type,
#             headers=headers
#         )
        
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error fetching content for CID {cid}: {str(e)}")
#         return {"error": f"Failed to retrieve content: {str(e)}"}
    
from typing import Optional
import re
from typing import Dict, Any
from fastapi import HTTPException
from pydantic import BaseModel

class QueryRequest(BaseModel):
    cid: str
    query: str

@app.post("/query")
async def query_data(request: QueryRequest):
    """
    Query patient data from IPFS using a SQL-like query
    
    Request body:
    {
        "cid": "QmHash123",
        "query": "select Age, Gender, Condition from table where DoctorId = 'Sara Jones'"
    }
    """
    logger.info(f"POST /query - Processing SQL query for CID: {request.cid}")
    
    try:
        # Parse the SQL query
        query = request.query.strip()
        logger.info(f"Processing query: {query}")
        
        # Extract the SELECT part to get columns
        select_pattern = re.compile(r"select\s+(.*?)\s+from", re.IGNORECASE)
        select_match = select_pattern.search(query)
        if not select_match:
            raise HTTPException(status_code=400, detail="Invalid SQL query: SELECT statement not found")
        
        # Get the columns to return
        columns_str = select_match.group(1).strip()
        if columns_str == "*":
            columns = None  # All columns
        else:
            columns = [col.strip() for col in columns_str.split(",")]
            logger.info(f"Columns to return: {columns}")
        
        # Extract the WHERE clause
        where_pattern = re.compile(r"where\s+(.*?)$", re.IGNORECASE)
        where_match = where_pattern.search(query)
        where_clause = where_match.group(1).strip() if where_match else None
        logger.info(f"Where clause: {where_clause}")
        
        # Fetch from IPFS gateway
        ipfs_url = f"http://ipfs:8080/ipfs/{request.cid}"
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
        
        # Apply WHERE clause if present
        if where_clause:
            # Handle simple equality conditions (won't handle complex SQL conditions)
            conditions = []
            current_condition = ""
            in_quotes = False
            quote_char = None
            
            # Parse the where clause character by character to handle quotes correctly
            for char in where_clause:
                if char in ["'", '"'] and (not quote_char or char == quote_char):
                    in_quotes = not in_quotes
                    if in_quotes:
                        quote_char = char
                    else:
                        quote_char = None
                    current_condition += char
                elif char == " " and not in_quotes and "and" in current_condition.lower():
                    # End of "and" condition
                    parts = current_condition.lower().split("and")
                    conditions.append(parts[0].strip())
                    current_condition = parts[1].strip()
                else:
                    current_condition += char
            
            # Add the last condition
            if current_condition:
                conditions.append(current_condition.strip())
            
            # Process each condition
            for condition in conditions:
                logger.info(f"Processing condition: {condition}")
                
                # Extract operator and values
                if "=" in condition:
                    operator = "=="
                    left, right = [s.strip() for s in condition.split("=", 1)]
                elif "!=" in condition:
                    operator = "!="
                    left, right = [s.strip() for s in condition.split("!=", 1)]
                elif ">=" in condition:
                    operator = ">="
                    left, right = [s.strip() for s in condition.split(">=", 1)]
                elif "<=" in condition:
                    operator = "<="
                    left, right = [s.strip() for s in condition.split("<=", 1)]
                elif ">" in condition:
                    operator = ">"
                    left, right = [s.strip() for s in condition.split(">", 1)]
                elif "<" in condition:
                    operator = "<"
                    left, right = [s.strip() for s in condition.split("<", 1)]
                else:
                    logger.warning(f"Unsupported condition format: {condition}")
                    continue
                
                # Handle quoted strings
                if right.startswith("'") and right.endswith("'"):
                    right = right[1:-1]  # Remove quotes
                elif right.startswith('"') and right.endswith('"'):
                    right = right[1:-1]  # Remove quotes
                else:
                    # Try to convert to number if possible
                    try:
                        right = float(right) if '.' in right else int(right)
                    except (ValueError, TypeError):
                        # Keep as is if not a number
                        pass
                
                # Apply the filter
                if left not in df.columns:
                    logger.warning(f"Column {left} not found in DataFrame, skipping filter")
                    continue
                
                if operator == "==":
                    df = df[df[left] == right]
                elif operator == "!=":
                    df = df[df[left] != right]
                elif operator == ">":
                    df = df[df[left] > right]
                elif operator == ">=":
                    df = df[df[left] >= right]
                elif operator == "<":
                    df = df[df[left] < right]
                elif operator == "<=":
                    df = df[df[left] <= right]
        
        # Select only requested columns if specified
        if columns:
            # Check if all requested columns exist
            missing_columns = [col for col in columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Columns not found: {missing_columns}")
                # Keep only columns that exist
                columns = [col for col in columns if col in df.columns]
            
            if columns:  # If we still have columns to return
                df = df[columns]
            else:
                logger.warning("No valid columns to return, returning all columns")
        
        # Convert DataFrame to JSON
        logger.info(f"Returning {len(df)} records after filtering")
        result = df.to_dict(orient="records")
        
        # Clean up
        del df
        gc.collect()
        
        return {"results": result, "count": len(result)}
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching content for CID {request.cid}: {str(e)}")
        return {"error": f"Failed to retrieve content: {str(e)}"}
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        gc.collect()
        return {"error": f"Failed to process query: {str(e)}"}