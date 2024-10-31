from pyspark.sql import SparkSession
import requests
import io

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IPFS Spark Application") \
    .getOrCreate()

# IPFS API endpoint
IPFS_API_URL = "http://ipfs:5001"

# CID of the data file in IPFS
CID = "QmW1HNuDfk3iJv6KXYqEctKLKYmQftU1VXe9WwUhYb2vuX"

def read_data_from_ipfs(cid):
    """Fetch data from IPFS using CID."""
    url = f"{IPFS_API_URL}/api/v0/cat?arg={cid}"
    response = requests.post(url)
    response.raise_for_status()
    return response.content.decode('utf-8')

# Fetch data from IPFS
data_str = read_data_from_ipfs(CID)

# Split the data into lines
data_list = data_str.splitlines()

# Create an RDD from the data
rdd = spark.sparkContext.parallelize(data_list)

# Convert RDD to DataFrame by reading CSV format
df = spark.read.csv(rdd, header=True, inferSchema=True)

# Continue with your DataFrame operations
df.createOrReplaceTempView("my_table")

# Perform a SQL query
result_df = spark.sql("SELECT * FROM my_table WHERE some_column > 100")

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()
