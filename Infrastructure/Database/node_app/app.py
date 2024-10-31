from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields
import requests
from pyspark.sql import SparkSession
import os
import json
import shutil
import os
import logging
def clear_warehouse_directory(warehouse_dir):
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)
    os.makedirs(warehouse_dir, exist_ok=True)  # Create the full path

def drop_all_tables(spark):
    tables = spark.sql("SHOW TABLES").collect()
    for table in tables:
        table_name = table['tableName']
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

app = Flask(__name__)
api = Api(app, version='1.0', title='IPFS Hive Query API', description='A simple API using Flask-RestX and IPFS')

IPFS_API_URL = os.environ.get('IPFS_API_URL', "http://ipfs:5001")
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://spark-master:7077')

class IPFSManager:
    @staticmethod
    def upload_to_ipfs(data):
        files = {'file': ('dump.sql', data)}
        response = requests.post(f"{IPFS_API_URL}/api/v0/add", files=files)
        if response.status_code == 200:
            ipfs_hash = response.json().get("Hash")
            return ipfs_hash
        else:
            raise ConnectionError(f"Failed to upload data to IPFS. Status code: {response.status_code}")

    @staticmethod
    def fetch_sql_dump_from_ipfs(data_hash):
        response = requests.post(f"{IPFS_API_URL}/api/v0/cat?arg={data_hash}")
        if response.status_code == 200:
            return response.text
        else:
            raise ConnectionError(f"Failed to fetch data from IPFS. Status code: {response.status_code}")

class QueryEngine:
    @staticmethod
    def execute_query(query, sql_dump=None):
        warehouse_dir = "/tmp/hive/warehouse"
        clear_warehouse_directory(warehouse_dir)

        spark = SparkSession.builder \
            .appName("Spark Hive Integration Test") \
            .master(SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("hive.metastore.warehouse.dir", warehouse_dir) \
            .config("hive.warehouse.subdir.inherit.perms", "true") \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .enableHiveSupport() \
            .getOrCreate()

        # Set the Hive warehouse directory explicitly
        spark.sql(f"SET hive.metastore.warehouse.dir={warehouse_dir}")

        # Drop all tables to ensure a clean state
        drop_all_tables(spark)

        # Re-create database state from dump
        if sql_dump:
            queries = json.loads(sql_dump)
            for q in queries:
                logging.info(f"Executing query: {q}")
                spark.sql(q)

        # Execute the current query
        result = spark.sql(query).collect()
        return result

query_ns = api.namespace('query', description='Query operations')

query_model = api.model('Query', {
    'query': fields.String(required=True, description='SQL Query'),
    'hash': fields.String(description='IPFS Hash of the existing database state'),
})

@query_ns.route('/')
class QueryResource(Resource):
    @api.expect(query_model)
    def post(self):
        '''Execute a query and return the results'''
        query = request.json.get("query")
        data_hash = request.json.get("hash")
        message = None
        new_hash = None

        try:
            modifying_queries = []
            sql_dump = None

            # Skip IPFS fetch for dummy hash or CREATE TABLE queries
            if data_hash != "dummy_ipfs_hash" and 'CREATE TABLE' not in query.upper():
                try:
                    sql_dump = IPFSManager.fetch_sql_dump_from_ipfs(data_hash)
                    modifying_queries = json.loads(sql_dump)
                except requests.exceptions.RequestException as e:
                    return {"message": "Failed to fetch data from IPFS", "error": str(e), "data": None, "hash": None}, 500

            # Execute the query
            raw_result = QueryEngine.execute_query(query, sql_dump if data_hash != "dummy_ipfs_hash" else None)
            formatted_result = [row.asDict() for row in raw_result]

            if 'SELECT' not in query.upper():
                # Update the dump for modifying queries
                modifying_queries.append(query)
                new_dump = json.dumps(modifying_queries)
                new_hash = IPFSManager.upload_to_ipfs(new_dump)
                message = "Query executed successfully, non-SELECT operation"
                return {"message": message, "data": formatted_result, "hash": new_hash}
            else:
                # For SELECT queries, just return the result
                message = "Query executed successfully, SELECT operation"
                return {"message": message, "data": formatted_result, "hash": data_hash}

        except Exception as e:
            return {"message": "An error occurred", "error": str(e), "data": None, "hash": None}, 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
