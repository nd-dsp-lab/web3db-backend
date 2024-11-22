from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields
from pyspark.sql import SparkSession
import tempfile
import os
import json
import sqlparse
from pyspark.sql.types import *
import time
import requests

app = Flask(__name__)
api = Api(app, version='1.0', title='Web3DB Decentralized Database API',
          description='API for performing SQL operations with Web3DB')

ns = api.namespace('api', description='Database operations')

query_input = api.model('QueryInput', {
    'query': fields.String(required=True, description='SQL query to execute'),
    'cid': fields.String(required=False, description='IPFS CID for the database state')
})

# Configuration
IPFS_HOST = os.getenv('IPFS_HOST', 'ipfs_node')
IPFS_PORT = os.getenv('IPFS_PORT', '5001')
IPFS_BASE_URL = f'http://{IPFS_HOST}:{IPFS_PORT}/api/v0'
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

def create_spark_session():
    """Create and return a Spark session with initial configuration"""
    warehouse_dir = os.path.abspath("/tmp/spark-warehouse")
    if not os.path.exists(warehouse_dir):
        os.makedirs(warehouse_dir)
        
    return SparkSession.builder \
        .appName("web3DB") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hive.HiveQlExtensions") \
        .enableHiveSupport() \
        .getOrCreate()

def clean_warehouse_directory(spark):
    """Clean up the warehouse directory and drop all tables"""
    # Drop all existing tables
    tables = spark.catalog.listTables()
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table.name}")
        
    # Clear warehouse directory
    warehouse_location = spark.conf.get('spark.sql.warehouse.dir')
    if os.path.exists(warehouse_location):
        for table_dir in os.listdir(warehouse_location):
            table_path = os.path.join(warehouse_location, table_dir)
            if os.path.isdir(table_path):
                for file in os.listdir(table_path):
                    file_path = os.path.join(table_path, file)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                os.rmdir(table_path)

def test_ipfs_connection(retries=MAX_RETRIES):
    """Test IPFS connection"""
    last_exception = None
    
    for attempt in range(retries):
        try:
            response = requests.post(f'{IPFS_BASE_URL}/id')
            if response.status_code == 200:
                return True
        except Exception as e:
            last_exception = e
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
                continue
    raise Exception(f"Failed to connect to IPFS node after {retries} attempts: {str(last_exception)}")

def get_query_type(query):
    """Determine the type of SQL query"""
    try:
        parsed = sqlparse.parse(query.strip())
        if not parsed:
            raise ValueError("Empty or invalid SQL query")
        return parsed[0].get_type().lower()
    except Exception as e:
        raise ValueError(f"Failed to parse SQL query: {str(e)}")

def extract_schema_statements(content):
    """Extract CREATE TABLE statements from SQL content"""
    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    return [stmt for stmt in statements if stmt.lower().startswith('create table')]

def extract_data_statements(content):
    """Extract INSERT statements from SQL content"""
    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    return [stmt for stmt in statements if stmt.lower().startswith('insert into')]

def load_state_from_ipfs(cid, spark, load_data=True):
    """Load database state from IPFS"""
    try:
        # Test IPFS connection first
        test_ipfs_connection()
        
        # Clean up existing state
        clean_warehouse_directory(spark)
        
        # Use IPFS HTTP API to get file
        response = requests.post(f'{IPFS_BASE_URL}/cat?arg={cid}')
        
        if response.status_code != 200:
            raise Exception(f"Failed to get file from IPFS: {response.text}")
            
        content = response.content.decode('utf-8')
        
        # First, process all CREATE TABLE statements
        schema_statements = extract_schema_statements(content)
        for stmt in schema_statements:
            if stmt.lower().startswith('create table'):
                # Parse the CREATE TABLE statement
                table_parts = stmt.lower().split('table', 1)[1]
                
                # Extract table name
                table_name = table_parts.split('(')[0].strip()
                if 'if not exists' in table_name:
                    table_name = table_name.split('if not exists')[1].strip()
                    
                # Extract column definitions
                col_defs = stmt[stmt.find('(') + 1:stmt.rfind(')')].strip()
                
                # Add storage-specific details for Spark execution
                warehouse_location = spark.conf.get('spark.sql.warehouse.dir')
                table_location = os.path.join(warehouse_location, table_name)
                
                # Create table with Spark
                modified_stmt = (
                    f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} "
                    f"({col_defs}) "
                    f"USING parquet LOCATION '{table_location}'"
                )
                spark.sql(modified_stmt)
        
        # Then, process INSERT statements if load_data is True
        if load_data:
            data_statements = extract_data_statements(content)
            for stmt in data_statements:
                spark.sql(stmt)
                
    except Exception as e:
        raise Exception(f"Failed to load state from IPFS: {str(e)}")

def save_current_state(spark, include_data=True):
    """Save current database state to IPFS"""
    temp_file = None
    try:
        # Test IPFS connection first
        test_ipfs_connection()
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        
        # Get all tables
        tables = spark.catalog.listTables()
        
        for table in tables:
            table_name = table.name
            
            # Get table schema
            schema_df = spark.sql(f"DESCRIBE TABLE {table_name}")
            if schema_df.isEmpty():
                continue
                
            # Create table statement
            create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            columns = []
            for row in schema_df.collect():
                col_name = row['col_name']
                col_type = row['data_type']
                columns.append(f"{col_name} {col_type}")
            
            create_stmt += ", ".join(columns) + ")"
            temp_file.write(f"{create_stmt};\n")
            
            # Get and write data if include_data is True
            if include_data:
                data = spark.table(table_name).distinct().collect()
                for row in data:
                    values = [f"'{str(v)}'" if isinstance(v, (str, bool)) else 'NULL' if v is None else str(v) for v in row]
                    insert_stmt = f"INSERT INTO {table_name} VALUES ({', '.join(values)})"
                    temp_file.write(f"{insert_stmt};\n")
                
        temp_file.flush()
        
        # Add to IPFS
        with open(temp_file.name, 'rb') as fp:
            files = {
                'file': ('state.sql', fp, 'application/sql')
            }
            response = requests.post(f'{IPFS_BASE_URL}/add', files=files)
            
            if response.status_code != 200:
                raise Exception(f"Failed to add file to IPFS: {response.text}")
                
            result = response.json()
            return result['Hash']
                
    except Exception as e:
        raise Exception(f"Failed to save state to IPFS: {str(e)}")
    finally:
        if temp_file:
            temp_file.close()
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)

@ns.route('/query')
class QueryResource(Resource):
    @ns.expect(query_input)
    def post(self):
        """Execute SQL query and manage database state"""
        spark = None
        try:
            data = request.json
            if not data:
                return {'error': 'No JSON data provided'}, 400
                
            query = data.get('query')
            if not query:
                return {'error': 'No query provided'}, 400
            
            # Split into multiple statements if present
            statements = [stmt.strip() for stmt in sqlparse.split(query) if stmt.strip()]
            if not statements:
                return {'error': 'No valid SQL statements found'}, 400
                
            # Get query type
            query_type = get_query_type(statements[0])
            spark = create_spark_session()
            
            if query_type == 'select':
                cid = data.get('cid')
                if not cid:
                    return {'error': 'CID is required for SELECT queries'}, 400
                    
                # Load state and execute query
                load_state_from_ipfs(cid, spark, load_data=True)
                result_df = spark.sql(statements[0])
                
                if result_df.isEmpty():
                    return jsonify([])
                    
                result = result_df.distinct().toPandas().to_dict(orient='records')
                return jsonify(result)
                
            elif query_type == 'create':
                # Handle CREATE statements
                # Save state without data
                for statement in statements:
                    if statement.lower().startswith('create table'):
                        # Parse and create table in Spark
                        table_parts = statement.lower().split('table', 1)[1]
                        table_name = table_parts.split('(')[0].strip()
                        if 'if not exists' in table_name:
                            table_name = table_name.split('if not exists')[1].strip()
                            
                        col_defs = statement[statement.find('(') + 1:statement.rfind(')')].strip()
                        
                        warehouse_location = spark.conf.get('spark.sql.warehouse.dir')
                        table_location = os.path.join(warehouse_location, table_name)
                        
                        spark_statement = (
                            f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} "
                            f"({col_defs}) "
                            f"USING parquet LOCATION '{table_location}'"
                        )
                        spark.sql(spark_statement)
                
                # Save state without including data
                new_cid = save_current_state(spark, include_data=False)
                return jsonify({'cid': new_cid})
                            
            elif query_type in ['insert', 'update', 'delete']:
                cid = data.get('cid')
                if not cid:
                    return {'error': 'CID is required for modification queries'}, 400
                    
                # Load state with data
                load_state_from_ipfs(cid, spark, load_data=True)
                
                # Execute statements
                for statement in statements:
                    spark.sql(statement)
                
                # Save new state with data
                new_cid = save_current_state(spark, include_data=True)
                return jsonify({'cid': new_cid})
                
            else:
                return {'error': f'Unsupported query type: {query_type}'}, 400
                
        except ValueError as e:
            return {'error': str(e)}, 400
        except Exception as e:
            return {'error': str(e)}, 500
        finally:
            if spark:
                spark.stop()

@ns.route('/ipfs/content/<string:cid>')
class IPFSContentResource(Resource):
    def get(self, cid):
        """Retrieve content from IPFS using CID"""
        try:
            if not cid:
                return {'error': 'CID is required'}, 400
                
            test_ipfs_connection()
            
            response = requests.post(f'{IPFS_BASE_URL}/cat?arg={cid}')
            
            if response.status_code != 200:
                return {'error': f'Failed to get file from IPFS: {response.text}'}, 500
                
            content = response.content.decode('utf-8')
            return jsonify({'content': content})
            
        except Exception as e:
            return {'error': f'Failed to retrieve content from IPFS: {str(e)}'}, 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)