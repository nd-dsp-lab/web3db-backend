from pyspark.sql import SparkSession
from typing import Dict, Optional, Tuple, Any
import os
import sqlparse
from pyspark.sql.types import StructType, StructField
from postgres_parser import PostgresSQLParser
from sql_executor import SQLExecutor
import re
from ipfs_handler import IPFSHandler

class SparkSQLShell:
    def __init__(self):
        # Set environment variables for local mode
        os.environ['SPARK_LOCAL_IP'] = 'localhost'
        os.environ['SPARK_WORKER_CORES'] = '2'  # Adjust based on your machine
        os.environ['SPARK_WORKER_MEMORY'] = '2g'  # Adjust based on your machine
        
        # Create Spark session with proper local configuration
        self.spark = (SparkSession.builder
            .appName("Web3DB")
            .master("local[*]")  # Use all available cores
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")  # Reduce for small datasets
            .config("spark.default.parallelism", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "1g")
            .getOrCreate()
        )

        # Set log level after SparkSession creation
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.sql("SET spark.sql.parser.error.detail=true")
        
        self.current_views = set()
        self.parser = PostgresSQLParser()
        self.executor = SQLExecutor()
        
        self.table_metadata = {}
        self.ipfs_handler = IPFSHandler()
        self.current_hash = None
        self.table_mapping = {}

    def execute_sql(self, sql: str, state_hash: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Execute SQL and return result with new state hash
        """
        try:
            if state_hash and state_hash != self.current_hash:
                if not self._load_state(state_hash):
                    raise Exception("Failed to load state")

            formatted_sql = sqlparse.format(sql, 
                                          keyword_case='upper',
                                          identifier_case='lower',
                                          strip_comments=True,
                                          reindent=True)
            
            statement = sqlparse.parse(formatted_sql)[0]
            stmt_type = statement.get_type()
            
            if stmt_type != 'CREATE':
                formatted_sql = self._rewrite_query(formatted_sql)
            
            if stmt_type == 'CREATE':
                self._handle_create(str(statement))
                new_hash = self._save_current_state()
                return {
                    "type": "create",
                    "hash": new_hash,
                    "data": None
                }
                
            elif stmt_type == 'INSERT':
                self._handle_insert(formatted_sql)
                new_hash = self._save_current_state()
                return {
                    "type": "insert",
                    "hash": new_hash,
                    "data": None
                }
                
            elif stmt_type == 'SELECT':
                result = self.spark.sql(formatted_sql)
                data = [row.asDict() for row in result.collect()]
                return {
                    "type": "select",
                    "hash": self.current_hash,
                    "data": data
                }
                
            else:
                result = self.spark.sql(formatted_sql)
                new_hash = self._save_current_state()
                return {
                    "type": "other",
                    "hash": new_hash,
                    "data": None
                }

        except Exception as e:
            raise Exception(f"Error executing SQL: {str(e)}")

    def _generate_internal_name(self, table_name: str) -> str:
        """Generate unique internal name using hash or random prefix"""
        prefix = self.current_hash[:8] if self.current_hash else os.urandom(4).hex()
        return f"{table_name}_{prefix}"

    def _save_current_state(self) -> Optional[str]:
        """Save current state to IPFS and return new hash"""
        try:
            print("\n=== Saving State ===")
            print(f"Current table mappings: {self.table_mapping}")
            print(f"Current table metadata: {self.table_metadata}")
            
            dump_statements = []
            
            for internal_name, metadata in self.table_metadata.items():
                original_name = metadata.get('original_name')
                if original_name:
                    print(f"Processing table: {original_name} (internal: {internal_name})")
                    
                    create_stmt = self.executor.generate_create_statement(
                        original_name, 
                        metadata
                    )
                    dump_statements.append(create_stmt)
                    print(f"Generated CREATE statement: {create_stmt}")
                    
                    table_df = self.spark.table(internal_name)
                    insert_stmt = self.executor.generate_insert_statements(
                        original_name,
                        table_df
                    )
                    if insert_stmt:
                        dump_statements.append(insert_stmt)
                        print(f"Generated INSERT statement: {insert_stmt}")

            sql_dump = "\n\n".join(dump_statements)
            print(f"Complete SQL dump: {sql_dump}")
            
            state = self.ipfs_handler.save_state(
                sql_dump,
                metadata={
                    'previous_hash': self.current_hash,
                    'table_mappings': self.table_mapping
                }
            )
            
            if state:
                self.current_hash = state['hash']
                print(f"State saved with hash: {self.current_hash}")
                return state['hash']
            
            return None

        except Exception as e:
            print(f"Error saving state: {str(e)}")
            return None

    def _load_state(self, hash: str) -> bool:
        """Load state from IPFS"""
        try:
            print("\n=== Loading State ===")
            print(f"Loading hash: {hash}")
            print(f"Current mappings before cleanup: {self.table_mapping}")
            
            self.cleanup()
            print("State cleaned up")
            
            result = self.ipfs_handler.load_state(hash)
            if not result:
                return False
                
            print(f"Loaded content from IPFS: {result}")
            sql_dump = result['content']
            self.current_hash = hash
            
            if 'table_mappings' in result:
                self.table_mapping = result['table_mappings']
                print(f"Restored table mappings: {self.table_mapping}")
            
            statements = sqlparse.split(sql_dump)
            print(f"Executing statements: {statements}")
            
            for stmt in statements:
                if stmt.strip():
                    print(f"Executing statement: {stmt}")
                    self._execute_without_save(stmt)
                    
            print(f"Final table mappings: {self.table_mapping}")
            return True

        except Exception as e:
            print(f"Error loading state: {str(e)}")
            return False

    # Keeping your existing Spark execution methods unchanged
    def _handle_create(self, sql: str):
        """Handle CREATE statements"""
        try:
            table_name, columns = self.parser.parse_create_table(sql)
            internal_name = self._generate_internal_name(table_name)
            
            # Update mapping
            self.table_mapping[table_name] = internal_name
            
            # Create schema
            schema_fields = []
            for col in columns:
                spark_type = self.parser.get_spark_type(
                    col.data_type,
                    col.length,
                    col.precision,
                    col.scale
                )
                schema_fields.append(
                    StructField(col.name, spark_type, col.nullable)
                )
            
            schema = StructType(schema_fields)
            
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.createOrReplaceTempView(internal_name)
            
            self.current_views.add(internal_name)
            self.table_metadata[internal_name] = {
                'columns': columns,
                'schema': schema,
                'original_name': table_name
            }
            
            print(f"Created table {table_name}")
            
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            raise

    def _handle_insert(self, sql: str):
        try:
            print("\n=== Handling INSERT ===")
            print(f"Original SQL: {sql}")
            print(f"Current table mappings: {self.table_mapping}")
            
            multi_value_match = re.match(
                r'INSERT INTO (\w+)(?:_[a-f0-9]+)?\s*(?:\((.*?)\))?\s*VALUES\s*(.+)',
                sql,
                re.IGNORECASE | re.DOTALL
            )
            
            if not multi_value_match:
                raise ValueError("Invalid INSERT statement format")

            table_name = multi_value_match.group(1)
            columns_str = multi_value_match.group(2)
            all_values_str = multi_value_match.group(3)
            
            print(f"Extracted base table name: {table_name}")
            print(f"Extracted columns: {columns_str}")
            print(f"Extracted values: {all_values_str}")
            
            internal_name = table_name if table_name in self.current_views else self.table_mapping.get(table_name)
            print(f"Using internal name: {internal_name}")
            
            if not internal_name or internal_name not in self.current_views:
                raise ValueError(f"Table {table_name} does not exist")

            schema = self.table_metadata[internal_name]['schema']
            
            if columns_str:
                column_names = [c.strip() for c in columns_str.split(',')]
                col_positions = {name: idx for idx, name in enumerate(schema.names)}
                for col in column_names:
                    if col not in col_positions:
                        raise ValueError(f"Column {col} does not exist in table {table_name}")
            else:
                column_names = schema.names

            value_pattern = r'\((.*?)\)'
            value_matches = re.finditer(value_pattern, all_values_str)
            
            all_rows = []
            for value_match in value_matches:
                values_str = value_match.group(1)
                values = self.executor._parse_values(values_str)
                
                row = [None] * len(schema.fields)
                for col_name, value in zip(column_names, values):
                    pos = schema.names.index(col_name)
                    field = schema.fields[pos]
                    row[pos] = self.executor._convert_value(value, field.dataType)
                
                all_rows.append(row)

            if not all_rows:
                raise ValueError("No valid values found in INSERT statement")

            new_df = self.spark.createDataFrame(all_rows, schema)
            existing_df = self.spark.table(internal_name)
            result_df = existing_df.union(new_df)
            result_df.createOrReplaceTempView(internal_name)
            
            print(f"Inserted {len(all_rows)} row(s) into {table_name}")

        except Exception as e:
            raise Exception(f"Error executing INSERT: {str(e)}")

    def _rewrite_query(self, sql: str) -> str:
        """Replace table names with their internal mapped versions"""
        print(f"Rewriting query: {sql}")
        print(f"Current mappings: {self.table_mapping}")
        
        modified_sql = sql
        for original_name, internal_name in self.table_mapping.items():
            # Replace table names but not within quotes or other words
            modified_sql = re.sub(
                f'\\b{original_name}\\b',
                internal_name,
                modified_sql,
                flags=re.IGNORECASE
            )
        
        print(f"Rewritten to: {modified_sql}")
        return modified_sql

    def cleanup(self):
        """Clean up all temporary views and state"""
        for view in list(self.current_views):
            self.spark.catalog.dropTempView(view)
        self.current_views.clear()
        self.table_metadata.clear()
        self.table_mapping.clear()
        self.current_hash = None
        print("Cleaned up all temporary views")

    def show_menu(self):
        print("\n=== PostgreSQL-Compatible Spark SQL Shell ===")
        if self.current_hash:
            print(f"Current State Hash: {self.current_hash}")
        print("1. Execute SQL query")
        print("2. Load specific version")
        print("3. Cleanup all tables")
        print("4. Exit")
        print("Enter your choice (1-4):")

    def run(self):
        while True:
            self.show_menu()
            choice = input().strip()
            
            if choice == '1':
                print("Enter your SQL query (end with semicolon):")
                query_lines = []
                while True:
                    line = input()
                    if line.strip().endswith(';'):
                        query_lines.append(line)
                        break
                    query_lines.append(line)
                
                query = ' '.join(query_lines)
                print("Enter version hash (or press Enter for latest):")
                version_hash = input().strip() or None
                result_hash = self.execute_sql(query, version_hash)
                if result_hash:
                    print(f"Operation successful. New state hash: {result_hash}")
            
            elif choice == '2':
                print("Enter IPFS hash:")
                hash = input().strip()
                if self._load_state(hash):
                    print(f"Successfully loaded state: {hash}")
                else:
                    print("Failed to load state")
            
            elif choice == '3':
                self.cleanup()
            
            elif choice == '4':
                print("Exiting...")
                self.cleanup()
                self.spark.stop()
                break
            
            else:
                print("Invalid choice, please try again")

    def _execute_without_save(self, sql: str):
        """Execute SQL statement without saving state"""
        try:
            formatted_sql = sqlparse.format(sql, 
                                          keyword_case='upper',
                                          identifier_case='lower',
                                          strip_comments=True,
                                          reindent=True)
            
            statement = sqlparse.parse(formatted_sql)[0]
            stmt_type = statement.get_type()
            
            # Don't rewrite CREATE statements
            if stmt_type != 'CREATE':
                formatted_sql = self._rewrite_query(formatted_sql)
            
            if stmt_type == 'CREATE':
                self._handle_create(str(statement))
                
            elif stmt_type == 'INSERT':
                self._handle_insert(formatted_sql)
                
            elif stmt_type == 'SELECT':
                result = self.spark.sql(formatted_sql)
                result.show()
                
            else:
                result = self.spark.sql(formatted_sql)
                if result is not None:
                    result.show()

        except Exception as e:
            print(f"Error executing SQL: {str(e)}")
            raise