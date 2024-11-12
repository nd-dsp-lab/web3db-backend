from postgres_types import PostgresDataType, ColumnDefinition  # Remove the dot
import sqlparse
from typing import Tuple, List, Optional
from pyspark.sql.types import (StringType, IntegerType, FloatType, DoubleType, 
                              BooleanType, DateType, TimestampType, DecimalType, 
                              LongType, ArrayType, BinaryType, ShortType)
import re

class PostgresSQLParser:
    def __init__(self):
        self.type_mapping = {
            # Numeric Types
            'SMALLINT': ShortType(),
            'INTEGER': IntegerType(),
            'BIGINT': LongType(),
            'DECIMAL': DecimalType(10, 0),
            'NUMERIC': DecimalType(10, 0),
            'REAL': FloatType(),
            'DOUBLE PRECISION': DoubleType(),
            'SERIAL': IntegerType(),
            'BIGSERIAL': LongType(),
            
            # Character Types
            'CHAR': StringType(),
            'VARCHAR': StringType(),
            'TEXT': StringType(),
            
            # Binary Types
            'BYTEA': BinaryType(),
            
            # Date/Time Types
            'TIMESTAMP': TimestampType(),
            'TIMESTAMPTZ': TimestampType(),
            'DATE': DateType(),
            'TIME': TimestampType(),
            'TIMETZ': TimestampType(),
            'INTERVAL': StringType(),
            
            # Boolean Type
            'BOOLEAN': BooleanType(),
            
            # UUID Type
            'UUID': StringType(),
            
            # JSON Types
            'JSON': StringType(),
            'JSONB': StringType(),
        }

    def parse_create_table(self, sql: str) -> Tuple[str, List[ColumnDefinition]]:
        """Parse CREATE TABLE statement and return table name and column definitions"""
        # Extract table name
        table_match = re.search(r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)", sql, re.IGNORECASE | re.DOTALL)
        if not table_match:
            raise ValueError("Invalid CREATE TABLE syntax")
        
        table_name = table_match.group(1)
        columns_str = table_match.group(2)
        
        # Parse columns
        columns = []
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue
                
            # Split into name and type
            parts = col_def.split(None, 1)
            if len(parts) < 2:
                continue
                
            col_name = parts[0].strip('"`[]')
            type_part = parts[1].upper()
            
            # Handle types with parameters (e.g., VARCHAR(255))
            type_match = re.match(r'(\w+)(?:\(([^)]+)\))?', type_part)
            if not type_match:
                continue
                
            base_type = type_match.group(1)
            params = type_match.group(2)
            
            # Map common types to PostgreSQL types
            type_map = {
                'STRING': 'VARCHAR',
                'INT': 'INTEGER'
            }
            
            base_type = type_map.get(base_type, base_type)
            
            # Parse length/precision/scale
            length = None
            precision = None
            scale = None
            
            if params:
                if base_type in ('VARCHAR', 'CHAR'):
                    length = int(params)
                elif base_type in ('DECIMAL', 'NUMERIC'):
                    prec_scale = params.split(',')
                    precision = int(prec_scale[0])
                    if len(prec_scale) > 1:
                        scale = int(prec_scale[1])
            
            try:
                col_type = PostgresDataType(base_type)
            except ValueError:
                raise ValueError(f"Unsupported data type: {base_type}")
            
            columns.append(ColumnDefinition(
                name=col_name,
                data_type=col_type,
                length=length,
                precision=precision,
                scale=scale
            ))
        
        return table_name, columns
    def _parse_data_type(self, type_str: str) -> PostgresDataType:
        """Parse PostgreSQL data type string"""
        type_str = type_str.upper()
        base_type = type_str.split('(')[0].strip()
        
        try:
            return PostgresDataType(base_type)
        except ValueError:
            raise ValueError(f"Unsupported data type: {type_str}")

    def get_spark_type(self, pg_type: PostgresDataType, length: Optional[int] = None,
                    precision: Optional[int] = None, scale: Optional[int] = None):
        """Convert PostgreSQL type to Spark SQL type"""
        if pg_type == PostgresDataType.DECIMAL or pg_type == PostgresDataType.NUMERIC:
            # Use provided precision and scale, or defaults
            return DecimalType(
                precision if precision is not None else 10,
                scale if scale is not None else 2  # Default scale of 2 for NUMERIC
            )
        elif pg_type == PostgresDataType.ARRAY:
            return ArrayType(StringType())
        elif pg_type in (PostgresDataType.VARCHAR, PostgresDataType.CHAR, PostgresDataType.TEXT):
            return StringType()
        else:
            return self.type_mapping.get(pg_type.value, StringType())