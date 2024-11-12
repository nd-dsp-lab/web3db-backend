from enum import Enum
from dataclasses import dataclass
from typing import Optional, Any
from pyspark.sql.types import (StringType, IntegerType, FloatType, DoubleType, 
                              BooleanType, DateType, TimestampType, DecimalType, 
                              LongType, ArrayType, BinaryType, ShortType)

class PostgresDataType(Enum):
    # Numeric Types
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"
    
    # Character Types
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    
    # Binary Types
    BYTEA = "BYTEA"
    
    # Date/Time Types
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    DATE = "DATE"
    TIME = "TIME"
    TIMETZ = "TIMETZ"
    INTERVAL = "INTERVAL"
    
    # Boolean Type
    BOOLEAN = "BOOLEAN"
    
    # Geometric Types
    POINT = "POINT"
    LINE = "LINE"
    LSEG = "LSEG"
    BOX = "BOX"
    PATH = "PATH"
    POLYGON = "POLYGON"
    CIRCLE = "CIRCLE"
    
    # Network Address Types
    CIDR = "CIDR"
    INET = "INET"
    MACADDR = "MACADDR"
    
    # JSON Types
    JSON = "JSON"
    JSONB = "JSONB"
    
    # Arrays
    ARRAY = "ARRAY"
    
    # UUID Type
    UUID = "UUID"

@dataclass
class ColumnDefinition:
    name: str
    data_type: PostgresDataType
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    default: Any = None
    primary_key: bool = False
    unique: bool = False
    references: Optional[str] = None
    check_constraint: Optional[str] = None