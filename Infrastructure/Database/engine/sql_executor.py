import datetime
import re
from decimal import Decimal
from typing import Any, List, Dict
from pyspark.sql.types import (StringType, IntegerType, FloatType, DoubleType,
                              BooleanType, DateType, TimestampType, DecimalType,
                              LongType, ShortType)

class SQLExecutor:
    def _parse_values(self, values_str: str) -> List[str]:
        """Parse VALUES clause handling quoted strings and special characters"""
        values = []
        current_value = ''
        in_quotes = False
        quote_char = None
        
        i = 0
        while i < len(values_str):
            char = values_str[i]
            
            if char in ("'", '"') and (i == 0 or values_str[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                current_value += char
            elif char == ',' and not in_quotes:
                values.append(current_value.strip())
                current_value = ''
            elif char == '\\' and i + 1 < len(values_str):
                current_value += values_str[i:i+2]
                i += 1
            else:
                current_value += char
            
            i += 1
        
        if current_value:
            values.append(current_value.strip())
        
        return values

    def _convert_value(self, value: str, spark_type) -> Any:
        """Convert string value to appropriate Spark type"""
        if value is None or value.upper() == 'NULL':
            return None

        # Remove quotes for all types
        value = value.strip("'\"")
        
        try:
            if isinstance(spark_type, IntegerType):
                return int(value)
            elif isinstance(spark_type, ShortType):
                return int(value)  # Spark will handle the conversion to short
            elif isinstance(spark_type, LongType):
                return int(value)
            elif isinstance(spark_type, FloatType):
                return float(value)
            elif isinstance(spark_type, DoubleType):
                return float(value)
            elif isinstance(spark_type, DecimalType):
                return Decimal(value)
            elif isinstance(spark_type, BooleanType):
                return value.lower() in ('true', 't', 'yes', 'y', '1')
            elif isinstance(spark_type, DateType):
                return datetime.datetime.strptime(value, '%Y-%m-%d').date()
            elif isinstance(spark_type, TimestampType):
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', 
                          '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
                    try:
                        return datetime.datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Invalid timestamp format: {value}")
            elif isinstance(spark_type, StringType):
                return value
            else:
                return value
        except Exception as e:
            raise ValueError(f"Cannot convert '{value}' to {spark_type}: {str(e)}")

    def generate_create_statement(self, table_name: str, metadata: Dict) -> str:
        """Generate CREATE TABLE statement for dump"""
        if not metadata or 'columns' not in metadata:
            return ""

        columns = []
        for col in metadata['columns']:
            col_def = [col.name, col.data_type.value]
            
            if col.length:
                col_def.append(f"({col.length})")
            elif col.precision is not None:
                if col.scale is not None:
                    col_def.append(f"({col.precision},{col.scale})")
                else:
                    col_def.append(f"({col.precision})")

            if not col.nullable:
                col_def.append("NOT NULL")
            if col.default is not None:
                col_def.append(f"DEFAULT {col.default}")
            if col.primary_key:
                col_def.append("PRIMARY KEY")
            if col.unique:
                col_def.append("UNIQUE")

            columns.append(" ".join(col_def))

        create_stmt = "CREATE TABLE {} (\n{}\n);".format(
            table_name,
            ',\n'.join('  ' + col for col in columns)
        )
        
        return create_stmt
    def generate_insert_statements(self, table_name: str, df) -> str:
        """Generate INSERT statements for dump"""
        rows = df.collect()
        if not rows:
            return ""

        # Get column names
        columns = df.schema.names
        column_list = ", ".join(columns)

        insert_values = []
        for row in rows:
            values = []
            for value, field in zip(row, df.schema.fields):
                if value is None:
                    values.append('NULL')
                elif isinstance(field.dataType, (StringType, DateType, TimestampType)):
                    values.append("'{}'".format(str(value).replace("'", "''")))
                else:
                    values.append(str(value))
            insert_values.append("({})".format(', '.join(values)))

        insert_stmt = "\nINSERT INTO {} ({}) VALUES\n{};\n".format(
            table_name,
            column_list,
            ',\n'.join(insert_values)
        )
        
        return insert_stmt