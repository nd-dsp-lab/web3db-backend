import pandas as pd
from pandasql import sqldf
import os

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# File path (using absolute path)
csv_file = os.path.join(script_dir, "../dataset/patient_data.csv")

print(f"Looking for CSV file at: {csv_file}")

# Read CSV into DataFrame
patient_data = pd.read_csv(csv_file)

# Check if CSV data is empty
if patient_data.empty:
    raise ValueError("Error: CSV file contains no data")

query_file = "/query/query.sql"

# Read the SQL query from file
with open(query_file, "r") as f:
    query = f.read()

# Check if query is empty
if not query.strip():
    raise ValueError("Error: SQL query file is empty")

# Execute SQL query
result = sqldf(query, locals())

# Write result to output directory mounted by Gramine
output_file = "/output/result.csv"
result.to_csv(output_file, index=False)
print(f"Result written to {output_file}")

# Print the result
print(result)