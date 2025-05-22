import pandas as pd
from pandasql import sqldf
import os

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# File path (using absolute path)
csv_file = os.path.join(script_dir, "../dataset/patient_data.csv")

print(f"Looking for CSV file at: {csv_file}")

# Read CSV into DataFrame
df = pd.read_csv(csv_file)

# Define SQL query
query = "SELECT * FROM df WHERE Age > 80"

# Execute SQL query
result = sqldf(query, locals())

# Print the result
print(result)