import os
import sys
import requests
import time
import concurrent.futures
from multiprocessing import cpu_count


# Constants
API_URL = "http://129.74.152.201:8000/upload/patient-data"
BASE_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(BASE_DIR, "../dataset")
RESPONSE_DIR = os.path.join(BASE_DIR, "../benchmark/upload_response")

# Create output directory if it doesn't exist
os.makedirs(RESPONSE_DIR, exist_ok=True)

# Get hospital number from command-line argument
if len(sys.argv) < 2:
    print("Usage: python upload_script.py <hospital_number>")
    sys.exit(1)

hospital_num = sys.argv[1]

# Find matching files for this hospital
pattern = f"hospital_{hospital_num}_"
csv_files = [f for f in os.listdir(DATASET_DIR)
             if f.endswith(".csv") and f.startswith(pattern)]
print(f"Found {len(csv_files)} files for hospital {hospital_num}.")

# Output file path
RESULTS_FILE = os.path.join(RESPONSE_DIR, f"hospital_{hospital_num}.txt")

# Upload function for a single file
def upload_file(filename):
    file_path = os.path.join(DATASET_DIR, filename)
    try:
        with open(file_path, "rb") as f:
            response = requests.post(
                API_URL,
                files={"file": (filename, f, "text/csv")},
                headers={"accept": "application/json"},
                timeout=30
            )
        result_json = response.json()
        return f"File: {filename}\nResponse:\n{result_json}\n{'=' * 80}\n"
    except Exception as e:
        return f"File: {filename}\nError: {str(e)}\n{'=' * 80}\n"

# Start the upload process

start_time = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    results = executor.map(upload_file, csv_files)

# Save results to file
with open(RESULTS_FILE, "w") as results_file:
    for result in results:
        results_file.write(result)

print(f"All files uploaded in {time.time() - start_time:.2f} seconds.")