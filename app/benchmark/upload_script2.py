import os
import sys
import requests
import time

# Constants
# API_URL = "http://129.74.152.201:8000/upload/patient-data"
API_URL = "http://129.74.155.237:8000/upload/patient-data"
BASE_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(BASE_DIR, "../dataset")
RESPONSE_DIR = os.path.join(BASE_DIR, "../benchmark/upload_response")

# Create output directory if it doesn't exist
os.makedirs(RESPONSE_DIR, exist_ok=True)

# Validate and read command-line arguments
if len(sys.argv) < 3:
    print("Usage: python upload_script.py <hospital_number> <end_day>")
    sys.exit(1)

hospital_num = sys.argv[1]
end_day = int(sys.argv[2])

# Generate target filenames
csv_files = [
    f"hospital_{hospital_num}_day_{day}.csv"
    for day in range(1, end_day + 1)
    if os.path.exists(os.path.join(DATASET_DIR, f"hospital_{hospital_num}_day_{day}.csv"))
]

print(f"Found {len(csv_files)} files for hospital {hospital_num} (day 1 to {end_day}).")

# Output file path
RESULTS_FILE = os.path.join(RESPONSE_DIR, f"hospital_{hospital_num}_days_1_to_{end_day}.txt")

# Upload function for a single file
def upload_file(filename):
    file_path = os.path.join(DATASET_DIR, filename)
    try:
        with open(file_path, "rb") as f:
            response = requests.post(
                API_URL,
                files={"file": (filename, f, "text/csv")},
                headers={"accept": "application/json"},
                timeout=60
            )
        result_json = response.json()
        return f"File: {filename}\nResponse:\n{result_json}\n{'=' * 80}\n"
    except Exception as e:
        return f"File: {filename}\nError: {str(e)}\n{'=' * 80}\n"

# Start the upload process
start_time = time.time()

with open(RESULTS_FILE, "w") as results_file:
    for filename in csv_files:
        print(f"Uploading {filename}...")
        result = upload_file(filename)
        results_file.write(result)
print(f"All files uploaded in {time.time() - start_time:.2f} seconds.")
