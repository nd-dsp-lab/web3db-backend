import os
import sys

# Constants
BASE_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(BASE_DIR, "../dataset")

# Validate and read command-line arguments
if len(sys.argv) < 3:
    print("Usage: python remove_csv_files.py <hospital_number> <end_day>")
    sys.exit(1)

hospital_num = sys.argv[1]
end_day = int(sys.argv[2])

# Generate target filenames
csv_files = [
    f"hospital_{hospital_num}_day_{day}.csv"
    for day in range(1, end_day + 1)
    if os.path.exists(os.path.join(DATASET_DIR, f"hospital_{hospital_num}_day_{day}.csv"))
]

print(f"Found {len(csv_files)} files to remove for hospital {hospital_num} (day 1 to {end_day}).")

# Remove files
for filename in csv_files:
    file_path = os.path.join(DATASET_DIR, filename)
    try:
        os.remove(file_path)
        print(f"Removed {filename}")
    except Exception as e:
        print(f"Error removing {filename}: {e}")

print("File removal complete.")
