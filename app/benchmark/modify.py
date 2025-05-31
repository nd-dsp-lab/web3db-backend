import os
import sys
import csv

# Constants
BASE_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(BASE_DIR, "../dataset")

# Validate and read command-line arguments
if len(sys.argv) < 4:
    print("Usage: python update_patient_id.py <hospital_number> <end_day> <patient_id>")
    sys.exit(1)

hospital_num = sys.argv[1]
end_day = int(sys.argv[2])
new_patient_id = sys.argv[3]

# Generate target filenames
csv_files = [
    f"hospital_{hospital_num}_day_{day}.csv"
    for day in range(1, end_day + 1)
    if os.path.exists(os.path.join(DATASET_DIR, f"hospital_{hospital_num}_day_{day}.csv"))
]

print(f"Found {len(csv_files)} files for hospital {hospital_num} (day 1 to {end_day}).")

# Modify first row PatientID and overwrite the file
def update_file(filename):
    file_path = os.path.join(DATASET_DIR, filename)

    try:
        with open(file_path, "r", newline='') as infile:
            reader = csv.reader(infile)
            rows = list(reader)
            if len(rows) > 1:
                rows[1][0] = new_patient_id  # Replace PatientID in the first data row
            else:
                print(f"File {filename} has no data rows.")
                return

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(rows)

        print(f"Updated {filename} — set first PatientID to {new_patient_id}")

    except Exception as e:
        print(f"Error processing {filename}: {e}")

# Start update
for filename in csv_files:
    update_file(filename)

print("PatientID update complete.")
