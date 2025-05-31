import os
import csv
import random
from multiprocessing import Pool, cpu_count

# Configuration
DATASET_DIR = "../dataset"
PATIENT_ID_MIN = 20000
PATIENT_ID_MAX = 30000

def replace_ids_in_file(filename):
    file_path = os.path.join(DATASET_DIR, filename)
    temp_path = file_path + ".tmp"

    try:
        with open(file_path, "r", newline='') as infile, open(temp_path, "w", newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            header = next(reader)
            writer.writerow(header)
            pid_index = header.index("PatientID")

            for row in reader:
                row[pid_index] = str(random.randint(PATIENT_ID_MIN, PATIENT_ID_MAX))
                writer.writerow(row)

        os.replace(temp_path, file_path)
        return f"Updated: {filename}"
    except Exception as e:
        return f"Error processing {filename}: {str(e)}"

def main():
    all_csv_files = [f for f in os.listdir(DATASET_DIR) if f.endswith(".csv")]
    print(f"Found {len(all_csv_files)} CSV files. Starting parallel replacement...")

    with Pool(processes=cpu_count()) as pool:
        for result in pool.imap_unordered(replace_ids_in_file, all_csv_files, chunksize=10):
            print(result)

if __name__ == "__main__":
    main()
