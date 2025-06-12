import csv
import random
import os
import time
from faker import Faker
from multiprocessing import Pool, cpu_count

fake = Faker()

# Constants
OUTPUT_DIR = '../dataset'
N_ROWS_PER_DAY = 100000
N_HOSPITALS = 1
N_DAYS = 1000
PATIENT_ID_MIN = 20000
PATIENT_ID_MAX = 30000

# Utility functions
def random_blood_type():
    return random.choice(['A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-'])

def random_condition():
    return random.choice([
        'Hypertension', 'Diabetes', 'Asthma', 'Coronary Artery Disease', 'Migraine',
        'Back Pain', 'COPD', 'Acute Tonsillitis', 'Heart Failure', 'Hypothyroidism'
    ])

def random_prescription(condition):
    mapping = {
        'Hypertension': 'Lisinopril',
        'Diabetes': 'Metformin',
        'Asthma': 'Albuterol',
        'Coronary Artery Disease': 'Aspirin',
        'Migraine': 'Sumatriptan',
        'Back Pain': 'Ibuprofen',
        'COPD': 'Tiotropium',
        'Acute Tonsillitis': 'Amoxicillin',
        'Heart Failure': 'Furosemide',
        'Hypothyroidism': 'Levothyroxine'
    }
    return mapping.get(condition, 'Vitamin D')

def random_diagnosis(condition):
    mapping = {
        'Hypertension': 'BP elevated, monitor regularly',
        'Diabetes': 'HbA1c above target, review meds',
        'Asthma': 'Wheezing, inhaler advised',
        'Coronary Artery Disease': 'Stress test scheduled',
        'Migraine': 'Frequent episodes, neurologist referral',
        'Back Pain': 'Muscle strain, therapy recommended',
        'COPD': 'Stable, maintain current meds',
        'Acute Tonsillitis': 'Positive for strep, antibiotics started',
        'Heart Failure': 'Fluid overload, adjust diuretic',
        'Hypothyroidism': 'TSH high, increase dose'
    }
    return mapping.get(condition, 'Routine check')

def random_gender():
    return random.choice(['M', 'F'])

def random_doctor():
    return 'Dr. ' + fake.last_name()

def generate_patient_id():
    return str(random.randint(PATIENT_ID_MIN, PATIENT_ID_MAX))

# File generation task
def generate_csv_task(args):
    hospital, day = args
    hospital_id = f"HOSP{hospital}"
    visit_date = f"2025-05-{str(day).zfill(2)}"
    filename = os.path.join(OUTPUT_DIR, f"hospital_{hospital}_day_{day}.csv")

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'PatientID', 'Name', 'Age', 'Gender', 'BloodType', 'Condition', 'VisitDate',
            'Doctor', 'HospitalID', 'Prescription', 'DiagnosisReport'
        ])
        for _ in range(N_ROWS_PER_DAY):
            condition = random_condition()
            row = [
                generate_patient_id(),
                fake.name(),
                random.randint(1, 99),
                random_gender(),
                random_blood_type(),
                condition,
                visit_date,
                random_doctor(),
                hospital_id,
                random_prescription(condition),
                random_diagnosis(condition)
            ]
            writer.writerow(row)

    print(f"Generated: {filename}")

# Main parallel runner
def main():
    start_time = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Create (hospital, day) job list
    jobs = [(hospital, day) for hospital in range(1, N_HOSPITALS + 1)
                               for day in range(1, N_DAYS + 1)]

    # Use all CPU cores or limit as needed
    with Pool(processes=cpu_count()) as pool:
        pool.map(generate_csv_task, jobs)
    print(f"All files generated in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
