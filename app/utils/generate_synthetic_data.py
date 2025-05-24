import csv
import random
import os
from faker import Faker

fake = Faker()

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
    # Short plausible diagnosis/notes
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
    # Could use a large number space to simulate unique IDs but allow repeats
    return random.randint(1000, 5000)

def generate_csv(filename, n_rows, hospital_id, visit_date):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'PatientID','Name','Age','Gender','BloodType','Condition','VisitDate',
            'Doctor','HospitalID','Prescription','DiagnosisReport'
        ])
        for _ in range(n_rows):
            patient_id = generate_patient_id()
            name = fake.name()
            age = random.randint(1, 99)
            gender = random_gender()
            blood_type = random_blood_type()
            condition = random_condition()
            prescription = random_prescription(condition)
            diagnosis = random_diagnosis(condition)
            doctor = random_doctor()
            row = [
                patient_id, name, age, gender, blood_type, condition,
                visit_date, doctor, hospital_id, prescription, diagnosis
            ]
            writer.writerow(row)

def main():
    number_of_hospitals = 10
    number_of_days = 10
    number_of_rows_per_day = 5000

    output_dir = '../dataset/synthetic_data'
    os.makedirs(output_dir, exist_ok=True)

    for hospital in range(1, number_of_hospitals + 1):
        hospital_id = f"HOSP{hospital}"
        for day in range(1, number_of_days + 1):
            visit_date = f"2025-05-{str(day).zfill(2)}"
            filename = os.path.join(output_dir, f"hospital_{hospital}_day_{day}.csv")
            generate_csv(filename, number_of_rows_per_day, hospital_id, visit_date)
            print(f"Generated: {filename}")

if __name__ == "__main__":
    main()
