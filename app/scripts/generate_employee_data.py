#!/usr/bin/env python3
"""
Generate a large employee dataset matching the schema of employee_100k.csv.

Usage:
    python generate_employee_data.py                          # 10M rows (default)
    python generate_employee_data.py --rows 5000000           # 5M rows
    python generate_employee_data.py --rows 1000000 -o out.csv
"""

import argparse
import csv
import random
import sys
import time
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Data pools (extracted from the original employee_100k.csv)
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "Oliver", "Owen", "Ella", "Mateo", "Benjamin", "Luna", "Emma", "James",
    "Elijah", "Ethan", "William", "Riley", "Nora", "Logan", "Theodore",
    "Lucas", "Jackson", "Ava", "Evelyn", "Henry", "Liam", "Levi", "Aiden",
    "Mason", "Hannah", "Sophia", "Mia", "Grace", "Aria", "Jack", "Lily",
    "Noah", "Isabella", "Sebastian", "Chloe", "Charlotte", "Olivia",
    "Harper", "Zoey", "Amelia",
]

LAST_NAMES = [
    "Martinez", "Martin", "Wilson", "Gonzalez", "Clark", "Jones",
    "Rodriguez", "Johnson", "Taylor", "Brown", "Hernandez", "Harris",
    "Thompson", "Lopez", "Jackson", "Davis", "Moore", "Anderson",
    "Thomas", "Lee", "White", "Garcia", "Smith", "Miller", "Lewis",
    "Robinson", "Perez", "Williams", "Sanchez", "Ramirez",
]

DEPARTMENTS = [
    "Marketing", "IT", "Product", "Sales", "Customer Support", "Legal",
    "Operations", "Finance", "Engineering", "HR",
]

TITLES = [
    "Engineer", "Lead", "Senior Manager", "Analyst", "Director",
    "Specialist", "Manager", "Associate", "VP", "Coordinator",
    "Senior Engineer",
]

# Salary ranges per department (roughly matching the original data)
SALARY_RANGES = {
    "Engineering":       (41947, 159915),
    "Product":           (31458, 160599),
    "IT":                (38248, 123461),
    "Legal":             (36709, 129580),
    "Finance":           (38828, 118842),
    "Marketing":         (33483, 102921),
    "Operations":        (30000, 101961),
    "Sales":             (30000, 100091),
    "HR":                (30000,  81711),
    "Customer Support":  (30000,  82833),
}

HIRE_DATE_START = date(2005, 1, 1)
HIRE_DATE_END   = date(2026, 12, 31)
HIRE_DATE_RANGE = (HIRE_DATE_END - HIRE_DATE_START).days

AGE_MIN, AGE_MAX = 18, 65

# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def generate_rows(num_rows: int, writer: csv.writer, seed: int = 42) -> None:
    """Write *num_rows* employee records to *writer*."""
    rng = random.Random(seed)

    # Pre-compute some things for speed
    first_names = FIRST_NAMES
    last_names  = LAST_NAMES
    departments = DEPARTMENTS
    titles      = TITLES
    hire_start  = HIRE_DATE_START
    hire_range  = HIRE_DATE_RANGE

    n_first = len(first_names)
    n_last  = len(last_names)
    n_dept  = len(departments)
    n_title = len(titles)

    # Progress reporting
    report_every = max(num_rows // 20, 1)  # every 5 %
    t0 = time.time()

    for i in range(1, num_rows + 1):
        first = first_names[rng.randint(0, n_first - 1)]
        last  = last_names[rng.randint(0, n_last - 1)]
        dept  = departments[rng.randint(0, n_dept - 1)]
        title = titles[rng.randint(0, n_title - 1)]

        email = f"{first.lower()}.{last.lower()}{i}@company.com"

        hire_date = hire_start + timedelta(days=rng.randint(0, hire_range))
        age = rng.randint(AGE_MIN, AGE_MAX)

        # experience_years: 0 .. min(age - 18, years_since_hire)
        years_since_hire = max(0, (HIRE_DATE_END - hire_date).days // 365)
        max_exp = min(age - AGE_MIN, years_since_hire)
        experience = rng.randint(0, max(max_exp, 0))

        sal_lo, sal_hi = SALARY_RANGES[dept]
        salary = rng.randint(sal_lo, sal_hi)

        writer.writerow([
            i,
            first,
            last,
            email,
            dept,
            title,
            hire_date.isoformat(),
            salary,
            age,
            experience,
        ])

        if i % report_every == 0:
            elapsed = time.time() - t0
            pct = i / num_rows * 100
            rate = i / elapsed if elapsed > 0 else 0
            print(
                f"\r  {pct:5.1f}%  |  {i:>12,} / {num_rows:,} rows  |  "
                f"{rate:,.0f} rows/s  |  {elapsed:.1f}s elapsed",
                end="", file=sys.stderr, flush=True,
            )

    elapsed = time.time() - t0
    print(
        f"\r  100.0%  |  {num_rows:>12,} / {num_rows:,} rows  |  "
        f"{num_rows / elapsed:,.0f} rows/s  |  {elapsed:.1f}s elapsed",
        file=sys.stderr, flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a large synthetic employee CSV dataset.",
    )
    parser.add_argument(
        "--rows", "-n",
        type=int,
        default=10_000_000,
        help="Number of rows to generate (default: 10,000,000)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="../dataset/employee_10m.csv",
        help="Output CSV file path (default: ../dataset/employee_10m.csv)",
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()

    print(f"Generating {args.rows:,} rows → {args.output}", file=sys.stderr)

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "employee_id", "first_name", "last_name", "email",
            "department", "title", "hire_date", "salary_usd",
            "age", "experience_years",
        ])
        generate_rows(args.rows, writer, seed=args.seed)

    print(f"Done! Saved to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
