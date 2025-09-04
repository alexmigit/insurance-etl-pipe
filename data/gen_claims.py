import pandas as pd
import random
from datetime import datetime, timedelta
import csv

def generate_synthetic_claims(csv_path: str, num_new_claims: int = 220, duplicate_rate: float = 0.05, validate: bool = True):
    """
    Generates synthetic claims and appends them to an existing CSV.

    Args:
        csv_path (str): Path to the existing CSV file.
        num_new_claims (int): Number of new synthetic claims to generate.
        duplicate_rate (float): Fraction of claims to intentionally duplicate (~0.05 = 5%).
        validate (bool): If True, validates the CSV after appending for correct number of columns.
    """
    # Read existing claims
    df_existing = pd.read_csv(csv_path)

    claim_types = ["Auto", "Home", "Health"]
    statuses = ["Approved", "Pending", "Denied"]
    notes_samples = [
        "Minor damages, covered in full.",
        "Investigation ongoing.",
        "Routine procedure approved.",
        "Claim exceeds policy limits.",
        "Urgent care visit reimbursed.",
        "Repair estimate pending.",
        "Police report submitted.",
        "Flood damage documented.",
        "Out-of-network provider denied.",
        "Customer reported accident."
    ]

    # Determine last claim number
    last_claim_num = max(int(cid[3:]) for cid in df_existing['claim_id'].str.upper())

    new_claims = []

    for i in range(1, num_new_claims + 1):
        claim_id = f"CLM{last_claim_num + i:03d}"

        # Introduce some duplicates
        if random.random() < duplicate_rate and len(new_claims) > 0:
            duplicate_row = random.choice(new_claims)
            new_claims.append(duplicate_row)
            continue

        policy_id = f"POL{random.randint(143, 400)}"
        customer_id = f"CUST{random.randint(1021, 1500)}"
        claim_type = random.choice(claim_types)
        status = random.choice(statuses)
        claim_amount = round(random.uniform(100.0, 50000.0), 2) if status != "Denied" else 0.00

        # Random incident date in 2023
        incident_date = datetime(2023, random.randint(1, 12), random.randint(1, 28))
        # Claim date 0–5 days after incident
        claim_date = incident_date + timedelta(days=random.randint(0, 5))

        adjuster_notes = random.choice(notes_samples)

        new_claims.append([
            claim_id, policy_id, customer_id, claim_amount,
            claim_date.strftime("%Y-%m-%d"),
            incident_date.strftime("%Y-%m-%d"),
            claim_type, status, adjuster_notes
        ])

    # Create DataFrame
    df_new = pd.DataFrame(new_claims, columns=df_existing.columns)

    # Append to CSV with quoting to handle commas in text fields
    df_new.to_csv(
        csv_path,
        mode='a',
        header=False,
        index=False,
        quoting=csv.QUOTE_ALL
    )

    print(f"✅ Added {len(df_new)} synthetic claims to {csv_path}")

    # Optional validation
    if validate:
        with open(csv_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if len(line.strip().split(',')) != len(df_existing.columns):
                    print(f"⚠️ Row {i} has wrong number of columns: {line.strip()}")

# Generate synthetic claims data (if needed)
generate_synthetic_claims("data/claims.csv", num_new_claims=200, duplicate_rate=0.05)
