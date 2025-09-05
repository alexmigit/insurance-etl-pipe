import pandas as pd
import random
import hashlib
from datetime import datetime, timedelta

def generate_payments(policies_csv, output_csv="payments.csv"):
    """
    Generate synthetic deterministic payments data for each POLICY_ID,
    with frequency tied to POLICY_TYPE for realism.

    Args:
        policies_csv (str): Path to policies.csv containing POLICY_ID, POLICY_TYPE, and PREMIUM_AMOUNT.
        output_csv (str): Path where payments.csv will be saved.

    Returns:
        pd.DataFrame: Payments dataframe.
    """
    # Load policies
    df_policies = pd.read_csv(policies_csv)

    # Validation
    for col in ["POLICY_ID", "PREMIUM_AMOUNT", "POLICY_TYPE"]:
        if col not in df_policies.columns:
            raise ValueError(f"{col} column missing in policies file")

    payments = []

    # Define frequency by policy type
    frequency_map = {
        "Auto": 12,        # Monthly
        "Home": 4,         # Quarterly
        "Health": 12,      # Monthly
        "Life": 1,         # Annual
        "Business": 4      # Quarterly
    }

    for _, row in df_policies.iterrows():
        policy_id = row["POLICY_ID"]
        premium = row["PREMIUM_AMOUNT"]
        policy_type = row["POLICY_TYPE"]

        # Default frequency if not in map
        freq = frequency_map.get(policy_type, random.choice([1, 4, 12]))
        payment_amount = round(premium / freq, 2)

        # Seed randomness on POLICY_ID for deterministic payment details
        seed = int(hashlib.sha256(str(policy_id).encode()).hexdigest(), 16) % (10**8)
        random.seed(seed)

        # Generate payments evenly spaced across a year
        start_date = datetime(2024, 1, 1)
        for i in range(freq):
            payment_id = f"PMT-{policy_id}-{i+1}"
            payment_date = start_date + timedelta(days=(365 // freq) * i)

            payments.append({
                "PAYMENT_ID": payment_id,
                "POLICY_ID": policy_id,
                "PAYMENT_DATE": payment_date.date().isoformat(),
                "PAYMENT_AMOUNT": payment_amount,
                "PAYMENT_METHOD": random.choice(["Credit Card", "Bank Transfer", "Check"]),
                "STATUS": random.choice(["Completed", "Pending", "Failed"])
            })

    df_payments = pd.DataFrame(payments)
    df_payments.to_csv(output_csv, index=False)

    print(df_payments.head(10))
    print(f"✅ Payments table saved to {output_csv} with {len(df_payments)} rows")
    
    return df_payments

if __name__ == "__main__":
    df = generate_payments("policies.csv")
    