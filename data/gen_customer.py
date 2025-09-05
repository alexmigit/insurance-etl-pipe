import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_customer_data(claims_csv: str, output_csv: str = "customers.csv", seed: int = 42):
    """
    Generate synthetic customer data based on claims file without external libraries.
    """
    np.random.seed(seed)
    random.seed(seed)

    # Load claims
    claims_df = pd.read_csv(claims_csv)
    claims_df.columns = claims_df.columns.str.strip().str.upper()

    if "CUSTOMER_ID" not in claims_df.columns:
        raise ValueError("Claims CSV must contain CUSTOMER_ID column")

    # Extract unique customer IDs
    unique_customers = claims_df[["CUSTOMER_ID"]].drop_duplicates()

    # Name pools
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Hernandez"]

    # State abbreviations
    states = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "MI", "GA", "NC"]

    customers = []
    for _, row in unique_customers.iterrows():
        customer_id = row["CUSTOMER_ID"]

        first = np.random.choice(first_names)
        last = np.random.choice(last_names)

        # Random DOB between 1940 and 2002
        start_date = datetime(1940, 1, 1)
        end_date = datetime(2002, 12, 31)
        dob = start_date + timedelta(days=np.random.randint(0, (end_date - start_date).days))

        gender = np.random.choice(["M", "F"])
        email = f"{first.lower()}.{last.lower()}{np.random.randint(100,999)}@example.com"
        phone = f"({np.random.randint(200,999)})-{np.random.randint(200,999)}-{np.random.randint(1000,9999)}"
        address = f"{np.random.randint(100,9999)} {np.random.choice(['Main St','Oak St','Pine Ave','Maple Rd','Cedar Blvd'])}"
        city = np.random.choice(["Sacramento","Austin","New York","Miami","Chicago","Philadelphia","Columbus","Detroit","Atlanta","Charlotte"])
        state = np.random.choice(states)
        zip_code = f"{np.random.randint(10000,99999)}"

        customers.append({
            "CUSTOMER_ID": customer_id,
            "FIRST_NAME": first,
            "LAST_NAME": last,
            "DATE_OF_BIRTH": dob.date(),
            "GENDER": gender,
            "EMAIL": email,
            "PHONE": phone,
            "ADDRESS": address,
            "CITY": city,
            "STATE": state,
            "ZIP_CODE": zip_code
        })

    customers_df = pd.DataFrame(customers)

    # Save to CSV
    customers_df.to_csv(output_csv, index=False)
    print(customers_df.head(10))
    print(f"✅ Generated {len(customers_df)} customers -> {output_csv}")

    return customers_df

if __name__ == "__main__":
    df = generate_customer_data("claims.csv")
