import pandas as pd
import numpy as np

def generate_policy_data(claims_csv: str, output_csv: str = "policies.csv", seed: int = 42):
    """
    Generate synthetic policy data based on claims file.

    Args:
        claims_csv (str): Path to claims CSV file.
        output_csv (str): Path to save generated policy data.
        seed (int): Random seed for reproducibility.

    Returns:
        pd.DataFrame: Generated policies DataFrame.
    """
    np.random.seed(seed)

    # Load claims
    claims_df = pd.read_csv(claims_csv)

    # Normalize column names
    claims_df.columns = claims_df.columns.str.strip().str.upper()

    if "POLICY_ID" not in claims_df.columns or "CUSTOMER_ID" not in claims_df.columns:
        raise ValueError("Claims CSV must contain POLICY_ID and CUSTOMER_ID columns")

    # Extract unique policies
    unique_policies = claims_df[["POLICY_ID", "CUSTOMER_ID"]].drop_duplicates()

    # Synthetic policy attributes
    policy_types = ["Auto", "Home", "Life", "Health", "Commercial"]
    statuses = ["Active", "Expired", "Cancelled"]

    policies = []
    for _, row in unique_policies.iterrows():
        policy_id = row["POLICY_ID"]
        customer_id = row["CUSTOMER_ID"]

        effective_date = pd.Timestamp("2015-01-01") + pd.to_timedelta(np.random.randint(0, 365 * 5), unit="D")
        expiration_date = effective_date + pd.to_timedelta(365, unit="D")

        policy = {
            "POLICY_ID": policy_id,
            "CUSTOMER_ID": customer_id,
            "POLICY_TYPE": np.random.choice(policy_types),
            "EFFECTIVE_DATE": effective_date.date(),
            "EXPIRATION_DATE": expiration_date.date(),
            "PREMIUM_AMOUNT": round(np.random.uniform(500, 5000), 2),
            "STATUS": np.random.choice(statuses),
            "AGENT_ID": f"AGENT_{np.random.randint(100, 999)}"
        }
        policies.append(policy)

    policies_df = pd.DataFrame(policies)

    # Save to CSV
    policies_df.to_csv(output_csv, index=False)
    print(f"✅ Generated {len(policies_df)} policies -> {output_csv}")

    return policies_df


if __name__ == "__main__":
    df = generate_policy_data("claims.csv")
    print(df.head(10))
