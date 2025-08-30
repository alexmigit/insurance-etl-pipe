import pandas as pd

def extract_claims_from_csv(path: str) -> pd.DataFrame:
    """
    Extracts claims data from a CSV file and normalizes column names
    to match the expected schema for Snowflake loading.

    Args:
        path (str): The file path to the CSV file containing claims data.

    Returns:
        pd.DataFrame: A normalized DataFrame ready for ETL.
    """
    # Define expected schema
    required_cols = [
        "CLAIM_ID", "POLICY_ID", "CUSTOMER_ID", "CLAIM_AMOUNT",
        "CLAIM_DATE", "INCIDENT_DATE", "CLAIM_TYPE", "STATUS", "ADJUSTER_NOTES"
    ]

    # Map common CSV variations to expected columns
    alias_map = {
        "CLAIMID": "CLAIM_ID",
        "CLAIM_NUMBER": "CLAIM_ID",
        "ID": "CLAIM_ID",
        "POLICYID": "POLICY_ID",
        "CUSTOMERID": "CUSTOMER_ID",
        "CLAIMAMOUNT": "CLAIM_AMOUNT",
        "CLAIMDATE": "CLAIM_DATE",
        "INCIDENTDATE": "INCIDENT_DATE",
        "CLAIMTYPE": "CLAIM_TYPE",
        "ADJUSTERNOTES": "ADJUSTER_NOTES",
    }

    try:
        df = pd.read_csv(path)

        # Normalize columns: strip spaces + uppercase
        df.columns = df.columns.str.strip().str.upper()

        # Apply alias mapping
        df = df.rename(columns={k: v for k, v in alias_map.items() if k in df.columns})

        # Warn about missing required columns
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"⚠️ Warning: Missing required columns: {missing}")

        # Optional: keep only expected columns
        df = df[[c for c in required_cols if c in df.columns]]

        print("✅ Columns after normalization:", df.columns.tolist())
        print("Sample data:\n", df.head(5))

        return df

    except Exception as e:
        print(f"Error reading CSV file at {path}: {e}")
        return pd.DataFrame()
