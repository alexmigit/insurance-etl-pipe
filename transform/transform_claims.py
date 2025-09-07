import pandas as pd

def transform_claims(df):
    """
    Transforms the claims DataFrame by performing necessary data cleaning and processing.
    
    Args:
        df (pd.DataFrame): The DataFrame containing claims data.
        
    Returns:
        pd.DataFrame: A transformed DataFrame with cleaned and processed claims data.
    """
    try:
        print("🔍 Incoming DataFrame shape:", df.shape)

        # Normalize column names to uppercase and strip whitespace
        df.columns = df.columns.str.strip().str.upper()

        # Column alias map (to guarantee CLAIM_ID is found)
        column_aliases = {
            "CLAIMID": "CLAIM_ID",
            "ID": "CLAIM_ID",
            "CLMID": "CLAIM_ID"
        }
        df = df.rename(columns={col: column_aliases[col] for col in df.columns if col in column_aliases})   

        if "claim_id" not in df.columns:
            raise ValueError("❌ CLAIM_ID column missing after normalization.")
        
        print("✅ Columns normalized:", df.columns.tolist())

        # Track changes for "no transformation" message
        original_df = df.copy() 

        # Numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if not numeric_cols.empty:
            df.loc[:, numeric_cols] = df[numeric_cols].astype(float)

        # Fill NaN claim_amount
        if "claim_amount" in df.columns:
            df.loc[:, "claim_amount"] = df["claim_amount"].fillna(0)

        # Deduplicate by CLAIM_ID only
        if "claim_id" in df.columns:
            df = df.drop_duplicates(subset="claim_id")
            print("After dedup:", df.shape)

        # Fill missing adjuster_notes
        if "adjuster_notes" in df.columns:
            df.loc[:, "adjuster_notes"] = df["adjuster_notes"].fillna("No notes provided")

        # Convert date columns
        for col in ["claim_date", "incident_date"]:
            if col in df.columns:
                df.loc[:, col] = pd.to_datetime(df[col], errors="coerce")

        # Check if transformations changed the data
        if df.equals(original_df):
            print("ℹ️ No transformations were required.")

        return df

    except Exception as e:
        print(f"❌ Error transforming claims DataFrame: {e}")
        return pd.DataFrame()
