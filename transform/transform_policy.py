import pandas as pd

def transform_policy(df):
    """
    Transforms the policy DataFrame by performing necessary data cleaning and processing.
    
    Args:
        df (pd.DataFrame): The DataFrame containing policy data.
        
    Returns:
        pd.DataFrame: A transformed DataFrame with cleaned and processed policy data.
    """
    try:
        print("🔍 Incoming DataFrame shape:", df.shape)

        # Normalize column names
        df.columns = df.columns.str.strip().str.upper()

        # Column alias map (to guarantee POLICY_ID is found)
        column_aliases = {
            "POLICYID": "POLICY_ID",
            "ID": "POLICY_ID",
            "POLID": "POLICY_ID"
        }
        df = df.rename(columns={col: column_aliases[col] for col in df.columns if col in column_aliases})

        if "POLICY_ID" not in df.columns:
            raise ValueError("❌ POLICY_ID column missing after normalization.")

        print("✅ Columns normalized:", df.columns.tolist())
        
        # Track changes for "no transformation" message
        original_df = df.copy()

        # Ensure numeric types
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].astype(float)
        
        # Deduplicate by POLICY_ID
        if "policy_id" in df.columns:
            df = df.drop_duplicates(subset="policy_id")
            print("After dedup:", df.shape)

        # Fill missing premium_amount
        if "premium_amount" in df.columns:
            df["premium_amount"] = df["premium_amount"].fillna(0)

        # Convert dates safely
        date_cols = ["effective_date", "expiration_date"]
        for col in date_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_datetime(df[col], errors="coerce")

        # Check if transformations changed the data
        if df.equals(original_df):
            print("ℹ️ No transformations were required.")

        return df

    except Exception as e:
        print(f"❌ Error transforming policy DataFrame: {e}")
        return pd.DataFrame()
