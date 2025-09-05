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
        # Normalize column names
        df = df.copy()
        df.columns = df.columns.str.strip().str.lower()

        # Ensure numeric types
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].astype(float)

        # Fill missing premium_amount
        if "premium_amount" in df.columns:
            df["premium_amount"] = df["premium_amount"].fillna(0)

        # Deduplicate by POLICY_ID
        df = df.drop_duplicates(subset="policy_id").copy()

        # Convert dates safely
        date_cols = ["effective_date", "expiration_date"]
        for col in date_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_datetime(df[col], errors="coerce")

        # Rename columns to match loader expectations
        df.columns = [col.upper() for col in df.columns]

        return df

    except Exception as e:
        print(f"Error transforming policy DataFrame: {e}")
        return pd.DataFrame()
