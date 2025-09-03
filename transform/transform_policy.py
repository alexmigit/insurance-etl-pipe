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
        # Optional: normalize columns first
        df.columns = df.columns.str.strip().str.lower()

        # Numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Fill NaN claim_amount
        df['premium_amount'] = df['premium_amount'].fillna(0)

        # Deduplicate by CLAIM_ID only
        df = df.drop_duplicates(subset='policy_id')

        # Convert date columns
        date_cols = ['effective_date', 'expiration_date']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Rename columns to match loader expectations (all caps)
        df.columns = [col.upper() for col in df.columns]

        return df

    except Exception as e:
        print(f"Error transforming DataFrame: {e}")
        return pd.DataFrame()
