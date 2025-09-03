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
        # Optional: normalize columns first
        df.columns = df.columns.str.strip().str.lower()

        # Numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Fill NaN claim_amount
        df['claim_amount'] = df['claim_amount'].fillna(0)

        # Deduplicate by CLAIM_ID only
        df = df.drop_duplicates(subset='claim_id')

        # Fill missing adjuster_notes
        df['adjuster_notes'] = df['adjuster_notes'].fillna("No notes provided")

        # Convert date columns
        date_cols = ['claim_date', 'incident_date']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Rename columns to match loader expectations (all caps)
        df.columns = [col.upper() for col in df.columns]

        return df

    except Exception as e:
        print(f"Error transforming claims DataFrame: {e}")
        return pd.DataFrame()
