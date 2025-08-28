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
        # Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]

        # Ensure numeric columns are of type float
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Fill NaN values in claim_amount column with 0   
        df['claim_amount'] = df['claim_amount'].fillna(0)

        # Drop duplicate claims based on 'claim_id'
        df = df.drop_duplicates(subset='claim_id')

        # Drop duplicate policies based on 'policy_id'
        df = df.drop_duplicated(subset='policy_id')

        # Fill missing values in 'adjuster_notes' with a default message
        df['adjuster_notes'] = df['adjuster_notes'].fillna("No notes provided")

        # Convert date columns to datetime format
        date_cols = ['claim_date', 'incident_date']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        return df
    
    except Exception as e:
        print(f"Error transforming DataFrame: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error