import pandas as pd

def transform_payment(df):
    """
    Transforms the payments DataFrame by performing necessary data cleaning and processing.
    
    Args:
        df (pd.DataFrame): The DataFrame containing payment data.
        
    Returns:
        pd.DataFrame: A transformed DataFrame with cleaned and processed payment data.
    """
    try:
        print("🔍 Incoming DataFrame shape:", df.shape)

        # Normalize column names to lowercase and strip whitespace
        df.columns = df.columns.str.strip().str.upper()

        # Column alias map (to guarantee PAYMENT_ID is found)
        column_aliases = {
            "PAY_ID": "PAYMENT_ID",
            "ID": "PAYMENT_ID",
            "PAYMENTID": "PAYMENT_ID"
        }
        df = df.rename(columns={col: column_aliases[col] for col in df.columns if col in column_aliases})

        if "PAYMENT_ID" not in df.columns:
            raise ValueError("❌ PAYMENT_ID column missing after normalization.")
        
        print("✅ Columns normalized:", df.columns.tolist())
        
        # Track changes for "no transformation" message
        original_df = df.copy()

        # Deduplicate by PAYMENT_ID
        df = df.drop_duplicates(subset="payment_id").copy()
        print("After dedup:", df.shape)
        
        # Ensure numeric types
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].astype(float)
        print("After numeric conversion:", df.shape)

        # Fill missing premium_amount
        if "payment_amount" in df.columns:
            df["payment_amount"] = df["payment_amount"].fillna(0)
        print("After filling missing payment_amount:", df.shape)

        # Convert dates safely
        date_col = ["payment_date"]
        for col in date_col:
            if col in df.columns:
                df.loc[:, col] = pd.to_datetime(df[col], errors="coerce")
        print("After date conversion:", df.shape)

        # Final shape check
        print("✅ Final DataFrame shape:", df.shape)
        if df.equals(original_df):
            print("⚠️ No transformations were applied to the DataFrame.")

        return df

    except Exception as e:
        print(f"❌ Error transforming payments DataFrame: {e}")
        return pd.DataFrame()
