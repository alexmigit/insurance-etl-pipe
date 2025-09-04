import pandas as pd

def transform_customer(df):
    """
    Transforms the customer DataFrame by performing necessary data cleaning and processing.
    
    Args:
        df (pd.DataFrame): The DataFrame containing customer data.
        
    Returns:
        pd.DataFrame: A transformed DataFrame with cleaned and processed customer data.
    """
    try:
        print("🔍 Incoming DataFrame shape:", df.shape)

        # Normalize columns to uppercase and strip whitespace
        df.columns = df.columns.str.strip().str.upper()

        # Column alias map (to guarantee CUSTOMER_ID is found)
        column_aliases = {
            "CUST_ID": "CUSTOMER_ID",
            "ID": "CUSTOMER_ID",
            "CUSTOMERID": "CUSTOMER_ID"
        }
        df = df.rename(columns={col: column_aliases[col] for col in df.columns if col in column_aliases})

        if "CUSTOMER_ID" not in df.columns:
            raise ValueError("❌ CUSTOMER_ID column missing after normalization.")

        print("✅ Columns normalized:", df.columns.tolist())

        # Track changes for "no transformation" message
        original_df = df.copy()

        # Deduplicate CUSTOMER_ID entries
        df = df.drop_duplicates(subset="CUSTOMER_ID")
        print("After dedup:", df.shape)

        # Standardize names to title case
        df["FIRST_NAME"] = df["FIRST_NAME"].str.strip().str.title()
        df["LAST_NAME"] = df["LAST_NAME"].str.strip().str.title()
        print("After name cleanup:", df.shape)

        # Clean gender column and map to 'M', 'F', 'O'
        df["GENDER"] = df["GENDER"].str.upper().str.strip().map({
            "MALE": "M", "M": "M",
            "FEMALE": "F", "F": "F"
        }).fillna("O")
        print("After gender cleanup:", df.shape)

        # Normalize email
        df["EMAIL"] = df["EMAIL"].str.strip().str.lower()

        # Clean address fields
        df["ADDRESS"] = df["ADDRESS"].str.strip().str.title()
        df["CITY"] = df["CITY"].str.strip().str.title()
        df["STATE"] = df["STATE"].str.strip().str.upper()
        df["ZIP_CODE"] = df["ZIP_CODE"].astype(str).str.zfill(5).str[:5]
        print("After address cleanup:", df.shape)

        # Standardize phone numbers
        df["PHONE"] = df["PHONE"].astype(str).str.replace(r'\D', '', regex=True)
        df["PHONE"] = df["PHONE"].apply(
            lambda x: f"{x[:3]}-{x[3:6]}-{x[6:10]}" if len(x) == 10 else "000-000-0000"
        )
        print("After phone cleanup:", df.shape)

        # Handle missing values
        df = df.fillna({
            "EMAIL": "unknown@example.com",
            "PHONE": "000-000-0000",
            "ADDRESS": "Unknown",
            "CITY": "Unknown",
            "STATE": "XX",
            "ZIP_CODE": "00000"
        })
        print("After filling nulls:", df.shape)

        # Check if transformations changed the data
        if df.equals(original_df):
            print("ℹ️ No transformations were required.")

        return df

    except Exception as e:
        print(f"❌ Error transforming customer DataFrame: {e}")
        return pd.DataFrame()
