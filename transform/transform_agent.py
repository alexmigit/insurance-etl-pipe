import pandas as pd

def transform_agent(df):
    """
    Transforms the agent DataFrame by performing necessary data cleaning and processing.
    
    Args:
        df (pd.DataFrame): The DataFrame containing agent data.
        
    Returns:
        pd.DataFrame: A transformed DataFrame with cleaned and processed agent data.
    """
    try:
        print("🔍 Incoming DataFrame shape:", df.shape)

        # Normalize columns to uppercase and strip whitespace
        df.columns = df.columns.str.strip().str.upper()

        # Column alias map (to guarantee AGENT_ID is found)
        column_aliases = {
            "AGENTID": "AGENT_ID",
            "ID": "AGENT_ID",
            "AGNTID": "AGENT_ID"
        }
        df = df.rename(columns={col: column_aliases[col] for col in df.columns if col in column_aliases})

        if "AGENT_ID" not in df.columns:
            raise ValueError("❌ AGENT_ID column missing after normalization.")

        print("✅ Columns normalized:", df.columns.tolist())

        # Track changes for "no transformation" message
        original_df = df.copy()

        # Deduplicate AGENT_ID entries
        df = df.drop_duplicates(subset="AGENT_ID")
        print("After dedup:", df.shape)

        # Standardize names to title case
        df["FIRST_NAME"] = df["FIRST_NAME"].str.strip().str.title()
        df["LAST_NAME"] = df["LAST_NAME"].str.strip().str.title()
        print("After name cleanup:", df.shape)

        # Normalize email
        df["EMAIL"] = df["EMAIL"].str.strip().str.lower()

        # Standardize phone numbers
        df["PHONE"] = df["PHONE"].astype(str).str.replace(r'\D', '', regex=True)
        df["PHONE"] = df["PHONE"].apply(
            lambda x: f"{x[:3]}-{x[3:6]}-{x[6:10]}" if len(x) == 10 else "000-000-0000"
        )
        print("After phone cleanup:", df.shape)

        # Fill missing AGENCY_NAME with 'Unknown'
        df["AGENCY_NAME"] = df["AGENCY_NAME"].fillna("Unknown").str.strip().str.title()
        print("After agency name cleanup:", df.shape)

        # Check if transformations changed the data
        if df.equals(original_df):
            print("ℹ️ No transformations were required.")

        return df

    except Exception as e:
        print(f"❌ Error transforming agent DataFrame: {e}")
        return pd.DataFrame()
