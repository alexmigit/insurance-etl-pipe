import pandas as pd

def extract_from_csv(path: str, required_cols: list[str], alias_map: dict[str, str]) -> pd.DataFrame:
    """
    Generic CSV extractor with schema normalization.
    """
    try:
        df = pd.read_csv(path)

        # Normalize columns
        df.columns = df.columns.str.strip().str.upper()

        # Apply alias mapping
        df = df.rename(columns={k: v for k, v in alias_map.items() if k in df.columns})

        # Warn about missing required columns
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"⚠️ Warning: Missing required columns: {missing}")

        # Keep only expected columns
        df = df[[c for c in required_cols if c in df.columns]]

        print(f"✅ Extracted {len(df)} records from {path}")
        return df

    except Exception as e:
        print(f"❌ Error reading CSV file at {path}: {e}")
        return pd.DataFrame()
