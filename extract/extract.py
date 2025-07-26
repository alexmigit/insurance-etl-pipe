import pandas as pd

def extract_claims_from_csv(path: str) -> pd.DataFrame:
    """
    Extracts claims data from a CSV file and returns it as a pandas DataFrame.
    
    Args:
        path (str): The file path to the CSV file containing claims data.
        
    Returns:
        pd.DataFrame: A DataFrame containing the claims data.
    """
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        print(f"Error reading CSV file at {path}: {e}")
        return pd.DataFrame()