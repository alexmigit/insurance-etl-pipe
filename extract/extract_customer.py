from .extract_utils import extract_from_csv

def extract_customer(path: str):
    required_cols = [
        "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "DATE_OF_BIRTH", "GENDER",
        "EMAIL", "PHONE", "ADDRESS", "CITY", "STATE", "ZIP_CODE"
    ]
    alias_map = {
        "CUSTOMERID": "CUSTOMER_ID",
        "FIRSTNAME": "FIRST_NAME",
        "LASTNAME": "LAST_NAME",                
        "DATEOFBIRTH": "DATE_OF_BIRTH",
        "GENDER": "GENDER",
        "EMAILADDRESS": "EMAIL",
        "PHONENUMBER": "PHONE",
        "ADDRESS": "ADDRESS",
        "CITY": "CITY",
        "STATE": "STATE",             
        "ZIP": "ZIP_CODE",
    }
    return extract_from_csv(path, required_cols, alias_map)
