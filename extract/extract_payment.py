from .extract_utils import extract_from_csv 

def extract_payment(path: str):
    required_cols = [
        "PAYMENT_ID", "POLICY_ID", "PAYMENT_DATE", "PAYMENT_AMOUNT", "PAYMENT_METHOD", "STATUS"
    ]
    alias_map = {
        "PAYMENTID": "PAYMENT_ID",
        "POLICYID": "POLICY_ID",
        "PAYMENTDATE": "PAYMENT_DATE",
        "PAYMENTAMOUNT": "PAYMENT_AMOUNT",
        "PAYMENTMETHOD": "PAYMENT_METHOD",
        "STATUS": "STATUS",
    }
    return extract_from_csv(path, required_cols, alias_map)
    