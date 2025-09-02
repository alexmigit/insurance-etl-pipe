from .extract_utils import extract_from_csv

def extract_claims(path: str):
    required_cols = [
        "CLAIM_ID", "POLICY_ID", "CUSTOMER_ID", "CLAIM_AMOUNT",
        "CLAIM_DATE", "INCIDENT_DATE", "CLAIM_TYPE", "STATUS", "ADJUSTER_NOTES"
    ]
    alias_map = {
        "CLAIMID": "CLAIM_ID",
        "CLAIM_NUMBER": "CLAIM_ID",
        "ID": "CLAIM_ID",
        "POLICYID": "POLICY_ID",
        "CUSTOMERID": "CUSTOMER_ID",
        "CLAIMAMOUNT": "CLAIM_AMOUNT",
        "CLAIMDATE": "CLAIM_DATE",
        "INCIDENTDATE": "INCIDENT_DATE",
        "CLAIMTYPE": "CLAIM_TYPE",
        "ADJUSTERNOTES": "ADJUSTER_NOTES",
    }
    return extract_from_csv(path, required_cols, alias_map)
