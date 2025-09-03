from .extract_utils import extract_from_csv

def extract_policy(path: str):
    required_cols = [
        "POLICY_ID", "CUSTOMER_ID", "POLICY_TYPE", "EFFECTIVE_DATE",
        "EXPIRATION_DATE", "PREMIUM_AMOUNT", "STATUS", "AGENT_ID"
    ]
    alias_map = {
        "POLICYID": "POLICY_ID",
        "CUSTOMERID": "CUSTOMER_ID",
        "POLICYTYPE": "POLICY_TYPE",
        "EFFECTIVEDATE": "EFFECTIVE_DATE",
        "EXPIRATIONDATE": "EXPIRATION_DATE",
        "PREMIUMAMOUNT": "PREMIUM_AMOUNT",
        "STATUS": "STATUS",
        "AGENTID": "AGENT_ID",
    }
    return extract_from_csv(path, required_cols, alias_map)

