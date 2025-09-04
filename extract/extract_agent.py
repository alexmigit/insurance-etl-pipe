from .extract_utils import extract_from_csv

def extract_agent(path: str):
    required_cols = [
        "AGENT_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", "AGENCY_NAME"
    ]
    alias_map = {
        "AGENTID": "AGENT_ID",
        "FIRSTNAME": "FIRST_NAME",
        "LASTNAME": "LAST_NAME",
        "AGENTEMAIL": "EMAIL",
        "AGENTPHONE": "PHONE",
        "AGENCYNAME": "AGENCY_NAME",
    }
    return extract_from_csv(path, required_cols, alias_map)