import pandas as pd
import random
import hashlib

def generate_agents(policies_csv, output_csv="agents.csv"):
    """
    Generate deterministic synthetic agent data from policy file.
    Each AGENT_ID will always map to the same agent attributes.
    
    Args:
        policies_csv (str): Path to the policies.csv file containing AGENT_ID.
        output_csv (str): Path where agents.csv will be written.
    """
    # Load policies
    df_policies = pd.read_csv(policies_csv)

    # Ensure AGENT_ID exists
    if "AGENT_ID" not in df_policies.columns:
        raise ValueError("AGENT_ID column not found in policies file.")

    # Unique agent IDs
    agent_ids = df_policies["AGENT_ID"].dropna().unique()

    # Pools for synthetic data
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Laura"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia"]
    agency_names = ["Prime Insurance", "SecureLife", "TrustGuard", "Shield Insurance", "SafeFuture"]

    # Build agent table
    agents = []
    for agent_id in agent_ids:
        # Deterministic seed based on agent_id
        seed = int(hashlib.sha256(str(agent_id).encode()).hexdigest(), 16) % (10**8)
        random.seed(seed)

        fn = random.choice(first_names)
        ln = random.choice(last_names)
        email = f"{fn.lower()}.{ln.lower()}@agency.com"
        phone = f"{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}"
        agency = random.choice(agency_names)

        agents.append({
            "AGENT_ID": agent_id,
            "FIRST_NAME": fn,
            "LAST_NAME": ln,
            "EMAIL": email,
            "PHONE": phone,
            "AGENCY_NAME": agency
        })

    df_agents = pd.DataFrame(agents)

    # Save
    df_agents.to_csv(output_csv, index=False)
    print(f"✅ Agent synthetic data saved to {output_csv}")
    return df_agents

if __name__ == "__main__":
    df_agents = generate_agents("policies.csv")
    print(df_agents.head(10))
