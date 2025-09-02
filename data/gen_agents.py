import pandas as pd
import numpy as np
import random

def generate_agent_data(num_agents: int = 20, output_csv: str = "agents.csv", seed: int = 42):
    """
    Generate synthetic insurance agent data without faker.
    """
    np.random.seed(seed)
    random.seed(seed)

    first_names = ["David", "Sarah", "Chris", "Ashley", "Daniel", "Emily", "Matthew", "Hannah", "Joshua", "Megan"]
    last_names = ["Anderson", "Clark", "Rodriguez", "Lewis", "Walker", "Hall", "Allen", "Young", "King", "Wright"]

    regions = ["West", "Midwest", "South", "Northeast"]

    agents = []
    for i in range(1, num_agents + 1):
        first = np.random.choice(first_names)
        last = np.random.choice(last_names)

        email = f"{first.lower()}.{last.lower()}{np.random.randint(10,99)}@insuranceco.com"
        phone = f"({np.random.randint(200,999)})-{np.random.randint(200,999)}-{np.random.randint(1000,9999)}"
        region = np.random.choice(regions)

        agents.append({
            "AGENT_ID": i,
            "FIRST_NAME": first,
            "LAST_NAME": last,
            "EMAIL": email,
            "PHONE": phone,
            "REGION": region
        })

    agents_df = pd.DataFrame(agents)

    # Save to CSV
    agents_df.to_csv(output_csv, index=False)
    print(f"✅ Generated {len(agents_df)} agents -> {output_csv}")

    return agents_df


if __name__ == "__main__":
    df = generate_agent_data()
    print(df.head(10))
