select distinct
    policy_id,
    customer_id,
    policy_type,
    effective_date,
    expiration_date,
    premium_amount,
    status,
    agent_id
from {{ ref('sample_policy') }}
