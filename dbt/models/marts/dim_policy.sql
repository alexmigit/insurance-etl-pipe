select distinct
    policy_id,
    policy_type,
    policy_start_date,
    policy_end_date,
    premium_amount
from {{ ref('stg_claims') }}
