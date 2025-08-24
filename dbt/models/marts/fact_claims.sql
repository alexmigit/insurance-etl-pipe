select
    claim_id,
    policy_id,
    customer_id,
    claim_amount,
    claim_date,
    incident_date,
    claim_type,
    status
from {{ ref('stg_claims') }}
where status = 'Approved'
  and claim_date >= dateadd(year, -1, current_date)