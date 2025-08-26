with raw as (
    select *
    from {{ source('raw', 'claims_table') }}
)
select
    claim_id,
    policy_id,
    customer_id,
    try_to_decimal(claim_amount, 18, 2) as claim_amount,
    to_date(claim_date) as claim_date,
    to_date(incident_date) as incident_date,
    claim_type,
    status,
    adjuster_notes
from raw
