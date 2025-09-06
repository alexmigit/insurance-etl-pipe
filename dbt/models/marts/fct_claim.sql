{{ config(materialized='table') }}

select
    claim_id,
    claim_date,
    claim_amount,
    status,
    policy_id,
    policy_type,
    premium_amount,
    policy_status,
    customer_id,
    agent_id

from {{ ref('int_claims_enriched') }}
