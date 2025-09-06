{{ config(materialized='table') }}

select
    policy_id,
    policy_type,
    effective_date,
    expiration_date,
    premium_amount,
    status as policy_status,
    customer_id,
    agent_id
from {{ ref('int_policy_enriched') }}
