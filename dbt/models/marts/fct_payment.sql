{{ config(materialized='table') }}

select
    payment_id,
    amount,
    payment_method,
    policy_id,
    policy_type,
    premium_amount,
    customer_id,
    agent_id,
    date
from {{ ref('int_payment_enriched') }}
