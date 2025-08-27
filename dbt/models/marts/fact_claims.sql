{{ config(
    materialized = "table",
    schema = "mart"
) }}

with claims as (
    select *
    from {{ ref('stg_claims') }}
),

aggregated as (
    select
        policy_id,
        customer_id,
        count(distinct claim_id) as total_claims,
        sum(claim_amount) as total_claim_amount,
        min(claim_date) as first_claim_date,
        max(claim_date) as last_claim_date
    from claims
    group by policy_id, customer_id
)

select * from aggregated
