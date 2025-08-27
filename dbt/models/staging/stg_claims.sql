{{ 
    config(
        materialized = "view",
        schema = "staging"
    ) 
}}

with 

raw as (
    
    select
        claim_id,
        policy_id,
        customer_id,
        claim_amount,
        claim_date,
        incident_date,
        claim_type,
        status,
        adjuster_notes
    
    from {{ source('raw', 'claims_table') }}

),

seeded as (

    select
        customer_id,
        customer_name,
        customer_address,
        customer_dob,
        customer_segment

    from {{ ref('sample_customers') }}

)

select
    r.claim_id,
    r.policy_id,
    r.customer_id,
    s.customer_name,
    s.customer_address,
    s.customer_dob,
    s.customer_segment,
    cast(r.claim_amount as number(18,2)) as claim_amount,
    to_date(r.claim_date) as claim_date,
    to_date(r.incident_date) as incident_date,
    r.claim_type,
    r.status,
    r.adjuster_notes

from raw r

join seeded s

on r.customer_id = s.customer_id
