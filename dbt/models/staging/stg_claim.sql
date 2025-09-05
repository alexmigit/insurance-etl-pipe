{{ 
    config(
        materialized = "view"
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
    
    from {{ source('raw', 'raw_claim') }}

),

customer as (

    select
        customer_id,
        first_name || ' ' || last_name as customer_name,
        date_of_birth as customer_dob,
        address as customer_address,
        --city as customer_city,
        --state as customer_state
        --zip as customer_zip

    from {{ source('raw', 'raw_customer') }}

)

select
    r.claim_id,
    r.policy_id,
    r.customer_id,
    c.customer_name,
    c.customer_address,
    --c.customer_city,
    --c.customer_state,
    --c.customer_zip,
    c.customer_dob,
    cast(r.claim_amount as number(18,2)) as claim_amount,
    to_date(r.claim_date) as claim_date,
    to_date(r.incident_date) as incident_date,
    r.claim_type,
    r.status,
    r.adjuster_notes

from raw r

join customer c

on r.customer_id = c.customer_id
