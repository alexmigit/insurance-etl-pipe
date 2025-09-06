{{ 
    config(
        materialized = "table"
    ) 
}}

with 

policies as (

    select distinct
        policy_id,
        customer_id,
        policy_type,
        premium_amount,
        status,
        effective_date,
        expiration_date,
        agent_id

    from {{ ref('int_policy_enriched') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['policy_id']) }} as policy_sk,  -- surrogate key
    policy_id,
    customer_id,
    policy_type,
    premium_amount,
    status,
    effective_date,
    expiration_date,
    agent_id

from policies
