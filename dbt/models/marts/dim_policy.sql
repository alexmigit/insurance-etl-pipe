{{ 
    config(
        materialized = "table",
        schema = "mart"
    ) 
}}

with 

policies as (

    select distinct
        policy_id,
        customer_id,
        policy_type,
        effective_date,
        expiration_date,
        premium_amount,
        status,
        agent_id

    from {{ ref('sample_policy') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['policy_id']) }} as policy_sk,  -- surrogate key
    policy_id,
    customer_id,
    policy_type,
    effective_date,
    expiration_date,
    premium_amount,
    status,
    agent_id

from policies
