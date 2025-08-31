{{ 
    config(
        materialized = "view",
        schema = "staging"
    ) 
}}

with 

policy as (

    select
        cast(policy_id as varchar) as policy_id,
        cast(customer_id as varchar) as customer_id,
        cast(agent_id as varchar) as agent_id,
        policy_type,
        cast(effective_date as date) as effective_date,
        cast(expiration_date as date) as expiration_date,
        cast(premium_amount as number(12,2)) as premium_amount,
        status

    from {{ source('raw', 'raw_policy') }}

)

select * from policy
