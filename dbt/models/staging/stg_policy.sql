{{ 
    config(
        materialized = "view"
    ) 
}}

with 

source as (
    
    select
        policy_id,
        customer_id,
        policy_type,
        effective_date,
        expiration_date,
        premium_amount,
        status,
        agent_id
    
    from {{ source('raw', 'raw_policy') }}

),

renamed as (

    select
        cast(policy_id as varchar) as policy_id,
        cast(customer_id as varchar) as customer_id,
        policy_type,
        cast(effective_date as date) as effective_date,
        cast(expiration_date as date) as expiration_date,
        cast(premium_amount as number(12,2)) as premium_amount,
        status,
        cast(agent_id as varchar) as agent_id

    from source

)

select * from renamed
