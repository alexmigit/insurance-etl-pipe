{{ 
    config(
        materialized='view'
    ) 
}}

with 

policies as (

    select * from {{ ref('stg_policy') }}

),

customers as (

    select * from {{ ref('stg_customer') }}

),

agents as (

    select * from {{ ref('stg_agent') }}

)

select
    p.policy_id,
    p.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email as customer_email,
    c.phone as customer_phone,
    c.address as customer_address,
    --c.city as customer_city,
    --c.state as customer_state,
    p.agent_id,
    a.agent_name,
    a.region as agent_region,
    p.policy_type,
    p.effective_date,
    p.expiration_date,
    p.premium_amount,
    p.status

from policies p

left join customers c
    on p.customer_id = c.customer_id

left join agents a
    on p.agent_id = a.agent_id
