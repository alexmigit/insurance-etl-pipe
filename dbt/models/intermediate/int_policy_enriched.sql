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
    c.customer_name,
    c.customer_email,
    c.customer_phone,
    c.customer_address,
    c.customer_city,
    c.customer_state,
    p.agent_id,
    a.agent_name,
    a.agent_agency,
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
