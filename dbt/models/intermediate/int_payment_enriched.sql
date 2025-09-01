{{ 
    config(
        materialized='view'
    ) 
}}

with 

payments as (

    select * from {{ ref('stg_payments') }}

),

policies as (

    select * from {{ ref('int_policy_enriched') }}

)

select
    pay.payment_id,
    pay.payment_date,
    pay.amount,
    pay.payment_type,
    p.policy_id,
    p.policy_type,
    p.premium_amount,
    p.customer_id,
    p.customer_name,
    p.customer_address,
    --p.customer_city,
    --p.customer_state,
    p.agent_id,
    p.agent_name

from payments pay

left join policies p
    on pay.policy_id = p.policy_id
