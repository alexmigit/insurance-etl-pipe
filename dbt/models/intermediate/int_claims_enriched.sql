{{ 
  config(
    materialized='view'
  ) 
}}

with 

claims as (

  select * from {{ ref('stg_claim') }}

),

policies as (

  select * from {{ ref('int_policy_enriched') }}

)

select
  c.claim_id,
  c.claim_date,
  c.claim_amount,
  c.status,
  p.policy_id,
  p.policy_type,
  p.premium_amount,
  p.status as policy_status,
  p.customer_id,
  p.customer_name,
  p.customer_address,
  --p.customer_city,
  --p.customer_state,
  p.agent_id,
  p.agent_name

from claims c

left join policies p
  on c.policy_id = p.policy_id
