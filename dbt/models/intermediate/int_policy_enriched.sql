with 

base as (

    select * from {{ ref('stg_policy') }}

),

with_agent as (

    select
        p.policy_id,
        p.customer_id,
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.premium_amount,
        p.status,
        p.agent_id,
        a.agent_name,
        a.region

    from base p

    left join {{ ref('stg_agent') }} a
      on p.agent_id = a.agent_id

)

select *

from with_agent
