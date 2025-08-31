with 

base as (

    select * from {{ ref('stg_claims') }}

),

joined as (

    select
        c.claim_id,
        c.policy_id,
        c.claim_date,
        c.claim_amount,
        c.status,
        p.policy_type,
        p.premium_amount,
        cust.customer_id,
        cust.first_name,
        cust.last_name,
        a.agent_id,
        a.agent_name,
        a.region

    from base c

    left join {{ ref('stg_policy') }} p
      on c.policy_id = p.policy_id

    left join {{ ref('stg_customer') }} cust
      on p.customer_id = cust.customer_id

    left join {{ ref('stg_agent') }} a
      on p.agent_id = a.agent_id

)

select *

from joined
