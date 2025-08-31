with 

base as (

    select * from {{ ref('stg_customer') }}

),

with_policy as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        c.gender,
        c.email,
        c.phone,
        c.address,
        count(distinct p.policy_id) as total_policies,
        max(p.effective_date) as most_recent_policy_date

    from base c

    left join {{ ref('stg_policy') }} p
      on c.customer_id = p.customer_id

    group by c.customer_id, c.first_name, c.last_name, 
             c.date_of_birth, c.gender, c.email, c.phone, c.address
)

select *

from with_policy
