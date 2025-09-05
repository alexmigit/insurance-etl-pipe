with 

base as (

    select 
        customer_id,
        customer_name,
        customer_dob,
        customer_sex,
        customer_email,
        customer_phone,
        customer_address
    
    from {{ ref('stg_customer') }}

),

with_policy as (

    select
        c.customer_id,
        c.customer_name,
        c.customer_dob,
        c.customer_sex,
        c.customer_email,
        c.customer_phone,
        c.customer_address,
        count(distinct p.policy_id) as total_policies,
        max(p.effective_date) as most_recent_policy_date

    from base c

    left join {{ ref('stg_policy') }} p
      on c.customer_id = p.customer_id

    group by 1, 2, 3, 4, 5, 6, 7
)

select * from with_policy
