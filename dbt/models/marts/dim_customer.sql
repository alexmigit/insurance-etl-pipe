{{
    config(
        materialized = "table"
    )
}}

with

customers as (
    
    select distinct 
        customer_id,
        customer_name,
        customer_dob,
        customer_address,
        customer_city,
        customer_state,
        customer_zip,
        case
            when customer_dob >= dateadd(year, -25, current_date) then 'Young Adult'
            when customer_dob >= dateadd(year, -45, current_date) then 'Middle Aged'
            when customer_dob >= dateadd(year, -65, current_date) then 'Senior'
            else 'Elderly'
        end as customer_segment,
        total_policies,
        most_recent_policy_date

    from {{ ref('int_customer_enriched') }}
    
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,  -- surrogate key
    customer_id,
    customer_name,
    customer_dob,
    customer_address,
    customer_city,
    customer_state,
    customer_zip,
    customer_segment,
    total_policies,
    most_recent_policy_date
    
from customers
