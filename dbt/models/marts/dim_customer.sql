{{
    config(
        materialized = "table",
        schema = "mart"
    )
}}

with

customers as (
    
    select distinct 
        customer_id,
        customer_name,
        customer_address,
        customer_dob,
        customer_segment

    from {{ ref('sample_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,  -- surrogate key
    customer_id,
    customer_name,
    customer_address,
    customer_dob,
    customer_segment
    
from customers
