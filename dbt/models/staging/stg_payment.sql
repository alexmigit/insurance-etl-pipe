{{ 
    config(
        materialized="view"
    )
}}

with 

source as (

    select
        payment_id,
        policy_id,
        payment_date,
        payment_amount,
        payment_method,
        status
    
    from {{ source('raw', 'raw_payment') }}

),

renamed as (

    select
        cast(payment_id as varchar) as payment_id,
        cast(policy_id as varchar) as policy_id,
        cast(payment_date as date) as payment_date,
        cast(payment_amount as numeric(12,2)) as amount,
        payment_method,
        status

    from source

)

select * from renamed
