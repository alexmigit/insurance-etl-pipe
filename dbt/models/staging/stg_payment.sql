{{ 
    config(
        materialized="view"
    )
}}

with 

source as (

    select * from {{ source('raw', 'raw_payment') }}

),

renamed as (

    select
        -- ids
        payment_id,
        policy_id,

        --strings
        payment_method,
        case 
            when payment_method in ('stripe', 'square', 'credit card') then 'Credit'
            when payment_method in ('ach', 'bank transfer') then 'Bank'
            when payment_method in ('cash', 'check', 'money order') then 'Cash'
            when payment_method in ('paypal', 'venmo', 'zelle') then 'Digital Wallet'   
            else 'Other'
        end as payment_type,
        status,

        -- booleans
        case
            when status = 'completed' then true
            else false
        end as is_completed,

        payment_amount as amount,

        -- dates
        date_trunc('month', cast(payment_date as date)) as payment_month,
        date_trunc('year', cast(payment_date as date)) as payment_year,
        date_trunc('quarter', cast(payment_date as date)) as payment_quarter,
        date_trunc('week', cast(payment_date as date)) as payment_week,
        date_trunc('day', cast(payment_date as date)) as payment_day,
        date_trunc('day', payment_date) as payment_day_full,

    from source

)

select * from renamed
