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
            when payment_method in ('Stripe', 'Square', 'Credit Card') then 'Credit'
            when payment_method in ('ACH', 'Bank Transfer') then 'Bank'
            when payment_method in ('Cash', 'Check', 'Money Order') then 'Cash'
            when payment_method in ('Paypal', 'Venmo', 'Zelle') then 'Digital Wallet'   
            else 'Other'
        end as payment_type,
        status,

        -- booleans
        case
            when status = 'Completed' then true
            else false
        end as is_completed,

        -- numerics
        payment_amount as amount,

        -- dates
        payment_date as date

    from source

)

select * from renamed
