{{ config(materialized="view") }}

with payments as (

    select
        cast(payment_id as string) as payment_id,
        cast(policy_id as string) as policy_id,
        cast(claim_id as string) as claim_id,
        cast(payment_date as date) as payment_date,
        cast(amount as numeric(12,2)) as amount,
        payment_type

    from {{ source('raw', 'raw_payments') }}

)

select * from payments
