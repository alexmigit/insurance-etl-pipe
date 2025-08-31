with 
source as (

    select * from {{ source('raw', 'raw_customer') }}

),

renamed as (

    select
        cast(customer_id as string) as customer_id,
        first_name,
        last_name,
        cast(date_of_birth as date) as date_of_birth,
        gender,
        email,
        phone,
        address

    from source

)

select * 

from renamed
