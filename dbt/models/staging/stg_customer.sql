with 

source as (

    select
        customer_id,
        first_name,
        last_name,
        date_of_birth,
        gender,
        email,
        phone,
        address,
        city,
        state,
        zip_code
    
    from {{ source('raw', 'raw_customer') }}

),

renamed as (

    select
        cast(customer_id as string) as customer_id,
        first_name || ' ' || last_name as customer_name,
        cast(date_of_birth as date) as customer_dob,
        gender as customer_sex,
        email as customer_email,
        phone as customer_phone,
        address as customer_address,
        city as customer_city,   
        state as customer_state,
        zip_code as customer_zip

    from source

)

select * 

from renamed
