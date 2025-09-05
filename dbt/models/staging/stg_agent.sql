with 

source as (

    select
        agent_id,
        first_name,
        last_name,
        email,
        phone,
        agency_name
    
    from {{ source('raw', 'raw_agent') }}

),

renamed as (

    select
        cast(agent_id as string) as agent_id,
        first_name || ' ' || last_name as agent_name,
        email as agent_email,
        phone as agent_phone,
        agency_name as agent_agency
        
    from source
)

select * from renamed
