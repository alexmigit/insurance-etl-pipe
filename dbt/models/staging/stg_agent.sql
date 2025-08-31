with 

source as (

    select * from {{ source('raw', 'raw_agent') }}

),

renamed as (

    select
        cast(agent_id as string) as agent_id,
        agent_name,
        region
        
    from source
)

select * from renamed
