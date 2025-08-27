select distinct
    customer_id,
    customer_name,
    customer_address,
    customer_dob,
    customer_segment
    
from {{ ref('stg_claims') }}
