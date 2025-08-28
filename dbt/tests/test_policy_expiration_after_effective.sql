-- dim_policy – expiration date must be after effective date
select *
from {{ ref('dim_policy') }}
where expiration_date <= effective_date