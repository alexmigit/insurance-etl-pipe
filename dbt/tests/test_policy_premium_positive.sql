-- dim_policy – premium amount must be positive
select *
from {{ ref('dim_policy') }}
where premium_amount <= 0