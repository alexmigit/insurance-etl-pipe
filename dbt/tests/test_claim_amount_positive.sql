-- fact_claims – claim amount must be positive
select *
from {{ ref('fact_claims') }}
where claim_amount <= 0