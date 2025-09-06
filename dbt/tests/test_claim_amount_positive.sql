-- fact_claims – claim amount must be positive
select *
from {{ ref('fct_claim') }}
where claim_amount <= 0