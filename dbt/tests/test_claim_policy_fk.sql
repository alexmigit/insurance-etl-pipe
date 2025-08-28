-- fact_claims – policy must exist in dim_policy
select f.*
from {{ ref('fact_claims') }} f
left join {{ ref('dim_policy') }} p on f.policy_sk = p.policy_sk
where p.policy_sk is null