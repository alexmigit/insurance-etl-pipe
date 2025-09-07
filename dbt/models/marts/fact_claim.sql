{{
    config(
        materialized = "incremental",
        schema = "mart",
        incremental_strategy = "merge"
    ) 
}}

with 

claims as (

    select
        claim_id,
        policy_id,
        customer_id,
        coalesce(claim_amount, 0) as claim_amount,
        claim_date

    from {{ ref('stg_claim') }}

    {% if is_incremental() %}
        -- Only new claims since last refresh
        where claim_date > coalesce((select max(last_claim_date) from {{ this }}), DATE '1900-01-01')
    {% endif %}

),

aggregated as (

    select
        policy_id,
        customer_id,
        min(claim_id) as claim_id, -- pick a representative claim_id per group
        count(distinct claim_id) as total_claims,
        sum(claim_amount) as total_claim_amount,
        min(claim_date) as first_claim_date,
        max(claim_date) as last_claim_date

    from claims

    group by policy_id, customer_id

),

with_dim_keys as (

    select
        a.*,
        p.policy_sk,     -- surrogate FK from dim_policy
        c.customer_sk    -- surrogate FK from dim_customer

    from aggregated a

    left join {{ ref('dim_policy') }} p
        on a.policy_id = p.policy_id

    left join {{ ref('dim_customer') }} c
        on a.customer_id = c.customer_id

)

select
    {{ dbt_utils.generate_surrogate_key(['policy_id','customer_id','claim_id']) }} as claim_fact_id,
    claim_id,
    policy_id,
    customer_id,
    policy_sk,
    customer_sk,
    total_claims,
    total_claim_amount,
    first_claim_date,
    last_claim_date,
    current_timestamp() as load_ts

from with_dim_keys
