{{
    config(
        materialized = "incremental",
        unique_key = "claim_fact_id",
        schema = "mart",
        incremental_strategy = "merge"
    )
}}

with claims as (

    select
        claim_id,
        policy_id,
        customer_id,
        coalesce(claim_amount, 0) as claim_amount,
        claim_date

    from {{ ref('stg_claims') }}

    {% if is_incremental() %}

        -- Only new claims since last refresh
        where claim_date > coalesce((select max(last_claim_date) from {{ this }}), '1900-01-01')

    {% endif %}

),

aggregated as (

    select
        policy_id,
        customer_id,
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
        p.policy_key,     -- surrogate FK from dim_policy
        c.customer_key    -- surrogate FK from dim_customer

    from aggregated a

    left join {{ ref('dim_policy') }} p
        on a.policy_id = p.policy_id
    
    left join {{ ref('dim_customer') }} c
        on a.customer_id = c.customer_id
    
    -- Ensure clustering keys exist on join columns in dimension tables for better performance
    -- Example (run in Snowflake console, not in dbt model):
    -- ALTER TABLE <database>.<schema>.dim_policy CLUSTER BY (policy_id);
    -- ALTER TABLE <database>.<schema>.dim_customer CLUSTER BY (customer_id);

)

select
    {{ dbt_utils.generate_surrogate_key(['policy_id','customer_id','claim_id']) }} as claim_fact_id,
    policy_id,
    customer_id,
    policy_key,
    customer_key,
    total_claims,
    total_claim_amount,
    first_claim_date,
    last_claim_date,
    {{ run_started_at }} as load_ts

from with_dim_keys
