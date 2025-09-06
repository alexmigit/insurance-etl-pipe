{{ config(materialized='table') }}

select distinct
    agent_id,
    agent_name,
    agent_agency
from {{ ref('int_policy_enriched') }}
