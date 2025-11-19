{{
  config(
    materialized='table'
  )
}}

with agent_requests as (
    select * from {{ ref('stg_ai_agent_requests') }}
),

workflows as (
    select * from {{ ref('stg_workflows') }}
),

workflow_costs as (
    select
        w.workflow_id,
        w.workflow_name,
        w.trigger_type,
        w.status as workflow_status,
        w.started_at,
        w.duration_seconds,
        count(ar.request_id) as total_agent_calls,
        sum(ar.input_tokens) as total_input_tokens,
        sum(ar.output_tokens) as total_output_tokens,
        sum(ar.total_tokens) as total_tokens,
        -- Cost calculation (example rates: $0.003 per 1K input tokens, $0.015 per 1K output tokens)
        (sum(ar.input_tokens) / 1000.0) * 0.003 + (sum(ar.output_tokens) / 1000.0) * 0.015 as estimated_cost_usd
    from workflows w
    left join agent_requests ar on w.workflow_id = ar.workflow_id
    group by w.workflow_id, w.workflow_name, w.trigger_type, w.status, w.started_at, w.duration_seconds
)

select
    workflow_id,
    workflow_name,
    trigger_type,
    workflow_status,
    started_at,
    duration_seconds,
    total_agent_calls,
    total_input_tokens,
    total_output_tokens,
    total_tokens,
    estimated_cost_usd,
    case
        when duration_seconds > 0
        then estimated_cost_usd / (duration_seconds / 3600.0)
        else 0
    end as cost_per_hour
from workflow_costs
order by started_at desc
