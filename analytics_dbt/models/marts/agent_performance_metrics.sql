{{
  config(
    materialized='table'
  )
}}

with agent_requests as (
    select * from {{ ref('stg_ai_agent_requests') }}
),

workflow_context as (
    select * from {{ ref('stg_workflows') }}
),

agent_metrics as (
    select
        ar.agent_name,
        ar.request_type,
        count(*) as total_requests,
        sum(ar.total_tokens) as total_tokens_used,
        avg(ar.total_tokens) as avg_tokens_per_request,
        avg(ar.execution_time_ms) as avg_execution_time_ms,
        sum(case when ar.status = 'SUCCESS' then 1 else 0 end) as successful_requests,
        sum(case when ar.status = 'FAILED' then 1 else 0 end) as failed_requests,
        date_trunc('day', ar.created_at) as request_date
    from agent_requests ar
    left join workflow_context wc on ar.workflow_id = wc.workflow_id
    group by ar.agent_name, ar.request_type, date_trunc('day', ar.created_at)
)

select
    agent_name,
    request_type,
    request_date,
    total_requests,
    total_tokens_used,
    avg_tokens_per_request,
    avg_execution_time_ms,
    successful_requests,
    failed_requests,
    case
        when total_requests > 0
        then (successful_requests::float / total_requests::float) * 100
        else 0
    end as success_rate_pct
from agent_metrics
order by request_date desc, total_requests desc
