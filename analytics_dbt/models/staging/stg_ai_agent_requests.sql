{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select
        request_id,
        agent_name,
        request_type,
        input_tokens,
        output_tokens,
        execution_time_ms,
        status,
        created_at,
        workflow_id
    from {{ source('raw', 'ai_agent_requests') }}
)

select
    request_id,
    agent_name,
    request_type,
    input_tokens,
    output_tokens,
    execution_time_ms,
    status,
    created_at,
    workflow_id,
    input_tokens + output_tokens as total_tokens
from source_data
where status != 'INVALID'
