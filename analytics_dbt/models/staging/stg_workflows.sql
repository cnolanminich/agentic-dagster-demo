{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select
        workflow_id,
        workflow_name,
        trigger_type,
        status,
        started_at,
        completed_at,
        git_commit_sha,
        created_by
    from {{ source('raw', 'workflows') }}
)

select
    workflow_id,
    workflow_name,
    trigger_type,
    status,
    started_at,
    completed_at,
    git_commit_sha,
    created_by,
    datediff('second', started_at, completed_at) as duration_seconds
from source_data
