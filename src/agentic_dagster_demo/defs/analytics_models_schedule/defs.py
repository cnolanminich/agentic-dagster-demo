"""Schedule for running dbt analytics models."""

from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job

# Define a job that runs all dbt assets
dbt_analytics_job = define_asset_job(
    name="dbt_analytics_job",
    selection=AssetSelection.keys(
        "stg_ai_agent_requests",
        "stg_workflows",
        "agent_performance_metrics",
        "workflow_cost_analysis",
    ),
    description="Daily job to run all dbt analytics models",
    tags={"job_type": "dbt_analytics"},
)

# Define a schedule that runs the job every 6 hours
dbt_analytics_schedule = ScheduleDefinition(
    name="dbt_analytics_schedule",
    job=dbt_analytics_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours at the top of the hour
    description="Runs dbt analytics models every 6 hours",
    tags={"schedule_type": "regular"},
)

defs = Definitions(
    jobs=[dbt_analytics_job],
    schedules=[dbt_analytics_schedule],
)
