import dagster as dg
import json
from typing import Any
from pathlib import Path
from datetime import datetime

class AiWorkflowGenerator(dg.Component, dg.Model, dg.Resolvable):
    """AI-powered workflow generator for dynamic Dagster job creation.

    This component simulates Claude (or other AI agents) generating Dagster workflow
    definitions that get committed to Git and auto-deployed through CI/CD. It replaces
    brittle n8n webhook chains with version-controlled, observable DAGs.

    Key features:
    - Agent-generated workflow definitions
    - Git-based versioning and deployment
    - Observability and monitoring
    - Demo mode for local testing without external dependencies
    """

    demo_mode: bool = False
    github_repo: str = "my-org/data-workflows"
    git_branch: str = "main"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions for AI workflow generation pipeline."""

        @dg.asset(
            kinds={"python", "ai", "claude"},
            group_name="ai_workflow_generation",
            description="Monitor for workflow requests from Claude or other AI agents"
        )
        def detect_workflow_request(context: dg.AssetExecutionContext) -> dict[str, Any]:
            """Detect incoming requests from AI agents to generate new workflows.

            In production: monitors API endpoint, queue, or database for requests
            In demo mode: simulates incoming requests from Claude
            """
            if self.demo_mode:
                context.log.info("🎭 DEMO MODE: Simulating workflow request from Claude")
                request = {
                    "request_id": "req_20250119_001",
                    "agent_name": "Claude",
                    "request_type": "create_workflow",
                    "description": "Create a daily customer analytics pipeline",
                    "requirements": {
                        "source": "snowflake.raw.customers",
                        "transformations": ["aggregate_by_region", "calculate_ltv"],
                        "output": "snowflake.analytics.customer_metrics",
                        "schedule": "0 2 * * *"
                    },
                    "created_at": datetime.now().isoformat()
                }
                context.log.info(f"Detected workflow request: {request['request_id']}")
                return request
            else:
                context.log.info("Checking for workflow requests from AI agents...")
                # In production: query API or database
                # from workflow_api import get_pending_requests
                # return get_pending_requests()
                return {"status": "no_requests"}

        @dg.asset(
            kinds={"python", "ai", "claude"},
            group_name="ai_workflow_generation",
            deps=[detect_workflow_request],
            description="Use Claude to generate Dagster workflow code from natural language requirements"
        )
        def generate_workflow_code(
            context: dg.AssetExecutionContext,
            detect_workflow_request: dict[str, Any]
        ) -> dict[str, Any]:
            """Generate Dagster workflow code using Claude API.

            Takes natural language requirements and generates production-ready
            Dagster asset definitions with proper dependencies and error handling.
            """
            if self.demo_mode:
                context.log.info("🎭 DEMO MODE: Simulating Claude code generation")
                request = detect_workflow_request

                # Simulate Claude generating Dagster code
                generated_code = '''
import dagster as dg
from dagster_snowflake import SnowflakeResource

@dg.asset(kinds={"snowflake"})
def customer_raw_data(snowflake: SnowflakeResource):
    """Extract raw customer data from Snowflake."""
    query = "SELECT * FROM raw.customers WHERE updated_at >= CURRENT_DATE - 1"
    return snowflake.execute_query(query)

@dg.asset(kinds={"python", "pandas"}, deps=[customer_raw_data])
def customer_aggregated_by_region():
    """Aggregate customer metrics by region."""
    # Aggregation logic here
    pass

@dg.asset(kinds={"python"}, deps=[customer_aggregated_by_region])
def customer_lifetime_value():
    """Calculate customer lifetime value."""
    # LTV calculation logic
    pass

daily_customer_schedule = dg.ScheduleDefinition(
    name="daily_customer_analytics",
    target="*",
    cron_schedule="0 2 * * *"
)
'''

                workflow = {
                    "request_id": request.get("request_id"),
                    "generated_code": generated_code,
                    "file_path": "src/agentic_dagster_demo/defs/generated_workflows/customer_analytics.py",
                    "assets_created": ["customer_raw_data", "customer_aggregated_by_region", "customer_lifetime_value"],
                    "schedule_created": "daily_customer_analytics",
                    "tokens_used": 2500,
                    "generation_time_ms": 1200,
                    "status": "success"
                }

                context.add_output_metadata({
                    "tokens_used": 2500,
                    "assets_count": len(workflow["assets_created"]),
                    "generation_time_ms": 1200
                })

                context.log.info(f"Generated workflow with {len(workflow['assets_created'])} assets")
                return workflow
            else:
                context.log.info("Calling Claude API to generate workflow code...")
                # In production: call Anthropic API
                # from anthropic import Anthropic
                # client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
                # response = client.messages.create(...)
                return {"status": "generated"}

        @dg.asset(
            kinds={"git", "github"},
            group_name="ai_workflow_generation",
            deps=[generate_workflow_code],
            description="Commit generated workflow code to Git repository"
        )
        def commit_to_git(
            context: dg.AssetExecutionContext,
            generate_workflow_code: dict[str, Any]
        ) -> dict[str, Any]:
            """Commit AI-generated workflow code to Git and push to GitHub.

            Creates a feature branch, commits the code, and prepares for PR creation.
            """
            if self.demo_mode:
                context.log.info("🎭 DEMO MODE: Simulating Git commit")
                workflow = generate_workflow_code

                branch_name = f"agent-generated/customer-analytics-{workflow['request_id']}"
                commit_sha = "abc123def456"

                commit_info = {
                    "request_id": workflow["request_id"],
                    "branch": branch_name,
                    "commit_sha": commit_sha,
                    "commit_message": f"Add AI-generated workflow: customer analytics\n\nGenerated by Claude based on request {workflow['request_id']}",
                    "files_changed": [workflow["file_path"]],
                    "repo": self.github_repo,
                    "timestamp": datetime.now().isoformat()
                }

                context.add_output_metadata({
                    "branch": branch_name,
                    "commit_sha": commit_sha,
                    "repo": self.github_repo
                })

                context.log.info(f"Committed to branch: {branch_name}")
                context.log.info(f"Commit SHA: {commit_sha}")
                return commit_info
            else:
                context.log.info("Committing generated code to Git...")
                # In production: use GitPython or GitHub API
                # import git
                # repo = git.Repo(".")
                # repo.git.checkout("-b", branch_name)
                # repo.index.add([file_path])
                # repo.index.commit(message)
                return {"status": "committed"}

        @dg.asset(
            kinds={"github", "ci_cd"},
            group_name="ai_workflow_generation",
            deps=[commit_to_git],
            description="Create pull request with CI/CD checks"
        )
        def create_pull_request(
            context: dg.AssetExecutionContext,
            commit_to_git: dict[str, Any]
        ) -> dict[str, Any]:
            """Create GitHub pull request with automated CI/CD checks.

            PR triggers GitHub Actions to validate the generated workflow:
            - Syntax validation
            - Type checking
            - Asset dependency validation
            - Integration tests
            """
            if self.demo_mode:
                context.log.info("🎭 DEMO MODE: Simulating PR creation")
                commit_info = commit_to_git

                pr_number = 42
                pr_url = f"https://github.com/{self.github_repo}/pull/{pr_number}"

                pr_info = {
                    "request_id": commit_info["request_id"],
                    "pr_number": pr_number,
                    "pr_url": pr_url,
                    "title": "Add AI-generated customer analytics workflow",
                    "body": f"""## AI-Generated Workflow

**Request ID:** {commit_info['request_id']}
**Generated by:** Claude
**Branch:** {commit_info['branch']}

### Assets Created:
- customer_raw_data
- customer_aggregated_by_region
- customer_lifetime_value

### Schedule:
- daily_customer_analytics (cron: 0 2 * * *)

🤖 This workflow was automatically generated by an AI agent and is ready for review.""",
                    "base_branch": self.git_branch,
                    "head_branch": commit_info["branch"],
                    "status": "open",
                    "checks_status": "pending",
                    "created_at": datetime.now().isoformat()
                }

                context.add_output_metadata({
                    "pr_number": pr_number,
                    "pr_url": pr_url
                })

                context.log.info(f"Created PR #{pr_number}: {pr_url}")
                return pr_info
            else:
                context.log.info("Creating pull request on GitHub...")
                # In production: use GitHub API
                # from github import Github
                # g = Github(os.environ["GITHUB_TOKEN"])
                # repo = g.get_repo(self.github_repo)
                # pr = repo.create_pull(...)
                return {"status": "pr_created"}

        @dg.asset(
            kinds={"github", "ci_cd"},
            group_name="ai_workflow_generation",
            deps=[create_pull_request],
            description="Monitor CI/CD checks and auto-merge on success"
        )
        def monitor_ci_and_merge(
            context: dg.AssetExecutionContext,
            create_pull_request: dict[str, Any]
        ) -> dict[str, Any]:
            """Monitor GitHub Actions checks and auto-merge when all pass.

            Watches for:
            - Unit tests passing
            - Type checks passing
            - Dagster definition validation
            - Integration tests passing

            Auto-merges to main branch when all checks pass, triggering
            automatic deployment to Dagster+ Cloud.
            """
            if self.demo_mode:
                context.log.info("🎭 DEMO MODE: Simulating CI/CD monitoring")
                pr_info = create_pull_request

                # Simulate CI checks passing
                checks = {
                    "syntax_validation": "passed",
                    "type_checking": "passed",
                    "dagster_validation": "passed",
                    "unit_tests": "passed",
                    "integration_tests": "passed"
                }

                merge_result = {
                    "request_id": pr_info["request_id"],
                    "pr_number": pr_info["pr_number"],
                    "pr_url": pr_info["pr_url"],
                    "ci_checks": checks,
                    "all_checks_passed": True,
                    "merged": True,
                    "merge_commit_sha": "xyz789abc123",
                    "deployment_status": "deploying_to_dagster_plus",
                    "merged_at": datetime.now().isoformat()
                }

                context.add_output_metadata({
                    "pr_merged": True,
                    "checks_passed": len(checks),
                    "deployment_status": "deploying"
                })

                context.log.info(f"✅ All CI checks passed for PR #{pr_info['pr_number']}")
                context.log.info("🚀 PR merged and deploying to Dagster+")
                return merge_result
            else:
                context.log.info("Monitoring CI/CD checks...")
                # In production: poll GitHub API for check status
                # from github import Github
                # g = Github(os.environ["GITHUB_TOKEN"])
                # repo = g.get_repo(self.github_repo)
                # pr = repo.get_pull(pr_number)
                # # Check status and merge if all pass
                return {"status": "monitoring"}

        # Define the workflow as a job with sensors
        @dg.sensor(
            name="ai_workflow_request_sensor",
            minimum_interval_seconds=60,
            description="Sensor that triggers when new AI workflow requests are detected"
        )
        def workflow_request_sensor(context: dg.SensorEvaluationContext):
            """Sensor to detect new workflow generation requests."""
            if self.demo_mode:
                # In demo mode, trigger occasionally for demonstration
                yield dg.RunRequest(
                    run_key=f"demo_run_{datetime.now().timestamp()}",
                    tags={"source": "demo_mode", "agent": "claude"}
                )
            else:
                # In production, check for actual requests
                # and yield RunRequest for each new request
                pass

        return dg.Definitions(
            assets=[
                detect_workflow_request,
                generate_workflow_code,
                commit_to_git,
                create_pull_request,
                monitor_ci_and_merge
            ],
            sensors=[workflow_request_sensor]
        )
