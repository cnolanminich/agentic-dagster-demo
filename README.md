# Agentic Dagster Demo: AI-Powered Workflow Generation

## Overview

This demo showcases how organizations can replace brittle orchestration tools (like n8n) and expensive AI infrastructure (like Cognition) with Dagster's unified platform for data and AI workflows. The primary use case demonstrates **AI agents (Claude) dynamically generating Dagster job definitions** that get pushed to Git and auto-deployed through CI/CD.

### Key Value Propositions

1. **Replace n8n webhook chains** with version-controlled, observable DAGs
2. **Reduce infrastructure costs** compared to dedicated AI workflow platforms
3. **Enable AI-driven workflow generation** with production-ready observability
4. **Unify analytics and AI workloads** in a single platform
5. **Git-based deployment** with proper CI/CD validation

## Demo Architecture

### AI Workflow Generation Pipeline

The demo includes a complete pipeline showing how Claude generates Dagster workflows:

```
┌─────────────────────┐
│ Detect Workflow     │  ← AI agent requests new workflow
│ Request             │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│ Generate Workflow   │  ← Claude generates Dagster code
│ Code (Claude API)   │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│ Commit to Git       │  ← Create feature branch & commit
│                     │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│ Create Pull Request │  ← Auto-create PR with description
│                     │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│ Monitor CI & Merge  │  ← Watch GitHub Actions, auto-merge
│                     │
└─────────────────────┘
```

### Technology Stack

- **Dagster** - Unified orchestration platform
- **Dagster+** - Cloud control plane (hybrid deployment)
- **Claude API** - AI-powered code generation
- **GitHub** - Version control and CI/CD
- **GitHub Actions** - Automated testing and deployment
- **Snowflake + dbt** - Analytics transformations (secondary use case)

## Project Structure

```
agentic-dagster-demo/
├── src/agentic_dagster_demo/
│   ├── components/
│   │   └── ai_workflow_generator.py    # Custom component for AI workflows
│   ├── defs/
│   │   └── claude_workflow_gen/        # Component instance
│   │       └── defs.yaml               # Configuration (demo_mode: true)
│   └── definitions.py                   # Main Dagster definitions
├── analytics_dbt/                       # dbt project (for secondary use case)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_ai_agent_requests.sql
│   │   │   └── stg_workflows.sql
│   │   └── marts/
│   │       ├── agent_performance_metrics.sql
│   │       └── workflow_cost_analysis.sql
│   └── dbt_project.yml
└── README.md
```

## Assets Defined

### AI Workflow Generation (5 assets)

1. **detect_workflow_request** - Monitors for incoming requests from AI agents
   - **Kinds**: `python`, `ai`, `claude`
   - **Description**: Detects when Claude or other agents request new workflows

2. **generate_workflow_code** - Uses Claude API to generate Dagster code
   - **Kinds**: `python`, `ai`, `claude`
   - **Dependencies**: `detect_workflow_request`
   - **Description**: Generates production-ready Dagster assets from natural language

3. **commit_to_git** - Commits generated code to Git repository
   - **Kinds**: `git`, `github`
   - **Dependencies**: `generate_workflow_code`
   - **Description**: Creates feature branch and commits AI-generated code

4. **create_pull_request** - Creates GitHub PR with automated CI/CD
   - **Kinds**: `github`, `ci_cd`
   - **Dependencies**: `commit_to_git`
   - **Description**: Opens PR and triggers validation checks

5. **monitor_ci_and_merge** - Monitors checks and auto-merges on success
   - **Kinds**: `github`, `ci_cd`
   - **Dependencies**: `create_pull_request`
   - **Description**: Watches GitHub Actions and merges when all tests pass

### Sensors

- **ai_workflow_request_sensor** - Triggers workflow generation pipeline when new requests detected

## Demo Mode

The component supports **demo mode** which allows running the entire pipeline locally without:
- Real Anthropic API keys
- GitHub repository access
- Snowflake credentials

### Toggling Demo Mode

Edit `src/agentic_dagster_demo/defs/claude_workflow_gen/defs.yaml`:

```yaml
type: agentic_dagster_demo.components.ai_workflow_generator.AiWorkflowGenerator

attributes:
  demo_mode: true          # Set to false for production
  github_repo: "your-org/your-repo"
  git_branch: "main"
```

## Running the Demo

### Prerequisites

- Python 3.10+
- `uv` package manager

### Installation

```bash
# Navigate to project
cd agentic-dagster-demo

# Sync dependencies
uv sync

# Validate definitions
uv run dg check defs

# List all assets
uv run dg list defs
```

### Launch Dagster UI

```bash
uv run dg dev
```

Then open http://localhost:3000 to view the Dagster UI.

### Materialize Assets

In the Dagster UI:
1. Navigate to **Assets** → **ai_workflow_generation** group
2. Select all 5 assets
3. Click **Materialize selected**
4. Watch the pipeline execute in demo mode

You'll see simulated:
- Claude generating workflow code
- Git commits and branch creation
- PR creation with AI-generated descriptions
- CI/CD checks passing
- Automatic merge to main

## Production Setup

### Required Environment Variables

For production deployment (demo_mode: false):

```bash
# Anthropic API
export ANTHROPIC_API_KEY="sk-ant-..."

# GitHub
export GITHUB_TOKEN="ghp_..."

# Snowflake (for dbt integration)
export SNOWFLAKE_ACCOUNT="xy12345"
export SNOWFLAKE_USER="dagster_user"
export SNOWFLAKE_PASSWORD="..."
export SNOWFLAKE_ROLE="TRANSFORMER"
export SNOWFLAKE_DATABASE="ANALYTICS"
export SNOWFLAKE_WAREHOUSE="TRANSFORMING"
```

### Dagster+ Cloud Deployment

1. Create Dagster+ organization
2. Set up hybrid deployment (your compute, Dagster's control plane)
3. Configure GitHub Actions for branch deploys:

```yaml
# .github/workflows/dagster-deploy.yml
name: Dagster Deploy
on:
  push:
    branches: [main]
  pull_request:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dagster-io/dagster-cloud-action/actions/build_deploy_python_executable@v0.1
        with:
          dagster_cloud_api_token: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
          location: agentic-dagster-demo
```

4. Enable auto-deploy on PR merge

## Secondary Use Cases

### dbt + Snowflake Transformations

The demo includes pre-built dbt models for analytics:

- **Staging models**: Clean raw data from operational systems
  - `stg_ai_agent_requests` - AI request metadata
  - `stg_workflows` - Dagster workflow execution data

- **Mart models**: Business-level aggregations
  - `agent_performance_metrics` - AI agent usage and success rates
  - `workflow_cost_analysis` - Cost tracking for AI-generated workflows

### Observability & Monitoring

Dagster provides built-in features that replace custom monitoring:
- Asset lineage visualization
- Run history and logs
- Failure alerts
- Performance metrics
- Data quality checks

## Migration from n8n

### Before (n8n)

❌ Webhook-based triggers (brittle, no versioning)
❌ Manual configuration in UI (no code review)
❌ Limited observability
❌ Difficult to test locally
❌ Expensive separate AI infrastructure

### After (Dagster)

✅ Git-based versioning and deployment
✅ Code review for all workflow changes
✅ Full lineage and observability
✅ Local development with demo mode
✅ Unified platform for data + AI

## Cost Comparison

| Platform | Monthly Cost (estimate) | Features |
|----------|------------------------|----------|
| **n8n + Cognition** | $5,000+ | Separate tools, limited observability |
| **Dagster+ (Hybrid)** | $1,500 | Unified platform, full observability |

**Savings: ~70% reduction in infrastructure costs**

## Next Steps

1. **Add more AI agents**: Extend to support GPT-4, Gemini, etc.
2. **Implement validation**: Add asset checks for generated code quality
3. **Create approval workflow**: Add human-in-the-loop for production deployments
4. **Expand dbt models**: Add more analytics for AI usage tracking
5. **Set up alerts**: Configure Slack/PagerDuty for workflow failures

## Demo Talking Points

### For Sales Engineers

1. **Show the asset lineage graph** - Visualize the entire AI workflow generation pipeline
2. **Materialize assets in demo mode** - Watch Claude "generate" code in real-time
3. **Highlight the kinds** - Point out `ai`, `claude`, `github`, `ci_cd` integrations
4. **Explain cost savings** - Unified platform vs. separate tools
5. **Demo local development** - Show how demo_mode enables offline development

### Key Differentiators

- **Version control first** - All workflows are code, reviewed, and tested
- **Observable by default** - No custom monitoring infrastructure needed
- **Developer friendly** - Local development, testing, and debugging
- **Cost effective** - One platform for data + AI workflows
- **Production ready** - Built-in reliability, retries, and alerting

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
- [Components Guide](https://docs.dagster.io/guides/build/components)

---

**🤖 This demo was generated using Claude Code to showcase the power of AI-driven workflow creation!**
