"""Custom Looker component with demo mode support.

This component extends the official LookerComponent to add demo mode functionality.
When demo_mode is enabled, it creates mock Looker dashboard and explore assets that are
downstream of dbt models, demonstrating the full data pipeline from raw data -> dbt transforms -> BI dashboards.
When demo_mode is disabled, it uses the real Looker integration.
"""

import dagster as dg
from dagster_looker import LookerComponent
from pydantic import ConfigDict


class CustomLookerComponent(LookerComponent):
    """Looker component that extends LookerComponent with demo mode support.

    This subclass adds a demo_mode field that allows running without real Looker credentials.
    When demo_mode=True, it creates mock Looker dashboard and explore assets that depend on dbt models.
    When demo_mode=False, it delegates to the parent LookerComponent for real Looker integration.
    """

    # Override parent's model config to allow our custom demo_mode field
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    demo_mode: bool = True

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions, using demo mode if enabled.

        When demo_mode is True, returns mock Looker assets.
        When demo_mode is False, calls parent's build_defs for real Looker integration.
        """
        if self.demo_mode:
            return self._build_demo_defs(context)
        else:
            # Call parent class for real Looker integration
            return super().build_defs(context)

    def _build_demo_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build demo mode definitions with mocked Looker assets downstream of dbt models."""
        # Create mock Looker dashboard assets that depend on dbt models
        @dg.asset(
            key=dg.AssetKey(["looker", "dashboards", "marketing_analytics"]),
            description="Mock Looker dashboard for marketing analytics built on agent performance data",
            kinds={"looker", "dashboard"},
            group_name="looker_dashboards",
            deps=[dg.AssetKey(["agent_performance_metrics"])],
        )
        def marketing_analytics_dashboard(context: dg.AssetExecutionContext):
            """Marketing Analytics Dashboard.

            A Looker dashboard showing key marketing metrics built on top of agent performance data including:
            - Campaign performance from AI agent requests
            - Conversion rates
            - Customer acquisition costs
            """
            context.log.info("Generating mock Looker dashboard based on agent_performance_metrics")
            return {
                "dashboard_id": "marketing_analytics",
                "dashboard_title": "Marketing Analytics Dashboard",
                "status": "demo_mode",
                "tiles_count": 8,
                "last_updated": "2025-01-15",
                "source_models": ["agent_performance_metrics"],
            }

        @dg.asset(
            key=dg.AssetKey(["looker", "dashboards", "sales_performance"]),
            description="Mock Looker dashboard for sales performance metrics based on workflow cost analysis",
            kinds={"looker", "dashboard"},
            group_name="looker_dashboards",
            deps=[dg.AssetKey(["workflow_cost_analysis"])],
        )
        def sales_performance_dashboard(context: dg.AssetExecutionContext):
            """Sales Performance Dashboard.

            A Looker dashboard tracking sales metrics built on workflow cost data including:
            - Revenue by region
            - Sales pipeline health from workflow costs
            - Quota attainment
            """
            context.log.info("Generating mock Looker dashboard based on workflow_cost_analysis")
            return {
                "dashboard_id": "sales_performance",
                "dashboard_title": "Sales Performance Dashboard",
                "status": "demo_mode",
                "tiles_count": 12,
                "last_updated": "2025-01-15",
                "source_models": ["workflow_cost_analysis"],
            }

        @dg.asset(
            key=dg.AssetKey(["looker", "explores", "user_behavior"]),
            description="Mock Looker explore for user behavior analysis based on agent requests and workflows",
            kinds={"looker", "explore"},
            group_name="looker_dashboards",
            deps=[
                dg.AssetKey(["stg_ai_agent_requests"]),
                dg.AssetKey(["stg_workflows"]),
            ],
        )
        def user_behavior_explore(context: dg.AssetExecutionContext):
            """User Behavior Explore.

            A Looker explore for analyzing user behavior patterns built on staging tables including:
            - Session duration from agent requests
            - Page views
            - Feature usage from workflows
            """
            context.log.info("Generating mock Looker explore based on staging tables")
            return {
                "explore_name": "user_behavior",
                "model": "analytics",
                "status": "demo_mode",
                "dimensions_count": 15,
                "measures_count": 8,
                "source_models": ["stg_ai_agent_requests", "stg_workflows"],
            }

        return dg.Definitions(
            assets=[
                marketing_analytics_dashboard,
                sales_performance_dashboard,
                user_behavior_explore,
            ],
        )
