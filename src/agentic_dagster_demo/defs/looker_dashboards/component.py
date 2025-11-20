"""Custom Looker component with demo mode support."""

import dagster as dg


class CustomLookerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Looker component with demo mode support for running without Looker credentials."""

    demo_mode: bool = True
    asset_key_prefix: str = "looker"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions with mocked Looker assets for demo purposes."""
        # Create mock Looker dashboard assets
        @dg.asset(
            key=dg.AssetKey([self.asset_key_prefix, "dashboards", "marketing_analytics"]),
            description="Mock Looker dashboard for marketing analytics",
            kinds={"looker", "dashboard"},
            group_name="looker_dashboards",
        )
        def marketing_analytics_dashboard():
            """Marketing Analytics Dashboard.

            A Looker dashboard showing key marketing metrics including:
            - Campaign performance
            - Conversion rates
            - Customer acquisition costs
            """
            return {
                "dashboard_id": "marketing_analytics",
                "dashboard_title": "Marketing Analytics Dashboard",
                "status": "demo_mode",
                "tiles_count": 8,
                "last_updated": "2025-01-15",
            }

        @dg.asset(
            key=dg.AssetKey([self.asset_key_prefix, "dashboards", "sales_performance"]),
            description="Mock Looker dashboard for sales performance metrics",
            kinds={"looker", "dashboard"},
            group_name="looker_dashboards",
        )
        def sales_performance_dashboard():
            """Sales Performance Dashboard.

            A Looker dashboard tracking sales metrics including:
            - Revenue by region
            - Sales pipeline health
            - Quota attainment
            """
            return {
                "dashboard_id": "sales_performance",
                "dashboard_title": "Sales Performance Dashboard",
                "status": "demo_mode",
                "tiles_count": 12,
                "last_updated": "2025-01-15",
            }

        @dg.asset(
            key=dg.AssetKey([self.asset_key_prefix, "explores", "user_behavior"]),
            description="Mock Looker explore for user behavior analysis",
            kinds={"looker", "explore"},
            group_name="looker_dashboards",
        )
        def user_behavior_explore():
            """User Behavior Explore.

            A Looker explore for analyzing user behavior patterns including:
            - Session duration
            - Page views
            - Feature usage
            """
            return {
                "explore_name": "user_behavior",
                "model": "analytics",
                "status": "demo_mode",
                "dimensions_count": 15,
                "measures_count": 8,
            }

        return dg.Definitions(
            assets=[
                marketing_analytics_dashboard,
                sales_performance_dashboard,
                user_behavior_explore,
            ],
        )
