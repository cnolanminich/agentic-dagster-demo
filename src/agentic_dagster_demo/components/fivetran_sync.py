from typing import Any
import dagster as dg


class FivetranSyncComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for Fivetran data sync as Dagster assets.

    This component simulates Fivetran syncing data from source databases
    into destination tables. Creates multiple assets for synced tables.

    Named after data output: Synced tables from Fivetran
    """

    asset_key_prefix: str = "fivetran"
    description: str
    source_schema: str
    tables: list[str]
    demo_mode: bool = True
    deps: list[str] = []

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []

        # Create an asset for each table being synced
        for table in self.tables:
            # Use AssetKey with path for hierarchical structure
            asset_key = dg.AssetKey([self.asset_key_prefix, self.source_schema, table])

            # We need to create a closure to capture the table name
            def make_asset_fn(table_name: str, key: dg.AssetKey):
                @dg.asset(
                    key=key,
                    deps=[dg.AssetKey(dep) for dep in self.deps],
                    description=f"{self.description} - {table_name} table",
                    metadata={
                        "demo_mode": self.demo_mode,
                        "source_schema": self.source_schema,
                        "table": table_name,
                        "integration": "fivetran",
                    }
                )
                def fivetran_sync_asset(context: dg.AssetExecutionContext) -> dict[str, Any]:
                    """Sync data from source via Fivetran"""

                    if self.demo_mode:
                        context.log.info(f"[DEMO MODE] Syncing table: {self.source_schema}.{table_name}")
                        context.log.info(f"[DEMO MODE] Fivetran connector running...")

                        # Return sample data based on table
                        if table_name == "ai_agent_requests":
                            sample_records = [
                                {
                                    "request_id": "req_001",
                                    "agent_name": "Claude",
                                    "request_type": "code_generation",
                                    "input_tokens": 1500,
                                    "output_tokens": 3200,
                                    "execution_time_ms": 2500,
                                    "status": "SUCCESS",
                                    "workflow_id": "wf_001"
                                },
                                {
                                    "request_id": "req_002",
                                    "agent_name": "GPT-4",
                                    "request_type": "workflow_creation",
                                    "input_tokens": 800,
                                    "output_tokens": 1200,
                                    "execution_time_ms": 1800,
                                    "status": "SUCCESS",
                                    "workflow_id": "wf_002"
                                },
                                {
                                    "request_id": "req_003",
                                    "agent_name": "Claude",
                                    "request_type": "analysis",
                                    "input_tokens": 2000,
                                    "output_tokens": 1500,
                                    "execution_time_ms": 3200,
                                    "status": "SUCCESS",
                                    "workflow_id": "wf_001"
                                },
                            ]
                        elif table_name == "workflows":
                            sample_records = [
                                {
                                    "workflow_id": "wf_001",
                                    "workflow_name": "data_pipeline_gen",
                                    "trigger_type": "agent_generated",
                                    "status": "SUCCESS",
                                    "created_by": "Claude"
                                },
                                {
                                    "workflow_id": "wf_002",
                                    "workflow_name": "etl_pipeline",
                                    "trigger_type": "git_push",
                                    "status": "SUCCESS",
                                    "created_by": "user@example.com"
                                },
                                {
                                    "workflow_id": "wf_003",
                                    "workflow_name": "ml_training",
                                    "trigger_type": "schedule",
                                    "status": "RUNNING",
                                    "created_by": "scheduler"
                                },
                            ]
                        else:
                            sample_records = [
                                {"id": 1, "name": f"Sample {table_name} record 1"},
                                {"id": 2, "name": f"Sample {table_name} record 2"},
                            ]

                        return {
                            "status": "demo_success",
                            "table": table_name,
                            "schema": self.source_schema,
                            "records_synced": len(sample_records),
                            "sample_data": sample_records[:3]  # Show first 3 records
                        }

                    # Production mode: Would connect to Fivetran API
                    try:
                        context.log.info(f"Syncing table: {self.source_schema}.{table_name} via Fivetran")
                        context.log.warning("Production mode requires Fivetran API credentials")

                        # TODO: Actual Fivetran API integration
                        # from fivetran import Fivetran
                        # client = Fivetran(api_key=..., api_secret=...)
                        # sync_result = client.sync_connector(connector_id)

                        return {
                            "status": "success",
                            "table": table_name,
                            "schema": self.source_schema,
                            "records_synced": 0,
                        }
                    except Exception as e:
                        context.log.error(f"Failed to sync via Fivetran: {e}")
                        raise

                return fivetran_sync_asset

            # Create the asset for this table
            asset_fn = make_asset_fn(table, asset_key)
            assets.append(asset_fn)

        return dg.Definitions(assets=assets)
