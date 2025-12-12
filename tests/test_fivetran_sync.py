"""Tests for the FivetranSyncComponent."""

import pytest
from unittest.mock import MagicMock

from agentic_dagster_demo.components.fivetran_sync import FivetranSyncComponent


class TestFivetranSyncComponentConfig:
    """Tests for FivetranSyncComponent configuration."""

    def test_required_fields(self):
        """Test that required fields must be provided."""
        # description, source_schema, and tables are required
        component = FivetranSyncComponent(
            description="Test description",
            source_schema="test_schema",
            tables=["table1", "table2"]
        )
        assert component.description == "Test description"
        assert component.source_schema == "test_schema"
        assert component.tables == ["table1", "table2"]

    def test_default_values(self):
        """Test default configuration values."""
        component = FivetranSyncComponent(
            description="Test",
            source_schema="schema",
            tables=["table"]
        )
        assert component.asset_key_prefix == "fivetran"
        assert component.demo_mode is True
        assert component.deps == []

    def test_custom_values(self):
        """Test custom configuration values."""
        component = FivetranSyncComponent(
            description="Custom desc",
            source_schema="custom_schema",
            tables=["t1", "t2", "t3"],
            asset_key_prefix="custom_prefix",
            demo_mode=False,
            deps=["upstream_dep"]
        )
        assert component.asset_key_prefix == "custom_prefix"
        assert component.demo_mode is False
        assert component.deps == ["upstream_dep"]
        assert len(component.tables) == 3


class TestFivetranSyncComponentBuildDefs:
    """Tests for FivetranSyncComponent.build_defs()."""

    def test_build_defs_returns_definitions(self):
        """Test that build_defs returns a valid Definitions object."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="Test",
            source_schema="schema",
            tables=["table1"]
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)

        assert isinstance(defs, dg.Definitions)

    def test_creates_asset_per_table(self):
        """Test that one asset is created per table."""
        import dagster as dg

        tables = ["table1", "table2", "table3"]
        component = FivetranSyncComponent(
            description="Test",
            source_schema="schema",
            tables=tables
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)

        assert len(list(defs.assets)) == len(tables)

    def test_asset_keys_have_correct_structure(self):
        """Test that asset keys have the expected hierarchical structure."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="Test",
            source_schema="test_schema",
            tables=["test_table"],
            asset_key_prefix="fivetran"
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)

        assert len(assets) == 1
        asset_key = assets[0].key
        # Key should be [prefix, schema, table]
        assert asset_key.path == ["fivetran", "test_schema", "test_table"]


def get_node_name_from_asset_key(asset_key):
    """Helper to get the node name from an asset key."""
    return "__".join(asset_key.path)


class TestFivetranSyncAssetExecution:
    """Tests for Fivetran sync asset execution."""

    def test_demo_mode_ai_agent_requests_table(self):
        """Test demo mode data for ai_agent_requests table."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["ai_agent_requests"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)

        result = materialize_to_memory(assets)

        assert result.success

        # Get node name from asset key
        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)

        assert output["status"] == "demo_success"
        assert output["table"] == "ai_agent_requests"
        assert output["schema"] == "raw"
        assert output["records_synced"] > 0
        assert "sample_data" in output

        # Verify sample data structure
        sample = output["sample_data"][0]
        expected_fields = ["request_id", "agent_name", "request_type",
                          "input_tokens", "output_tokens", "execution_time_ms",
                          "status", "workflow_id"]
        for field in expected_fields:
            assert field in sample

    def test_demo_mode_workflows_table(self):
        """Test demo mode data for workflows table."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["workflows"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)

        result = materialize_to_memory(assets)

        assert result.success
        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)

        assert output["status"] == "demo_success"
        assert output["table"] == "workflows"
        assert output["records_synced"] > 0

        # Verify sample data structure
        sample = output["sample_data"][0]
        expected_fields = ["workflow_id", "workflow_name", "trigger_type",
                          "status", "created_by"]
        for field in expected_fields:
            assert field in sample

    def test_demo_mode_unknown_table(self):
        """Test demo mode data for unknown table returns generic data."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["unknown_table"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)

        result = materialize_to_memory(assets)

        assert result.success
        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)

        assert output["status"] == "demo_success"
        assert output["table"] == "unknown_table"
        assert output["records_synced"] == 2

        # Generic data has id and name
        sample = output["sample_data"][0]
        assert "id" in sample
        assert "name" in sample

    def test_production_mode_returns_success(self):
        """Test production mode returns success status."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["test_table"],
            demo_mode=False
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)

        result = materialize_to_memory(assets)

        assert result.success
        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)

        assert output["status"] == "success"
        assert output["table"] == "test_table"
        assert output["records_synced"] == 0


class TestFivetranSyncAssetMetadata:
    """Tests for Fivetran sync asset metadata."""

    def test_asset_has_correct_metadata(self):
        """Test that assets have expected metadata."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="Test sync",
            source_schema="test_schema",
            tables=["test_table"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        asset = list(defs.assets)[0]

        # Access metadata from the asset - it's in metadata_by_key
        metadata = asset.metadata_by_key[asset.key]

        assert metadata["demo_mode"] is True
        assert metadata["source_schema"] == "test_schema"
        assert metadata["table"] == "test_table"
        assert metadata["integration"] == "fivetran"

    def test_asset_has_description(self):
        """Test that assets have a description."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="My description",
            source_schema="schema",
            tables=["my_table"]
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        asset = list(defs.assets)[0]

        # Access description from descriptions_by_key
        desc = asset.descriptions_by_key.get(asset.key, "")

        assert "My description" in desc
        assert "my_table" in desc


class TestFivetranSyncMultipleTables:
    """Tests for syncing multiple tables."""

    def test_multiple_tables_have_unique_keys(self):
        """Test that multiple tables have unique asset keys."""
        import dagster as dg

        tables = ["table1", "table2", "table3"]
        component = FivetranSyncComponent(
            description="Test",
            source_schema="schema",
            tables=tables
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)

        keys = [asset.key for asset in assets]
        # All keys should be unique
        assert len(keys) == len(set(tuple(k.path) for k in keys))

    def test_multiple_table_asset_count(self):
        """Test that correct number of assets are created for multiple tables."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["ai_agent_requests", "workflows", "events"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)

        assert len(list(defs.assets)) == 3


class TestAssetKeyPath:
    """Tests for asset key path structure."""

    def test_custom_prefix_in_key(self):
        """Test that custom prefix appears in asset key."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="Test",
            source_schema="my_schema",
            tables=["my_table"],
            asset_key_prefix="custom_prefix"
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        asset = list(defs.assets)[0]

        assert asset.key.path[0] == "custom_prefix"
        assert asset.key.path[1] == "my_schema"
        assert asset.key.path[2] == "my_table"

    def test_default_fivetran_prefix(self):
        """Test that default prefix is 'fivetran'."""
        import dagster as dg

        component = FivetranSyncComponent(
            description="Test",
            source_schema="schema",
            tables=["table"]
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        asset = list(defs.assets)[0]

        assert asset.key.path[0] == "fivetran"


class TestSampleDataContent:
    """Tests for sample data content in demo mode."""

    def test_ai_agent_requests_has_claude_data(self):
        """Test that ai_agent_requests sample data includes Claude agent."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["ai_agent_requests"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)
        result = materialize_to_memory(assets)

        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)
        sample_data = output["sample_data"]

        agent_names = [record["agent_name"] for record in sample_data]
        assert "Claude" in agent_names

    def test_workflows_has_different_trigger_types(self):
        """Test that workflows sample data has various trigger types."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["workflows"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)
        result = materialize_to_memory(assets)

        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)
        sample_data = output["sample_data"]

        trigger_types = {record["trigger_type"] for record in sample_data}
        assert len(trigger_types) > 1  # Multiple trigger types

    def test_sample_data_limited_to_three_records(self):
        """Test that sample_data is limited to first 3 records."""
        import dagster as dg
        from dagster import materialize_to_memory

        component = FivetranSyncComponent(
            description="Test",
            source_schema="raw",
            tables=["ai_agent_requests"],
            demo_mode=True
        )
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = component.build_defs(mock_context)
        assets = list(defs.assets)
        result = materialize_to_memory(assets)

        node_name = get_node_name_from_asset_key(assets[0].key)
        output = result.output_for_node(node_name)

        assert len(output["sample_data"]) <= 3
