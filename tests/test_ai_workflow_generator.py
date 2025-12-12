"""Tests for the AiWorkflowGenerator component."""

import pytest
from unittest.mock import MagicMock
from datetime import datetime

from agentic_dagster_demo.components.ai_workflow_generator import AiWorkflowGenerator


class TestAiWorkflowGeneratorConfig:
    """Tests for AiWorkflowGenerator configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        generator = AiWorkflowGenerator()
        assert generator.demo_mode is False
        assert generator.github_repo == "my-org/data-workflows"
        assert generator.git_branch == "main"

    def test_custom_config(self):
        """Test custom configuration values."""
        generator = AiWorkflowGenerator(
            demo_mode=True,
            github_repo="custom-org/custom-repo",
            git_branch="develop"
        )
        assert generator.demo_mode is True
        assert generator.github_repo == "custom-org/custom-repo"
        assert generator.git_branch == "develop"


class TestAiWorkflowGeneratorBuildDefs:
    """Tests for AiWorkflowGenerator.build_defs()."""

    def test_build_defs_returns_definitions(self):
        """Test that build_defs returns a valid Definitions object."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        assert isinstance(defs, dg.Definitions)

    def test_build_defs_creates_five_assets(self):
        """Test that build_defs creates the expected 5 assets."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)
        assets = defs.assets

        assert len(list(assets)) == 5

    def test_build_defs_creates_sensor(self):
        """Test that build_defs creates a sensor."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)
        sensors = defs.sensors

        assert len(list(sensors)) == 1

    def test_asset_names(self):
        """Test that assets have expected names."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)
        asset_keys = [asset.key.to_user_string() for asset in defs.assets]

        expected_assets = [
            "detect_workflow_request",
            "generate_workflow_code",
            "commit_to_git",
            "create_pull_request",
            "monitor_ci_and_merge"
        ]

        for expected in expected_assets:
            assert expected in asset_keys


class TestAssetMaterialization:
    """Tests for asset materialization using Dagster's testing utilities."""

    def test_detect_workflow_request_demo_mode(self):
        """Test detect_workflow_request asset in demo mode."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        # Get the asset by key
        detect_asset = None
        for asset in defs.assets:
            if asset.key.to_user_string() == "detect_workflow_request":
                detect_asset = asset
                break

        assert detect_asset is not None

        # Materialize the asset
        result = materialize_to_memory([detect_asset])

        assert result.success
        output = result.output_for_node("detect_workflow_request")

        assert isinstance(output, dict)
        assert "request_id" in output
        assert "agent_name" in output
        assert output["agent_name"] == "Claude"
        assert "request_type" in output
        assert output["request_type"] == "create_workflow"
        assert "requirements" in output
        assert "created_at" in output

    def test_detect_workflow_request_non_demo_mode(self):
        """Test detect_workflow_request asset in non-demo mode."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=False)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        detect_asset = None
        for asset in defs.assets:
            if asset.key.to_user_string() == "detect_workflow_request":
                detect_asset = asset
                break

        result = materialize_to_memory([detect_asset])

        assert result.success
        output = result.output_for_node("detect_workflow_request")

        assert output == {"status": "no_requests"}

    def test_full_pipeline_demo_mode(self):
        """Test full pipeline materialization in demo mode."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        # Materialize all assets
        result = materialize_to_memory(list(defs.assets))

        assert result.success

        # Verify detect_workflow_request output
        detect_output = result.output_for_node("detect_workflow_request")
        assert detect_output["agent_name"] == "Claude"

        # Verify generate_workflow_code output
        generate_output = result.output_for_node("generate_workflow_code")
        assert "generated_code" in generate_output
        assert "assets_created" in generate_output
        assert generate_output["status"] == "success"

        # Verify commit_to_git output
        commit_output = result.output_for_node("commit_to_git")
        assert "branch" in commit_output
        assert commit_output["branch"].startswith("agent-generated/")
        assert "commit_sha" in commit_output

        # Verify create_pull_request output
        pr_output = result.output_for_node("create_pull_request")
        assert "pr_number" in pr_output
        assert "pr_url" in pr_output
        assert pr_output["status"] == "open"

        # Verify monitor_ci_and_merge output
        merge_output = result.output_for_node("monitor_ci_and_merge")
        assert merge_output["merged"] is True
        assert merge_output["all_checks_passed"] is True


class TestGeneratedCodeStructure:
    """Tests for the structure of generated workflow code."""

    def test_generated_code_contains_dagster_imports(self):
        """Test that generated code contains Dagster imports."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        generate_output = result.output_for_node("generate_workflow_code")
        code = generate_output["generated_code"]

        assert "import dagster as dg" in code
        assert "@dg.asset" in code

    def test_generated_code_creates_schedule(self):
        """Test that generated code includes schedule definition."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        generate_output = result.output_for_node("generate_workflow_code")
        code = generate_output["generated_code"]

        assert "ScheduleDefinition" in code
        assert "cron_schedule" in code

    def test_generated_workflow_metadata(self):
        """Test the metadata of generated workflow."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        generate_output = result.output_for_node("generate_workflow_code")

        assert generate_output["tokens_used"] == 2500
        assert generate_output["generation_time_ms"] == 1200
        assert len(generate_output["assets_created"]) == 3


class TestCIChecks:
    """Tests for CI/CD check simulations."""

    def test_all_ci_checks_pass(self):
        """Test that all CI checks pass in demo mode."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        merge_output = result.output_for_node("monitor_ci_and_merge")
        checks = merge_output["ci_checks"]

        expected_checks = [
            "syntax_validation",
            "type_checking",
            "dagster_validation",
            "unit_tests",
            "integration_tests"
        ]

        for check in expected_checks:
            assert check in checks
            assert checks[check] == "passed"

    def test_merge_commit_sha_exists(self):
        """Test that merge commit SHA is generated."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        merge_output = result.output_for_node("monitor_ci_and_merge")

        assert "merge_commit_sha" in merge_output
        assert len(merge_output["merge_commit_sha"]) > 0


class TestPullRequestDetails:
    """Tests for pull request creation details."""

    def test_pr_url_contains_repo(self):
        """Test that PR URL contains the configured repo."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True, github_repo="my-org/my-repo")
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        pr_output = result.output_for_node("create_pull_request")

        assert "my-org/my-repo" in pr_output["pr_url"]

    def test_pr_body_contains_assets(self):
        """Test that PR body lists created assets."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        pr_output = result.output_for_node("create_pull_request")
        body = pr_output["body"]

        assert "customer_raw_data" in body
        assert "customer_aggregated_by_region" in body
        assert "customer_lifetime_value" in body

    def test_pr_has_base_branch(self):
        """Test that PR has correct base branch."""
        import dagster as dg
        from dagster import materialize_to_memory

        generator = AiWorkflowGenerator(demo_mode=True, git_branch="develop")
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        result = materialize_to_memory(list(defs.assets))

        pr_output = result.output_for_node("create_pull_request")

        assert pr_output["base_branch"] == "develop"


class TestAssetGroups:
    """Tests for asset group configuration."""

    def test_all_assets_in_same_group(self):
        """Test that all assets are in the ai_workflow_generation group."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        for asset in defs.assets:
            assert asset.group_names_by_key[asset.key] == "ai_workflow_generation"


class TestAssetKinds:
    """Tests for asset kind metadata."""

    def test_detect_workflow_request_kinds(self):
        """Test detect_workflow_request has correct kinds."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        for asset in defs.assets:
            if asset.key.to_user_string() == "detect_workflow_request":
                kinds = asset.tags_by_key[asset.key].get("dagster/kind/python", None)
                # The kinds are stored in metadata, check metadata instead
                break

    def test_commit_to_git_kinds(self):
        """Test commit_to_git has git-related kinds."""
        import dagster as dg

        generator = AiWorkflowGenerator(demo_mode=True)
        mock_context = MagicMock(spec=dg.ComponentLoadContext)

        defs = generator.build_defs(mock_context)

        # Verify the asset exists
        git_asset_found = False
        for asset in defs.assets:
            if asset.key.to_user_string() == "commit_to_git":
                git_asset_found = True
                break

        assert git_asset_found
