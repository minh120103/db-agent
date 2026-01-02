#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Location: ./tests/test_git_agent.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Git Agent Team

Tests for Git Agent MCP Server
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from git_agent.server import GitAgent


@pytest.fixture
def git_agent():
    """Create a git agent instance."""
    return GitAgent()


class TestGitAgent:
    """Test suite for GitAgent."""

    def test_agent_initialization(self, git_agent):
        """Test agent initialization."""
        assert git_agent is not None

    def test_check_commit_status_normal(self, git_agent):
        """Test normal commit status check."""
        result = git_agent.check_commit_status("Check latest commit")
        
        assert result["success"] is True
        assert "commit_hash" in result
        assert "processing_time_ms" in result
        assert result["is_slow"] is False
        assert result["status"] == "NORMAL"

    def test_check_commit_status_slow(self, git_agent):
        """Test slow commit detection."""
        result = git_agent.check_commit_status("Check slow commits")
        
        assert result["success"] is True
        assert result["is_slow"] is True
        assert result["status"] == "SLOW COMMIT"
        assert "recommendations" in result
        assert len(result["recommendations"]) > 0

    def test_check_merge_conflict_active(self, git_agent):
        """Test active merge conflict detection."""
        result = git_agent.check_merge_conflict("detect active conflicts")
        
        assert result["success"] is True
        assert "conflicts_detected" in result
        assert result["status"] in ["ACTIVE MERGE CONFLICTS", "NO ACTIVE CONFLICTS"]

    def test_check_merge_conflict_history(self, git_agent):
        """Test merge conflict history."""
        result = git_agent.check_merge_conflict("show conflict history")
        
        assert result["success"] is True
        assert result["conflicts_detected"] is True
        assert result["status"] == "CONFLICT HISTORY FOUND"
        assert "latest_event" in result

    def test_check_repo_size_prod(self, git_agent):
        """Test repository size check for prod."""
        result = git_agent.check_repo_size("Check prod repository size")
        
        assert result["success"] is True
        assert result["repository"] == "prod_repo"
        assert "current_size_mb" in result
        assert "usage_percent" in result
        assert result["status"] in ["NORMAL", "WARNING", "CRITICAL"]

    def test_check_repo_size_dev(self, git_agent):
        """Test repository size check for dev."""
        result = git_agent.check_repo_size("Check dev repo size")
        
        assert result["success"] is True
        assert result["repository"] == "dev_repo"
        assert "growth_rate_mb_per_week" in result
        assert "recommendations" in result

    def test_check_abnormal_commits(self, git_agent):
        """Test abnormal commit detection."""
        result = git_agent.check_abnormal_commits("Find suspicious commits")
        
        assert result["success"] is True
        assert "has_abnormal_commits" in result
        assert result["status"] in ["ABNORMAL COMMITS DETECTED", "NORMAL"]
        
        if result["has_abnormal_commits"]:
            assert "anomaly_count" in result
            assert "anomalies" in result

    def test_check_batch_operations(self, git_agent):
        """Test batch operations check."""
        result = git_agent.check_batch_operations("Check batch cherry-pick")
        
        assert result["success"] is True
        assert "has_issues" in result
        assert result["status"] in ["BATCH OPERATION ISSUES DETECTED", "NORMAL"]
        
        if result["has_issues"]:
            assert "failure_rate_percent" in result
            assert "batch_details" in result

    def test_all_tools_return_success(self, git_agent):
        """Test that all tools return success=True."""
        tools = [
            git_agent.check_commit_status,
            git_agent.check_merge_conflict,
            git_agent.check_repo_size,
            git_agent.check_abnormal_commits,
            git_agent.check_batch_operations,
        ]
        
        for tool in tools:
            result = tool("test input")
            assert result["success"] is True
            assert "input" in result

    def test_all_tools_have_status(self, git_agent):
        """Test that all tools return a status field."""
        tools = [
            git_agent.check_commit_status,
            git_agent.check_merge_conflict,
            git_agent.check_repo_size,
            git_agent.check_abnormal_commits,
            git_agent.check_batch_operations,
        ]
        
        for tool in tools:
            result = tool("test input")
            assert "status" in result
            assert isinstance(result["status"], str)

    def test_keyword_based_routing(self, git_agent):
        """Test keyword-based mock data selection."""
        # Test slow commit keyword
        result = git_agent.check_commit_status("optimize performance")
        assert result["is_slow"] is True
        
        # Test conflict keywords
        result = git_agent.check_merge_conflict("show conflict log")
        assert result["status"] == "CONFLICT HISTORY FOUND"
        
        # Test repo selection keywords
        result = git_agent.check_repo_size("staging environment")
        assert "staging" in result["repository"]


class TestErrorHandling:
    """Test error handling in GitAgent."""

    def test_commit_status_with_empty_input(self, git_agent):
        """Test commit status with empty input."""
        result = git_agent.check_commit_status("")
        assert result["success"] is True  # Should still work with empty input

    def test_all_tools_accept_any_string(self, git_agent):
        """Test that all tools accept any string input."""
        test_inputs = ["", "test", "123", "special @#$ chars", "very " * 100 + "long"]
        
        for test_input in test_inputs:
            assert git_agent.check_commit_status(test_input)["success"]
            assert git_agent.check_merge_conflict(test_input)["success"]
            assert git_agent.check_repo_size(test_input)["success"]
            assert git_agent.check_abnormal_commits(test_input)["success"]
            assert git_agent.check_batch_operations(test_input)["success"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
