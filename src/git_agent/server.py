#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Location: ./src/git_agent/server.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Git Agent Team

Git Agent MCP Server

Comprehensive git operations server providing 12 git tools via MCP.
All operations return mock data without connecting to actual git repositories.
"""

import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Optional
import random

from fastmcp import FastMCP
from pydantic import Field

# Configure logging to stderr to avoid MCP protocol interference
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)

# Create FastMCP server instance
mcp = FastMCP(name="git-agent", version="1.0.0")


# Mock data pools for realistic git responses
MOCK_STATUS_OUTPUTS = [
    """On branch main
Your branch is up to date with 'origin/main'.

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   src/main.py
	modified:   README.md

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	new_feature.py

no changes added to commit (use "git add" and/or "git commit -a")""",
    """On branch feature/new-auth
Your branch is ahead of 'origin/feature/new-auth' by 2 commits.
  (use "git push" to publish your local commits)

Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
	new file:   auth/oauth.py
	modified:   auth/config.py

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   tests/test_auth.py""",
    """On branch main
nothing to commit, working tree clean"""
]

MOCK_DIFF_UNSTAGED = """diff --git a/src/main.py b/src/main.py
index 1a2b3c4..5d6e7f8 100644
--- a/src/main.py
+++ b/src/main.py
@@ -10,7 +10,7 @@ def main():
     parser = argparse.ArgumentParser()
-    parser.add_argument('--port', type=int, default=8080)
+    parser.add_argument('--port', type=int, default=9000)
     args = parser.parse_args()
     
-    print(f"Starting server on port {args.port}")
+    logger.info(f"Starting server on port {args.port}")
"""

MOCK_DIFF_STAGED = """diff --git a/auth/oauth.py b/auth/oauth.py
new file mode 100644
index 0000000..a1b2c3d
--- /dev/null
+++ b/auth/oauth.py
@@ -0,0 +1,15 @@
+import hashlib
+
+def authenticate_user(token: str) -> bool:
+    if not token:
+        return False
+    return validate_token(token)
+
+def validate_token(token: str) -> bool:
+    return hashlib.sha256(token.encode()).hexdigest()
"""

MOCK_COMMITS = [
    {
        "hash": "a1b2c3d4e5f6",
        "author": "John Doe",
        "email": "john@example.com",
        "date": (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
        "message": "feat: add OAuth authentication support"
    },
    {
        "hash": "f6e5d4c3b2a1",
        "author": "Jane Smith",
        "email": "jane@example.com",
        "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "message": "fix: resolve database connection timeout\n\nAdded retry logic with exponential backoff"
    },
    {
        "hash": "1234567890ab",
        "author": "Bob Johnson",
        "email": "bob@example.com",
        "date": (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"),
        "message": "refactor: optimize query performance"
    },
    {
        "hash": "abcdef123456",
        "author": "Alice Williams",
        "email": "alice@example.com",
        "date": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S"),
        "message": "docs: update API documentation"
    },
    {
        "hash": "9876543210fe",
        "author": "Charlie Brown",
        "email": "charlie@example.com",
        "date": (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S"),
        "message": "chore: update dependencies to latest versions"
    }
]

MOCK_BRANCHES = {
    "local": ["main", "develop", "feature/new-auth", "feature/api-v2", "hotfix/critical-bug"],
    "remote": ["origin/main", "origin/develop", "origin/feature/new-auth", "upstream/main"],
    "all": ["main", "develop", "feature/new-auth", "feature/api-v2", "hotfix/critical-bug",
            "remotes/origin/main", "remotes/origin/develop", "remotes/upstream/main"]
}

MOCK_COMMIT_SHOW = """commit {commit_hash}
Author: {author} <{email}>
Date:   {date}

    {message}

diff --git a/auth/oauth.py b/auth/oauth.py
new file mode 100644
index 0000000..a1b2c3d
--- /dev/null
+++ b/auth/oauth.py
@@ -0,0 +1,15 @@
+import hashlib
+
+def authenticate_user(token: str) -> bool:
+    if not token:
+        return False
+    return validate_token(token)
+
+def validate_token(token: str) -> bool:
+    return hashlib.sha256(token.encode()).hexdigest()
"""


class GitAgent:
    """Git operations agent providing 12 git tools with mock responses."""

    def __init__(self):
        """Initialize the git agent."""
        logger.info("Git Agent initialized with 12 git tools.")

    def git_status(self, repo_path: str) -> dict[str, Any]:
        """Shows the working tree status."""
        try:
            status_output = random.choice(MOCK_STATUS_OUTPUTS)
            return {
                "success": True,
                "repo_path": repo_path,
                "status": status_output,
                "message": "Git status retrieved successfully"
            }
        except Exception as e:
            logger.error(f"Git status error: {e}")
            return {"success": False, "error": str(e)}

    def git_diff_unstaged(self, repo_path: str, context_lines: int = 3) -> dict[str, Any]:
        """Shows changes in working directory not yet staged."""
        try:
            has_changes = random.choice([True, False])
            
            if has_changes:
                return {
                    "success": True,
                    "repo_path": repo_path,
                    "context_lines": context_lines,
                    "diff": MOCK_DIFF_UNSTAGED,
                    "has_changes": True,
                    "message": "Unstaged changes found"
                }
            else:
                return {
                    "success": True,
                    "repo_path": repo_path,
                    "context_lines": context_lines,
                    "diff": "",
                    "has_changes": False,
                    "message": "No unstaged changes"
                }
        except Exception as e:
            logger.error(f"Git diff unstaged error: {e}")
            return {"success": False, "error": str(e)}

    def git_diff_staged(self, repo_path: str, context_lines: int = 3) -> dict[str, Any]:
        """Shows changes that are staged for commit."""
        try:
            has_staged = random.choice([True, False])
            
            if has_staged:
                return {
                    "success": True,
                    "repo_path": repo_path,
                    "context_lines": context_lines,
                    "diff": MOCK_DIFF_STAGED,
                    "has_changes": True,
                    "message": "Staged changes found"
                }
            else:
                return {
                    "success": True,
                    "repo_path": repo_path,
                    "context_lines": context_lines,
                    "diff": "",
                    "has_changes": False,
                    "message": "No staged changes"
                }
        except Exception as e:
            logger.error(f"Git diff staged error: {e}")
            return {"success": False, "error": str(e)}

    def git_diff(self, repo_path: str, target: str, context_lines: int = 3) -> dict[str, Any]:
        """Shows differences between branches or commits."""
        try:
            diff_output = f"""diff --git a/src/api.py b/src/api.py
index abc123..def456 100644
--- a/src/api.py
+++ b/src/api.py
@@ -15,6 +15,8 @@ class API:
     def __init__(self):
         self.router = Router()
+        self.auth = AuthMiddleware()
+        self.cache = CacheLayer()
     
     def setup(self):
"""
            
            return {
                "success": True,
                "repo_path": repo_path,
                "target": target,
                "context_lines": context_lines,
                "diff": diff_output,
                "message": f"Diff between current branch and {target}"
            }
        except Exception as e:
            logger.error(f"Git diff error: {e}")
            return {"success": False, "error": str(e)}

    def git_commit(self, repo_path: str, message: str) -> dict[str, Any]:
        """Records changes to the repository."""
        try:
            commit_hash = ''.join(random.choices('0123456789abcdef', k=12))
            files_changed = random.randint(1, 8)
            insertions = random.randint(10, 200)
            deletions = random.randint(5, 50)
            
            return {
                "success": True,
                "repo_path": repo_path,
                "commit_hash": commit_hash,
                "message": message,
                "files_changed": files_changed,
                "insertions": insertions,
                "deletions": deletions,
                "summary": f"[{commit_hash[:7]}] {message}\n {files_changed} files changed, {insertions} insertions(+), {deletions} deletions(-)"
            }
        except Exception as e:
            logger.error(f"Git commit error: {e}")
            return {"success": False, "error": str(e)}

    def git_add(self, repo_path: str, files: list[str]) -> dict[str, Any]:
        """Adds file contents to the staging area."""
        try:
            return {
                "success": True,
                "repo_path": repo_path,
                "files_staged": files,
                "count": len(files),
                "message": f"Added {len(files)} file(s) to staging area"
            }
        except Exception as e:
            logger.error(f"Git add error: {e}")
            return {"success": False, "error": str(e)}

    def git_reset(self, repo_path: str) -> dict[str, Any]:
        """Unstages all staged changes."""
        try:
            files_unstaged = random.randint(1, 5)
            
            return {
                "success": True,
                "repo_path": repo_path,
                "files_unstaged": files_unstaged,
                "message": f"Unstaged {files_unstaged} file(s)"
            }
        except Exception as e:
            logger.error(f"Git reset error: {e}")
            return {"success": False, "error": str(e)}

    def git_log(self, repo_path: str, max_count: int = 10, 
                start_timestamp: Optional[str] = None, 
                end_timestamp: Optional[str] = None) -> dict[str, Any]:
        """Shows the commit logs with optional date filtering."""
        try:
            commits = MOCK_COMMITS[:min(max_count, len(MOCK_COMMITS))]
            
            return {
                "success": True,
                "repo_path": repo_path,
                "max_count": max_count,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "commit_count": len(commits),
                "commits": commits,
                "message": f"Retrieved {len(commits)} commit(s)"
            }
        except Exception as e:
            logger.error(f"Git log error: {e}")
            return {"success": False, "error": str(e)}

    def git_create_branch(self, repo_path: str, branch_name: str, 
                         base_branch: Optional[str] = None) -> dict[str, Any]:
        """Creates a new branch."""
        try:
            base = base_branch or "main"
            
            return {
                "success": True,
                "repo_path": repo_path,
                "branch_name": branch_name,
                "base_branch": base,
                "message": f"Created branch '{branch_name}' from '{base}'"
            }
        except Exception as e:
            logger.error(f"Git create branch error: {e}")
            return {"success": False, "error": str(e)}

    def git_checkout(self, repo_path: str, branch_name: str) -> dict[str, Any]:
        """Switches branches."""
        try:
            return {
                "success": True,
                "repo_path": repo_path,
                "branch_name": branch_name,
                "previous_branch": "main",
                "message": f"Switched to branch '{branch_name}'"
            }
        except Exception as e:
            logger.error(f"Git checkout error: {e}")
            return {"success": False, "error": str(e)}

    def git_show(self, repo_path: str, revision: str) -> dict[str, Any]:
        """Shows the contents of a commit."""
        try:
            commit = next((c for c in MOCK_COMMITS if c["hash"].startswith(revision)), MOCK_COMMITS[0])
            
            show_output = MOCK_COMMIT_SHOW.format(
                commit_hash=commit["hash"],
                author=commit["author"],
                email=commit["email"],
                date=commit["date"],
                message=commit["message"]
            )
            
            return {
                "success": True,
                "repo_path": repo_path,
                "revision": revision,
                "commit_hash": commit["hash"],
                "author": f"{commit['author']} <{commit['email']}>",
                "date": commit["date"],
                "message": commit["message"],
                "content": show_output
            }
        except Exception as e:
            logger.error(f"Git show error: {e}")
            return {"success": False, "error": str(e)}

    def git_branch(self, repo_path: str, branch_type: str = "local",
                   contains: Optional[str] = None, 
                   not_contains: Optional[str] = None) -> dict[str, Any]:
        """List Git branches."""
        try:
            branches = MOCK_BRANCHES.get(branch_type, MOCK_BRANCHES["local"])
            
            if contains:
                branches = branches[:len(branches)//2]
            if not_contains:
                branches = branches[len(branches)//2:]
            
            return {
                "success": True,
                "repo_path": repo_path,
                "branch_type": branch_type,
                "contains": contains,
                "not_contains": not_contains,
                "branch_count": len(branches),
                "branches": branches,
                "current_branch": "main",
                "message": f"Retrieved {len(branches)} {branch_type} branch(es)"
            }
        except Exception as e:
            logger.error(f"Git branch error: {e}")
            return {"success": False, "error": str(e)}


# Initialize git agent
git_agent = GitAgent()


# ============================================================================
# MCP Tool Registrations
# ============================================================================

@mcp.tool(description="Shows the working tree status")
async def git_status(
    repo_path: str = Field(..., description="Path to Git repository"),
):
    """Get git status for repository."""
    return git_agent.git_status(repo_path)


@mcp.tool(description="Shows changes in working directory not yet staged")
async def git_diff_unstaged(
    repo_path: str = Field(..., description="Path to Git repository"),
    context_lines: int = Field(3, description="Number of context lines to show"),
):
    """Get unstaged changes diff."""
    return git_agent.git_diff_unstaged(repo_path, context_lines)


@mcp.tool(description="Shows changes that are staged for commit")
async def git_diff_staged(
    repo_path: str = Field(..., description="Path to Git repository"),
    context_lines: int = Field(3, description="Number of context lines to show"),
):
    """Get staged changes diff."""
    return git_agent.git_diff_staged(repo_path, context_lines)


@mcp.tool(description="Shows differences between branches or commits")
async def git_diff(
    repo_path: str = Field(..., description="Path to Git repository"),
    target: str = Field(..., description="Target branch or commit to compare with"),
    context_lines: int = Field(3, description="Number of context lines to show"),
):
    """Get diff between current state and target."""
    return git_agent.git_diff(repo_path, target, context_lines)


@mcp.tool(description="Records changes to the repository")
async def git_commit(
    repo_path: str = Field(..., description="Path to Git repository"),
    message: str = Field(..., description="Commit message"),
):
    """Create a git commit."""
    return git_agent.git_commit(repo_path, message)


@mcp.tool(description="Adds file contents to the staging area")
async def git_add(
    repo_path: str = Field(..., description="Path to Git repository"),
    files: list[str] = Field(..., description="Array of file paths to stage"),
):
    """Stage files for commit."""
    return git_agent.git_add(repo_path, files)


@mcp.tool(description="Unstages all staged changes")
async def git_reset(
    repo_path: str = Field(..., description="Path to Git repository"),
):
    """Reset staged changes."""
    return git_agent.git_reset(repo_path)


@mcp.tool(description="Shows the commit logs with optional date filtering")
async def git_log(
    repo_path: str = Field(..., description="Path to Git repository"),
    max_count: int = Field(10, description="Maximum number of commits to show"),
    start_timestamp: Optional[str] = Field(None, description="Start timestamp for filtering commits"),
    end_timestamp: Optional[str] = Field(None, description="End timestamp for filtering commits"),
):
    """Get commit history."""
    return git_agent.git_log(repo_path, max_count, start_timestamp, end_timestamp)


@mcp.tool(description="Creates a new branch")
async def git_create_branch(
    repo_path: str = Field(..., description="Path to Git repository"),
    branch_name: str = Field(..., description="Name of the new branch"),
    base_branch: Optional[str] = Field(None, description="Base branch to create from"),
):
    """Create a new git branch."""
    return git_agent.git_create_branch(repo_path, branch_name, base_branch)


@mcp.tool(description="Switches branches")
async def git_checkout(
    repo_path: str = Field(..., description="Path to Git repository"),
    branch_name: str = Field(..., description="Name of branch to checkout"),
):
    """Checkout a git branch."""
    return git_agent.git_checkout(repo_path, branch_name)


@mcp.tool(description="Shows the contents of a commit")
async def git_show(
    repo_path: str = Field(..., description="Path to Git repository"),
    revision: str = Field(..., description="The revision (commit hash, branch name, tag) to show"),
):
    """Show commit contents."""
    return git_agent.git_show(repo_path, revision)


@mcp.tool(description="List Git branches")
async def git_branch(
    repo_path: str = Field(..., description="Path to the Git repository"),
    branch_type: str = Field("local", description="Type of branches: 'local', 'remote', or 'all'"),
    contains: Optional[str] = Field(None, description="The commit sha that branch should contain"),
    not_contains: Optional[str] = Field(None, description="The commit sha that branch should NOT contain"),
):
    """List git branches."""
    return git_agent.git_branch(repo_path, branch_type, contains, not_contains)


def main():
    """Main server entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Git Agent MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="Transport mode (stdio or http)",
    )
    parser.add_argument("--host", default="0.0.0.0", help="HTTP host")
    parser.add_argument("--port", type=int, default=9002, help="HTTP port")

    args = parser.parse_args()

    if args.transport == "http":
        logger.info(f"Starting Git Agent MCP Server on HTTP at {args.host}:{args.port}")
        mcp.run(transport="http", host=args.host, port=args.port)
    else:
        logger.info("Starting Git Agent MCP Server on stdio")
        mcp.run()


if __name__ == "__main__":
    main()
