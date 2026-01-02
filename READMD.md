# Git Agent MCP Server

Mock-based Git operations server built with FastMCP.

## Quick Start

Run the server:

```bash
# Stdio mode (for MCP clients)
python -m git_agent.server

# HTTP mode (for REST API)
python -m git_agent.server --transport http --host 0.0.0.0 --port 9002
```

## Available Tools

All 12 tools return mock data (no actual git operations):

1. **git_status** - Shows working tree status
2. **git_diff_unstaged** - Shows unstaged changes
3. **git_diff_staged** - Shows staged changes
4. **git_diff** - Shows differences between branches/commits
5. **git_commit** - Records changes to repository
6. **git_add** - Adds files to staging area
7. **git_reset** - Unstages all staged changes
8. **git_log** - Shows commit logs with filtering
9. **git_create_branch** - Creates a new branch
10. **git_checkout** - Switches branches
11. **git_show** - Shows contents of a commit
12. **git_branch** - Lists git branches

## Installation

```bash
pip install -e .
```

## Docker

```bash
docker build -t git-agent-mcp .
docker run -p 9002:9002 git-agent-mcp
```