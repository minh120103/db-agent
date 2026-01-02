# Git Agent MCP Server - AI Agent Instructions

## Project Overview
Git repository monitoring MCP (Model Context Protocol) server built with FastMCP. Provides 5 mock git monitoring tools via stdio/HTTP, simulating real-world git operations without actual git repository connections.

**Key Architecture**: This is a **mock/simulation server** - all git operations return realistic mock data from predefined pools (e.g., `MOCK_MERGE_CONFLICTS`, `MOCK_SLOW_COMMITS`). No actual git repositories are accessed.

## Core Components

### Server Structure
- **Entry point**: [src/git_agent/server.py](src/git_agent/server.py) - FastMCP server with 5 async tools
- **Agent class**: `GitAgent` - Stateless mock data generator (lines 196-498)
- **FastMCP instance**: `mcp = FastMCP(name="git-agent", version="1.0.0")` (line 30)
- **Transport modes**: stdio (default) or HTTP (with `--transport http`)

### The 5 Git Monitoring Tools
All tools follow the pattern: single `input: str` parameter → dict response with mock data

1. **check_commit_status** - Detects slow commits
   - Keywords "slow/performance/large/optimize" → returns mock slow commit
   - Otherwise → random 50-500ms normal commit
   
2. **check_merge_conflict** - Detects merge conflicts
   - "active/detect/current/files" → active conflict with file details
   - "log/history/latest/past" → historical conflict from logs
   - Otherwise → random true/false conflict status

3. **check_repo_size** - Monitors repository growth
   - "prod/staging/dev" → selects specific mock repository
   - Returns size, usage %, growth rate, weeks until full
   
4. **check_abnormal_commits** - Finds commit anomalies
   - Random selection from `MOCK_ABNORMAL_COMMITS`
   - Returns large files, suspicious patterns, poor messages, etc.
   
5. **check_batch_operations** - Checks batch git operation issues
   - Random selection from `MOCK_BATCH_OPERATIONS`
   - Returns cherry-pick failures, rebase conflicts, permission errors

## Development Workflows

### Running the Server
```bash
# Stdio mode (default MCP transport)
python -m git_agent.server

# HTTP mode for REST API access
python -m git_agent.server --transport http --host 0.0.0.0 --port 9002

# Using Makefile shortcuts
make dev-git-agent        # stdio mode
make dev-git-http         # HTTP mode
```

### Testing
```bash
# Run pytest suite (tests/test_db_agent.py)
make test

# Manual testing with examples
python examples/test_all_tools.py
```

### Docker Deployment
```bash
docker build -t git-agent-mcp .
docker run -p 9002:9002 git-agent-mcp
```

## Code Patterns & Conventions

### Mock Data Pattern
All mock data pools are module-level constants (lines 33-165):
```python
MOCK_MERGE_CONFLICTS = [...]  # Pre-defined realistic scenarios
MOCK_SLOW_COMMITS = [...]      # Example slow commits with recommendations
```

Tools use `random.choice()` or `random.sample()` to simulate variability.

### Response Structure
All tools return consistent dict structure:
```python
{
    "success": bool,
    "input": str,           # Echo user input
    "status": str,          # NORMAL/WARNING/CRITICAL/etc
    # ... tool-specific fields
    "recommendations": [...]  # Optional actionable advice
}
```

### Logging Convention
- Log to `sys.stderr` only (line 22-27) to avoid interfering with MCP stdio protocol
- Use `logger.error()` in exception handlers, `logger.info()` for lifecycle events

### Error Handling Pattern
```python
try:
    # Tool logic with mock data
    return {"success": True, ...}
except Exception as e:
    logger.error(f"Tool name error: {e}")
    return {"success": False, "error": str(e)}
```

## MCP Integration

### Tool Registration
FastMCP uses decorator pattern:
```python
@mcp.tool(description="Check git commit status and detect slow commits")
async def check_commit_status(
    input: str = Field(..., description="user input to check"),
):
    return git_agent.check_commit_status(input)  # Sync method wrapped in async
```

### Transport Modes
- **stdio**: Default MCP transport for VS Code/Claude Desktop integration
- **HTTP**: RESTful API at `/mcp/` endpoint with FastAPI docs at `/docs`

## Project-Specific Notes

### Why Mock Data?
This server demonstrates MCP tool patterns without git repository setup complexity. Real implementations would replace `GitAgent` methods with actual GitPython or subprocess calls.

### Test Structure
- Unit tests use pytest fixtures for agent setup ([tests/test_db_agent.py](tests/test_db_agent.py))
- Example script shows all 5 tools in action ([examples/test_all_tools.py](examples/test_all_tools.py))

### Dependencies
Minimal by design:
- `fastmcp==2.11.3` - MCP server framework
- `pydantic>=2.5.0` - Input validation
- No actual git libraries (GitPython, etc.)

### File Locations
- `tmp/` - Gitignored workspace for experiments (contains git MCP server example)
- `__pycache__/` - Python bytecode (gitignored)
- Entry script: `git-agent` command installs via `[project.scripts]` in pyproject.toml

## Common Tasks

**Add new monitoring tool**: 
1. Add mock data constant at module level
2. Create method in `GitAgent` class
3. Register with `@mcp.tool()` decorator wrapper

**Modify mock responses**: 
Edit constants like `MOCK_MERGE_CONFLICTS` directly - no git operations needed

**Change HTTP port**: 
Use `--port` flag or `HTTP_PORT` env var with Makefile

**Debug tool responses**: 
Check stderr output (where logger writes) - stdout is reserved for MCP protocol
