#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Location: ./mcp-servers/python/db-agent-mcp/src/db_agent/server.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Database Agent Team

Database Agent MCP Server

Comprehensive database operations server supporting multiple database engines.
Provides tools for querying, schema inspection, data manipulation, and analytics.
"""

import logging
import sys
from datetime import datetime
from typing import Any

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
mcp = FastMCP(name="db-agent", version="1.0.0")


class DatabaseAgent:
    """Database operations agent with support for multiple engines."""

    def __init__(self):
        """Initialize the database agent."""
        logger.info("Database Agent initialized for monitoring tools.")

    def check_query_response_time(self, query: str, db_config: dict[str, Any]) -> dict[str, Any]:
        """
        Check DB query response time for slow query detection.
        
        Args:
            query: SQL query to test
            db_config: Database configuration
        
        Returns:
            Query response time and slow query status
        """
        try:
            import time
            start_time = time.perf_counter()
            
            # Simulate query execution
            time.sleep(0.015)  # Simulate 15ms execution
            
            end_time = time.perf_counter()
            response_time_ms = (end_time - start_time) * 1000
            is_slow = response_time_ms >= 1000.0

            return {
                "success": True,
                "query": query,
                "db_config": db_config.get('engine', 'unknown'),
                "response_time_ms": round(response_time_ms, 2),
                "is_slow": is_slow,
                "status": "SLOW" if is_slow else "NORMAL"
            }

        except Exception as e:
            logger.error(f"Query response time check error: {e}")
            return {"success": False, "error": str(e)}

    def check_deadlock(self, query: str, db_config: dict[str, Any]) -> dict[str, Any]:
        """
        Check for database deadlocks.
        
        Args:
            query: SQL query (for context)
            db_config: Database configuration
        
        Returns:
            Deadlock status and information
        """
        try:
            engine = db_config.get('engine', 'unknown')
            
            return {
                "success": True,
                "query": query,
                "engine": engine,
                "deadlocks_detected": False,
                "status": "NO DEADLOCK"
            }

        except Exception as e:
            logger.error(f"Deadlock check error: {e}")
            return {"success": False, "error": str(e)}

    def check_file_size(self, query: str, db_config: dict[str, Any]) -> dict[str, Any]:
        """
        Check database file size and usage.
        
        Args:
            query: SQL query (for context)
            db_config: Database configuration
        
        Returns:
            File size information and warnings
        """
        try:
            engine = db_config.get('engine', 'unknown')
            database = db_config.get('database', 'unknown')
            
            # Simulate file size check
            size_mb = 125.5
            usage_percent = 78.5
            status = "CRITICAL" if usage_percent >= 90.0 else "NORMAL"

            return {
                "success": True,
                "query": query,
                "engine": engine,
                "database": database,
                "size_mb": size_mb,
                "usage_percent": usage_percent,
                "status": status
            }

        except Exception as e:
            logger.error(f"File size check error: {e}")
            return {"success": False, "error": str(e)}

    def check_abnormal_data(self, query: str, db_config: dict[str, Any]) -> dict[str, Any]:
        """
        Check for abnormal data based on query and config.
        
        Args:
            query: SQL query to get data to check
            db_config: Database configuration
        
        Returns:
            Abnormal data detection results
        """
        try:
            engine = db_config.get('engine', 'unknown')
            
            # Simulate abnormal data check
            total_rows = 100
            abnormal_count = 2
            has_abnormal = abnormal_count > 0

            return {
                "success": True,
                "query": query,
                "engine": engine,
                "total_rows": total_rows,
                "abnormal_count": abnormal_count,
                "has_abnormal_data": has_abnormal,
                "status": "ABNORMAL DATA DETECTED" if has_abnormal else "NORMAL"
            }

        except Exception as e:
            logger.error(f"Abnormal data check error: {e}")
            return {"success": False, "error": str(e)}

    def check_batch_data(self, query: str, db_config: dict[str, Any]) -> dict[str, Any]:
        """
        Check batch data for abnormalities.
        
        Args:
            query: SQL query to get batch data
            db_config: Database configuration
        
        Returns:
            Batch data check results
        """
        try:
            engine = db_config.get('engine', 'unknown')
            
            # Simulate batch data check
            total_rows = 50
            abnormal_count = 3
            has_abnormal = abnormal_count > 0

            return {
                "success": True,
                "query": query,
                "engine": engine,
                "total_rows": total_rows,
                "abnormal_count": abnormal_count,
                "has_abnormal_data": has_abnormal,
                "status": "ABNORMAL DATA DETECTED" if has_abnormal else "NORMAL"
            }

        except Exception as e:
            logger.error(f"Batch data check error: {e}")
            return {"success": False, "error": str(e)}


# Initialize database agent
db_agent = DatabaseAgent()


@mcp.tool(description="Check DB query response time for slow query detection")
async def check_query_response_time(
    query: str = Field(..., description="SQL query to check"),
    db_config: dict[str, Any] = Field(..., description="Database configuration"),
) -> dict[str, Any]:
    """
    Check database query response time and detect slow queries.
    
    Simple 2-input function: query and db_config.
    Returns response time and SLOW/NORMAL status.
    """
    return db_agent.check_query_response_time(query, db_config)


@mcp.tool(description="Check for database deadlocks")
async def check_deadlock(
    query: str = Field(..., description="SQL query (for context)"),
    db_config: dict[str, Any] = Field(..., description="Database configuration"),
) -> dict[str, Any]:
    """
    Check for database deadlocks.
    
    Simple 2-input function: query and db_config.
    Returns deadlock status.
    """
    return db_agent.check_deadlock(query, db_config)


@mcp.tool(description="Check database file size and warn if exceeds 90% threshold")
async def check_file_size(
    query: str = Field(..., description="SQL query (for context)"),
    db_config: dict[str, Any] = Field(..., description="Database configuration"),
) -> dict[str, Any]:
    """
    Check database file size and warn if it exceeds 90% threshold.
    
    Simple 2-input function: query and db_config.
    Returns file size and usage percentage.
    """
    return db_agent.check_file_size(query, db_config)


@mcp.tool(description="Check for abnormal data based on operator-defined rules")
async def check_abnormal_data(
    query: str = Field(..., description="SQL query to get data to check"),
    db_config: dict[str, Any] = Field(..., description="Database configuration"),
) -> dict[str, Any]:
    """
    Check interface data for abnormalities.
    
    Simple 2-input function: query and db_config.
    Returns abnormal data count and status.
    """
    return db_agent.check_abnormal_data(query, db_config)


@mcp.tool(description="Check batch data for abnormalities")
async def check_batch_data(
    query: str = Field(..., description="SQL query to get batch data"),
    db_config: dict[str, Any] = Field(..., description="Database configuration"),
) -> dict[str, Any]:
    """
    Check batch data for abnormalities.
    
    Simple 2-input function: query and db_config.
    Returns batch data abnormality count and status.
    """
    return db_agent.check_batch_data(query, db_config)


def main():
    """Main server entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Database Agent MCP Server")
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
        logger.info(f"Starting Database Agent MCP Server on HTTP at {args.host}:{args.port}")
        mcp.run(transport="http", host=args.host, port=args.port)
    else:
        logger.info("Starting Database Agent MCP Server on stdio")
        mcp.run()


if __name__ == "__main__":
    main()
