#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Location: ./mcp-servers/python/db-agent-mcp/tests/test_db_agent.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Database Agent Team

Tests for Database Agent MCP Server
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from db_agent.server import DatabaseAgent


@pytest.fixture
def db_agent():
    """Create a database agent instance."""
    return DatabaseAgent()


@pytest.fixture
def sqlite_connection(db_agent):
    """Create a test SQLite connection."""
    result = db_agent.connect_database(
        connection_id="test_db",
        engine="sqlite",
        connection_string=":memory:"
    )
    assert result["success"]
    yield "test_db"
    # Cleanup
    db_agent.disconnect_database("test_db")


class TestDatabaseAgent:
    """Test suite for DatabaseAgent."""

    def test_agent_initialization(self, db_agent):
        """Test agent initialization."""
        assert db_agent is not None
        assert "sqlite" in db_agent.supported_engines
        assert db_agent.supported_engines["sqlite"] is True

    def test_connect_sqlite(self, db_agent):
        """Test SQLite connection."""
        result = db_agent.connect_database(
            connection_id="test_conn",
            engine="sqlite",
            connection_string=":memory:"
        )
        
        assert result["success"] is True
        assert result["connection_id"] == "test_conn"
        assert result["engine"] == "sqlite"
        
        # Cleanup
        db_agent.disconnect_database("test_conn")

    def test_create_table(self, db_agent, sqlite_connection):
        """Test table creation."""
        result = db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            fetch_mode="none"
        )
        
        assert result["success"] is True

    def test_insert_data(self, db_agent, sqlite_connection):
        """Test data insertion."""
        # Create table
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            fetch_mode="none"
        )
        
        # Insert data
        result = db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_users",
            data={"name": "Alice", "age": 30},
            return_ids=True
        )
        
        assert result["success"] is True
        assert result["rows_inserted"] == 1
        assert len(result["inserted_ids"]) == 1

    def test_query_data(self, db_agent, sqlite_connection):
        """Test data querying."""
        # Create and populate table
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_users",
            data={"name": "Bob", "age": 25}
        )
        
        # Query data
        result = db_agent.execute_query(
            connection_id=sqlite_connection,
            query="SELECT * FROM test_users WHERE name = ?",
            params=["Bob"],
            fetch_mode="all"
        )
        
        assert result["success"] is True
        assert len(result["results"]) == 1
        assert result["results"][0]["name"] == "Bob"
        assert result["results"][0]["age"] == 25

    def test_update_data(self, db_agent, sqlite_connection):
        """Test data update."""
        # Setup
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_users",
            data={"name": "Charlie", "age": 35}
        )
        
        # Update
        result = db_agent.update_data(
            connection_id=sqlite_connection,
            table="test_users",
            updates={"age": 40},
            where_clause="name = ?",
            where_params=["Charlie"]
        )
        
        assert result["success"] is True
        assert result["rows_updated"] == 1
        
        # Verify
        query_result = db_agent.execute_query(
            connection_id=sqlite_connection,
            query="SELECT age FROM test_users WHERE name = ?",
            params=["Charlie"],
            fetch_mode="one"
        )
        assert query_result["results"]["age"] == 40

    def test_delete_data(self, db_agent, sqlite_connection):
        """Test data deletion."""
        # Setup
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_users",
            data={"name": "DeleteMe"}
        )
        
        # Delete
        result = db_agent.delete_data(
            connection_id=sqlite_connection,
            table="test_users",
            where_clause="name = ?",
            where_params=["DeleteMe"]
        )
        
        assert result["success"] is True
        assert result["rows_deleted"] == 1
        
        # Verify
        query_result = db_agent.execute_query(
            connection_id=sqlite_connection,
            query="SELECT COUNT(*) as count FROM test_users",
            fetch_mode="one"
        )
        assert query_result["results"]["count"] == 0

    def test_get_schema(self, db_agent, sqlite_connection):
        """Test schema inspection."""
        # Create table
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_schema (id INTEGER PRIMARY KEY, data TEXT)",
            fetch_mode="none"
        )
        
        # Get schema
        result = db_agent.get_schema_info(
            connection_id=sqlite_connection,
            table_name="test_schema"
        )
        
        assert result["success"] is True
        assert result["table"] == "test_schema"
        assert len(result["columns"]) == 2

    def test_table_stats(self, db_agent, sqlite_connection):
        """Test table statistics."""
        # Setup
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_stats (id INTEGER PRIMARY KEY, value INTEGER)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_stats",
            data=[{"value": i} for i in range(10)]
        )
        
        # Get stats
        result = db_agent.get_table_stats(
            connection_id=sqlite_connection,
            table="test_stats"
        )
        
        assert result["success"] is True
        assert result["row_count"] == 10
        assert result["columns"] == 2

    def test_batch_insert(self, db_agent, sqlite_connection):
        """Test batch data insertion."""
        # Create table
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_batch (id INTEGER PRIMARY KEY, name TEXT)",
            fetch_mode="none"
        )
        
        # Batch insert
        data = [
            {"name": f"User{i}"}
            for i in range(100)
        ]
        result = db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_batch",
            data=data,
            return_ids=True
        )
        
        assert result["success"] is True
        assert result["rows_inserted"] == 100
        assert len(result["inserted_ids"]) == 100

    def test_list_connections(self, db_agent):
        """Test listing connections."""
        # Create connections
        db_agent.connect_database("conn1", "sqlite", ":memory:")
        db_agent.connect_database("conn2", "sqlite", ":memory:")
        
        result = db_agent.list_connections()
        
        assert result["success"] is True
        assert len(result["connections"]) >= 2
        
        # Cleanup
        db_agent.disconnect_database("conn1")
        db_agent.disconnect_database("conn2")

    def test_invalid_connection(self, db_agent):
        """Test operations on invalid connection."""
        result = db_agent.execute_query(
            connection_id="nonexistent",
            query="SELECT 1",
            fetch_mode="one"
        )
        
        assert result["success"] is False
        assert "Connection not found" in result["error"]

    def test_sql_injection_protection(self, db_agent, sqlite_connection):
        """Test that parameterized queries prevent SQL injection."""
        # Setup
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_security (id INTEGER PRIMARY KEY, username TEXT)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_security",
            data={"username": "admin"}
        )
        
        # Attempt SQL injection (should be safe with parameters)
        malicious_input = "admin' OR '1'='1"
        result = db_agent.execute_query(
            connection_id=sqlite_connection,
            query="SELECT * FROM test_security WHERE username = ?",
            params=[malicious_input],
            fetch_mode="all"
        )
        
        # Should return no results (injection prevented)
        assert result["success"] is True
        assert len(result["results"]) == 0

    def test_check_query_response_time(self, db_agent, sqlite_connection):
        """Test query response time checking."""
        # Setup
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_response (id INTEGER PRIMARY KEY, data TEXT)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_response",
            data=[{"data": f"test{i}"} for i in range(100)]
        )
        
        # Test response time check
        result = db_agent.check_query_response_time(
            connection_id=sqlite_connection,
            query="SELECT * FROM test_response WHERE id < ?",
            params=[50],
            slow_threshold_ms=1000.0
        )
        
        assert result["success"] is True
        assert "response_time_ms" in result
        assert result["response_time_ms"] >= 0
        assert "is_slow" in result
        assert "status" in result
        assert result["status"] in ["NORMAL", "SLOW"]
        assert "threshold_ms" in result

    def test_slow_query_detection(self, db_agent, sqlite_connection):
        """Test slow query detection with very low threshold."""
        # Setup
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_slow (id INTEGER PRIMARY KEY, data TEXT)",
            fetch_mode="none"
        )
        db_agent.insert_data(
            connection_id=sqlite_connection,
            table="test_slow",
            data=[{"data": f"test{i}"} for i in range(1000)]
        )
        
        # Test with very low threshold
        result = db_agent.check_query_response_time(
            connection_id=sqlite_connection,
            query="SELECT * FROM test_slow",
            slow_threshold_ms=0.1  # Very low threshold to detect as slow
        )
        
        assert result["success"] is True
        assert isinstance(result["is_slow"], bool)
        assert result["response_time_ms"] >= 0

    def test_deadlock_check(self, db_agent, sqlite_connection):
        """Test deadlock detection."""
        result = db_agent.check_deadlock(
            connection_id=sqlite_connection
        )
        
        assert result["success"] is True
        assert "deadlocks_detected" in result
        assert "deadlock_count" in result
        assert "deadlocks" in result
        assert "status" in result
        assert result["status"] in ["NO DEADLOCK", "DEADLOCK DETECTED"]
        
        # SQLite typically has no deadlocks in simple scenarios
        assert result["deadlocks_detected"] is False
        assert result["deadlock_count"] == 0

    def test_deadlock_check_nonexistent_connection(self, db_agent):
        """Test deadlock check with nonexistent connection."""
        result = db_agent.check_deadlock(connection_id="nonexistent")
        assert result["success"] is False
        assert "error" in result

    def test_file_size_check(self, db_agent, sqlite_connection):
        """Test database file size check."""
        result = db_agent.check_file_size(
            connection_id=sqlite_connection
        )
        
        assert result["success"] is True
        assert "size_bytes" in result
        assert "size_mb" in result
        assert "size_gb" in result
        assert result["size_bytes"] >= 0
        assert result["status"] in ["NORMAL", "CRITICAL"]

    def test_file_size_check_with_threshold(self, db_agent, sqlite_connection):
        """Test file size check with threshold."""
        result = db_agent.check_file_size(
            connection_id=sqlite_connection,
            max_size_gb=0.001,  # Very small threshold
            warn_threshold_percent=90.0
        )
        
        assert result["success"] is True
        assert "usage_percent" in result
        assert "warnings" in result
        assert result["max_size_gb"] == 0.001

    def test_file_size_check_nonexistent_connection(self, db_agent):
        """Test file size check with nonexistent connection."""
        result = db_agent.check_file_size(connection_id="nonexistent")
        assert result["success"] is False
        assert "error" in result

    def test_abnormal_data_check(self, db_agent, sqlite_connection):
        """Test abnormal data check with rules."""
        # Setup test data
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_data (id INTEGER PRIMARY KEY, value INTEGER, status TEXT)",
            fetch_mode="none"
        )
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="INSERT INTO test_data (value, status) VALUES (50, 'ok'), (-10, 'ok'), (150, 'bad')",
            fetch_mode="none"
        )
        
        # Check with rules
        result = db_agent.check_abnormal_data(
            connection_id=sqlite_connection,
            query="SELECT * FROM test_data",
            check_rules={
                "value": {"min": 0, "max": 100},
                "status": {"allowed_values": ["ok", "good"]}
            }
        )
        
        assert result["success"] is True
        assert result["total_rows"] == 3
        assert "abnormal_count" in result
        assert "has_abnormal_data" in result
        assert result["status"] in ["NORMAL", "ABNORMAL DATA DETECTED"]

    def test_abnormal_data_check_no_rules(self, db_agent, sqlite_connection):
        """Test abnormal data check without rules."""
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE test_simple (id INTEGER PRIMARY KEY, name TEXT)",
            fetch_mode="none"
        )
        
        result = db_agent.check_abnormal_data(
            connection_id=sqlite_connection,
            query="SELECT * FROM test_simple"
        )
        
        assert result["success"] is True
        assert result["has_abnormal_data"] is False

    def test_batch_data_check(self, db_agent, sqlite_connection):
        """Test batch data check."""
        # Setup batch table
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE batch_test (id INTEGER PRIMARY KEY, batch_id TEXT, value INTEGER)",
            fetch_mode="none"
        )
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="INSERT INTO batch_test (batch_id, value) VALUES ('B001', 50), ('B001', 150), ('B002', 30)",
            fetch_mode="none"
        )
        
        # Check batch B001
        result = db_agent.check_batch_data(
            connection_id=sqlite_connection,
            table_name="batch_test",
            batch_column="batch_id",
            batch_id="B001",
            check_rules={"value": {"min": 0, "max": 100}}
        )
        
        assert result["success"] is True
        assert result["batch_id"] == "B001"
        assert result["total_rows"] == 2
        assert "has_abnormal_data" in result
        assert result["status"] in ["NORMAL", "ABNORMAL DATA DETECTED"]

    def test_batch_data_check_no_rules(self, db_agent, sqlite_connection):
        """Test batch data check without rules."""
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="CREATE TABLE batch_simple (id INTEGER PRIMARY KEY, batch_id TEXT)",
            fetch_mode="none"
        )
        db_agent.execute_query(
            connection_id=sqlite_connection,
            query="INSERT INTO batch_simple (batch_id) VALUES ('B001')",
            fetch_mode="none"
        )
        
        result = db_agent.check_batch_data(
            connection_id=sqlite_connection,
            table_name="batch_simple",
            batch_column="batch_id",
            batch_id="B001"
        )
        
        assert result["success"] is True
        assert result["has_abnormal_data"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
