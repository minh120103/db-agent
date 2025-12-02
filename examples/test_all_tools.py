#!/usr/bin/env python3
"""Complete example testing all 5 DB Agent tools with user inputs."""

import sys
import os

# Add src to path to import db_agent
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from db_agent.server import DatabaseAgent


def test_db_agent(user_query: str):
    """
    Test all 5 DB Agent tools with user query.
    
    Args:
        user_query: SQL query from user
    
    Returns:
        dict: Results from all 5 tools
    """
    print("=" * 70)
    print("DB AGENT - TESTING WITH USER INPUTS")
    print("=" * 70)
    print(f"\nUser Query: {user_query}")
    
    # Initialize agent
    agent = DatabaseAgent()
    results = {}

    # ================================================================
    # TOOL 1: Check Query Response Time
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 1: CHECK QUERY RESPONSE TIME")
    print("=" * 70)
    
    result = agent.check_query_response_time(user_query)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Query: {result['query'][:50]}...")
    print(f"✓ Response time: {result['response_time_ms']} ms")
    print(f"✓ Is slow: {result['is_slow']}")
    print(f"✓ Status: {result['status']}")
    results['query_response_time'] = result

    # ================================================================
    # TOOL 2: Check Deadlock
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 2: CHECK DEADLOCK")
    print("=" * 70)
    
    result = agent.check_deadlock(user_query)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Query: {result['query'][:50]}...")
    print(f"✓ Deadlocks detected: {result['deadlocks_detected']}")
    print(f"✓ Status: {result['status']}")
    results['deadlock'] = result

    # ================================================================
    # TOOL 3: Check File Size
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 3: CHECK FILE SIZE")
    print("=" * 70)
    
    result = agent.check_file_size(user_query)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Query: {result['query'][:50]}...")
    print(f"✓ Size: {result['size_mb']} MB")
    print(f"✓ Usage: {result['usage_percent']}%")
    print(f"✓ Status: {result['status']}")
    results['file_size'] = result

    # ================================================================
    # TOOL 4: Check Abnormal Data
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 4: CHECK ABNORMAL DATA")
    print("=" * 70)
    
    result = agent.check_abnormal_data(user_query)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Query: {result['query'][:50]}...")
    print(f"✓ Total rows: {result['total_rows']}")
    print(f"✓ Abnormal count: {result['abnormal_count']}")
    print(f"✓ Has abnormal data: {result['has_abnormal_data']}")
    print(f"✓ Status: {result['status']}")
    results['abnormal_data'] = result

    # ================================================================
    # TOOL 5: Check Batch Data
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 5: CHECK BATCH DATA")
    print("=" * 70)
    
    result = agent.check_batch_data(user_query)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Query: {result['query'][:50]}...")
    print(f"✓ Total rows: {result['total_rows']}")
    print(f"✓ Abnormal count: {result['abnormal_count']}")
    print(f"✓ Has abnormal data: {result['has_abnormal_data']}")
    print(f"✓ Status: {result['status']}")
    results['batch_data'] = result

    # ================================================================
    # Summary
    # ================================================================
    print("\n" + "=" * 70)
    print("SUMMARY - ALL TESTS COMPLETED")
    print("=" * 70)
    print("\n✓ All 5 tools executed successfully")
    print(f"✓ Query tested: {user_query[:50]}...")
    print("\nResults:")
    print(f"  1. Query Response Time: {results['query_response_time']['status']}")
    print(f"  2. Deadlock Check: {results['deadlock']['status']}")
    print(f"  3. File Size: {results['file_size']['status']}")
    print(f"  4. Abnormal Data: {results['abnormal_data']['status']}")
    print(f"  5. Batch Data: {results['batch_data']['status']}")
    print("\n" + "=" * 70)
    
    return results


def main():
    """Example usage with user inputs."""
    # INPUT: User Query
    user_query = "check for deadlock"
    
    # Run all 5 tests
    results = test_db_agent(user_query)
    
    # Return results
    return results


if __name__ == "__main__":
    results = main()
    print(f"\n✓ All tests returned success: {all(r['success'] for r in results.values())}")
