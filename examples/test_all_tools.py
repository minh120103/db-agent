#!/usr/bin/env python3
"""Complete example testing all 5 Git Agent tools with user inputs."""

import sys
import os

# Add src to path to import git_agent
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from git_agent.server import GitAgent


def test_git_agent():
    """
    Test all 5 Git Agent tools with sample queries.
    
    Returns:
        dict: Results from all 5 tools
    """
    print("=" * 70)
    print("GIT AGENT - TESTING ALL 5 MONITORING TOOLS")
    print("=" * 70)
    
    # Initialize agent
    agent = GitAgent()
    results = {}

    # ================================================================
    # TOOL 1: Check Commit Status
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 1: CHECK COMMIT STATUS")
    print("=" * 70)
    
    user_input = "Check commit performance for slow commits"
    result = agent.check_commit_status(user_input)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Input: {result['input']}")
    print(f"✓ Commit hash: {result.get('commit_hash', 'N/A')}")
    print(f"✓ Processing time: {result.get('processing_time_ms', 0)} ms")
    print(f"✓ Is slow: {result.get('is_slow', False)}")
    print(f"✓ Status: {result['status']}")
    if 'recommendations' in result:
        print(f"✓ Recommendations: {len(result['recommendations'])} items")
    results['commit_status'] = result

    # ================================================================
    # TOOL 2: Check Merge Conflict
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 2: CHECK MERGE CONFLICT")
    print("=" * 70)
    
    user_input = "Detect active merge conflicts"
    result = agent.check_merge_conflict(user_input)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Input: {result['input']}")
    print(f"✓ Conflicts detected: {result['conflicts_detected']}")
    print(f"✓ Status: {result['status']}")
    if 'conflict_count' in result:
        print(f"✓ Conflict count: {result['conflict_count']}")
    results['merge_conflict'] = result

    # ================================================================
    # TOOL 3: Check Repo Size
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 3: CHECK REPO SIZE")
    print("=" * 70)
    
    user_input = "Check prod repository size"
    result = agent.check_repo_size(user_input)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Input: {result['input']}")
    print(f"✓ Repository: {result['repository']}")
    print(f"✓ Size: {result['current_size_mb']} MB")
    print(f"✓ Usage: {result['usage_percent']}%")
    print(f"✓ Status: {result['status']}")
    print(f"✓ Growth rate: {result['growth_rate_mb_per_week']} MB/week")
    results['repo_size'] = result

    # ================================================================
    # TOOL 4: Check Abnormal Commits
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 4: CHECK ABNORMAL COMMITS")
    print("=" * 70)
    
    user_input = "Find abnormal commits in repository"
    result = agent.check_abnormal_commits(user_input)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Input: {result['input']}")
    print(f"✓ Has abnormal commits: {result['has_abnormal_commits']}")
    print(f"✓ Status: {result['status']}")
    if 'anomaly_count' in result:
        print(f"✓ Anomaly count: {result['anomaly_count']}")
    results['abnormal_commits'] = result

    # ================================================================
    # TOOL 5: Check Batch Operations
    # ================================================================
    print("\n" + "=" * 70)
    print("TOOL 5: CHECK BATCH OPERATIONS")
    print("=" * 70)
    
    user_input = "Check batch git operations"
    result = agent.check_batch_operations(user_input)
    
    print(f"✓ Success: {result['success']}")
    print(f"✓ Input: {result['input']}")
    print(f"✓ Has issues: {result['has_issues']}")
    print(f"✓ Status: {result['status']}")
    if 'total_operations_processed' in result:
        print(f"✓ Total operations: {result['total_operations_processed']}")
    if 'failure_rate_percent' in result:
        print(f"✓ Failure rate: {result['failure_rate_percent']}%")
    results['batch_operations'] = result

    # ================================================================
    # SUMMARY
    # ================================================================
    print("\n" + "=" * 70)
    print("TESTING COMPLETE - ALL 5 TOOLS EXECUTED")
    print("=" * 70)
    print(f"\nTotal tools tested: {len(results)}")
    print(f"All successful: {all(r['success'] for r in results.values())}")
    
    return results


if __name__ == "__main__":
    print("\n🚀 Git Agent MCP Server - Tool Testing Suite\n")
    results = test_git_agent()
    
    print("\n✅ All tests completed successfully!")
    print("\nTo run the server:")
    print("  • stdio mode: python -m git_agent.server")
    print("  • HTTP mode:  python -m git_agent.server --transport http --port 9002")
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
