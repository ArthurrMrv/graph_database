#!/usr/bin/env python3
"""
Project-level test script for Docker initialization and verification.
Runs Docker commands to set up and test the e-commerce graph database stack.
"""

import subprocess
import sys
import time
from pathlib import Path


def run_command(cmd, description, check_output=True, timeout=60):
    """
    Run a shell command and return the result.
    
    Args:
        cmd: Command to run (list of strings)
        description: Description of what the command does
        check_output: Whether to capture output
        timeout: Command timeout in seconds
    
    Returns:
        Tuple of (success: bool, output: str)
    """
    print(f"\n{'=' * 60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('=' * 60)
    
    try:
        if check_output:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False
            )
            output = result.stdout + result.stderr
            print(output)
            return result.returncode == 0, output
        else:
            result = subprocess.run(cmd, timeout=timeout, check=False)
            return result.returncode == 0, ""
    except subprocess.TimeoutExpired:
        print(f"✗ Command timed out after {timeout} seconds")
        return False, ""
    except Exception as e:
        print(f"✗ Error running command: {e}")
        return False, ""


def test_1_start_stack():
    """Test 1: Start Docker Compose stack."""
    print("\n" + "=" * 60)
    print("TEST 1: Starting Docker Compose Stack")
    print("=" * 60)
    
    # Check if docker-compose.yml exists
    compose_file = Path("docker-compose.yml")
    if not compose_file.exists():
        print("✗ docker-compose.yml not found")
        return False
    
    # Start the stack
    success, output = run_command(
        ["docker", "compose", "up", "-d"],
        "Starting Docker Compose services",
        check_output=True,
        timeout=120
    )
    
    if success:
        print("\n✓ TEST 1 PASSED: Docker stack started")
        print("Waiting for services to be ready...")
        time.sleep(10)  # Give services time to initialize
        return True
    else:
        print("\n✗ TEST 1 FAILED: Could not start Docker stack")
        return False


def test_2_check_containers():
    """Test 2: Check container status."""
    print("\n" + "=" * 60)
    print("TEST 2: Checking Container Status")
    print("=" * 60)
    
    success, output = run_command(
        ["docker", "compose", "ps"],
        "Checking container status",
        check_output=True
    )
    
    if success:
        # Check if expected containers are running
        expected_containers = ["postgres", "neo4j", "app"]
        all_running = all(container in output for container in expected_containers)
        
        if all_running:
            print("\n✓ TEST 2 PASSED: All containers are running")
            return True
        else:
            print("\n✗ TEST 2 FAILED: Not all expected containers are running")
            print(f"Expected: {expected_containers}")
            return False
    else:
        print("\n✗ TEST 2 FAILED: Could not check container status")
        return False


def test_3_check_postgres_schema():
    """Test 3: Check Postgres schema exists."""
    print("\n" + "=" * 60)
    print("TEST 3: Checking Postgres Schema")
    print("=" * 60)
    
    success, output = run_command(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", "app", "-d", "shop", "-c", "\\dt"
        ],
        "Listing Postgres tables",
        check_output=True
    )
    
    if success:
        # Check for expected tables
        expected_tables = [
            "categories", "customers", "events",
            "order_items", "orders", "products"
        ]
        all_tables_present = all(table in output for table in expected_tables)
        
        if all_tables_present:
            print("\n✓ TEST 3 PASSED: All expected tables exist")
            return True
        else:
            print("\n✗ TEST 3 FAILED: Not all expected tables found")
            print(f"Expected: {expected_tables}")
            return False
    else:
        print("\n✗ TEST 3 FAILED: Could not query Postgres schema")
        return False


def test_4_validate_row_counts():
    """Test 4: Validate Postgres row counts."""
    print("\n" + "=" * 60)
    print("TEST 4: Validating Postgres Row Counts")
    print("=" * 60)
    
    queries = [
        ("customers", "SELECT count(*) FROM customers;"),
        ("orders", "SELECT count(*) FROM orders;"),
        ("events", "SELECT count(*) FROM events;"),
    ]
    
    all_passed = True
    for table_name, query in queries:
        success, output = run_command(
            [
                "docker", "compose", "exec", "-T", "postgres",
                "psql", "-U", "app", "-d", "shop", "-c", query
            ],
            f"Counting rows in {table_name}",
            check_output=True
        )
        
        if not success or "count" not in output.lower():
            print(f"✗ Failed to get count for {table_name}")
            all_passed = False
        else:
            print(f"✓ {table_name} count retrieved")
    
    if all_passed:
        print("\n✓ TEST 4 PASSED: All row counts validated")
        return True
    else:
        print("\n✗ TEST 4 FAILED: Some row count queries failed")
        return False


def test_5_run_etl():
    """Test 5: Run ETL process."""
    print("\n" + "=" * 60)
    print("TEST 5: Running ETL Process")
    print("=" * 60)
    
    success, output = run_command(
        [
            "docker", "compose", "exec", "-T", "app",
            "python", "etl.py"
        ],
        "Running ETL from Postgres to Neo4j",
        check_output=True,
        timeout=180
    )
    
    if success and "ETL done." in output:
        print("\n✓ TEST 5 PASSED: ETL completed successfully")
        return True
    else:
        print("\n✗ TEST 5 FAILED: ETL did not complete successfully")
        if "ETL done." not in output:
            print("  Missing 'ETL done.' message")
        return False


def test_6_check_logs():
    """Test 6: Check service logs for errors."""
    print("\n" + "=" * 60)
    print("TEST 6: Checking Service Logs")
    print("=" * 60)
    
    services = ["postgres", "neo4j", "app"]
    all_healthy = True
    
    for service in services:
        success, output = run_command(
            ["docker", "compose", "logs", "--tail", "20", service],
            f"Checking {service} logs",
            check_output=True
        )
        
        if success:
            # Check for common error indicators
            error_keywords = ["error", "fatal", "failed", "exception"]
            has_errors = any(
                keyword in output.lower() 
                for keyword in error_keywords
            )
            
            if has_errors:
                print(f"⚠ Warning: {service} logs contain potential errors")
            else:
                print(f"✓ {service} logs look healthy")
        else:
            print(f"✗ Could not retrieve {service} logs")
            all_healthy = False
    
    if all_healthy:
        print("\n✓ TEST 6 PASSED: Service logs checked")
        return True
    else:
        print("\n✗ TEST 6 FAILED: Some service logs could not be retrieved")
        return False


def run_all_tests():
    """Run all Docker initialization tests."""
    print("\n" + "=" * 60)
    print("DOCKER INITIALIZATION TEST SUITE")
    print("=" * 60)
    print("\nThis script will:")
    print("  1. Start the Docker Compose stack")
    print("  2. Check container status")
    print("  3. Verify Postgres schema")
    print("  4. Validate row counts")
    print("  5. Run ETL process")
    print("  6. Check service logs")
    print("\n" + "=" * 60)
    
    results = []
    
    # Run all tests
    results.append(("Start Docker Stack", test_1_start_stack()))
    results.append(("Check Containers", test_2_check_containers()))
    results.append(("Check Postgres Schema", test_3_check_postgres_schema()))
    results.append(("Validate Row Counts", test_4_validate_row_counts()))
    results.append(("Run ETL", test_5_run_etl()))
    results.append(("Check Service Logs", test_6_check_logs()))
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{status}: {test_name}")
    
    print("\n" + "-" * 60)
    print(f"Total: {passed}/{total} tests passed")
    print("=" * 60)
    
    if passed == total:
        print("\n🎉 All tests passed! Stack is ready to use.")
        print("\nNext steps:")
        print("  - Access Neo4j Browser: http://localhost:7474")
        print("  - Access FastAPI: http://localhost:8000")
        print("  - View API docs: http://localhost:8000/docs")
    else:
        print("\n⚠ Some tests failed. Check the output above for details.")
        print("\nTroubleshooting:")
        print("  - Check logs: docker compose logs -f")
        print("  - Check status: docker compose ps")
        print("  - Restart stack: docker compose restart")
    
    return passed == total


if __name__ == "__main__":
    try:
        success = run_all_tests()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest suite interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

