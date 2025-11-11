#!/usr/bin/env python3
"""
Test script to verify the e-commerce graph database stack.
Runs 4 key tests to ensure everything is working correctly.
"""

import os
import sys
import subprocess
import requests
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "app"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
    "database": os.getenv("POSTGRES_DB", "shop"),
}


def test_1_fastapi_health():
    """
    Test 1: Check FastAPI health endpoint.
    Expected: {"ok": true}
    """
    print("\n" + "=" * 60)
    print("TEST 1: FastAPI Health Check")
    print("=" * 60)
    print(f"GET {API_URL}/health")
    
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        response.raise_for_status()
        data = response.json()
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {data}")
        
        if data.get("ok") is True:
            print("✓ TEST 1 PASSED: FastAPI health check OK")
            return True
        else:
            print(f"✗ TEST 1 FAILED: Expected 'ok': true, got {data}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ TEST 1 FAILED: Could not connect to FastAPI: {e}")
        return False


def test_2_postgres_orders():
    """
    Test 2: Query Postgres orders table.
    Expected: Should return at least 3 orders
    """
    print("\n" + "=" * 60)
    print("TEST 2: Postgres Orders Query")
    print("=" * 60)
    print("SELECT * FROM orders LIMIT 5;")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM orders LIMIT 5;")
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"\nColumns: {columns}")
        print(f"Rows returned: {len(results)}")
        for row in results:
            print(f"  {row}")
        
        cursor.close()
        conn.close()
        
        if len(results) >= 3:
            print(f"\n✓ TEST 2 PASSED: Found {len(results)} orders")
            return True
        else:
            print(f"\n✗ TEST 2 FAILED: Expected at least 3 orders, got {len(results)}")
            return False
            
    except psycopg2.Error as e:
        print(f"✗ TEST 2 FAILED: Postgres connection error: {e}")
        return False


def test_3_postgres_now():
    """
    Test 3: Query Postgres current timestamp.
    Expected: Should return current timestamp
    """
    print("\n" + "=" * 60)
    print("TEST 3: Postgres Current Timestamp")
    print("=" * 60)
    print("SELECT now();")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT now();")
        result = cursor.fetchone()
        
        print(f"\nCurrent timestamp: {result[0]}")
        
        cursor.close()
        conn.close()
        
        if result and result[0]:
            print("✓ TEST 3 PASSED: Postgres timestamp query OK")
            return True
        else:
            print("✗ TEST 3 FAILED: No timestamp returned")
            return False
            
    except psycopg2.Error as e:
        print(f"✗ TEST 3 FAILED: Postgres connection error: {e}")
        return False


def test_4_etl_execution():
    """
    Test 4: Execute ETL script.
    Expected: Should complete with "ETL done." message
    """
    print("\n" + "=" * 60)
    print("TEST 4: ETL Execution")
    print("=" * 60)
    
    # Determine ETL script path
    etl_path = Path(__file__).parent / "etl.py"
    
    if not etl_path.exists():
        print(f"✗ TEST 4 FAILED: ETL script not found at {etl_path}")
        return False
    
    print(f"Executing: python {etl_path}")
    
    try:
        # Run ETL script
        result = subprocess.run(
            [sys.executable, str(etl_path)],
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout
        )
        
        output = result.stdout + result.stderr
        print("\nETL Output:")
        print("-" * 60)
        print(output)
        print("-" * 60)
        
        if result.returncode == 0 and "ETL done." in output:
            print("\n✓ TEST 4 PASSED: ETL completed successfully")
            return True
        else:
            print(f"\n✗ TEST 4 FAILED: ETL exit code {result.returncode}")
            if "ETL done." not in output:
                print("  Missing 'ETL done.' message in output")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ TEST 4 FAILED: ETL execution timed out")
        return False
    except Exception as e:
        print(f"✗ TEST 4 FAILED: Error executing ETL: {e}")
        return False


def run_all_tests():
    """
    Run all 4 tests and return summary.
    """
    print("\n" + "=" * 60)
    print("E-COMMERCE GRAPH DATABASE - TEST SUITE")
    print("=" * 60)
    
    results = []
    
    # Run all tests
    results.append(("FastAPI Health Check", test_1_fastapi_health()))
    results.append(("Postgres Orders Query", test_2_postgres_orders()))
    results.append(("Postgres Timestamp Query", test_3_postgres_now()))
    results.append(("ETL Execution", test_4_etl_execution()))
    
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
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)

